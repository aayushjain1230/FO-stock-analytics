"""Respectful SEC EDGAR client and filing parsers."""

from __future__ import annotations

import json
import random
import re
import time
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

from app.models.sec_filing import FilingRecord
from app.services.sec_cache import SecCache
from app.services.sec_identity import get_sec_user_agent
from app.services.watchlist import normalize_ticker

SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"
MAX_RESPONSE_BYTES = 5_000_000


class SecClientError(RuntimeError):
    """Raised for SEC client failures that should be isolated per ticker."""


class SecEdgarClient:
    """Thin SEC client with identity, throttling, retries, and local caching."""

    def __init__(
        self,
        user_agent: str | None = None,
        cache: SecCache | None = None,
        min_interval_seconds: float = 0.35,
        timeout_seconds: int = 20,
        max_retries: int = 2,
    ) -> None:
        self.user_agent = user_agent if user_agent is not None else get_sec_user_agent()
        self.cache = cache or SecCache()
        self.min_interval_seconds = min_interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._last_request_at = 0.0

    @property
    def enabled(self) -> bool:
        """Return whether SEC calls are allowed."""
        return bool(self.user_agent and requests is not None)

    def get_ticker_cik_map(self, ttl_seconds: int = 7 * 86400) -> dict[str, str]:
        """Return ticker-to-CIK mapping from SEC company tickers JSON."""
        cached = self.cache.get("ticker_cik", "company_tickers", ttl_seconds)
        if cached:
            return dict(cached.value)
        data = self._get_json("https://www.sec.gov/files/company_tickers.json", cache_namespace="ticker_cik", cache_key="company_tickers", ttl_seconds=ttl_seconds)
        mapping = {
            normalize_ticker(row.get("ticker", "")): str(row.get("cik_str", "")).zfill(10)
            for row in data.values()
            if isinstance(row, dict) and row.get("ticker") and row.get("cik_str")
        }
        self.cache.set("ticker_cik", "company_tickers", mapping)
        return mapping

    def ticker_to_cik(self, ticker: str) -> str | None:
        """Resolve a ticker to zero-padded CIK."""
        return self.get_ticker_cik_map().get(normalize_ticker(ticker))

    def get_company_submissions(self, cik: str, ttl_seconds: int = 6 * 3600) -> dict[str, Any]:
        """Fetch normalized company submissions JSON."""
        cik = str(cik).lstrip("0").zfill(10)
        url = f"{SEC_DATA_BASE}/submissions/CIK{cik}.json"
        return self._get_json(url, cache_namespace="submissions", cache_key=cik, ttl_seconds=ttl_seconds)

    def get_filing_document(self, source_url: str, ttl_seconds: int = 30 * 86400) -> str:
        """Fetch a source filing document with size and URL validation."""
        validate_sec_url(source_url)
        cached = self.cache.get("filing_doc", source_url, ttl_seconds)
        if cached:
            return str(cached.value)
        text = self._get_text(source_url, cache_namespace="filing_doc", cache_key=source_url, ttl_seconds=ttl_seconds)
        self.cache.set("filing_doc", source_url, text)
        return text

    def recent_filing_metadata(self, ticker: str, forms: set[str], limit: int = 20) -> list[dict[str, Any]]:
        """Return recent metadata rows for requested forms."""
        cik = self.ticker_to_cik(ticker)
        if not cik:
            return []
        submissions = self.get_company_submissions(cik)
        recent = submissions.get("filings", {}).get("recent", {})
        output: list[dict[str, Any]] = []
        for idx, form in enumerate(recent.get("form", [])):
            if form not in forms:
                continue
            accession = recent.get("accessionNumber", [None])[idx]
            primary_doc = recent.get("primaryDocument", [None])[idx]
            if not accession or not primary_doc:
                continue
            source_url = filing_source_url(cik, accession, primary_doc)
            output.append(
                {
                    "ticker": normalize_ticker(ticker),
                    "cik": cik,
                    "form": form,
                    "accession_number": accession,
                    "filing_date": recent.get("filingDate", [None])[idx],
                    "report_date": recent.get("reportDate", [None])[idx],
                    "source_url": source_url,
                }
            )
            if len(output) >= limit:
                break
        return output

    def _get_json(self, url: str, cache_namespace: str, cache_key: str, ttl_seconds: int) -> dict[str, Any]:
        cached = self.cache.get(cache_namespace, cache_key, ttl_seconds)
        if cached:
            return dict(cached.value)
        text = self._request(url)
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise SecClientError("SEC returned invalid JSON.") from exc
        self.cache.set(cache_namespace, cache_key, data)
        return data

    def _get_text(self, url: str, cache_namespace: str, cache_key: str, ttl_seconds: int) -> str:
        cached = self.cache.get(cache_namespace, cache_key, ttl_seconds)
        if cached:
            return str(cached.value)
        text = self._request(url)
        self.cache.set(cache_namespace, cache_key, text)
        return text

    def _request(self, url: str) -> str:
        validate_sec_url(url)
        if not self.enabled:
            raise SecClientError("SEC synchronization is disabled because SEC_USER_AGENT is not configured.")
        assert requests is not None
        headers = {"User-Agent": self.user_agent or "", "Accept-Encoding": "gzip, deflate", "Host": urlparse(url).netloc}
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._throttle()
            try:
                response = requests.get(url, headers=headers, timeout=self.timeout_seconds)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise SecClientError(f"SEC temporary response: {response.status_code}")
                if response.status_code != 200:
                    raise SecClientError(f"SEC response status: {response.status_code}")
                if len(response.content) > MAX_RESPONSE_BYTES:
                    raise SecClientError("SEC response exceeded safe size limit.")
                return response.text
            except Exception as exc:  # isolate and retry capped transient failures
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep((2**attempt) * 0.5 + random.uniform(0, 0.25))
        raise SecClientError(str(last_error or "SEC request failed."))

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.min_interval_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()


def validate_sec_url(url: str) -> None:
    """Reject non-SEC or non-HTTPS URLs."""
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise SecClientError("SEC URLs must use HTTPS.")
    if parsed.netloc not in {"www.sec.gov", "data.sec.gov"}:
        raise SecClientError("Only SEC domains are allowed.")


def filing_source_url(cik: str, accession: str, primary_document: str) -> str:
    """Build a direct SEC Archives URL for a filing document."""
    clean_cik = str(cik).lstrip("0")
    clean_accession = accession.replace("-", "")
    return f"{SEC_ARCHIVES_BASE}/edgar/data/{clean_cik}/{clean_accession}/{primary_document}"


def dedupe_filings(records: list[FilingRecord]) -> list[FilingRecord]:
    """Deduplicate filing records by accession and transaction identity."""
    seen: set[str] = set()
    output: list[FilingRecord] = []
    for record in records:
        key = record.identity_key()
        if key in seen:
            continue
        seen.add(key)
        output.append(record)
    return output


def parse_form4_xml(xml_text: str, ticker: str, cik: str, accession_number: str, filing_date: date, source_url: str) -> list[FilingRecord]:
    """Parse Form 4 XML and classify insider transaction codes cautiously."""
    try:
        root = ET.fromstring(_strip_doctype(xml_text[:MAX_RESPONSE_BYTES]))
    except Exception:
        return [
            FilingRecord(accession_number, normalize_ticker(ticker), cik, "4", filing_date, None, None, None, None, None, None, None, None, None, None, source_url, parser_status="failed", warnings=["Form 4 parser could not read XML."])
        ]
    owner = _text(root, ".//reportingOwner/reportingOwnerId/rptOwnerName")
    relationship = _owner_relationship(root)
    records: list[FilingRecord] = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = _text(txn, ".//transactionCoding/transactionCode")
        shares = _float(_text(txn, ".//transactionAmounts/transactionShares/value"))
        price = _float(_text(txn, ".//transactionAmounts/transactionPricePerShare/value"))
        event_date = _date(_text(txn, ".//transactionDate/value"))
        direct = _text(txn, ".//ownershipNature/directOrIndirectOwnership/value")
        transaction_type, warnings = classify_form4_code(code, direct)
        value = shares * price if shares is not None and price is not None else None
        records.append(
            FilingRecord(
                accession_number=accession_number,
                ticker=normalize_ticker(ticker),
                cik=cik,
                form_type="4",
                filing_date=filing_date,
                event_date=event_date,
                reporting_owner=owner,
                reporting_owner_type=relationship,
                transaction_type=transaction_type,
                shares=shares,
                price=price,
                transaction_value=value,
                ownership_percent=None,
                position_change=None,
                purpose="Automatic trading plan may apply if disclosed in the source filing." if _contains_10b5(root) else None,
                source_url=source_url,
                warnings=warnings,
            )
        )
    return dedupe_filings(records)


def classify_form4_code(code: str | None, ownership: str | None = None) -> tuple[str, list[str]]:
    """Map Form 4 transaction codes to cautious categories."""
    code = (code or "").upper()
    warnings: list[str] = []
    mapping = {
        "P": "Open-market purchase",
        "S": "Open-market sale",
        "M": "Option exercise",
        "A": "Grant or award",
        "G": "Gift",
        "F": "Tax withholding",
        "D": "Sale or transfer back to issuer",
    }
    result = mapping.get(code, "Unknown or unsupported transaction code")
    if result.startswith("Unknown"):
        warnings.append(f"Unsupported Form 4 transaction code: {code or 'missing'}.")
    if ownership and ownership.upper() == "I":
        warnings.append("Transaction was reported as indirect ownership.")
    return result, warnings


def parse_schedule_13(text: str, ticker: str, cik: str, form_type: str, accession_number: str, filing_date: date, source_url: str) -> FilingRecord:
    """Parse 13D/13G text for ownership context without overclaiming intent."""
    safe = text[:MAX_RESPONSE_BYTES]
    owner = _regex(safe, r"(?:NAME OF REPORTING PERSON|Reporting Person)\s*[:\n]\s*([^\n<]+)")
    ownership = _float(_regex(safe, r"(\d+(?:\.\d+)?)\s*%"))
    shares = _float((_regex(safe, r"([\d,]+)\s+shares") or "").replace(",", ""))
    purpose = None
    upper = safe.upper()
    if form_type.upper().startswith("SC 13D"):
        purpose = "Potential control-related filing; read the filing for exact purpose."
    elif form_type.upper().startswith("SC 13G"):
        purpose = "Generally passive or qualifying ownership context; not an activist claim."
    amendment = "Amendment" if "/A" in form_type.upper() or "AMENDMENT" in upper else "Initial filing"
    return FilingRecord(accession_number, normalize_ticker(ticker), cik, form_type, filing_date, None, owner, "Major holder", amendment, shares, None, None, ownership, None, purpose, source_url)


def compare_13f_holdings(previous: dict[str, float], current: dict[str, float], ticker: str, manager_name: str, manager_cik: str, accession_number: str, filing_date: date, quarter_end: date, source_url: str, manager_type: str = "Unknown") -> FilingRecord | None:
    """Compare delayed 13F holdings for one ticker."""
    ticker = normalize_ticker(ticker)
    prev = float(previous.get(ticker, 0) or 0)
    cur = float(current.get(ticker, 0) or 0)
    if prev == cur:
        return None
    if prev == 0 and cur > 0:
        txn = "Delayed 13F new position"
        change = None
    elif cur == 0 and prev > 0:
        txn = "Delayed 13F exited position"
        change = -1.0
    else:
        change = (cur / prev - 1) if prev else None
        txn = "Delayed 13F increase" if cur > prev else "Delayed 13F decrease"
    warnings = ["13F filings are delayed and do not confirm what the manager owns today."]
    if manager_type.lower().startswith("passive"):
        warnings.append("Passive index-manager changes should not be interpreted like concentrated active-manager purchases.")
    return FilingRecord(accession_number, ticker, manager_cik, "13F-HR", filing_date, quarter_end, manager_name, manager_type, txn, cur, None, None, None, change, "Delayed ownership-trend evidence only.", source_url, warnings=warnings)


def _strip_doctype(text: str) -> str:
    return re.sub(r"<!DOCTYPE[^>]*>", "", text, flags=re.IGNORECASE)


def _text(root: ET.Element, path: str) -> str | None:
    node = root.find(path)
    if node is None or node.text is None:
        return None
    return node.text.strip()


def _float(value: str | None) -> float | None:
    try:
        return float(value) if value not in {None, ""} else None
    except ValueError:
        return None


def _date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(value or "")
    except ValueError:
        return None


def _owner_relationship(root: ET.Element) -> str | None:
    rel = root.find(".//reportingOwner/reportingOwnerRelationship")
    if rel is None:
        return None
    labels = []
    for tag in ["isDirector", "isOfficer", "isTenPercentOwner", "officerTitle"]:
        value = _text(rel, tag)
        if value and value not in {"0", "false", "False"}:
            labels.append("Officer" if tag == "officerTitle" else tag.replace("is", "").replace("Owner", " Owner"))
            if tag == "officerTitle":
                labels[-1] = value
    return ", ".join(labels) if labels else None


def _contains_10b5(root: ET.Element) -> bool:
    text = " ".join(root.itertext()).lower()
    return "10b5-1" in text or "trading plan" in text


def _regex(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None
