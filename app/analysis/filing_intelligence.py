"""Plain-English SEC filing interpretation for watchlist users."""

from __future__ import annotations

from app.models.sec_filing import FilingInsight, FilingRecord


def filing_to_insight(record: FilingRecord) -> FilingInsight:
    """Convert a normalized filing into cautious user-facing language."""
    form = record.form_type.upper()
    if form == "4":
        return _form4_insight(record)
    if form.startswith("SC 13D"):
        return FilingInsight(
            record.ticker,
            "Confirmed major-holder filing",
            "A major investor reported ownership above 5%.",
            "Schedule 13D filings can matter because they may include plans to influence the company.",
            "Do not assume the investor just bought the full stake; read the source for exact purpose.",
            "SEC Schedule 13D",
            record.source_url,
            "Medium",
            "confirmed_filing",
            record,
        )
    if form.startswith("SC 13G"):
        return FilingInsight(
            record.ticker,
            "Confirmed passive or qualifying holder filing",
            "A major holder reported ownership above 5% under Schedule 13G.",
            "This can confirm material ownership, but it is generally not an activist-intent signal.",
            "Do not infer control intent from a 13G.",
            "SEC Schedule 13G",
            record.source_url,
            "Medium",
            "confirmed_filing",
            record,
        )
    if form.startswith("13F"):
        return FilingInsight(
            record.ticker,
            "Delayed ownership update",
            f"{record.reporting_owner or 'A tracked manager'} reported a {record.transaction_type or 'position change'} during the prior quarter.",
            "It can show ownership-trend evidence from a known manager.",
            "13F filings are delayed and do not confirm current holdings or trades today.",
            "SEC Form 13F",
            record.source_url,
            "Low",
            "delayed_ownership",
            record,
        )
    return FilingInsight(record.ticker, "SEC filing detected", "A filing was detected but not classified.", "It may be relevant after source review.", "The parser could not produce a high-confidence interpretation.", "SEC filing", record.source_url, "Low", "unknown", record)


def _form4_insight(record: FilingRecord) -> FilingInsight:
    tx = record.transaction_type or "Unknown transaction"
    owner = record.reporting_owner or "An insider"
    value = f" of approximately ${record.transaction_value:,.0f}" if record.transaction_value else ""
    if tx == "Open-market purchase":
        return FilingInsight(record.ticker, "Confirmed insider filing", f"{owner} reported an open-market purchase{value}.", "Executives committing personal capital can be meaningful supporting evidence.", "One purchase does not guarantee future gains and may be small relative to compensation or holdings.", "SEC Form 4", record.source_url, "Medium", "confirmed_filing", record)
    if tx == "Open-market sale":
        return FilingInsight(record.ticker, "Confirmed insider sale filing", f"{owner} reported an open-market sale{value}.", "Insider selling can be worth monitoring, especially if repeated or unusually large.", "Sales can be planned, tax-related, or diversification-driven.", "SEC Form 4", record.source_url, "Medium", "confirmed_filing", record)
    if tx in {"Option exercise", "Grant or award", "Gift", "Tax withholding"}:
        return FilingInsight(record.ticker, "Insider filing detected", f"{owner} reported a {tx.lower()}.", "The filing updates insider ownership records.", "This should not be described as insider buying unless transaction data supports that conclusion.", "SEC Form 4", record.source_url, "Low", "confirmed_filing", record)
    return FilingInsight(record.ticker, "Insider filing detected", "A Form 4 was detected, but the transaction type could not be classified reliably.", "It may still be relevant after source review.", "Unsupported transaction codes should not be treated as bullish or bearish evidence.", "SEC Form 4", record.source_url, "Low", "unknown", record)


def material_filing_insights(records: list[FilingRecord], limit: int = 5) -> list[FilingInsight]:
    """Return the most useful filing insights for UI and Telegram."""
    priority = {"SC 13D": 0, "SC 13G": 1, "4": 2, "13F-HR": 3}
    sorted_records = sorted(records, key=lambda r: (priority.get(r.form_type.upper().split("/")[0], 9), r.filing_date), reverse=False)
    return [filing_to_insight(record) for record in sorted_records[:limit]]
