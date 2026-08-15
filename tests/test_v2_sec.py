from datetime import date

import pytest

from app.analysis.filing_intelligence import filing_to_insight
from app.services.sec_edgar import (
    SecClientError,
    classify_form4_code,
    compare_13f_holdings,
    dedupe_filings,
    parse_form4_xml,
    parse_schedule_13,
    validate_sec_url,
)
from app.services.sec_identity import is_valid_user_agent


FORM4 = """
<ownershipDocument>
  <reportingOwner><reportingOwnerId><rptOwnerName>Jane CFO</rptOwnerName></reportingOwnerId>
  <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CFO</officerTitle></reportingOwnerRelationship></reportingOwner>
  <nonDerivativeTable><nonDerivativeTransaction>
    <transactionDate><value>2026-08-10</value></transactionDate>
    <transactionCoding><transactionCode>{code}</transactionCode></transactionCoding>
    <transactionAmounts><transactionShares><value>1000</value></transactionShares><transactionPricePerShare><value>{price}</value></transactionPricePerShare></transactionAmounts>
    <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
  </nonDerivativeTransaction></nonDerivativeTable>
</ownershipDocument>
"""


def test_sec_user_agent_validation():
    assert is_valid_user_agent("FO Stock Analytics test@example.com")
    assert not is_valid_user_agent("anonymous bot")


def test_validate_sec_url_requires_https_and_sec_domain():
    validate_sec_url("https://www.sec.gov/Archives/test.xml")
    with pytest.raises(SecClientError):
        validate_sec_url("http://www.sec.gov/Archives/test.xml")
    with pytest.raises(SecClientError):
        validate_sec_url("https://example.com/test.xml")


@pytest.mark.parametrize(
    "code,expected",
    [
        ("P", "Open-market purchase"),
        ("S", "Open-market sale"),
        ("M", "Option exercise"),
        ("A", "Grant or award"),
        ("G", "Gift"),
        ("F", "Tax withholding"),
        ("Z", "Unknown or unsupported transaction code"),
    ],
)
def test_form4_code_classification(code, expected):
    assert classify_form4_code(code)[0] == expected


def test_parse_form4_open_market_purchase_value_and_source():
    records = parse_form4_xml(FORM4.format(code="P", price="420"), "AAPL", "0000320193", "acc1", date(2026, 8, 11), "https://www.sec.gov/Archives/test.xml")
    assert records[0].transaction_type == "Open-market purchase"
    assert records[0].transaction_value == 420000
    insight = filing_to_insight(records[0])
    assert "open-market purchase" in insight.what_changed
    assert "guarantee" in insight.caution


def test_parse_form4_missing_price_does_not_fail():
    records = parse_form4_xml(FORM4.format(code="S", price=""), "MSFT", "0001", "acc2", date(2026, 8, 11), "https://www.sec.gov/Archives/test.xml")
    assert records[0].price is None
    assert records[0].transaction_type == "Open-market sale"


def test_schedule_13g_has_no_false_activist_claim():
    record = parse_schedule_13("NAME OF REPORTING PERSON\nLong Fund\n7.2%", "ABC", "0002", "SC 13G", "acc3", date(2026, 8, 1), "https://www.sec.gov/Archives/13g.txt")
    insight = filing_to_insight(record)
    assert "not an activist-intent signal" in insight.why_it_matters
    assert "Do not infer control intent" in insight.caution


def test_schedule_13d_is_cautious():
    record = parse_schedule_13("NAME OF REPORTING PERSON\nLong Fund\n8.1%", "ABC", "0002", "SC 13D/A", "acc4", date(2026, 8, 1), "https://www.sec.gov/Archives/13d.txt")
    insight = filing_to_insight(record)
    assert "may include plans" in insight.why_it_matters
    assert "Do not assume" in insight.caution


def test_13f_comparison_delayed_language_and_context():
    record = compare_13f_holdings({"AAPL": 100}, {"AAPL": 150}, "AAPL", "Passive Manager", "0003", "acc5", date(2026, 8, 14), date(2026, 6, 30), "https://www.sec.gov/Archives/13f.txt", "Passive index manager")
    assert record is not None
    assert record.transaction_type == "Delayed 13F increase"
    assert any("do not confirm" in warning for warning in record.warnings)
    assert any("Passive" in warning for warning in record.warnings)


def test_filing_deduplication():
    records = parse_form4_xml(FORM4.format(code="P", price="10"), "AAPL", "0001", "acc6", date(2026, 8, 1), "https://www.sec.gov/Archives/test.xml")
    assert len(dedupe_filings(records + records)) == 1
