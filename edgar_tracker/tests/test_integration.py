"""Integration tests for the EDGAR tracker pipeline.

These tests require network access and hit the real SEC EDGAR API.
They are skipped by default. To run them, set the environment variable:

    EDGAR_INTEGRATION_TEST=1

Usage:
    EDGAR_INTEGRATION_TEST=1 pytest edgar_tracker/tests/test_integration.py -v
"""

from __future__ import annotations

import os
import re

import pytest

from edgar_tracker.models import (
    FilingMeta,
    FilingRaw,
    ParsedFiling,
    ScoreResult,
    SECTION_KEYS,
)
from edgar_tracker.parse import parse_filing
from edgar_tracker.score import score, load_taxonomy

pytestmark = pytest.mark.skipif(
    not os.environ.get("EDGAR_INTEGRATION_TEST"),
    reason="Set EDGAR_INTEGRATION_TEST=1 to run",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# SEC requires a descriptive User-Agent with contact info.
_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "EdgarTrackerIntegrationTest test@example.com",
)

_TAXONOMY_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "config", "keywords.yaml"
)

# JPMorgan Chase CIK (zero-padded to 10 digits)
_JPM_CIK = "0000019617"


def _fetch_one_filing_html(cik: str, max_filings: int = 1) -> tuple[FilingMeta, str]:
    """Fetch the most recent 10-K filing HTML for *cik* from SEC EDGAR.

    This function hits the real SEC EDGAR JSON API and downloads the
    primary document HTML.

    Parameters
    ----------
    cik:
        The zero-padded CIK (10 digits).
    max_filings:
        How many recent 10-K filings to search through.

    Returns
    -------
    tuple[FilingMeta, str]
        The filing metadata and the raw HTML content.

    Raises
    ------
    RuntimeError
        If no suitable 10-K filing is found or the download fails.
    """
    import requests
    import time

    session = requests.Session()
    session.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept": "*/*",
    })

    # Fetch submissions JSON
    subs_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    time.sleep(0.2)  # Respect rate limits
    resp = session.get(subs_url, timeout=30)
    resp.raise_for_status()
    subs = resp.json()

    company_name = subs.get("name", "Unknown")
    tickers = subs.get("tickers", [])
    ticker = tickers[0] if tickers else "UNKNOWN"

    recent = subs.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    # Find the most recent 10-K
    found_meta: FilingMeta | None = None
    found_idx: int = -1
    count = 0
    for i in range(len(accessions)):
        if i >= len(forms):
            break
        form = (forms[i] or "").strip().upper()
        if form == "10-K":
            found_meta = FilingMeta(
                cik=cik,
                ticker=ticker,
                company_name=company_name,
                form="10-K",
                filing_date=filing_dates[i] if i < len(filing_dates) else "",
                report_date=report_dates[i] if i < len(report_dates) else "",
                accession_number=accessions[i],
                primary_document=primary_docs[i] if i < len(primary_docs) else "",
            )
            found_idx = i
            count += 1
            if count >= max_filings:
                break

    if found_meta is None:
        raise RuntimeError(f"No 10-K filing found for CIK {cik}")

    # Download the primary document
    cik_no_zeros = str(int(cik))
    acc_no_dashes = found_meta.accession_number.replace("-", "")
    doc_filename = os.path.basename(found_meta.primary_document)
    doc_url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_no_zeros}/{acc_no_dashes}/{doc_filename}"
    )

    time.sleep(0.2)  # Respect rate limits
    resp = session.get(doc_url, timeout=60)
    resp.raise_for_status()
    html = resp.text

    return found_meta, html


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestPipelineSingleCompany:
    """test_pipeline_single_company: Run pipeline for JPM (CIK 0000019617)
    with 1 filing, verify output structure."""

    def test_fetch_parse_and_score_jpm(self) -> None:
        """End-to-end: fetch JPM 10-K, parse it, score it, verify structure."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)

        # Verify metadata
        assert meta.cik == _JPM_CIK
        assert meta.form == "10-K"
        assert meta.accession_number != ""
        assert meta.filing_date != ""

        # Parse the filing
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        # Verify parsing
        assert isinstance(parsed, ParsedFiling)
        assert parsed.full_text != ""
        assert parsed.token_count > 0
        assert parsed.meta.cik == _JPM_CIK

        # Verify sections dict has expected keys
        for key in SECTION_KEYS:
            assert key in parsed.sections

        # Score the filing
        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        # Verify score result structure
        assert isinstance(result, ScoreResult)
        assert result.meta.cik == _JPM_CIK
        assert result.full_text_scores.section_name == "full_text"
        assert len(result.full_text_scores.group_scores) > 0

        # Verify wide dict export
        wide = result.to_wide_dict()
        assert isinstance(wide, dict)
        assert "contains_ai" in wide
        assert "count_ai" in wide

    def test_parsed_filing_has_text(self) -> None:
        """The parsed filing should contain substantial text."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        # A real 10-K should have many thousands of tokens.
        assert parsed.token_count > 1000, (
            f"Expected >1000 tokens, got {parsed.token_count}"
        )

        # The full text should be non-trivial.
        assert len(parsed.full_text) > 5000


class TestScoreResultShape:
    """test_score_result_shape: verify ScoreResult has correct groups
    and sections."""

    def test_groups_match_taxonomy(self) -> None:
        """Every group in the taxonomy should appear in full_text_scores."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        # Full text scores should have one GroupScore per taxonomy group.
        score_group_names = {
            gs.group_name for gs in result.full_text_scores.group_scores
        }
        taxonomy_group_names = set(taxonomy.group_names())
        assert score_group_names == taxonomy_group_names

    def test_section_scores_are_subset_of_section_keys(self) -> None:
        """Section scores should only contain keys from SECTION_KEYS."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        for sec_key in result.section_scores:
            assert sec_key in SECTION_KEYS, (
                f"Unexpected section key '{sec_key}' not in SECTION_KEYS"
            )

    def test_section_scores_have_all_groups(self) -> None:
        """Each section's scores should include all taxonomy groups."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        taxonomy_group_names = set(taxonomy.group_names())

        for sec_key, sec_scores in result.section_scores.items():
            sec_group_names = {gs.group_name for gs in sec_scores.group_scores}
            assert sec_group_names == taxonomy_group_names, (
                f"Section '{sec_key}' missing groups: "
                f"{taxonomy_group_names - sec_group_names}"
            )

    def test_keyword_scores_have_correct_labels(self) -> None:
        """Each KeywordScore label should match a keyword in the taxonomy."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        # Build a mapping of group_name -> set of keyword labels
        expected_labels: dict[str, set[str]] = {}
        for group in taxonomy.groups:
            expected_labels[group.name] = {kw.label for kw in group.keywords}

        for gs in result.full_text_scores.group_scores:
            actual_labels = {ks.label for ks in gs.keyword_scores}
            assert actual_labels == expected_labels[gs.group_name], (
                f"Group '{gs.group_name}' has unexpected labels: "
                f"expected {expected_labels[gs.group_name]}, got {actual_labels}"
            )

    def test_jpm_likely_has_ai_mentions(self) -> None:
        """JPMorgan's 10-K is likely to mention AI-related terms."""
        meta, html = _fetch_one_filing_html(_JPM_CIK, max_filings=1)
        filing_raw = FilingRaw(meta=meta, html=html)
        parsed = parse_filing(filing_raw)

        taxonomy = load_taxonomy(_TAXONOMY_PATH)
        result = score(parsed, taxonomy)

        ai_gs = next(
            gs for gs in result.full_text_scores.group_scores
            if gs.group_name == "ai"
        )
        # JPM filings generally mention AI/ML/automation in recent years.
        # This is a soft assertion -- skip if it somehow doesn't match.
        if ai_gs.total_count == 0:
            pytest.skip(
                "JPM 10-K did not contain AI keywords -- "
                "this may happen with very old filings"
            )
        assert ai_gs.contains is True
        assert ai_gs.total_count > 0
