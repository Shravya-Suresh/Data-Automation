"""Unit tests for the parse module (edgar_tracker.parse)."""

from __future__ import annotations

import re

import pytest

from edgar_tracker.parse import (
    parse_filing,
    _clean_html,
    _extract_sections,
    _find_section_boundaries,
)
from edgar_tracker.models import FilingRaw, FilingMeta, SECTION_KEYS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_filing_meta(form: str = "10-K") -> FilingMeta:
    """Return a minimal dummy FilingMeta for tests."""
    return FilingMeta(
        cik="0000019617",
        ticker="JPM",
        company_name="JPMorgan Chase & Co.",
        form=form,
        filing_date="2024-02-15",
        report_date="2023-12-31",
        accession_number="0000019617-24-000000",
        primary_document="jpm10k2023.htm",
    )


def _make_filing_raw(html: str, form: str = "10-K") -> FilingRaw:
    """Wrap HTML in a FilingRaw object for testing."""
    return FilingRaw(
        meta=_make_filing_meta(form=form),
        html=html,
    )


# ---------------------------------------------------------------------------
# Synthetic 10-K HTML
# ---------------------------------------------------------------------------

SYNTHETIC_10K_HTML = """\
<html>
<head>
  <title>10-K Filing</title>
  <script>var x = "should be stripped";</script>
  <style>.sec-header { color: red; }</style>
</head>
<body>
  <div>
    <h2>ITEM 1. BUSINESS</h2>
    <p>The company operates in financial services. We provide banking, investment,
    and asset management services to clients worldwide. Our business spans
    consumer banking, commercial banking, and investment banking operations.
    We continue to invest in technology and innovation to serve our customers.</p>

    <h2>ITEM 1A. RISK FACTORS</h2>
    <p>Our business faces various risks including credit risk, market risk,
    and operational risk. Economic downturns could adversely affect our
    financial performance. Regulatory changes may increase compliance costs.
    Cybersecurity threats continue to evolve. Interest rate fluctuations
    could impact our net interest margin significantly.</p>

    <h2>ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</h2>
    <p>Revenue increased 15% year over year. Net income grew to $48.3 billion.
    Our digital transformation initiatives drove efficiency improvements.
    We invested significantly in artificial intelligence capabilities.
    Total assets under management reached $3.4 trillion. Operating expenses
    were managed within our guidance range. Return on equity improved to 17%.</p>

    <h2>ITEM 7A. QUANTITATIVE AND QUALITATIVE</h2>
    <p>We use value-at-risk models to measure market risk. Our VaR methodology
    incorporates historical simulation. Interest rate sensitivity analysis
    shows moderate exposure to rate changes. Foreign exchange risk is hedged
    through derivative instruments strategically.</p>

    <h2>ITEM 8. FINANCIAL STATEMENTS</h2>
    <p>See consolidated financial statements attached.</p>
  </div>
</body>
</html>
"""


PARTIAL_10K_HTML = """\
<html>
<body>
  <h2>ITEM 1. BUSINESS</h2>
  <p>We are a technology company building AI products. Our platform serves
  millions of users globally. We continue to expand our market presence
  through strategic investments in infrastructure and innovation.</p>

  <h2>ITEM 8. FINANCIAL STATEMENTS</h2>
  <p>See consolidated financial statements and supplementary data.</p>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Tests for _clean_html
# ---------------------------------------------------------------------------


class TestCleanHtmlStripsScripts:
    """test_clean_html_strips_scripts: HTML with <script> tags produces
    clean text without any script content."""

    def test_script_content_removed(self) -> None:
        html = '<html><body><script>alert("evil")</script><p>Hello world</p></body></html>'
        result = _clean_html(html)
        assert "alert" not in result
        assert "evil" not in result
        assert "Hello world" in result

    def test_multiple_scripts_removed(self) -> None:
        html = (
            "<html><body>"
            "<script>var a = 1;</script>"
            "<p>Content here</p>"
            "<script>var b = 2;</script>"
            "</body></html>"
        )
        result = _clean_html(html)
        assert "var a" not in result
        assert "var b" not in result
        assert "Content here" in result

    def test_noscript_removed(self) -> None:
        html = (
            "<html><body>"
            "<noscript>JavaScript is required</noscript>"
            "<p>Main text</p>"
            "</body></html>"
        )
        result = _clean_html(html)
        assert "JavaScript is required" not in result
        assert "Main text" in result


class TestCleanHtmlStripsStyles:
    """test_clean_html_strips_styles: HTML with <style> tags produces
    clean text without any style content."""

    def test_style_content_removed(self) -> None:
        html = (
            "<html><head><style>body { color: red; }</style></head>"
            "<body><p>Styled text</p></body></html>"
        )
        result = _clean_html(html)
        assert "color: red" not in result
        assert "body {" not in result
        assert "Styled text" in result

    def test_inline_style_tags_removed(self) -> None:
        html = (
            "<html><body>"
            "<style>.header { font-size: 14px; }</style>"
            "<div>Important content</div>"
            "</body></html>"
        )
        result = _clean_html(html)
        assert "font-size" not in result
        assert "Important content" in result

    def test_combined_script_and_style_removed(self) -> None:
        result = _clean_html(SYNTHETIC_10K_HTML)
        assert "should be stripped" not in result
        assert ".sec-header" not in result
        # But the actual filing text remains.
        assert "financial services" in result


# ---------------------------------------------------------------------------
# Tests for section splitting
# ---------------------------------------------------------------------------


class TestSectionSplitting10K:
    """test_section_splitting_10k: a synthetic 10-K with Item 1, Item 1A,
    Item 7 headings correctly produces sections."""

    def test_all_expected_sections_found(self) -> None:
        cleaned = _clean_html(SYNTHETIC_10K_HTML)
        sections = _extract_sections(cleaned, "10-K")

        # The synthetic doc has Item 1, 1A, 7, 7A.
        assert sections["item1"] != ""
        assert sections["item1a"] != ""
        assert sections["item7"] != ""
        assert sections["item7a"] != ""

    def test_section_content_matches(self) -> None:
        cleaned = _clean_html(SYNTHETIC_10K_HTML)
        sections = _extract_sections(cleaned, "10-K")

        assert "financial services" in sections["item1"]
        assert "credit risk" in sections["item1a"]
        assert "Revenue increased" in sections["item7"]
        assert "value-at-risk" in sections["item7a"]

    def test_section_boundaries_ordered(self) -> None:
        cleaned = _clean_html(SYNTHETIC_10K_HTML)
        boundaries = _find_section_boundaries(cleaned)
        positions = [pos for _key, pos in boundaries]
        assert positions == sorted(positions), "Boundaries should be in document order"


class TestSectionSplittingMissingSection:
    """test_section_splitting_missing_section: only some items present,
    parse_warnings populated."""

    def test_missing_sections_produce_warnings(self) -> None:
        result = parse_filing(_make_filing_raw(PARTIAL_10K_HTML))

        # Only item1 is present; item1a, item7, item7a, item1c are missing.
        assert result.sections["item1"] != ""

        # parse_warnings should mention missing sections.
        warning_text = " ".join(result.parse_warnings)
        assert "item1a" in warning_text or "item7" in warning_text

    def test_empty_sections_are_empty_strings(self) -> None:
        result = parse_filing(_make_filing_raw(PARTIAL_10K_HTML))

        # item7 is not in the partial doc.
        assert result.sections["item7"] == ""


class TestSectionParseOkFlag:
    """test_section_parse_ok_flag: True when >=2 sections found,
    False otherwise."""

    def test_ok_with_multiple_sections(self) -> None:
        result = parse_filing(_make_filing_raw(SYNTHETIC_10K_HTML))
        assert result.section_parse_ok is True

    def test_not_ok_with_single_section(self) -> None:
        result = parse_filing(_make_filing_raw(PARTIAL_10K_HTML))
        # Only item1 is present, so section_parse_ok should be False.
        assert result.section_parse_ok is False

    def test_not_ok_with_no_sections(self) -> None:
        html = "<html><body><p>Just a plain paragraph with no items.</p></body></html>"
        result = parse_filing(_make_filing_raw(html))
        assert result.section_parse_ok is False


# ---------------------------------------------------------------------------
# Tests for token counting
# ---------------------------------------------------------------------------


class TestTokenCounting:
    """test_token_counting: verify token_count matches expected."""

    def test_simple_token_count(self) -> None:
        html = "<html><body><p>one two three four five</p></body></html>"
        result = parse_filing(_make_filing_raw(html))
        # "one two three four five" = 5 alphabetic tokens
        assert result.token_count == 5

    def test_tokens_ignore_numbers(self) -> None:
        html = "<html><body><p>revenue was 100 million in 2024</p></body></html>"
        result = parse_filing(_make_filing_raw(html))
        # "revenue", "was", "million", "in" = 4 alphabetic tokens
        # The token regex r"[A-Za-z]+" only matches pure alphabetic sequences.
        assert result.token_count == 4

    def test_tokens_in_synthetic_doc(self) -> None:
        result = parse_filing(_make_filing_raw(SYNTHETIC_10K_HTML))
        # The document has many words; just verify it is a reasonable positive number.
        assert result.token_count > 50

    def test_empty_html_token_count(self) -> None:
        html = "<html><body></body></html>"
        result = parse_filing(_make_filing_raw(html))
        assert result.token_count == 0


# ---------------------------------------------------------------------------
# Tests for whitespace collapsing
# ---------------------------------------------------------------------------


class TestWhitespaceCollapse:
    """test_whitespace_collapse: multiple spaces/newlines collapsed
    appropriately."""

    def test_multiple_spaces_collapsed(self) -> None:
        html = "<html><body><p>word1     word2      word3</p></body></html>"
        result = _clean_html(html)
        # No runs of multiple spaces should remain.
        assert "  " not in result
        assert "word1" in result
        assert "word2" in result
        assert "word3" in result

    def test_multiple_newlines_collapsed(self) -> None:
        html = (
            "<html><body>"
            "<p>para1</p>"
            "<br/><br/><br/><br/><br/>"
            "<p>para2</p>"
            "</body></html>"
        )
        result = _clean_html(html)
        # Should not have more than 2 consecutive newlines.
        assert "\n\n\n" not in result
        assert "para1" in result
        assert "para2" in result

    def test_tabs_collapsed(self) -> None:
        html = "<html><body><p>word1\t\t\tword2</p></body></html>"
        result = _clean_html(html)
        assert "\t" not in result
        assert "word1" in result
        assert "word2" in result

    def test_mixed_whitespace(self) -> None:
        html = "<html><body><p>a  \t  \t  b</p></body></html>"
        result = _clean_html(html)
        # All whitespace between a and b should collapse to a single space.
        assert "a b" in result


# ---------------------------------------------------------------------------
# Additional edge case tests
# ---------------------------------------------------------------------------


class TestParseFilingEdgeCases:
    def test_empty_html(self) -> None:
        result = parse_filing(_make_filing_raw(""))
        assert result.full_text == ""
        assert result.section_parse_ok is False
        assert result.token_count == 0
        assert len(result.parse_warnings) > 0

    def test_meta_preserved(self) -> None:
        result = parse_filing(_make_filing_raw(SYNTHETIC_10K_HTML))
        assert result.meta.ticker == "JPM"
        assert result.meta.form == "10-K"
        assert result.meta.cik == "0000019617"

    def test_find_section_boundaries_returns_sorted(self) -> None:
        cleaned = _clean_html(SYNTHETIC_10K_HTML)
        boundaries = _find_section_boundaries(cleaned)
        # Verify return is a list of (key, position) tuples.
        assert isinstance(boundaries, list)
        for item in boundaries:
            assert len(item) == 2
            key, pos = item
            assert isinstance(key, str)
            assert isinstance(pos, int)
