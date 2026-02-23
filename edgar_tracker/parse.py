"""Convert SEC filing HTML to clean text and split into Item sections.

This module handles two transformations:

1. **HTML cleaning** -- strip scripts/styles, collapse whitespace, preserve
   paragraph boundaries so downstream token counting remains accurate.

2. **Section splitting** -- locate Item headings (1, 1A, 7, 7A, 1C) using
   layered regex heuristics and extract the text between consecutive headings.
   Works for both 10-K and 10-Q filings (the latter use a Part I / Part II
   structure).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from edgar_tracker.models import SECTION_KEYS, FilingMeta, FilingRaw, ParsedFiling

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Regex patterns for item headings
# ---------------------------------------------------------------------------

# Maps each section key to a list of compiled patterns, ordered from most
# specific (preferred) to least specific (fallback).  The first capture group
# in every pattern is the canonical item label (e.g. "ITEM 1A").
#
# Patterns are applied case-insensitively.  They require a word boundary after
# the item number so that "Item 1" does not match inside "Item 10".

_HEADING_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "item1": [
        re.compile(
            r"^\s*(ITEM\s+1)\b"
            r"(?!\s*[A-Z0-9])"          # not followed by a letter/digit (avoid 1A, 1C, 10)
            r"[\.\s\u2014\u2013\-:]*"   # optional punctuation / dash
            r"\s*(?:BUSINESS)?",
            re.IGNORECASE | re.MULTILINE,
        ),
    ],
    "item1a": [
        re.compile(
            r"^\s*(ITEM\s+1\s*A)\b"
            r"[\.\s\u2014\u2013\-:]*"
            r"\s*(?:RISK\s+FACTORS)?",
            re.IGNORECASE | re.MULTILINE,
        ),
    ],
    "item7": [
        re.compile(
            r"^\s*(ITEM\s+7)\b"
            r"(?!\s*[A-Z0-9])"
            r"[\.\s\u2014\u2013\-:]*"
            r"\s*(?:MANAGEMENT[\u2019\u2018']?S?\s+DISCUSSION\s+AND\s+ANALYSIS)?",
            re.IGNORECASE | re.MULTILINE,
        ),
    ],
    "item7a": [
        re.compile(
            r"^\s*(ITEM\s+7\s*A)\b"
            r"[\.\s\u2014\u2013\-:]*"
            r"\s*(?:QUANTITATIVE\s+AND\s+QUALITATIVE)?",
            re.IGNORECASE | re.MULTILINE,
        ),
    ],
    "item1c": [
        re.compile(
            r"^\s*(ITEM\s+1\s*C)\b"
            r"[\.\s\u2014\u2013\-:]*"
            r"\s*(?:CYBERSECURITY)?",
            re.IGNORECASE | re.MULTILINE,
        ),
    ],
}

# A broad "any item heading" pattern used to find the *next* heading after a
# matched section start.  It matches Item followed by a number and an optional
# letter suffix (e.g. Item 1, Item 1A, Item 7A, Item 15).
#
# The ``^`` anchor (with ``re.MULTILINE``) ensures we only match headings at
# the start of a line, filtering out inline cross-references like
# "see Part II, Item 8 of this Form 10-K".
_ANY_ITEM_HEADING = re.compile(
    r"^\s*ITEM\s+(\d{1,2}\s*[A-Z]?)\b"
    r"[\.\s\u2014\u2013\-:]*",
    re.IGNORECASE | re.MULTILINE,
)

# Pattern to extract the normalised item identifier (e.g. "1", "1A", "7")
# from a heading match, stripping internal whitespace and upper-casing.
_ITEM_ID_RE = re.compile(r"ITEM\s+(\d{1,2}\s*[A-Z]?)\b", re.IGNORECASE)


def _normalise_item_id(text_at_pos: str) -> str:
    """Return the normalised item identifier (e.g. '1A', '7') from heading text."""
    m = _ITEM_ID_RE.search(text_at_pos)
    if m:
        return re.sub(r"\s+", "", m.group(1)).upper()
    return ""

# Part I / Part II markers (useful for 10-Q disambiguation).
_PART_PATTERN = re.compile(r"\bPART\s+(I{1,3}|IV|[1-4])\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------

def _clean_html(html: str) -> str:
    """Strip tags and normalise whitespace from SEC filing HTML.

    Processing steps:
    1. Parse with ``BeautifulSoup`` (html.parser).
    2. Remove ``<script>``, ``<style>``, and ``<noscript>`` elements.
    3. Insert newlines around block-level elements so paragraph boundaries
       survive the text extraction.
    4. Collapse runs of whitespace while keeping double-newlines (paragraph
       breaks) intact.

    Parameters
    ----------
    html:
        Raw HTML string of the filing.

    Returns
    -------
    str
        Cleaned plain text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove unwanted elements.
    for tag_name in ("script", "style", "noscript"):
        for element in soup.find_all(tag_name):
            element.decompose()

    # Insert newlines around block-level tags to preserve paragraph structure.
    _BLOCK_TAGS = {
        "p", "div", "br", "hr", "h1", "h2", "h3", "h4", "h5", "h6",
        "table", "tr", "li", "ul", "ol", "blockquote", "pre", "section",
        "article", "header", "footer", "nav", "aside",
    }
    for tag in soup.find_all(True):
        if tag.name in _BLOCK_TAGS:
            tag.insert_before("\n")
            tag.insert_after("\n")

    raw_text: str = soup.get_text()

    # Collapse spaces/tabs within lines (but keep newlines).
    raw_text = re.sub(r"[^\S\n]+", " ", raw_text)

    # Collapse 3+ consecutive newlines down to 2 (paragraph boundary).
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    # Strip leading/trailing whitespace on each line.
    lines = [line.strip() for line in raw_text.splitlines()]
    text = "\n".join(lines)

    # Final overall strip.
    return text.strip()


# ---------------------------------------------------------------------------
# Section boundary detection
# ---------------------------------------------------------------------------

def _find_section_boundaries(text: str) -> list[tuple[str, int]]:
    """Locate all Item headings in *text* and return their positions.

    Each entry is ``(section_key, char_offset)`` where *section_key* is one of
    :data:`SECTION_KEYS` (e.g. ``"item1a"``).  The list is sorted by
    *char_offset* ascending.

    The function scans the text for every pattern in :data:`_HEADING_PATTERNS`.
    When multiple matches overlap for the same section key, only the *last*
    substantial match is kept (SEC filings often have a table of contents that
    lists item names early in the document -- the actual content heading
    appears later).

    Parameters
    ----------
    text:
        The full cleaned text of the filing.

    Returns
    -------
    list[tuple[str, int]]
        Sorted list of ``(section_key, position)`` pairs.
    """
    # For each section key we collect *all* match positions, then choose the
    # best one.  Heuristic: prefer the last match that is followed by a
    # reasonable amount of text (> 200 chars before the next heading).  This
    # skips table-of-contents entries.

    # (start, end, item_id) for every heading hit
    all_item_positions: list[tuple[int, int, str]] = []
    for m in _ANY_ITEM_HEADING.finditer(text):
        item_id = _normalise_item_id(text[m.start():m.end()])
        all_item_positions.append((m.start(), m.end(), item_id))

    def _next_different_heading_after(pos: int, skip_item_id: str) -> int | None:
        """Return start of the first heading after *pos* for a DIFFERENT item.

        Running page headers repeat the same item heading on every page.
        By skipping headings with the same item_id we avoid false boundaries.
        """
        for start, _end, iid in all_item_positions:
            if start > pos and iid != skip_item_id:
                return start
        return None

    candidates: dict[str, list[tuple[int, int]]] = {key: [] for key in SECTION_KEYS}

    for key, patterns in _HEADING_PATTERNS.items():
        for pat in patterns:
            for m in pat.finditer(text):
                candidates[key].append((m.start(), m.end()))

    # Map section key -> normalised item id for running-header filtering.
    _KEY_TO_ITEM_ID: dict[str, str] = {
        "item1": "1", "item1a": "1A", "item7": "7",
        "item7a": "7A", "item1c": "1C",
    }

    # Pick the best candidate for each key.
    boundaries: dict[str, int] = {}
    for key, hits in candidates.items():
        if not hits:
            continue

        # Sort by position.
        hits.sort(key=lambda t: t[0])

        skip_id = _KEY_TO_ITEM_ID.get(key, "")

        # Prefer the last "substantial" match -- i.e. one where the section
        # body between this heading and the next *different* heading is > 200
        # characters.  Using _next_different_heading_after avoids false cuts
        # caused by running page headers that repeat the same item heading.
        best: tuple[int, int] | None = None
        for start, end in reversed(hits):
            next_heading = _next_different_heading_after(end, skip_id)
            body_len = (next_heading - end) if next_heading is not None else (len(text) - end)
            if body_len > 200:
                best = (start, end)
                break

        # Fallback: just take the last hit.
        if best is None:
            best = hits[-1]

        boundaries[key] = best[0]

    # Sort by position and return.
    result = sorted(boundaries.items(), key=lambda t: t[1])
    return result


# ---------------------------------------------------------------------------
# Section extraction
# ---------------------------------------------------------------------------

def _extract_sections(text: str, form_type: str) -> dict[str, str]:
    """Extract target sections from the cleaned filing text.

    Parameters
    ----------
    text:
        Full cleaned filing text.
    form_type:
        SEC form type (``"10-K"``, ``"10-Q"``, etc.).  Used to adjust
        heuristics for quarterly vs. annual filings.

    Returns
    -------
    dict[str, str]
        Mapping from section key to the extracted section text.  Keys that
        could not be located map to ``""``.
    """
    boundaries = _find_section_boundaries(text)

    # Map section key -> normalised item id for running-header filtering.
    _KEY_TO_ITEM_ID: dict[str, str] = {
        "item1": "1", "item1a": "1A", "item7": "7",
        "item7a": "7A", "item1c": "1C",
    }

    # Build an ordered list of *all* item headings (not just our targets) so
    # we can determine where each target section ends.
    # Each entry: (key_or_None, position, item_id)
    all_headings: list[tuple[str | None, int, str]] = []

    # Add our target boundaries.
    for key, pos in boundaries:
        all_headings.append((key, pos, _KEY_TO_ITEM_ID.get(key, "")))

    # Also add every generic item heading position so we can find the end of
    # each section.
    for m in _ANY_ITEM_HEADING.finditer(text):
        # Avoid duplicates with target headings (allow small fuzz).
        if not any(abs(m.start() - pos) < 5 for _, pos in boundaries):
            item_id = _normalise_item_id(text[m.start():m.end()])
            all_headings.append((None, m.start(), item_id))

    # Also add Part headings as potential endpoints.
    for m in _PART_PATTERN.finditer(text):
        if not any(abs(m.start() - hpos) < 5 for _, hpos, _ in all_headings):
            all_headings.append((None, m.start(), ""))

    all_headings.sort(key=lambda t: t[1])

    # For 10-Q filings the relevant items live under Part I and Part II; the
    # same item numbers can appear in both parts.  We do not attempt to
    # disambiguate here -- the heading-selection heuristic in
    # _find_section_boundaries already prefers the later (content) heading.

    sections: dict[str, str] = {key: "" for key in SECTION_KEYS}

    for key, pos in boundaries:
        # Find the next heading *after* this one that belongs to a DIFFERENT
        # item.  Running page headers repeat the same item heading on every
        # page of the filing; skipping same-item headings avoids premature
        # section truncation.
        this_item_id = _KEY_TO_ITEM_ID.get(key, "")
        next_pos: int | None = None
        for _k, p, iid in all_headings:
            # Skip headings for the same item (running page headers) and
            # headings with no item id (bare Part markers that often
            # accompany running page headers like "PART I\nItem 1").
            if p > pos and iid and iid != this_item_id:
                next_pos = p
                break

        if next_pos is not None:
            section_text = text[pos:next_pos].strip()
        else:
            section_text = text[pos:].strip()

        sections[key] = section_text

    return sections


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_filing(filing_raw: FilingRaw) -> ParsedFiling:
    """Convert filing HTML to clean text with section splitting.

    Parameters
    ----------
    filing_raw:
        A :class:`FilingRaw` instance containing the filing metadata and raw
        HTML content.

    Returns
    -------
    ParsedFiling
        Parsed result with cleaned full text, per-section text, token count,
        and diagnostic warnings.
    """
    warnings: list[str] = []

    # 1. Clean HTML --------------------------------------------------------
    full_text = _clean_html(filing_raw.html)

    if not full_text:
        return ParsedFiling(
            meta=filing_raw.meta,
            full_text="",
            sections={key: "" for key in SECTION_KEYS},
            section_parse_ok=False,
            token_count=0,
            parse_warnings=["empty document after HTML cleaning"],
        )

    # 2. Token count -------------------------------------------------------
    token_count = len(re.findall(r"[A-Za-z]+", full_text))

    # 3. Section splitting -------------------------------------------------
    sections = _extract_sections(full_text, filing_raw.meta.form)

    # 4. Diagnostics -------------------------------------------------------
    found_count = 0
    for key in SECTION_KEYS:
        if sections.get(key):
            found_count += 1
        else:
            warnings.append(f"{key} not found")

    section_parse_ok = found_count >= 2

    # Check for suspicious overlaps (a section starts inside another).
    boundary_list = _find_section_boundaries(full_text)
    boundary_positions = {key: pos for key, pos in boundary_list}
    sorted_keys = [key for key, _pos in boundary_list]
    for i, key in enumerate(sorted_keys):
        if i + 1 < len(sorted_keys):
            next_key = sorted_keys[i + 1]
            section_text = sections.get(key, "")
            next_start = boundary_positions[next_key]
            this_start = boundary_positions[key]
            # If the next section starts *before* the end of this section text
            # that is expected (they are contiguous).  But if two non-adjacent
            # sections appear very close together it can signal a TOC remnant.
            section_body_len = len(section_text)
            if section_body_len < 100 and sections.get(next_key, ""):
                warnings.append(
                    f"sections overlap or {key} suspiciously short "
                    f"({section_body_len} chars)"
                )

    if not section_parse_ok and found_count > 0:
        warnings.append(
            f"only {found_count} of {len(SECTION_KEYS)} sections found; "
            "section_parse_ok set to False"
        )

    return ParsedFiling(
        meta=filing_raw.meta,
        full_text=full_text,
        sections=sections,
        section_parse_ok=section_parse_ok,
        token_count=token_count,
        parse_warnings=warnings,
    )
