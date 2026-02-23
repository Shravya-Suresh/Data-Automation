"""Keyword scoring engine for SEC filings.

Scores parsed filing text against a keyword taxonomy loaded from YAML.
Supports both single-token word-boundary matching and multi-word phrase
matching, all case-insensitive.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Union

import yaml

from edgar_tracker.models import (
    FilingMeta,
    GroupScore,
    Keyword,
    KeywordGroup,
    KeywordScore,
    ParsedFiling,
    ScoreResult,
    SectionScores,
    SECTION_KEYS,
    Taxonomy,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _count_pattern(text: str, pattern: re.Pattern[str]) -> int:
    """Count all non-overlapping matches of *pattern* in *text*.

    Parameters
    ----------
    text:
        The haystack string to search.
    pattern:
        A pre-compiled ``re.Pattern`` (expected to use ``re.IGNORECASE``).

    Returns
    -------
    int
        The number of non-overlapping matches found.
    """
    return len(pattern.findall(text))


def _score_text(text: str, group: KeywordGroup) -> GroupScore:
    """Score a single block of text against every keyword in *group*.

    Parameters
    ----------
    text:
        The text to search (full filing or a single section).
    group:
        A :class:`KeywordGroup` containing one or more keywords.

    Returns
    -------
    GroupScore
        Aggregated counts and per-keyword breakdowns.
    """
    keyword_scores: list[KeywordScore] = []
    total_count: int = 0
    group_contains: bool = False

    for kw in group.keywords:
        compiled: re.Pattern[str] = re.compile(kw.pattern, re.IGNORECASE)
        count: int = _count_pattern(text, compiled)
        contains: bool = count > 0

        keyword_scores.append(
            KeywordScore(
                label=kw.label,
                count=count,
                contains=contains,
            )
        )

        total_count += count
        if contains:
            group_contains = True

    return GroupScore(
        group_name=group.name,
        display_name=group.display_name,
        total_count=total_count,
        contains=group_contains,
        keyword_scores=keyword_scores,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_taxonomy(yaml_path: Union[str, Path]) -> Taxonomy:
    """Load a keyword taxonomy from a YAML file.

    Expected YAML structure::

        groups:
          - name: ai
            display_name: "AI"
            keywords:
              - label: "artificial intelligence"
                pattern: "\\\\bartificial\\\\s+intelligence\\\\b"
                is_phrase: true
              - label: "AI"
                pattern: "\\\\bAI\\\\b"
                is_phrase: false

    Parameters
    ----------
    yaml_path:
        Path to the ``keywords.yaml`` file.

    Returns
    -------
    Taxonomy
        A fully constructed :class:`Taxonomy` with :class:`KeywordGroup` and
        :class:`Keyword` objects.

    Raises
    ------
    FileNotFoundError
        If *yaml_path* does not exist.
    ValueError
        If the YAML structure is missing required keys.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Taxonomy file not found: {path}")

    with open(path, "r", encoding="utf-8") as fh:
        raw: dict = yaml.safe_load(fh)

    if not isinstance(raw, dict) or "groups" not in raw:
        raise ValueError(
            f"Taxonomy YAML must contain a top-level 'groups' key: {path}"
        )

    groups: list[KeywordGroup] = []

    for g in raw["groups"]:
        keywords: list[Keyword] = []
        for kw in g.get("keywords", []):
            keywords.append(
                Keyword(
                    label=kw["label"],
                    pattern=kw["pattern"],
                    is_phrase=bool(kw.get("is_phrase", False)),
                )
            )
        groups.append(
            KeywordGroup(
                name=g["name"],
                display_name=g["display_name"],
                keywords=tuple(keywords),
            )
        )

    return Taxonomy(groups=tuple(groups))


def score(parsed_filing: ParsedFiling, taxonomy: Taxonomy) -> ScoreResult:
    """Score a parsed filing against the keyword taxonomy.

    For every keyword group in the taxonomy, this function:

    1. Scores the filing's ``full_text`` and records results in
       ``ScoreResult.full_text_scores``.
    2. Scores each available section (from :data:`SECTION_KEYS`) present in
       ``parsed_filing.sections`` and records results in
       ``ScoreResult.section_scores``.

    Parameters
    ----------
    parsed_filing:
        A :class:`ParsedFiling` containing the text and sections to score.
    taxonomy:
        A :class:`Taxonomy` containing keyword groups to match against.

    Returns
    -------
    ScoreResult
        Complete scoring breakdown for the filing.
    """
    # -- Score full text -----------------------------------------------------
    full_text_group_scores: list[GroupScore] = [
        _score_text(parsed_filing.full_text, group)
        for group in taxonomy.groups
    ]

    full_text_scores = SectionScores(
        section_name="full_text",
        group_scores=full_text_group_scores,
    )

    # -- Score each available section ----------------------------------------
    section_scores: dict[str, SectionScores] = {}

    for section_key in SECTION_KEYS:
        section_text: str | None = parsed_filing.sections.get(section_key)
        if section_text is None:
            continue

        sec_group_scores: list[GroupScore] = [
            _score_text(section_text, group)
            for group in taxonomy.groups
        ]

        section_scores[section_key] = SectionScores(
            section_name=section_key,
            group_scores=sec_group_scores,
        )

    return ScoreResult(
        meta=parsed_filing.meta,
        full_text_scores=full_text_scores,
        section_scores=section_scores,
    )
