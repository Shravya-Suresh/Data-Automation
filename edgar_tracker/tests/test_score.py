"""Unit tests for the scoring engine (edgar_tracker.score)."""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest
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
    Taxonomy,
    SECTION_KEYS,
)
from edgar_tracker.score import score, load_taxonomy, _count_pattern, _score_text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_filing_meta() -> FilingMeta:
    """Return a minimal dummy FilingMeta for tests."""
    return FilingMeta(
        cik="0000019617",
        ticker="JPM",
        company_name="JPMorgan Chase & Co.",
        form="10-K",
        filing_date="2024-02-15",
        report_date="2023-12-31",
        accession_number="0000019617-24-000000",
        primary_document="jpm10k2023.htm",
    )


def _make_parsed_filing(
    text: str,
    sections: dict[str, str] | None = None,
) -> ParsedFiling:
    """Create a ParsedFiling with *text* and optional *sections*."""
    meta = _make_filing_meta()
    secs = sections if sections is not None else {}
    return ParsedFiling(
        meta=meta,
        full_text=text,
        sections=secs,
        section_parse_ok=bool(secs and len([v for v in secs.values() if v]) >= 2),
        token_count=len(re.findall(r"[A-Za-z]+", text)),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ai_group() -> KeywordGroup:
    """A small AI keyword group with three keywords."""
    return KeywordGroup(
        name="ai",
        display_name="AI",
        keywords=(
            Keyword(
                label="artificial intelligence",
                pattern=r"\bartificial\s+intelligence\b",
                is_phrase=True,
            ),
            Keyword(
                label="AI",
                pattern=r"\bAI\b",
                is_phrase=False,
            ),
            Keyword(
                label="machine learning",
                pattern=r"\bmachine\s+learning\b",
                is_phrase=True,
            ),
        ),
    )


@pytest.fixture()
def genai_group() -> KeywordGroup:
    """A group with GenAI and AI keywords for overlap testing."""
    return KeywordGroup(
        name="genai_test",
        display_name="GenAI Test",
        keywords=(
            Keyword(
                label="GenAI",
                pattern=r"\bGenAI\b",
                is_phrase=False,
            ),
            Keyword(
                label="AI",
                pattern=r"\bAI\b",
                is_phrase=False,
            ),
        ),
    )


@pytest.fixture()
def digital_group() -> KeywordGroup:
    """A small digital keyword group."""
    return KeywordGroup(
        name="digital",
        display_name="Digital",
        keywords=(
            Keyword(
                label="digital transformation",
                pattern=r"\bdigital\s+transformation\b",
                is_phrase=True,
            ),
            Keyword(
                label="e-commerce",
                pattern=r"\be[\s-]commerce\b",
                is_phrase=False,
            ),
        ),
    )


@pytest.fixture()
def small_taxonomy(ai_group: KeywordGroup, digital_group: KeywordGroup) -> Taxonomy:
    """A taxonomy with two groups for full pipeline tests."""
    return Taxonomy(groups=(ai_group, digital_group))


@pytest.fixture()
def sample_text() -> str:
    """A representative paragraph mentioning AI and digital keywords."""
    return (
        "The company is investing heavily in artificial intelligence and AI systems. "
        "Our machine learning platform powers personalization. "
        "We are pursuing a digital transformation strategy to accelerate e-commerce. "
        "AI continues to be a strategic priority. "
        "Machine Learning models are deployed across all business units."
    )


# ---------------------------------------------------------------------------
# Tests for _count_pattern
# ---------------------------------------------------------------------------


class TestCountPattern:
    def test_basic_count(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("AI is great and AI is here", pattern) == 2

    def test_zero_count(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("nothing relevant here", pattern) == 0

    def test_phrase_count(self) -> None:
        pattern = re.compile(r"\bmachine\s+learning\b", re.IGNORECASE)
        text = "machine learning is a subset of machine learning research"
        assert _count_pattern(text, pattern) == 2


# ---------------------------------------------------------------------------
# Tests for _score_text
# ---------------------------------------------------------------------------


class TestScoreText:
    def test_returns_group_score(self, ai_group: KeywordGroup) -> None:
        gs = _score_text("AI is great", ai_group)
        assert gs.group_name == "ai"
        assert gs.display_name == "AI"
        assert gs.contains is True
        assert gs.total_count >= 1

    def test_keyword_breakdown(self, ai_group: KeywordGroup) -> None:
        gs = _score_text("artificial intelligence and AI", ai_group)
        labels = {ks.label: ks.count for ks in gs.keyword_scores}
        assert labels["artificial intelligence"] == 1
        assert labels["AI"] == 1
        assert labels["machine learning"] == 0


# ---------------------------------------------------------------------------
# Test cases (per specification)
# ---------------------------------------------------------------------------


class TestWordBoundaryAI:
    """test_word_boundary_ai: 'AI' matches in 'AI is great' but not in
    'SAID' or 'FAIR' or 'MAIL'."""

    def test_ai_matches_standalone(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("AI is great", pattern) == 1

    def test_ai_not_in_said(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("She SAID something", pattern) == 0

    def test_ai_not_in_fair(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("The FAIR value", pattern) == 0

    def test_ai_not_in_mail(self) -> None:
        pattern = re.compile(r"\bAI\b", re.IGNORECASE)
        assert _count_pattern("Check your MAIL", pattern) == 0


class TestPhraseMatching:
    """test_phrase_matching: 'artificial intelligence' matches case-insensitively
    and counts multiple occurrences."""

    def test_case_insensitive_match(self) -> None:
        pattern = re.compile(r"\bartificial\s+intelligence\b", re.IGNORECASE)
        assert _count_pattern("Artificial Intelligence is the future", pattern) == 1

    def test_multiple_occurrences(self) -> None:
        pattern = re.compile(r"\bartificial\s+intelligence\b", re.IGNORECASE)
        text = (
            "artificial intelligence transforms markets. "
            "We invest in Artificial Intelligence research."
        )
        assert _count_pattern(text, pattern) == 2

    def test_all_caps(self) -> None:
        pattern = re.compile(r"\bartificial\s+intelligence\b", re.IGNORECASE)
        assert _count_pattern("ARTIFICIAL INTELLIGENCE STRATEGY", pattern) == 1


class TestGroupAggregation:
    """test_group_aggregation: multiple keywords in a group, verify
    total_count == sum of individual counts."""

    def test_total_equals_sum(self, ai_group: KeywordGroup) -> None:
        text = (
            "AI and artificial intelligence plus machine learning. "
            "More AI and more machine learning."
        )
        gs = _score_text(text, ai_group)
        individual_sum = sum(ks.count for ks in gs.keyword_scores)
        assert gs.total_count == individual_sum

    def test_individual_counts(self, ai_group: KeywordGroup) -> None:
        text = (
            "AI and artificial intelligence plus machine learning. "
            "More AI and more machine learning."
        )
        gs = _score_text(text, ai_group)
        labels = {ks.label: ks.count for ks in gs.keyword_scores}
        assert labels["AI"] == 2
        assert labels["artificial intelligence"] == 1
        assert labels["machine learning"] == 2


class TestContainsFlag:
    """test_contains_flag: contains=True when count>0, False when count==0."""

    def test_contains_true_when_matched(self, ai_group: KeywordGroup) -> None:
        gs = _score_text("AI is here", ai_group)
        assert gs.contains is True
        ai_ks = next(ks for ks in gs.keyword_scores if ks.label == "AI")
        assert ai_ks.contains is True

    def test_contains_false_when_no_match(self, ai_group: KeywordGroup) -> None:
        gs = _score_text("nothing relevant at all", ai_group)
        assert gs.contains is False
        for ks in gs.keyword_scores:
            assert ks.contains is False


class TestSectionScoring:
    """test_section_scoring: scores are computed per-section when sections
    are provided."""

    def test_section_scores_present(
        self, small_taxonomy: Taxonomy
    ) -> None:
        sections = {
            "item1": "Our business uses AI and machine learning.",
            "item7": "Revenue grew due to digital transformation and e-commerce.",
        }
        pf = _make_parsed_filing("Full text with AI and digital transformation", sections)
        result = score(pf, small_taxonomy)

        # Full text scores should always exist.
        assert result.full_text_scores.section_name == "full_text"
        assert len(result.full_text_scores.group_scores) == 2

        # Section scores only for provided sections.
        assert "item1" in result.section_scores
        assert "item7" in result.section_scores
        # item1a was not provided, so it should not appear.
        assert "item1a" not in result.section_scores

    def test_section_specific_counts(
        self, small_taxonomy: Taxonomy
    ) -> None:
        sections = {
            "item1": "AI AI AI",  # 3 matches for AI
            "item7": "digital transformation",  # 1 match for digital
        }
        pf = _make_parsed_filing("Full text", sections)
        result = score(pf, small_taxonomy)

        # item1 should have AI count = 3
        item1_ai = next(
            gs for gs in result.section_scores["item1"].group_scores
            if gs.group_name == "ai"
        )
        ai_kw = next(ks for ks in item1_ai.keyword_scores if ks.label == "AI")
        assert ai_kw.count == 3

        # item7 should have digital transformation count = 1
        item7_digital = next(
            gs for gs in result.section_scores["item7"].group_scores
            if gs.group_name == "digital"
        )
        dt_kw = next(
            ks for ks in item7_digital.keyword_scores
            if ks.label == "digital transformation"
        )
        assert dt_kw.count == 1


class TestEmptyText:
    """test_empty_text: empty text returns zero counts."""

    def test_empty_full_text(self, ai_group: KeywordGroup) -> None:
        gs = _score_text("", ai_group)
        assert gs.total_count == 0
        assert gs.contains is False
        for ks in gs.keyword_scores:
            assert ks.count == 0

    def test_empty_filing(self, small_taxonomy: Taxonomy) -> None:
        pf = _make_parsed_filing("")
        result = score(pf, small_taxonomy)
        for gs in result.full_text_scores.group_scores:
            assert gs.total_count == 0
            assert gs.contains is False


class TestCaseInsensitive:
    """test_case_insensitive: 'Machine Learning' matches 'machine learning'
    and 'MACHINE LEARNING'."""

    def test_lowercase(self) -> None:
        pattern = re.compile(r"\bmachine\s+learning\b", re.IGNORECASE)
        assert _count_pattern("machine learning", pattern) == 1

    def test_titlecase(self) -> None:
        pattern = re.compile(r"\bmachine\s+learning\b", re.IGNORECASE)
        assert _count_pattern("Machine Learning", pattern) == 1

    def test_uppercase(self) -> None:
        pattern = re.compile(r"\bmachine\s+learning\b", re.IGNORECASE)
        assert _count_pattern("MACHINE LEARNING", pattern) == 1

    def test_mixed_case(self) -> None:
        pattern = re.compile(r"\bmachine\s+learning\b", re.IGNORECASE)
        assert _count_pattern("mAcHiNe lEaRnInG", pattern) == 1

    def test_via_score_text(self, ai_group: KeywordGroup) -> None:
        gs_lower = _score_text("machine learning", ai_group)
        gs_upper = _score_text("MACHINE LEARNING", ai_group)
        ml_lower = next(ks for ks in gs_lower.keyword_scores if ks.label == "machine learning")
        ml_upper = next(ks for ks in gs_upper.keyword_scores if ks.label == "machine learning")
        assert ml_lower.count == ml_upper.count == 1


class TestKeywordNotationPatternVsLabel:
    """Keyword vs key_word: pattern notation affects match counts; label notation
    only affects output column names (and can collide).
    """

    def test_pattern_space_vs_underscore_different_results(self) -> None:
        # Only the PATTERN is used for matching. Space (\\s+) matches whitespace
        # in document text; underscore matches literal underscore.
        text_with_space = "We use machine learning and machine learning again."
        text_with_underscore = "We use machine_learning in our code."
        group_space = KeywordGroup(
            name="test",
            display_name="Test",
            keywords=(
                Keyword(
                    label="machine learning",
                    pattern=r"\bmachine\s+learning\b",
                    is_phrase=True,
                ),
            ),
        )
        group_underscore = KeywordGroup(
            name="test",
            display_name="Test",
            keywords=(
                Keyword(
                    label="machine_learning",
                    pattern=r"\bmachine_learning\b",
                    is_phrase=False,
                ),
            ),
        )
        gs_space_in_text = _score_text(text_with_space, group_space)
        gs_underscore_in_text = _score_text(text_with_underscore, group_underscore)
        # "machine learning" (space) appears 2x; "machine_learning" (underscore) 1x
        assert gs_space_in_text.total_count == 2
        assert gs_underscore_in_text.total_count == 1
        # Cross-check: space pattern does not match underscore text
        gs_space_on_underscore_text = _score_text(text_with_underscore, group_space)
        gs_underscore_on_space_text = _score_text(text_with_space, group_underscore)
        assert gs_space_on_underscore_text.total_count == 0
        assert gs_underscore_on_space_text.total_count == 0

    def test_label_notation_does_not_affect_matching(self) -> None:
        # The LABEL is only used for the output column name (after normalizing
        # spaces and hyphens to underscores). It has no effect on what gets matched.
        same_pattern = KeywordGroup(
            name="x",
            display_name="X",
            keywords=(
                Keyword(label="key word", pattern=r"\bkeyword\b", is_phrase=False),
                Keyword(label="key_word", pattern=r"\bkeyword\b", is_phrase=False),
            ),
        )
        text = "We have one keyword here."
        gs = _score_text(text, same_pattern)
        # Both keywords use the same pattern so both match the same 1 occurrence
        assert gs.total_count == 2  # each keyword counted once
        counts = {ks.label: ks.count for ks in gs.keyword_scores}
        assert counts["key word"] == 1
        assert counts["key_word"] == 1

    def test_label_normalization_collision_in_wide_dict(self) -> None:
        # When flattening to CSV, label is normalized: space and hyphen -> underscore.
        # So "key word" and "key_word" both become "key_word"; the second overwrites.
        result = ScoreResult(
            meta=_make_filing_meta(),
            full_text_scores=SectionScores(
                section_name="full_text",
                group_scores=[
                    GroupScore(
                        group_name="test",
                        display_name="Test",
                        total_count=2,
                        contains=True,
                        keyword_scores=[
                            KeywordScore(label="key word", count=1, contains=True),
                            KeywordScore(label="key_word", count=1, contains=True),
                        ],
                    ),
                ],
            ),
            section_scores={},
        )
        wide = result.to_wide_dict()
        # Both labels normalize to count_test_key_word; only one key survives
        assert "count_test_key_word" in wide
        # Last keyword in the list wins (value 1 from key_word)
        assert wide["count_test_key_word"] == 1


class TestOverlappingPatterns:
    """test_overlapping_patterns: 'GenAI' and 'AI' -- 'GenAI' should count
    for GenAI pattern. 'AI' within 'GenAI' should also count for AI pattern
    since they are separate patterns (but word boundary on AI prevents it
    from matching inside GenAI, while standalone AI still counts)."""

    def test_genai_counts_separately(self, genai_group: KeywordGroup) -> None:
        text = "GenAI is the future. AI is already here."
        gs = _score_text(text, genai_group)
        labels = {ks.label: ks.count for ks in gs.keyword_scores}
        # GenAI matches "GenAI"
        assert labels["GenAI"] == 1
        # AI matches "AI" (standalone) -- "GenAI" does NOT match \bAI\b
        # because 'Gen' is alphanumeric so there is no word boundary before 'AI' in 'GenAI'
        assert labels["AI"] == 1

    def test_genai_only_text(self, genai_group: KeywordGroup) -> None:
        text = "We use GenAI for everything."
        gs = _score_text(text, genai_group)
        labels = {ks.label: ks.count for ks in gs.keyword_scores}
        assert labels["GenAI"] == 1
        # \bAI\b should NOT match inside "GenAI" (no word boundary)
        assert labels["AI"] == 0

    def test_both_present(self, genai_group: KeywordGroup) -> None:
        text = "GenAI and AI are different."
        gs = _score_text(text, genai_group)
        assert gs.total_count == 2  # 1 GenAI + 1 AI


class TestLoadTaxonomyFromYAML:
    """test_load_taxonomy_from_yaml: create a temp YAML file and verify
    it loads correctly."""

    def test_load_valid_yaml(self, tmp_path: Path) -> None:
        yaml_content = {
            "groups": [
                {
                    "name": "test_group",
                    "display_name": "Test Group",
                    "keywords": [
                        {
                            "label": "test keyword",
                            "pattern": r"\btest\s+keyword\b",
                            "is_phrase": True,
                        },
                        {
                            "label": "TK",
                            "pattern": r"\bTK\b",
                            "is_phrase": False,
                        },
                    ],
                },
                {
                    "name": "another",
                    "display_name": "Another Group",
                    "keywords": [
                        {
                            "label": "sample",
                            "pattern": r"\bsample\b",
                            "is_phrase": False,
                        },
                    ],
                },
            ]
        }
        yaml_file = tmp_path / "test_keywords.yaml"
        yaml_file.write_text(yaml.dump(yaml_content), encoding="utf-8")

        taxonomy = load_taxonomy(yaml_file)

        assert isinstance(taxonomy, Taxonomy)
        assert len(taxonomy.groups) == 2

        g0 = taxonomy.groups[0]
        assert g0.name == "test_group"
        assert g0.display_name == "Test Group"
        assert len(g0.keywords) == 2
        assert g0.keywords[0].label == "test keyword"
        assert g0.keywords[0].is_phrase is True
        assert g0.keywords[1].label == "TK"
        assert g0.keywords[1].is_phrase is False

        g1 = taxonomy.groups[1]
        assert g1.name == "another"
        assert len(g1.keywords) == 1

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_taxonomy(tmp_path / "nonexistent.yaml")

    def test_load_invalid_structure(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("not_groups: []", encoding="utf-8")
        with pytest.raises(ValueError, match="groups"):
            load_taxonomy(yaml_file)

    def test_taxonomy_group_names(self, tmp_path: Path) -> None:
        yaml_content = {
            "groups": [
                {"name": "ai", "display_name": "AI", "keywords": []},
                {"name": "digital", "display_name": "Digital", "keywords": []},
            ]
        }
        yaml_file = tmp_path / "names.yaml"
        yaml_file.write_text(yaml.dump(yaml_content), encoding="utf-8")

        taxonomy = load_taxonomy(yaml_file)
        assert taxonomy.group_names() == ["ai", "digital"]

    def test_loaded_taxonomy_scores_correctly(self, tmp_path: Path) -> None:
        """End-to-end: load YAML, create filing, score it."""
        yaml_content = {
            "groups": [
                {
                    "name": "tech",
                    "display_name": "Technology",
                    "keywords": [
                        {
                            "label": "cloud computing",
                            "pattern": r"\bcloud\s+computing\b",
                            "is_phrase": True,
                        },
                    ],
                },
            ]
        }
        yaml_file = tmp_path / "tech.yaml"
        yaml_file.write_text(yaml.dump(yaml_content), encoding="utf-8")

        taxonomy = load_taxonomy(yaml_file)
        pf = _make_parsed_filing("We rely on cloud computing for our cloud computing needs.")
        result = score(pf, taxonomy)

        tech_gs = result.full_text_scores.group_scores[0]
        assert tech_gs.group_name == "tech"
        assert tech_gs.total_count == 2
        assert tech_gs.contains is True


# ---------------------------------------------------------------------------
# Full scoring pipeline tests
# ---------------------------------------------------------------------------


class TestFullScorePipeline:
    def test_score_result_structure(
        self, small_taxonomy: Taxonomy, sample_text: str
    ) -> None:
        pf = _make_parsed_filing(sample_text)
        result = score(pf, small_taxonomy)

        assert result.meta.ticker == "JPM"
        assert result.full_text_scores.section_name == "full_text"
        assert len(result.full_text_scores.group_scores) == len(small_taxonomy.groups)

    def test_to_wide_dict(
        self, small_taxonomy: Taxonomy, sample_text: str
    ) -> None:
        pf = _make_parsed_filing(sample_text)
        result = score(pf, small_taxonomy)
        wide = result.to_wide_dict()

        assert "contains_ai" in wide
        assert "count_ai" in wide
        assert "contains_digital" in wide
        assert "count_digital" in wide
        assert isinstance(wide["contains_ai"], bool)
        assert isinstance(wide["count_ai"], int)

    def test_to_wide_dict_with_sections(
        self, small_taxonomy: Taxonomy
    ) -> None:
        sections = {
            "item1": "AI is transformative.",
            "item7": "digital transformation drives growth.",
        }
        pf = _make_parsed_filing("Full text with AI", sections)
        result = score(pf, small_taxonomy)
        wide = result.to_wide_dict()

        assert "contains_ai_item1" in wide
        assert "count_ai_item1" in wide
        assert "contains_digital_item7" in wide
        assert "count_digital_item7" in wide
