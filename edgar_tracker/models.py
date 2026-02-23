"""Shared data models for the EDGAR tracker pipeline.

All modules exchange data through these typed dataclasses.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Keyword taxonomy models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Keyword:
    """A single searchable term or phrase."""
    label: str                          # human-readable name, e.g. "artificial intelligence"
    pattern: str                        # regex pattern (compiled at scoring time)
    is_phrase: bool = False             # True if multi-word phrase


@dataclass(frozen=True)
class KeywordGroup:
    """A named group of related keywords (one pillar)."""
    name: str                           # e.g. "ai", "purpose", "digital"
    display_name: str                   # e.g. "AI", "Purpose", "Digital"
    keywords: tuple[Keyword, ...] = ()


@dataclass(frozen=True)
class Taxonomy:
    """Complete keyword taxonomy (all pillars/groups)."""
    groups: tuple[KeywordGroup, ...] = ()

    def group_names(self) -> list[str]:
        return [g.name for g in self.groups]


# ---------------------------------------------------------------------------
# Filing metadata / retrieval models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FilingMeta:
    """Metadata for a single SEC filing."""
    cik: str                            # CIK10 (zero-padded)
    ticker: str
    company_name: str
    form: str                           # "10-K" or "10-Q"
    filing_date: str                    # ISO date string YYYY-MM-DD
    report_date: str                    # period-of-report date
    accession_number: str
    primary_document: str               # filename within the accession folder


@dataclass
class FilingRaw:
    """Downloaded filing content."""
    meta: FilingMeta
    html: str                           # raw HTML content
    cache_path: str = ""                # local cache path


# ---------------------------------------------------------------------------
# Parsed filing models
# ---------------------------------------------------------------------------

SECTION_KEYS = ("item1", "item1a", "item7", "item7a", "item1c")

@dataclass
class ParsedFiling:
    """Result of parsing a filing's HTML into clean text and sections."""
    meta: FilingMeta
    full_text: str
    sections: dict[str, str] = field(default_factory=dict)
    # sections keys: "item1", "item1a", "item7", "item7a", "item1c"
    section_parse_ok: bool = False
    token_count: int = 0
    parse_warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Scoring models
# ---------------------------------------------------------------------------

@dataclass
class KeywordScore:
    """Score for a single keyword within one context (full text or section)."""
    label: str
    count: int = 0
    contains: bool = False


@dataclass
class GroupScore:
    """Aggregated score for a keyword group (pillar)."""
    group_name: str
    display_name: str
    total_count: int = 0
    contains: bool = False              # True if any keyword matched
    keyword_scores: list[KeywordScore] = field(default_factory=list)


@dataclass
class SectionScores:
    """Scores for one section (or full_text)."""
    section_name: str                   # "full_text", "item1", "item1a", etc.
    group_scores: list[GroupScore] = field(default_factory=list)


@dataclass
class ScoreResult:
    """Complete scoring result for one filing."""
    meta: FilingMeta
    full_text_scores: SectionScores = field(default_factory=lambda: SectionScores("full_text"))
    section_scores: dict[str, SectionScores] = field(default_factory=dict)

    def to_wide_dict(self) -> dict[str, Any]:
        """Flatten to one dict with columns like contains_ai, count_ai, count_ai_item7, etc."""
        row: dict[str, Any] = {}
        # Full-text scores
        for gs in self.full_text_scores.group_scores:
            row[f"contains_{gs.group_name}"] = gs.contains
            row[f"count_{gs.group_name}"] = gs.total_count
            for ks in gs.keyword_scores:
                safe = ks.label.replace(" ", "_").replace("-", "_")
                row[f"count_{gs.group_name}_{safe}"] = ks.count
        # Per-section scores
        for sec_name, sec_scores in self.section_scores.items():
            for gs in sec_scores.group_scores:
                row[f"contains_{gs.group_name}_{sec_name}"] = gs.contains
                row[f"count_{gs.group_name}_{sec_name}"] = gs.total_count
        return row


# ---------------------------------------------------------------------------
# AI spend models
# ---------------------------------------------------------------------------

@dataclass
class XBRLFact:
    """A single extracted XBRL financial fact."""
    tag: str                            # XBRL tag name
    label: str                          # human-readable label
    value: float | None = None
    unit: str = "USD"
    period_end: str = ""                # YYYY-MM-DD
    source: str = ""                    # "xbrl-api" or "text-derived"


@dataclass
class TextEvidence:
    """A snippet of text evidence for AI investment activity."""
    category: str                       # e.g. "ai_infrastructure", "gpu_compute"
    snippet: str                        # short excerpt
    section: str = ""                   # which section it was found in
    confidence: float = 0.0             # 0.0 to 1.0


@dataclass
class AISpendResult:
    """Combined AI expenditure analysis for one filing."""
    meta: FilingMeta
    # Lane 1: XBRL numeric facts
    xbrl_facts: list[XBRLFact] = field(default_factory=list)
    # Lane 2: text evidence
    text_evidence: list[TextEvidence] = field(default_factory=list)
    # Derived scores
    ai_investment_mentions: int = 0
    ai_infrastructure_mentions: int = 0
    ai_spend_disclosure: bool = False
    ai_intensity_score: float = 0.0     # 0.0 to 1.0 heuristic
    # XBRL summary
    capex_total: float | None = None
    rd_expense: float | None = None
    software_intangibles: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ai_investment_mentions": self.ai_investment_mentions,
            "ai_infrastructure_mentions": self.ai_infrastructure_mentions,
            "ai_spend_disclosure": self.ai_spend_disclosure,
            "ai_intensity_score": round(self.ai_intensity_score, 4),
            "capex_total": self.capex_total,
            "rd_expense": self.rd_expense,
            "software_intangibles": self.software_intangibles,
            "xbrl_facts_count": len(self.xbrl_facts),
            "text_evidence_count": len(self.text_evidence),
        }


# ---------------------------------------------------------------------------
# Pipeline / export row
# ---------------------------------------------------------------------------

@dataclass
class PipelineRow:
    """One row in the final output dataset (one filing)."""
    # Filing metadata
    company_name: str = ""
    ticker: str = ""
    cik: str = ""
    form: str = ""
    filing_date: str = ""
    report_date: str = ""
    accession_number: str = ""
    filing_url: str = ""
    # Parse quality
    section_parse_ok: bool = False
    token_count: int = 0
    # Keyword scores (dynamic — populated from ScoreResult.to_wide_dict())
    keyword_scores: dict[str, Any] = field(default_factory=dict)
    # AI spend (populated from AISpendResult.to_dict())
    ai_spend: dict[str, Any] = field(default_factory=dict)
    # Processing metadata
    status: str = "ok"
    error: str = ""
    run_utc: str = ""

    def to_flat_dict(self) -> dict[str, Any]:
        """Flatten everything into a single dict for export."""
        d: dict[str, Any] = {
            "company_name": self.company_name,
            "ticker": self.ticker,
            "cik": self.cik,
            "form": self.form,
            "filing_date": self.filing_date,
            "report_date": self.report_date,
            "accession_number": self.accession_number,
            "filing_url": self.filing_url,
            "section_parse_ok": self.section_parse_ok,
            "token_count": self.token_count,
        }
        d.update(self.keyword_scores)
        d.update(self.ai_spend)
        d.update({
            "status": self.status,
            "error": self.error,
            "run_utc": self.run_utc,
        })
        return d
