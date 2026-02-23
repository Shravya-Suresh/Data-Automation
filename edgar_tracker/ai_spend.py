"""AI expenditure extraction with two lanes.

Lane 1 -- XBRL-based numeric extraction from the SEC XBRL company facts API.
Lane 2 -- Text-based AI investment evidence discovery and scoring.

Both lanes feed into a single :class:`AISpendResult` that is consumed
downstream by the pipeline assembler.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import requests

from edgar_tracker.models import (
    AISpendResult,
    FilingMeta,
    ParsedFiling,
    TextEvidence,
    XBRLFact,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEC_XBRL_COMPANY_FACTS_URL = (
    "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
)

_XBRL_CACHE_SUBDIR = "xbrl_facts"

# XBRL tag groups: (result_attribute, human_label, ordered tag fallbacks)
_XBRL_TAG_GROUPS: list[tuple[str, str, list[str]]] = [
    (
        "capex",
        "Capital Expenditures",
        [
            "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
            "us-gaap:CapitalExpendituresIncurredButNotYetPaid",
        ],
    ),
    (
        "rd",
        "Research & Development",
        [
            "us-gaap:ResearchAndDevelopmentExpense",
            "us-gaap:ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
        ],
    ),
    (
        "software_intangibles",
        "Software / Intangibles",
        [
            "us-gaap:PaymentsToAcquireIntangibleAssets",
            "us-gaap:CapitalizedComputerSoftwareAdditions",
        ],
    ),
]

# ---------------------------------------------------------------------------
# AI spend text-evidence categories
# ---------------------------------------------------------------------------

# Each entry: (category_name, list of regex patterns)
_EVIDENCE_CATEGORIES: list[tuple[str, list[str]]] = [
    (
        "ai_investment",
        [
            r"AI\s+invest",
            r"artificial\s+intelligence\s+invest",
            r"AI\s+spending",
            r"AI\s+budget",
        ],
    ),
    (
        "ai_infrastructure",
        [
            r"data\s+center",
            r"cloud\s+infrastructure",
            r"computing\s+infrastructure",
        ],
    ),
    (
        "gpu_compute",
        [
            r"GPU",
            r"graphic\s+processing",
            r"accelerator",
            r"compute\s+capacity",
            r"NVIDIA",
            r"TPU",
        ],
    ),
    (
        "ai_modernization",
        [
            r"modernization",
            r"digital\s+transformation",
            r"technology\s+transformation",
        ],
    ),
    (
        "capitalized_software",
        [
            r"capitalized\s+software",
            r"internal[\-\u2010\u2011\u2012\u2013\u2014]use\s+software",
            r"software\s+development\s+cost",
        ],
    ),
    (
        "model_training",
        [
            r"model\s+training",
            r"training\s+infrastructure",
            r"fine[\-\u2010\u2011\u2012\u2013\u2014]tuning",
            r"foundation\s+model",
        ],
    ),
]

_MAX_SNIPPETS_PER_CATEGORY: int = 3
_SNIPPET_CONTEXT_CHARS: int = 200


# ---------------------------------------------------------------------------
# Internal helpers -- XBRL (Lane 1)
# ---------------------------------------------------------------------------


def _write_json_atomic(path: Path, data: Any) -> None:
    """Write *data* as JSON to *path* atomically via a temp file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> Any:
    """Read JSON from *path* and return the parsed object."""
    return json.loads(path.read_text(encoding="utf-8"))


def _fetch_company_facts(
    cik10: str,
    session: requests.Session,
    limiter: Any,
    cache_dir: Path | None = None,
) -> dict[str, Any] | None:
    """Fetch XBRL company facts JSON from the SEC API, with local caching.

    Parameters
    ----------
    cik10:
        Zero-padded 10-digit CIK string.
    session:
        A ``requests.Session`` pre-configured with a valid ``User-Agent``.
    limiter:
        A rate-limiter object exposing a ``wait()`` method.
    cache_dir:
        Optional local cache root.  If provided, the JSON is stored under
        ``<cache_dir>/xbrl_facts/CIK<cik10>.json``.

    Returns
    -------
    dict or None
        The parsed JSON payload, or ``None`` on failure.
    """
    cache_path: Path | None = None
    if cache_dir is not None:
        cache_path = cache_dir / _XBRL_CACHE_SUBDIR / f"CIK{cik10}.json"
        if cache_path.exists():
            try:
                return _read_json(cache_path)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Corrupt XBRL cache for CIK %s, refetching: %s", cik10, exc)

    url = SEC_XBRL_COMPANY_FACTS_URL.format(cik10=cik10)
    try:
        limiter.wait()
        resp = session.get(url, timeout=30.0)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("XBRL company facts fetch failed for CIK %s: %s", cik10, exc)
        return None

    if cache_path is not None:
        try:
            _write_json_atomic(cache_path, data)
        except OSError as exc:
            logger.warning("Failed to cache XBRL facts for CIK %s: %s", cik10, exc)

    return data


def _extract_fact_value(
    facts_json: dict[str, Any],
    tag: str,
    report_date: str,
) -> float | None:
    """Look up a single XBRL tag value for the given report period.

    Parameters
    ----------
    facts_json:
        The full company facts JSON payload from the SEC API.
    tag:
        A qualified XBRL tag, e.g. ``"us-gaap:ResearchAndDevelopmentExpense"``.
    report_date:
        The filing's ``report_date`` (ISO ``YYYY-MM-DD``) used to select the
        matching period end.

    Returns
    -------
    float or None
        The extracted numeric value, or ``None`` if no matching fact exists.
    """
    parts = tag.split(":", 1)
    if len(parts) != 2:
        return None
    taxonomy, tag_name = parts  # e.g. "us-gaap", "ResearchAndDevelopmentExpense"

    facts_block: dict[str, Any] = facts_json.get("facts", {})
    taxonomy_block: dict[str, Any] = facts_block.get(taxonomy, {})
    tag_block: dict[str, Any] = taxonomy_block.get(tag_name, {})
    units: dict[str, Any] = tag_block.get("units", {})

    # Try USD first, then any available unit.
    unit_entries: list[dict[str, Any]] = units.get("USD", [])
    if not unit_entries:
        for _unit_key, entries in units.items():
            if isinstance(entries, list) and entries:
                unit_entries = entries
                break

    if not unit_entries:
        return None

    # Find the fact whose period end matches the filing's report_date.
    for entry in unit_entries:
        end_date = entry.get("end", "")
        if end_date == report_date:
            val = entry.get("val")
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    continue

    return None


def _run_xbrl_lane(
    filing_meta: FilingMeta,
    session: requests.Session | None = None,
    limiter: Any | None = None,
    cache_dir: Path | None = None,
    xbrl_facts_json: dict[str, Any] | None = None,
) -> tuple[list[XBRLFact], float | None, float | None, float | None]:
    """Execute Lane 1: XBRL numeric fact extraction.

    Parameters
    ----------
    xbrl_facts_json:
        Pre-fetched XBRL company facts JSON.  When provided, the network
        fetch is skipped entirely (useful for process-pool workers that
        lack access to the shared HTTP session).

    Returns
    -------
    tuple
        ``(xbrl_facts, capex_total, rd_expense, software_intangibles)``
    """
    if xbrl_facts_json is not None:
        facts_json = xbrl_facts_json
    elif session is not None and limiter is not None:
        facts_json = _fetch_company_facts(
            filing_meta.cik, session, limiter, cache_dir=cache_dir
        )
    else:
        facts_json = None
    if facts_json is None:
        return [], None, None, None

    xbrl_facts: list[XBRLFact] = []
    summary: dict[str, float | None] = {
        "capex": None,
        "rd": None,
        "software_intangibles": None,
    }

    for group_key, group_label, tags in _XBRL_TAG_GROUPS:
        for tag in tags:
            value = _extract_fact_value(facts_json, tag, filing_meta.report_date)
            if value is not None:
                xbrl_facts.append(
                    XBRLFact(
                        tag=tag,
                        label=group_label,
                        value=value,
                        unit="USD",
                        period_end=filing_meta.report_date,
                        source="xbrl-api",
                    )
                )
                # Use the first successful tag as the summary value for
                # this group (primary tag takes precedence over fallbacks).
                if summary[group_key] is None:
                    summary[group_key] = value

    return xbrl_facts, summary["capex"], summary["rd"], summary["software_intangibles"]


# ---------------------------------------------------------------------------
# Internal helpers -- Text evidence (Lane 2)
# ---------------------------------------------------------------------------


def _compile_category_patterns() -> list[tuple[str, re.Pattern[str]]]:
    """Compile all evidence-category patterns into a list of (name, regex) pairs.

    Each category's individual patterns are joined with ``|`` into a single
    compiled regex using ``re.IGNORECASE``.
    """
    compiled: list[tuple[str, re.Pattern[str]]] = []
    for category_name, patterns in _EVIDENCE_CATEGORIES:
        combined = "|".join(f"(?:{p})" for p in patterns)
        compiled.append((category_name, re.compile(combined, re.IGNORECASE)))
    return compiled


_COMPILED_CATEGORIES: list[tuple[str, re.Pattern[str]]] | None = None


def _get_compiled_categories() -> list[tuple[str, re.Pattern[str]]]:
    """Return the lazily-compiled category patterns (singleton)."""
    global _COMPILED_CATEGORIES  # noqa: PLW0603
    if _COMPILED_CATEGORIES is None:
        _COMPILED_CATEGORIES = _compile_category_patterns()
    return _COMPILED_CATEGORIES


def _extract_snippet(text: str, match: re.Match[str], context: int = _SNIPPET_CONTEXT_CHARS) -> str:
    """Extract a snippet of *context* characters around the regex *match*.

    Parameters
    ----------
    text:
        The full text being searched.
    match:
        A ``re.Match`` object within *text*.
    context:
        Total desired snippet length (the match itself plus surrounding
        characters, roughly split evenly before/after).

    Returns
    -------
    str
        A cleaned snippet string, up to *context* characters.
    """
    start = match.start()
    end = match.end()
    match_len = end - start
    padding = max(0, (context - match_len) // 2)

    snip_start = max(0, start - padding)
    snip_end = min(len(text), end + padding)
    snippet = text[snip_start:snip_end].strip()

    # Collapse internal whitespace for readability.
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet


def _identify_section(
    char_offset: int,
    section_boundaries: list[tuple[int, int, str]],
) -> str:
    """Return the section name that contains *char_offset*, or ``"unknown"``.

    Parameters
    ----------
    char_offset:
        Character position in the full text.
    section_boundaries:
        Sorted list of ``(start, end, section_name)`` tuples.
    """
    for sec_start, sec_end, sec_name in section_boundaries:
        if sec_start <= char_offset < sec_end:
            return sec_name
    return "unknown"


def _build_section_boundaries(
    full_text: str,
    sections: dict[str, str],
) -> list[tuple[int, int, str]]:
    """Build a list of ``(start, end, section_name)`` from known sections.

    Uses a simple ``str.find`` against ``full_text`` to locate each section's
    text.  Falls back gracefully if a section cannot be located.
    """
    boundaries: list[tuple[int, int, str]] = []
    for sec_name, sec_text in sections.items():
        if not sec_text:
            continue
        idx = full_text.find(sec_text[:200])
        if idx >= 0:
            boundaries.append((idx, idx + len(sec_text), sec_name))
    boundaries.sort(key=lambda t: t[0])
    return boundaries


def _run_text_lane(
    parsed_filing: ParsedFiling,
) -> tuple[list[TextEvidence], dict[str, int]]:
    """Execute Lane 2: text-based evidence extraction.

    Returns
    -------
    tuple
        ``(text_evidence_list, category_counts)`` where *category_counts*
        maps each category name to its total match count.
    """
    full_text: str = parsed_filing.full_text
    if not full_text:
        return [], {}

    section_boundaries = _build_section_boundaries(full_text, parsed_filing.sections)
    compiled_categories = _get_compiled_categories()

    all_evidence: list[TextEvidence] = []
    category_counts: dict[str, int] = {}

    for category_name, pattern in compiled_categories:
        matches: list[re.Match[str]] = list(pattern.finditer(full_text))
        count = len(matches)
        category_counts[category_name] = count

        # Collect up to _MAX_SNIPPETS_PER_CATEGORY snippets.
        for m in matches[:_MAX_SNIPPETS_PER_CATEGORY]:
            snippet = _extract_snippet(full_text, m)
            section = _identify_section(m.start(), section_boundaries)
            all_evidence.append(
                TextEvidence(
                    category=category_name,
                    snippet=snippet,
                    section=section,
                    confidence=min(1.0, count / 10.0),
                )
            )

    return all_evidence, category_counts


# ---------------------------------------------------------------------------
# Derived scores
# ---------------------------------------------------------------------------


def _compute_derived_scores(
    category_counts: dict[str, int],
    token_count: int,
) -> tuple[int, int, bool, float]:
    """Compute the four derived AI-spend scores.

    Parameters
    ----------
    category_counts:
        Mapping of category name to total match count, as returned by
        :func:`_run_text_lane`.
    token_count:
        The filing's token count (from :attr:`ParsedFiling.token_count`).

    Returns
    -------
    tuple
        ``(ai_investment_mentions, ai_infrastructure_mentions,
        ai_spend_disclosure, ai_intensity_score)``
    """
    ai_investment_mentions: int = (
        category_counts.get("ai_investment", 0)
        + category_counts.get("model_training", 0)
    )

    ai_infrastructure_mentions: int = (
        category_counts.get("ai_infrastructure", 0)
        + category_counts.get("gpu_compute", 0)
    )

    total_mentions: int = sum(category_counts.values())
    ai_spend_disclosure: bool = total_mentions > 0

    # ai_intensity_score: heuristic 0.0--1.0
    total_categories = len(_EVIDENCE_CATEGORIES)
    categories_with_hits = sum(1 for c in category_counts.values() if c > 0)

    diversity_component: float = (
        (categories_with_hits / total_categories) * 0.5
        if total_categories > 0
        else 0.0
    )

    density_component: float = 0.0
    if token_count > 0:
        density_component = (total_mentions / token_count * 1000) * 0.5

    ai_intensity_score: float = min(1.0, diversity_component + density_component)

    return (
        ai_investment_mentions,
        ai_infrastructure_mentions,
        ai_spend_disclosure,
        ai_intensity_score,
    )


# ---------------------------------------------------------------------------
# Cache directory resolution
# ---------------------------------------------------------------------------


def _resolve_cache_dir() -> Path | None:
    """Attempt to locate the project-level cache directory.

    Looks for ``edgar_cache`` alongside the ``scraping/`` directory that is
    a sibling of this package, consistent with the pattern used by
    ``fetch_edgar_wordcounts.py``.

    Returns ``None`` if the directory cannot be determined.
    """
    pkg_dir = Path(__file__).resolve().parent  # edgar_tracker/
    project_root = pkg_dir.parent              # Scraping/

    candidate = project_root / "scraping" / "edgar_cache"
    if candidate.is_dir():
        return candidate

    # Fallback: look for edgar_cache directly under project root.
    candidate = project_root / "edgar_cache"
    if candidate.is_dir():
        return candidate

    # Create a default cache directory inside the package.
    default = pkg_dir / "cache" / "edgar_cache"
    default.mkdir(parents=True, exist_ok=True)
    return default


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_xbrl_company_facts(
    cik10: str,
    session: requests.Session,
    limiter: Any,
    cache_dir: Path | None = None,
) -> dict[str, Any] | None:
    """Pre-fetch XBRL company facts for later use in process-pool workers.

    This is a thin public wrapper around the internal fetch so that
    ``pipeline.py`` can download XBRL data in a threaded I/O phase and
    pass the result to CPU-bound workers that lack a network session.

    Parameters
    ----------
    cik10:
        Zero-padded 10-digit CIK string.
    session:
        A ``requests.Session`` pre-configured with a valid ``User-Agent``.
    limiter:
        A rate-limiter object exposing a ``wait()`` method.
    cache_dir:
        Optional local cache root.

    Returns
    -------
    dict or None
        The parsed JSON payload, or ``None`` on failure.
    """
    return _fetch_company_facts(cik10, session, limiter, cache_dir=cache_dir)


def extract_ai_spend(
    filing_meta: FilingMeta,
    parsed_filing: ParsedFiling,
    session: requests.Session | None = None,
    limiter: Any | None = None,
    cache_dir: Path | None = None,
    enable_xbrl: bool = True,
    xbrl_facts_json: dict[str, Any] | None = None,
) -> AISpendResult:
    """Extract AI spend data from a filing using XBRL and text evidence.

    This function orchestrates two parallel extraction lanes:

    **Lane 1 -- XBRL Facts** (requires *session*, *limiter*, and *enable_xbrl*):
        Fetches structured financial facts from the SEC XBRL company facts API
        and extracts Capital Expenditures, R&D Expense, and Software /
        Intangible Assets values for the filing's report period.

    **Lane 2 -- Text Evidence** (always runs):
        Scans the parsed filing text for mentions of AI investment activity
        across six evidence categories and computes derived scores including
        an overall ``ai_intensity_score``.

    Parameters
    ----------
    filing_meta:
        Metadata for the filing being analysed.
    parsed_filing:
        The parsed filing containing ``full_text``, ``sections``, and
        ``token_count``.
    session:
        An optional :class:`requests.Session` configured with a valid SEC
        ``User-Agent``.  When ``None``, Lane 1 (XBRL) is skipped unless
        *xbrl_facts_json* is provided.
    limiter:
        An optional rate-limiter object with a ``wait()`` method.  When
        ``None``, Lane 1 (XBRL) is skipped unless *xbrl_facts_json* is
        provided.
    cache_dir:
        Optional directory for caching XBRL company facts JSON.  When
        ``None``, falls back to :func:`_resolve_cache_dir`.
    enable_xbrl:
        If ``False``, skip Lane 1 (XBRL) entirely.  Default ``True``.
    xbrl_facts_json:
        Pre-fetched XBRL company facts JSON payload.  When provided the
        XBRL lane uses this data directly instead of fetching from the SEC
        API, which allows this function to run without a network session
        (e.g. inside a ``ProcessPoolExecutor`` worker).

    Returns
    -------
    AISpendResult
        A combined result containing XBRL facts, text evidence, and derived
        scores.
    """
    # -- Lane 1: XBRL facts --------------------------------------------------
    xbrl_facts: list[XBRLFact] = []
    capex_total: float | None = None
    rd_expense: float | None = None
    software_intangibles: float | None = None

    if enable_xbrl and (xbrl_facts_json is not None or (session is not None and limiter is not None)):
        xbrl_cache = cache_dir if cache_dir is not None else _resolve_cache_dir()
        try:
            xbrl_facts, capex_total, rd_expense, software_intangibles = (
                _run_xbrl_lane(
                    filing_meta, session, limiter,
                    cache_dir=xbrl_cache,
                    xbrl_facts_json=xbrl_facts_json,
                )
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "XBRL lane failed for %s (%s); continuing with text lane only",
                filing_meta.ticker,
                filing_meta.accession_number,
                exc_info=True,
            )

    # -- Lane 2: Text evidence ------------------------------------------------
    text_evidence, category_counts = _run_text_lane(parsed_filing)

    # -- Derived scores -------------------------------------------------------
    (
        ai_investment_mentions,
        ai_infrastructure_mentions,
        ai_spend_disclosure,
        ai_intensity_score,
    ) = _compute_derived_scores(category_counts, parsed_filing.token_count)

    return AISpendResult(
        meta=filing_meta,
        xbrl_facts=xbrl_facts,
        text_evidence=text_evidence,
        ai_investment_mentions=ai_investment_mentions,
        ai_infrastructure_mentions=ai_infrastructure_mentions,
        ai_spend_disclosure=ai_spend_disclosure,
        ai_intensity_score=ai_intensity_score,
        capex_total=capex_total,
        rd_expense=rd_expense,
        software_intangibles=software_intangibles,
    )
