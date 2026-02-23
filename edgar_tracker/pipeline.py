"""Main orchestration module for the EDGAR keyword + AI spend pipeline.

Chains together retrieval, parsing, scoring, AI-spend extraction, and export
to produce a comprehensive dataset of SEC filing analysis results.
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import logging
import os
import sys
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import requests

from edgar_tracker.models import FilingMeta, FilingRaw, PipelineRow, Taxonomy
from edgar_tracker.retrieval import (
    create_session,
    fetch_filing,
    list_filings,
    RateLimiter,
    resolve_cik,
)
from edgar_tracker.parse import parse_filing
from edgar_tracker.score import load_taxonomy, score
from edgar_tracker.ai_spend import extract_ai_spend, fetch_xbrl_company_facts
from edgar_tracker.export import export_csv, export_excel, export_parquet

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

_DEFAULT_FORMS: list[str] = ["10-K", "10-Q"]


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline run.

    Attributes
    ----------
    user_agent:
        SEC-compliant User-Agent string (required by EDGAR fair-access policy).
    start_date:
        Earliest filing date to include (ISO format ``YYYY-MM-DD``).
    end_date:
        Latest filing date to include (ISO format ``YYYY-MM-DD``).
    forms:
        SEC form types to retrieve (e.g. ``["10-K", "10-Q"]``).
    taxonomy_path:
        Path to the ``keywords.yaml`` taxonomy file.  If empty, the bundled
        default (``edgar_tracker/config/keywords.yaml``) is used.
    cache_dir:
        Directory for caching downloaded filings.
    output_dir:
        Directory for writing final output files.
    max_rps:
        Maximum requests per second to the SEC EDGAR API.
    workers:
        Number of concurrent threads for I/O operations (network fetches,
        company discovery).
    cpu_workers:
        Number of worker processes for CPU-bound filing processing (HTML
        parsing, keyword scoring, AI-spend text analysis).  Defaults to
        ``0`` which auto-selects ``min(os.cpu_count(), 8)``.
    document_source:
        Which document to fetch from the filing index (``"primary-html"``).
    include_amendments:
        Whether to include amendment forms (e.g. ``10-K/A``).
    enable_xbrl:
        Whether to attempt XBRL fact extraction in the AI-spend module.
    force_download:
        If ``True``, bypass the local cache and re-download filings.
    log_every:
        Log a progress message every *N* filings processed.
    output_format:
        Which output files to write: ``"csv"``, ``"excel"``, ``"parquet"``,
        or ``"both"`` (CSV + Excel + Parquet).
    """

    user_agent: str
    start_date: str = "2022-01-01"
    end_date: str = "2025-12-31"
    forms: list[str] = field(default_factory=lambda: list(_DEFAULT_FORMS))
    taxonomy_path: str = ""
    cache_dir: str = "edgar_cache"
    output_dir: str = "edgar_output"
    max_rps: float = 5.0
    workers: int = 4
    cpu_workers: int = 0
    document_source: str = "primary-html"
    include_amendments: bool = False
    enable_xbrl: bool = True
    force_download: bool = False
    log_every: int = 25
    output_format: str = "both"


# ---------------------------------------------------------------------------
# Thread-safe progress tracker
# ---------------------------------------------------------------------------


class _ProgressTracker:
    """Thread-safe counter for tracking pipeline progress."""

    def __init__(self, total: int, log_every: int = 25) -> None:
        self._total = total
        self._log_every = log_every
        self._done = 0
        self._errors = 0
        self._lock = threading.Lock()

    def increment(self, *, error: bool = False) -> None:
        """Record completion of one filing (optionally with an error)."""
        with self._lock:
            self._done += 1
            if error:
                self._errors += 1
            if self._done % self._log_every == 0 or self._done == self._total:
                logger.info(
                    "Progress: %d / %d filings processed (%d errors)",
                    self._done,
                    self._total,
                    self._errors,
                )

    @property
    def summary(self) -> str:
        """Return a human-readable summary string."""
        with self._lock:
            return (
                f"{self._done} filings processed, "
                f"{self._errors} errors, "
                f"{self._total} total"
            )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_filing_url(meta: FilingMeta) -> str:
    """Construct the EDGAR filing URL from filing metadata.

    Parameters
    ----------
    meta:
        Filing metadata containing CIK, accession number, and document name.

    Returns
    -------
    str
        Full URL to the filing on SEC EDGAR.
    """
    accession_flat = meta.accession_number.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{meta.cik}/{accession_flat}/{meta.primary_document}"
    )


def _resolve_taxonomy_path(config: PipelineConfig) -> str:
    """Determine the taxonomy YAML path.

    If ``config.taxonomy_path`` is set and exists, return it.  Otherwise fall
    back to the bundled ``edgar_tracker/config/keywords.yaml``.

    Parameters
    ----------
    config:
        The pipeline configuration.

    Returns
    -------
    str
        Resolved absolute path to the taxonomy YAML file.

    Raises
    ------
    FileNotFoundError
        If no taxonomy file can be located.
    """
    if config.taxonomy_path:
        path = Path(config.taxonomy_path)
        if path.exists():
            return str(path.resolve())
        raise FileNotFoundError(
            f"Specified taxonomy path not found: {config.taxonomy_path}"
        )
    # Fall back to bundled config
    bundled = Path(__file__).resolve().parent / "config" / "keywords.yaml"
    if bundled.exists():
        return str(bundled)
    raise FileNotFoundError(
        "No taxonomy file found.  Provide --taxonomy or place keywords.yaml in "
        "edgar_tracker/config/."
    )


# ---------------------------------------------------------------------------
# CPU worker for ProcessPoolExecutor (module-level for pickling)
# ---------------------------------------------------------------------------


def _cpu_process_filing(
    filing_raw: FilingRaw,
    taxonomy: Taxonomy,
    xbrl_facts_json: dict[str, Any] | None,
    enable_xbrl: bool,
    run_utc: str,
) -> PipelineRow:
    """Process a single filing through parse -> score -> ai_spend (CPU-bound).

    Designed to run in a :class:`ProcessPoolExecutor` worker.  All network
    I/O (filing download, XBRL fact fetch) must be completed before calling
    this function; the pre-fetched data is passed in directly.

    Parameters
    ----------
    filing_raw:
        The downloaded filing content (HTML + metadata).
    taxonomy:
        Loaded keyword taxonomy.
    xbrl_facts_json:
        Pre-fetched XBRL company facts JSON, or ``None``.
    enable_xbrl:
        Whether to run the XBRL extraction lane.
    run_utc:
        ISO-formatted UTC timestamp of this pipeline run.

    Returns
    -------
    PipelineRow
        A fully populated row (``status="error"`` on failure).
    """
    meta = filing_raw.meta
    row = PipelineRow(
        company_name=meta.company_name,
        ticker=meta.ticker,
        cik=meta.cik,
        form=meta.form,
        filing_date=meta.filing_date,
        report_date=meta.report_date,
        accession_number=meta.accession_number,
        filing_url=_build_filing_url(meta),
        run_utc=run_utc,
    )

    try:
        # Parse HTML into structured text
        parsed = parse_filing(filing_raw)
        row.section_parse_ok = parsed.section_parse_ok
        row.token_count = parsed.token_count

        # Score against keyword taxonomy
        score_result = score(parsed, taxonomy)
        row.keyword_scores = score_result.to_wide_dict()

        # Extract AI spend signals (no network — uses pre-fetched XBRL data)
        ai_result = extract_ai_spend(
            filing_meta=meta,
            parsed_filing=parsed,
            enable_xbrl=enable_xbrl,
            xbrl_facts_json=xbrl_facts_json,
        )
        row.ai_spend = ai_result.to_dict()

        row.status = "ok"

    except Exception as exc:
        row.status = "error"
        row.error = f"{type(exc).__name__}: {exc}"

    return row


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_pipeline(
    companies: list[dict[str, str]],
    config: PipelineConfig,
) -> list[PipelineRow]:
    """Run the full EDGAR keyword + AI spend pipeline.

    For each company the pipeline:

    1. Resolves the CIK via :func:`resolve_cik` (skips if not found).
    2. Lists all matching filings via :func:`list_filings` (skips if none).
    3. For each filing:
       a. Fetches the filing content.
       b. Parses the HTML into structured text.
       c. Scores keywords against the taxonomy.
       d. Extracts AI-spend signals.
       e. Builds a :class:`PipelineRow` from the results.

    Parameters
    ----------
    companies:
        List of dicts, each containing ``"ticker"`` and ``"company_name"``.
    config:
        Pipeline configuration controlling dates, forms, concurrency, etc.

    Returns
    -------
    list[PipelineRow]
        One row per successfully or unsuccessfully processed filing.  Rows
        with errors have ``status="error"`` and a populated ``error`` field.
    """
    run_utc = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
    logger.info("Pipeline run started at %s", run_utc)
    effective_cpu = config.cpu_workers or min(os.cpu_count() or 4, 8)
    logger.info(
        "Config: %d companies, forms=%s, date range=%s to %s, "
        "I/O threads=%d, CPU workers=%d",
        len(companies),
        config.forms,
        config.start_date,
        config.end_date,
        config.workers,
        effective_cpu,
    )

    # -- Load taxonomy -------------------------------------------------------
    taxonomy_path = _resolve_taxonomy_path(config)
    logger.info("Loading taxonomy from %s", taxonomy_path)
    taxonomy = load_taxonomy(taxonomy_path)
    logger.info(
        "Taxonomy loaded: %d groups, %d total keywords",
        len(taxonomy.groups),
        sum(len(g.keywords) for g in taxonomy.groups),
    )

    # -- Create session and rate limiter -------------------------------------
    session = create_session(user_agent=config.user_agent)
    rate_limiter = RateLimiter(max_rps=config.max_rps)

    # -- Phase 1: Resolve CIKs and list filings (parallel) ----------------
    all_filing_metas: list[FilingMeta] = []
    discovery_lock = threading.Lock()
    discovery_done = 0

    def _discover_company(
        idx: int, company: dict[str, str]
    ) -> list[FilingMeta]:
        nonlocal discovery_done
        ticker = company["ticker"]
        company_name = company.get("company_name", ticker)
        logger.info(
            "[%d/%d] Resolving CIK for %s (%s)",
            idx,
            len(companies),
            ticker,
            company_name,
        )

        try:
            cik = resolve_cik(
                ticker,
                session=session,
                limiter=rate_limiter,
                cache_dir=Path(config.cache_dir),
            )
        except Exception as exc:
            logger.warning(
                "Could not resolve CIK for %s: %s — skipping", ticker, exc
            )
            return []

        if not cik:
            logger.warning("CIK not found for %s — skipping", ticker)
            return []

        logger.info("Resolved %s -> CIK %s", ticker, cik)

        try:
            filings = list_filings(
                cik=cik,
                ticker=ticker,
                company_name=company_name,
                start_date=config.start_date,
                end_date=config.end_date,
                forms=config.forms,
                session=session,
                limiter=rate_limiter,
                cache_dir=Path(config.cache_dir),
                include_amendments=config.include_amendments,
            )
        except Exception as exc:
            logger.warning(
                "Could not list filings for %s (CIK %s): %s — skipping",
                ticker,
                cik,
                exc,
            )
            return []

        with discovery_lock:
            discovery_done += 1
            if discovery_done % config.log_every == 0:
                logger.info(
                    "Discovery progress: %d / %d companies resolved",
                    discovery_done,
                    len(companies),
                )

        if not filings:
            logger.info("No filings found for %s in date range — skipping", ticker)
            return []

        logger.info("Found %d filings for %s", len(filings), ticker)
        return filings

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(_discover_company, idx, company): company
            for idx, company in enumerate(companies, start=1)
        }
        for future in as_completed(futures):
            company = futures[future]
            try:
                metas = future.result()
                all_filing_metas.extend(metas)
            except Exception as exc:
                logger.error(
                    "Unexpected error discovering %s: %s",
                    company.get("ticker", "?"),
                    exc,
                )

    logger.info(
        "Total filings to process: %d across %d companies",
        len(all_filing_metas),
        len(companies),
    )

    if not all_filing_metas:
        logger.warning("No filings to process — returning empty results")
        return []

    # -- Phase 2a: Fetch filings + XBRL facts (I/O, threaded) ---------------
    results: list[PipelineRow] = []
    fetched: dict[str, FilingRaw] = {}       # accession -> FilingRaw
    fetch_errors: dict[str, str] = {}        # accession -> error msg
    xbrl_facts_map: dict[str, dict[str, Any] | None] = {}  # cik -> JSON

    def _fetch_one(meta: FilingMeta) -> tuple[str, FilingRaw]:
        raw = fetch_filing(
            meta,
            session=session,
            limiter=rate_limiter,
            cache_dir=Path(config.cache_dir),
            force_download=config.force_download,
        )
        return meta.accession_number, raw

    def _fetch_xbrl(cik: str) -> tuple[str, dict[str, Any] | None]:
        facts = fetch_xbrl_company_facts(
            cik, session, rate_limiter, Path(config.cache_dir),
        )
        return cik, facts

    unique_ciks = {m.cik for m in all_filing_metas}
    logger.info(
        "Phase 2a: Fetching %d filings + %d XBRL fact sets (%d I/O threads)",
        len(all_filing_metas),
        len(unique_ciks) if config.enable_xbrl else 0,
        config.workers,
    )

    with ThreadPoolExecutor(max_workers=config.workers) as io_pool:
        # Submit filing downloads and XBRL fetches to the same pool so
        # they share the rate limiter and interleave naturally.
        filing_futs = {
            io_pool.submit(_fetch_one, m): m for m in all_filing_metas
        }
        xbrl_futs: dict[Any, str] = {}
        if config.enable_xbrl:
            xbrl_futs = {
                io_pool.submit(_fetch_xbrl, cik): cik for cik in unique_ciks
            }

        fetch_done = 0
        for future in as_completed(filing_futs):
            fetch_done += 1
            if fetch_done % config.log_every == 0 or fetch_done == len(filing_futs):
                logger.info(
                    "Phase 2a: %d / %d filings fetched",
                    fetch_done,
                    len(filing_futs),
                )
            meta = filing_futs[future]
            try:
                acc, raw = future.result()
                fetched[acc] = raw
            except Exception as exc:
                logger.warning(
                    "Fetch failed for %s %s: %s",
                    meta.ticker, meta.accession_number, exc,
                )
                fetch_errors[meta.accession_number] = (
                    f"{type(exc).__name__}: {exc}"
                )

        xbrl_done = 0
        for future in as_completed(xbrl_futs):
            xbrl_done += 1
            if xbrl_done % max(1, config.log_every) == 0 or xbrl_done == len(xbrl_futs):
                logger.info(
                    "Phase 2a: %d / %d XBRL fact sets received",
                    xbrl_done,
                    len(xbrl_futs),
                )
            cik = xbrl_futs[future]
            try:
                _, facts = future.result()
                xbrl_facts_map[cik] = facts
            except Exception:
                xbrl_facts_map[cik] = None

    logger.info(
        "Phase 2a complete: %d fetched, %d failed",
        len(fetched), len(fetch_errors),
    )

    # Record error rows for filings that could not be downloaded.
    for meta in all_filing_metas:
        err = fetch_errors.get(meta.accession_number)
        if err:
            results.append(
                PipelineRow(
                    company_name=meta.company_name,
                    ticker=meta.ticker,
                    cik=meta.cik,
                    form=meta.form,
                    filing_date=meta.filing_date,
                    report_date=meta.report_date,
                    accession_number=meta.accession_number,
                    filing_url=_build_filing_url(meta),
                    status="error",
                    error=err,
                    run_utc=run_utc,
                )
            )

    # -- Phase 2b: Parse + Score + AI-spend (CPU, multiprocess) -------------
    cpu_tasks: list[tuple[FilingRaw, dict[str, Any] | None]] = []
    for meta in all_filing_metas:
        raw = fetched.get(meta.accession_number)
        if raw is not None:
            cpu_tasks.append((raw, xbrl_facts_map.get(meta.cik)))

    cpu_workers = config.cpu_workers or min(os.cpu_count() or 4, 8)
    logger.info(
        "Phase 2b: Processing %d filings across %d CPU workers",
        len(cpu_tasks),
        cpu_workers,
    )

    tracker = _ProgressTracker(
        total=len(all_filing_metas),
        log_every=config.log_every,
    )
    # Count the fetch errors already recorded above.
    for _ in fetch_errors:
        tracker.increment(error=True)

    with ProcessPoolExecutor(max_workers=cpu_workers) as cpu_pool:
        cpu_futures = {
            cpu_pool.submit(
                _cpu_process_filing,
                raw,
                taxonomy,
                xbrl_json,
                config.enable_xbrl,
                run_utc,
            ): raw.meta
            for raw, xbrl_json in cpu_tasks
        }
        for future in as_completed(cpu_futures):
            meta = cpu_futures[future]
            try:
                row = future.result()
                results.append(row)
                tracker.increment(error=(row.status != "ok"))
            except Exception as exc:
                logger.error(
                    "Process worker error for %s %s: %s",
                    meta.ticker,
                    meta.accession_number,
                    exc,
                )
                results.append(
                    PipelineRow(
                        company_name=meta.company_name,
                        ticker=meta.ticker,
                        cik=meta.cik,
                        form=meta.form,
                        filing_date=meta.filing_date,
                        report_date=meta.report_date,
                        accession_number=meta.accession_number,
                        filing_url=_build_filing_url(meta),
                        status="error",
                        error=f"ProcessError: {type(exc).__name__}: {exc}",
                        run_utc=run_utc,
                    )
                )
                tracker.increment(error=True)

    # Sort results by ticker then filing date for deterministic output
    results.sort(key=lambda r: (r.ticker, r.filing_date))

    logger.info("Processing complete: %s", tracker.summary)

    # -- Phase 3: Export results --------------------------------------------
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Date-stamped stem so multiple runs don't overwrite: edgar_results_YYYY-MM-DD
    run_date = (
        results[0].run_utc[:10] if results and results[0].run_utc
        else _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    )
    stem = f"edgar_results_{run_date}"

    fmt = config.output_format
    if fmt in ("csv", "both"):
        csv_path = str(output_dir / f"{stem}.csv")
        logger.info("Exporting CSV to %s", csv_path)
        export_csv(results, csv_path)
    if fmt in ("excel", "both"):
        excel_path = str(output_dir / f"{stem}.xlsx")
        logger.info("Exporting Excel to %s", excel_path)
        export_excel(results, excel_path)
    if fmt in ("parquet", "both"):
        parquet_path = str(output_dir / f"{stem}.parquet")
        logger.info("Exporting Parquet to %s", parquet_path)
        export_parquet(results, parquet_path)

    logger.info("Pipeline run complete. %d rows written.", len(results))
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _load_companies_from_csv(
    csv_path: str,
    ticker_col: str,
    company_col: str,
    limit: int | None = None,
) -> list[dict[str, str]]:
    """Load company list from a CSV file.

    Parameters
    ----------
    csv_path:
        Path to the CSV file.
    ticker_col:
        Column name for the ticker symbol.
    company_col:
        Column name for the company name.
    limit:
        If set, only return the first *limit* companies.

    Returns
    -------
    list[dict[str, str]]
        List of ``{"ticker": ..., "company_name": ...}`` dicts.

    Raises
    ------
    FileNotFoundError
        If *csv_path* does not exist.
    KeyError
        If the required columns are not found in the CSV.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    companies: list[dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file appears to be empty: {csv_path}")
        if ticker_col not in reader.fieldnames:
            raise KeyError(
                f"Ticker column '{ticker_col}' not found in CSV. "
                f"Available columns: {reader.fieldnames}"
            )
        if company_col not in reader.fieldnames:
            raise KeyError(
                f"Company column '{company_col}' not found in CSV. "
                f"Available columns: {reader.fieldnames}"
            )
        for row in reader:
            ticker = row[ticker_col].strip()
            company_name = row[company_col].strip()
            if not ticker:
                continue
            companies.append(
                {"ticker": ticker, "company_name": company_name or ticker}
            )
            if limit is not None and len(companies) >= limit:
                break

    return companies


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the pipeline CLI.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser with all pipeline arguments.
    """
    parser = argparse.ArgumentParser(
        description="EDGAR 10-K/10-Q keyword & AI-spend pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment variables:\n"
            "  SEC_USER_AGENT    Fallback for --user-agent if not supplied.\n"
        ),
    )

    # -- Input ---------------------------------------------------------------
    parser.add_argument(
        "--input-csv",
        default="data/processed/merged_companies_with_financials.csv",
        help="Path to CSV with company list (default: %(default)s)",
    )
    parser.add_argument(
        "--ticker-col",
        default="ticker",
        help="Column name for ticker symbols (default: %(default)s)",
    )
    parser.add_argument(
        "--company-col",
        default="universal_name",
        help="Column name for company names (default: %(default)s)",
    )

    # -- Taxonomy / filtering ------------------------------------------------
    parser.add_argument(
        "--taxonomy",
        default="",
        help=(
            "Path to keywords.yaml taxonomy file "
            "(default: edgar_tracker/config/keywords.yaml)"
        ),
    )
    parser.add_argument(
        "--start-date",
        default="2022-01-01",
        help="Earliest filing date (default: %(default)s)",
    )
    parser.add_argument(
        "--end-date",
        default="2025-12-31",
        help="Latest filing date (default: %(default)s)",
    )
    parser.add_argument(
        "--forms",
        nargs="+",
        default=["10-K", "10-Q"],
        help="SEC form types to retrieve (default: 10-K 10-Q)",
    )

    # -- SEC access ----------------------------------------------------------
    parser.add_argument(
        "--user-agent",
        default=None,
        help="SEC-compliant User-Agent (or set SEC_USER_AGENT env var)",
    )

    # -- Directories ---------------------------------------------------------
    parser.add_argument(
        "--cache-dir",
        default="edgar_cache",
        help="Cache directory for downloaded filings (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        default="edgar_output",
        help="Output directory for results (default: %(default)s)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Write all logs to this file as well as stderr. "
        "Default: <output-dir>/edgar_pipeline_<timestamp>.log",
    )

    # -- Performance ---------------------------------------------------------
    parser.add_argument(
        "--max-rps",
        type=float,
        default=5.0,
        help="Max requests per second to EDGAR (default: %(default)s)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of I/O threads for network fetches (default: %(default)s)",
    )
    parser.add_argument(
        "--cpu-workers",
        type=int,
        default=0,
        help="Number of CPU processes for parsing/scoring "
        "(default: 0 = auto-select min(cpu_count, 8))",
    )

    # -- Limits / options ----------------------------------------------------
    parser.add_argument(
        "--limit-companies",
        type=int,
        default=None,
        help="Process only the first N companies (default: all)",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "excel", "parquet", "both"],
        default="both",
        help="Output format: csv, excel, parquet, or both (all three). "
        "Files are named edgar_results_YYYY-MM-DD.<ext> (default: %(default)s)",
    )
    parser.add_argument(
        "--no-xbrl",
        action="store_true",
        help="Disable XBRL fact extraction in the AI-spend step",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Bypass cache and re-download all filings",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for the EDGAR pipeline.

    Parses command-line arguments, loads companies from CSV, builds a
    :class:`PipelineConfig`, and calls :func:`run_pipeline`. All log output
    is written to stderr and, when enabled, to a log file for full audit.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # -- Configure logging: stderr + optional log file -----------------------
    log_format = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stderr),
    ]
    log_path: Path | None = None
    if args.log_file:
        log_path = Path(args.log_file)
    else:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / (
            f"edgar_pipeline_{_dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))
    handlers.append(file_handler)
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=log_datefmt,
        handlers=handlers,
        force=True,
    )
    logger.info("Log file: %s", log_path.resolve())

    # -- Resolve user agent --------------------------------------------------
    user_agent: str | None = args.user_agent or os.environ.get("SEC_USER_AGENT")
    if not user_agent:
        parser.error(
            "A User-Agent is required.  Supply --user-agent or set the "
            "SEC_USER_AGENT environment variable.\n"
            "Format: 'Company Name admin@example.com'"
        )

    # -- Load companies ------------------------------------------------------
    logger.info("Loading companies from %s", args.input_csv)
    companies = _load_companies_from_csv(
        csv_path=args.input_csv,
        ticker_col=args.ticker_col,
        company_col=args.company_col,
        limit=args.limit_companies,
    )
    logger.info("Loaded %d companies", len(companies))

    if not companies:
        logger.error("No companies loaded — exiting")
        sys.exit(1)

    # -- Build config --------------------------------------------------------
    config = PipelineConfig(
        user_agent=user_agent,
        start_date=args.start_date,
        end_date=args.end_date,
        forms=args.forms,
        taxonomy_path=args.taxonomy,
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        max_rps=args.max_rps,
        workers=args.workers,
        cpu_workers=args.cpu_workers,
        output_format=args.output_format,
        enable_xbrl=not args.no_xbrl,
        force_download=args.force_download,
    )

    # -- Run pipeline --------------------------------------------------------
    try:
        results = run_pipeline(companies, config)
        logger.info("Done. %d rows produced.", len(results))
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (Ctrl+C). Shutting down.")
        logging.shutdown()
        sys.exit(130)


if __name__ == "__main__":
    main()
