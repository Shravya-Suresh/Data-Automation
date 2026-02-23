"""SEC EDGAR filing discovery and download.

Discovers filings via the SEC's modern JSON endpoints, filters by form type
and date range, and downloads filing documents with gzip caching.

Reuses patterns from ``scraping/fetch_edgar_wordcounts.py`` (rate-limiter,
exponential backoff, gzip caching, pagination) but exposes a cleaner
two-function interface suitable for the ``edgar_tracker`` pipeline.

SEC endpoints used:
    - https://www.sec.gov/files/company_tickers.json
    - https://data.sec.gov/submissions/CIK##########.json
    - https://www.sec.gov/Archives/edgar/data/...
"""

from __future__ import annotations

import datetime as _dt
import gzip
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from threading import Lock
from typing import Any, Iterator, Optional

import requests

from edgar_tracker.models import FilingMeta, FilingRaw

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SEC URL templates
# ---------------------------------------------------------------------------

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

SEC_SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_SUBMISSIONS_FILE_URL_TMPL = "https://data.sec.gov/submissions/{name}"

SEC_ARCHIVES_PRIMARY_DOC_TMPL = (
    "https://www.sec.gov/Archives/edgar/data/"
    "{cik_no_zeros}/{accession_no_dashes}/{primary_doc}"
)
SEC_ARCHIVES_COMPLETE_TXT_TMPL = (
    "https://www.sec.gov/Archives/edgar/data/"
    "{cik_no_zeros}/{accession_no_dashes}/{accession_with_dashes}.txt"
)


# ---------------------------------------------------------------------------
# Rate limiter (thread-safe, monotonic clock)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Thread-safe rate limiter using a monotonic clock.

    Guarantees at most ``max_rps`` requests per second across all threads
    sharing the same instance.

    Parameters
    ----------
    max_rps:
        Maximum requests per second.  Must be positive.
    """

    def __init__(self, max_rps: float = 8.0) -> None:
        if max_rps <= 0:
            raise ValueError("max_rps must be > 0")
        self._min_interval: float = 1.0 / max_rps
        self._lock = Lock()
        self._next_allowed: float = time.monotonic()

    def wait(self) -> None:
        """Block the calling thread until a request is allowed."""
        sleep_for = 0.0
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_for = self._next_allowed - now
                self._next_allowed += self._min_interval
            else:
                self._next_allowed = now + self._min_interval
        if sleep_for > 0:
            time.sleep(sleep_for)


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def create_session(user_agent: str) -> requests.Session:
    """Create a :class:`requests.Session` with SEC-compliant headers.

    Parameters
    ----------
    user_agent:
        A descriptive User-Agent string that includes contact information
        (e.g. ``"MyApp myemail@example.com"``).  The SEC requires this.

    Returns
    -------
    requests.Session
        A session pre-configured with ``User-Agent``, ``Accept``, and
        ``Accept-Encoding`` headers.

    Raises
    ------
    ValueError
        If *user_agent* does not appear to contain contact information.
    """
    ua = (user_agent or "").strip()
    if len(ua) < 8 or "@" not in ua or not re.search(r"\s", ua):
        raise ValueError(
            "User-Agent must include contact info "
            "(example: 'YourName your.email@domain.com'). "
            "Provide a descriptive string or set the SEC_USER_AGENT env var."
        )

    session = requests.Session()
    session.headers.update({
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
    })
    return session


# ---------------------------------------------------------------------------
# Internal HTTP helpers (retries, caching)
# ---------------------------------------------------------------------------

def _is_retryable_status(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)


def _http_get_bytes(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    *,
    timeout_s: float = 30.0,
    max_retries: int = 5,
) -> bytes:
    """GET *url* and return the response body, with retries + backoff.

    Retries on transient HTTP errors (429, 5xx) and network-level
    :class:`requests.RequestException`.  Uses exponential backoff with
    random jitter.

    Raises
    ------
    RuntimeError
        If all retry attempts are exhausted.
    """
    last_err: Optional[BaseException] = None
    for attempt in range(max_retries):
        try:
            limiter.wait()
            resp = session.get(url, timeout=timeout_s)
            if resp.status_code == 200:
                return resp.content
            if _is_retryable_status(resp.status_code):
                backoff = min(60.0, (2.0 ** attempt) + random.random())
                logger.warning(
                    "[retryable %d] %s (sleep %.1fs)", resp.status_code, url, backoff,
                )
                time.sleep(backoff)
                continue
            resp.raise_for_status()
        except requests.RequestException as exc:
            last_err = exc
            backoff = min(60.0, (2.0 ** attempt) + random.random())
            logger.warning(
                "[retryable exception] %s: %s (sleep %.1fs)", url, exc, backoff,
            )
            time.sleep(backoff)
            continue
    raise RuntimeError(
        f"Failed to fetch after {max_retries} attempts: {url}"
    ) from last_err


# ---------------------------------------------------------------------------
# JSON read/write helpers
# ---------------------------------------------------------------------------

def _write_json_atomic(path: Path, data: Any) -> None:
    """Write *data* as JSON to *path* atomically (write-tmp-then-rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _http_get_json_cached(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    cache_path: Path,
    *,
    timeout_s: float = 30.0,
    max_retries: int = 5,
) -> Any:
    """GET *url*, parse as JSON, and cache the result.

    Returns the cached version on subsequent calls.
    """
    if cache_path.exists():
        return _read_json(cache_path)
    raw = _http_get_bytes(
        session, limiter, url, timeout_s=timeout_s, max_retries=max_retries,
    )
    data = json.loads(raw.decode("utf-8", errors="replace"))
    _write_json_atomic(cache_path, data)
    return data


# ---------------------------------------------------------------------------
# CIK resolution
# ---------------------------------------------------------------------------

def _get_company_ticker_map(
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
) -> dict[str, str]:
    """Download (or load from cache) the SEC ticker-to-CIK10 mapping.

    Returns a dict mapping upper-cased ticker symbols to zero-padded
    10-digit CIK strings.  Automatically creates ``BRK.A`` / ``BRK-A``
    style aliases.
    """
    cache_path = cache_dir / "metadata" / "company_tickers.json"
    data = _http_get_json_cached(session, limiter, SEC_COMPANY_TICKERS_URL, cache_path)

    mapping: dict[str, str] = {}
    for entry in data.values():
        ticker = entry.get("ticker")
        cik_str = entry.get("cik_str")
        if not ticker or cik_str is None:
            continue
        cik10 = str(int(cik_str)).zfill(10)
        t = str(ticker).strip().upper()
        if not t:
            continue
        mapping[t] = cik10

    # Alias dot/dash variants (BRK-A <-> BRK.A) to improve match rates.
    for t, cik10 in list(mapping.items()):
        if "-" in t:
            mapping.setdefault(t.replace("-", "."), cik10)
        if "." in t:
            mapping.setdefault(t.replace(".", "-"), cik10)

    return mapping


def _ticker_candidates(ticker: str) -> list[str]:
    """Generate lookup variants for a ticker symbol."""
    t = (ticker or "").strip().upper()
    out: list[str] = []
    seen: set[str] = set()

    def _add(x: str) -> None:
        x = x.strip().upper()
        if x and x not in seen:
            out.append(x)
            seen.add(x)

    _add(t)
    if "." in t:
        _add(t.replace(".", "-"))
    if "-" in t:
        _add(t.replace("-", "."))
    return out


def resolve_cik(
    ticker: str,
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
) -> str | None:
    """Resolve a ticker symbol to a zero-padded 10-digit CIK string.

    Uses the SEC ``company_tickers.json`` endpoint (cached locally).

    Parameters
    ----------
    ticker:
        Company ticker symbol (e.g. ``"AAPL"``).
    session:
        A :class:`requests.Session` with SEC-compliant headers.
    limiter:
        A :class:`RateLimiter` instance shared across the pipeline.
    cache_dir:
        Root cache directory.

    Returns
    -------
    str or None
        The 10-digit CIK (e.g. ``"0000320193"``) or ``None`` if the
        ticker is not found in the SEC map.
    """
    ticker_map = _get_company_ticker_map(session, limiter, cache_dir)
    for candidate in _ticker_candidates(ticker):
        cik10 = ticker_map.get(candidate)
        if cik10:
            return cik10
    return None


# ---------------------------------------------------------------------------
# CIK / accession formatting helpers
# ---------------------------------------------------------------------------

def _cik_no_leading_zeros(cik10: str) -> str:
    """Strip leading zeros from a CIK (for URL construction)."""
    return str(int(cik10))


def _accession_no_dashes(accession: str) -> str:
    """Remove dashes from an accession number (for URL construction)."""
    return accession.replace("-", "")


# ---------------------------------------------------------------------------
# Submissions API + pagination
# ---------------------------------------------------------------------------

def _get_submissions_json(
    cik10: str,
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
) -> dict[str, Any]:
    """Fetch the primary submissions JSON for a CIK."""
    url = SEC_SUBMISSIONS_URL_TMPL.format(cik10=cik10)
    cache_path = cache_dir / "submissions" / f"CIK{cik10}.json"
    return _http_get_json_cached(session, limiter, url, cache_path)


def _get_submissions_file_json(
    name: str,
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
) -> dict[str, Any]:
    """Fetch a paginated submissions file (e.g. ``CIK####-submissions-001.json``)."""
    url = SEC_SUBMISSIONS_FILE_URL_TMPL.format(name=name)
    cache_path = cache_dir / "submissions" / "files" / name
    return _http_get_json_cached(session, limiter, url, cache_path)


def _filings_arrays(payload: dict[str, Any]) -> dict[str, list[Any]]:
    """Extract the parallel arrays of filing metadata from a submissions payload.

    Handles both the top-level submissions JSON (nested under
    ``filings.recent``) and paginated files (arrays at top level).
    """
    if "filings" in payload and isinstance(payload.get("filings"), dict):
        recent = payload["filings"].get("recent")
        if isinstance(recent, dict) and "accessionNumber" in recent:
            return recent  # type: ignore[return-value]
    return payload  # type: ignore[return-value]


def _iter_raw_filings(
    payload: dict[str, Any],
) -> Iterator[dict[str, str]]:
    """Yield dicts of filing metadata from parallel arrays in *payload*."""
    arrays = _filings_arrays(payload)
    acc = arrays.get("accessionNumber") or []
    forms = arrays.get("form") or []
    filing_dates = arrays.get("filingDate") or []
    report_dates = arrays.get("reportDate") or []
    primary_docs = arrays.get("primaryDocument") or []

    n = min(len(acc), len(forms), len(filing_dates))
    for i in range(n):
        yield {
            "accessionNumber": str(acc[i]),
            "form": str(forms[i]),
            "filingDate": str(filing_dates[i]),
            "reportDate": str(report_dates[i]) if i < len(report_dates) else "",
            "primaryDocument": str(primary_docs[i]) if i < len(primary_docs) else "",
        }


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> _dt.date | None:
    """Parse an ISO date string (``YYYY-MM-DD``).  Returns ``None`` on failure."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return _dt.date.fromisoformat(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Form matching (amendments, numeric suffixes)
# ---------------------------------------------------------------------------

def _matches_form(
    raw_form: str,
    allowed: set[str],
    include_amendments: bool,
) -> bool:
    """Return whether *raw_form* matches one of *allowed* forms.

    Handles ``/A`` amendment suffixes and older numeric suffix variants
    like ``10-K405``.

    Parameters
    ----------
    raw_form:
        The form type string as reported by the SEC (e.g. ``"10-K"``,
        ``"10-K/A"``, ``"10-K405"``).
    allowed:
        Set of upper-cased base form types to match (e.g. ``{"10-K", "10-Q"}``).
    include_amendments:
        If ``False``, ``/A`` amendment forms are excluded.
    """
    rf = (raw_form or "").strip().upper()
    if not rf:
        return False

    is_amend = "/A" in rf
    if is_amend and not include_amendments:
        return False

    base = rf.split("/")[0]
    if base in allowed:
        return True

    # Handle older numeric suffix variants (e.g. 10-K405).
    for a in allowed:
        if base.startswith(a):
            suffix = base[len(a):]
            if suffix.isdigit():
                return True
    return False


# ---------------------------------------------------------------------------
# Collect & filter filings (with pagination)
# ---------------------------------------------------------------------------

def _collect_raw_filings(
    submissions: dict[str, Any],
    start_date: _dt.date,
    end_date: _dt.date,
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
) -> list[dict[str, str]]:
    """Collect all filing dicts from submissions JSON, paginating as needed.

    Pagination files are fetched only if the current batch of filings has
    not yet reached far enough back to cover *start_date*.
    """
    filings: list[dict[str, str]] = []
    seen: set[str] = set()

    for f in _iter_raw_filings(submissions):
        acc = f["accessionNumber"]
        if acc and acc not in seen:
            filings.append(f)
            seen.add(acc)

    def _oldest_filing_date() -> _dt.date | None:
        dates = [_parse_date(f["filingDate"]) for f in filings]
        valid = [d for d in dates if d is not None]
        return min(valid) if valid else None

    oldest = _oldest_filing_date()
    # If the oldest filing in the primary payload is already at or before
    # start_date, we have enough data -- no need to paginate.
    if oldest is not None and oldest <= start_date:
        return filings

    # Otherwise, walk through the pagination files.
    files = submissions.get("filings", {}).get("files", [])
    if not isinstance(files, list) or not files:
        return filings

    def _filing_to(entry: dict[str, Any]) -> str:
        return str(entry.get("filingTo") or "")

    for entry in sorted(files, key=_filing_to, reverse=True):
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not name:
            continue
        # If the entire page covers dates before our start window, skip it.
        to_d = _parse_date(str(entry.get("filingTo") or ""))
        if to_d is not None and to_d < start_date:
            continue

        payload = _get_submissions_file_json(
            str(name), session, limiter, cache_dir,
        )
        for f in _iter_raw_filings(payload):
            acc = f["accessionNumber"]
            if acc and acc not in seen:
                filings.append(f)
                seen.add(acc)

        oldest = _oldest_filing_date()
        if oldest is not None and oldest <= start_date:
            break

    return filings


def _filter_filings(
    raw_filings: list[dict[str, str]],
    allowed_forms: set[str],
    start_date: _dt.date,
    end_date: _dt.date,
    include_amendments: bool,
) -> list[dict[str, str]]:
    """Filter raw filing dicts by form type and filing-date range.

    Returns filings sorted by ``(filing_date, accession_number)``.
    """
    selected: list[dict[str, str]] = []
    for f in raw_filings:
        d = _parse_date(f["filingDate"])
        if d is None:
            continue
        if d < start_date or d > end_date:
            continue
        if not _matches_form(f["form"], allowed_forms, include_amendments):
            continue
        selected.append(f)

    selected.sort(key=lambda x: (x["filingDate"], x["accessionNumber"]))
    return selected


# ---------------------------------------------------------------------------
# Document URL construction
# ---------------------------------------------------------------------------

def _build_document_url(
    cik10: str,
    accession_number: str,
    primary_document: str,
    document_source: str,
) -> str:
    """Build the download URL for a filing document.

    Parameters
    ----------
    cik10:
        Zero-padded 10-digit CIK.
    accession_number:
        Accession number with dashes (e.g. ``"0000320193-24-000001"``).
    primary_document:
        The ``primaryDocument`` filename from the submissions API.
    document_source:
        Either ``"primary-html"`` (the primary HTML document) or
        ``"complete-txt"`` (the full-text submission).

    Returns
    -------
    str
        The fully qualified URL for the document.

    Raises
    ------
    ValueError
        If *document_source* is unknown, or if ``primary_document`` is empty
        when ``primary-html`` is requested.
    """
    cik0 = _cik_no_leading_zeros(cik10)
    acc_nd = _accession_no_dashes(accession_number)

    if document_source == "primary-html":
        if not primary_document:
            raise ValueError(
                "primaryDocument missing for filing; cannot fetch primary-html"
            )
        return SEC_ARCHIVES_PRIMARY_DOC_TMPL.format(
            cik_no_zeros=cik0,
            accession_no_dashes=acc_nd,
            primary_doc=os.path.basename(primary_document),
        )

    if document_source == "complete-txt":
        return SEC_ARCHIVES_COMPLETE_TXT_TMPL.format(
            cik_no_zeros=cik0,
            accession_no_dashes=acc_nd,
            accession_with_dashes=accession_number,
        )

    raise ValueError(f"Unknown document_source: {document_source!r}")


# ---------------------------------------------------------------------------
# Gzip-cached document download
# ---------------------------------------------------------------------------

def _cache_fetch(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    cache_path: Path,
    *,
    force_download: bool = False,
    timeout_s: float = 60.0,
    read_timeout_s: float = 300.0,
    max_retries: int = 5,
) -> tuple[Path, int]:
    """Download *url* to *cache_path* (gzip-compressed).

    Returns ``(cache_path, download_bytes_this_call)``.  If the file was
    already cached and *force_download* is ``False``, the second element
    is ``0``. Uses (timeout_s, read_timeout_s) so streamed reads cannot hang indefinitely.
    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists() and not force_download:
        return cache_path, 0

    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()

    last_err: Optional[BaseException] = None
    timeout_tuple = (timeout_s, read_timeout_s)
    for attempt in range(max_retries):
        try:
            limiter.wait()
            with session.get(url, stream=True, timeout=timeout_tuple) as resp:
                if resp.status_code != 200:
                    if _is_retryable_status(resp.status_code):
                        backoff = min(60.0, (2.0 ** attempt) + random.random())
                        logger.warning(
                            "[retryable %d] %s (sleep %.1fs)",
                            resp.status_code, url, backoff,
                        )
                        time.sleep(backoff)
                        continue
                    resp.raise_for_status()

                downloaded = 0
                with gzip.open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=256 * 1024):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        f.write(chunk)
                tmp.replace(cache_path)
                return cache_path, downloaded
        except requests.RequestException as exc:
            last_err = exc
            backoff = min(60.0, (2.0 ** attempt) + random.random())
            logger.warning(
                "[retryable exception] %s: %s (sleep %.1fs)", url, exc, backoff,
            )
            time.sleep(backoff)
            continue

    raise RuntimeError(
        f"Failed to download after {max_retries} attempts: {url}"
    ) from last_err


def _read_cached_html(cache_path: Path) -> str:
    """Read gzip-compressed cached content and return it as a string."""
    raw = gzip.open(cache_path, "rb").read()
    text = raw.decode("utf-8", errors="replace")
    if "\ufffd" in text:
        # Best-effort fallback when filings include legacy encodings.
        text = raw.decode("latin-1", errors="replace")
    return text


# ===================================================================
# PUBLIC API
# ===================================================================

def list_filings(
    cik: str,
    ticker: str,
    company_name: str,
    start_date: str,
    end_date: str,
    forms: list[str],
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
    *,
    include_amendments: bool = False,
) -> list[FilingMeta]:
    """Discover SEC filings for a company within a date range.

    Queries the SEC EDGAR submissions API, paginates through historical
    filing pages as needed, and returns metadata for every filing that
    matches the requested form types and date window.

    Parameters
    ----------
    cik:
        Zero-padded 10-digit CIK for the company (e.g. ``"0000320193"``).
        If empty, the function attempts to resolve it from *ticker* via
        :func:`resolve_cik`.
    ticker:
        Company ticker symbol (e.g. ``"AAPL"``).
    company_name:
        Display name for the company (carried through to returned metadata).
    start_date:
        Inclusive start of the filing-date window as an ISO string
        (``"YYYY-MM-DD"``).
    end_date:
        Inclusive end of the filing-date window as an ISO string
        (``"YYYY-MM-DD"``).
    forms:
        List of form types to include (e.g. ``["10-K", "10-Q"]``).
    session:
        A :class:`requests.Session` with SEC-compliant headers
        (see :func:`create_session`).
    limiter:
        A :class:`RateLimiter` instance.
    cache_dir:
        Root directory for cached API responses and documents.
    include_amendments:
        If ``True``, amended forms (``10-K/A``, ``10-Q/A``) are included
        when the corresponding base form is in *forms*.  Default ``False``.

    Returns
    -------
    list[FilingMeta]
        Filing metadata objects sorted by ``(filing_date, accession_number)``.
        Returns an empty list if the CIK cannot be resolved or no filings
        match.

    Raises
    ------
    ValueError
        If *start_date* or *end_date* are not valid ISO date strings, or
        if *forms* is empty.
    """
    # -- Validate inputs ------------------------------------------------
    sd = _parse_date(start_date)
    ed = _parse_date(end_date)
    if sd is None:
        raise ValueError(f"Invalid start_date: {start_date!r}")
    if ed is None:
        raise ValueError(f"Invalid end_date: {end_date!r}")
    if sd > ed:
        raise ValueError(
            f"start_date ({start_date}) is after end_date ({end_date})"
        )

    allowed_forms: set[str] = {f.strip().upper() for f in forms if f.strip()}
    if not allowed_forms:
        raise ValueError("forms list is empty after stripping whitespace")

    # -- Resolve CIK if not provided ------------------------------------
    cik10 = (cik or "").strip()
    if cik10:
        cik10 = cik10.zfill(10)
    else:
        resolved = resolve_cik(ticker, session, limiter, cache_dir)
        if resolved is None:
            logger.warning(
                "Could not resolve CIK for ticker %r; returning empty list.",
                ticker,
            )
            return []
        cik10 = resolved

    # -- Fetch submissions (with pagination) ----------------------------
    try:
        submissions = _get_submissions_json(cik10, session, limiter, cache_dir)
    except Exception:
        logger.exception("Failed to fetch submissions for CIK %s", cik10)
        return []

    raw_filings = _collect_raw_filings(
        submissions, sd, ed, session, limiter, cache_dir,
    )

    # -- Filter by form + date range ------------------------------------
    selected = _filter_filings(raw_filings, allowed_forms, sd, ed, include_amendments)

    # -- Convert to FilingMeta ------------------------------------------
    results: list[FilingMeta] = []
    for f in selected:
        results.append(
            FilingMeta(
                cik=cik10,
                ticker=ticker.strip().upper(),
                company_name=company_name,
                form=f["form"],
                filing_date=f["filingDate"],
                report_date=f["reportDate"],
                accession_number=f["accessionNumber"],
                primary_document=f["primaryDocument"],
            )
        )
    return results


def fetch_filing(
    filing_meta: FilingMeta,
    session: requests.Session,
    limiter: RateLimiter,
    cache_dir: Path,
    document_source: str = "primary-html",
    force_download: bool = False,
) -> FilingRaw:
    """Download the document for a single filing and return its content.

    The downloaded document is gzip-compressed and cached locally.
    Subsequent calls with the same filing and ``force_download=False``
    will read from cache without hitting the network.

    Parameters
    ----------
    filing_meta:
        Metadata for the filing to download (as returned by
        :func:`list_filings`).
    session:
        A :class:`requests.Session` with SEC-compliant headers.
    limiter:
        A :class:`RateLimiter` instance.
    cache_dir:
        Root directory for cached documents.
    document_source:
        Which document to download:

        - ``"primary-html"`` -- the primary HTML filing document.
        - ``"complete-txt"`` -- the full-text submission file.
    force_download:
        If ``True``, re-download even if a cached copy exists.

    Returns
    -------
    FilingRaw
        The downloaded filing content with associated metadata.

    Raises
    ------
    ValueError
        If *document_source* is unknown or the filing lacks a
        ``primary_document`` when ``"primary-html"`` is requested.
    RuntimeError
        If the download fails after all retry attempts.
    """
    url = _build_document_url(
        cik10=filing_meta.cik,
        accession_number=filing_meta.accession_number,
        primary_document=filing_meta.primary_document,
        document_source=document_source,
    )

    # Build cache path: cache_dir/filings/<cik>/<accession>/<source>/<file>.gz
    acc_nd = _accession_no_dashes(filing_meta.accession_number)
    if document_source == "primary-html":
        filename = os.path.basename(filing_meta.primary_document)
    else:
        filename = f"{filing_meta.accession_number}.txt"
    filename = os.path.basename(filename)

    cache_path = (
        cache_dir
        / "filings"
        / filing_meta.cik
        / acc_nd
        / document_source
        / f"{filename}.gz"
    )

    cached_path, _dl_bytes = _cache_fetch(
        session,
        limiter,
        url,
        cache_path,
        force_download=force_download,
    )

    html = _read_cached_html(cached_path)

    return FilingRaw(
        meta=filing_meta,
        html=html,
        cache_path=str(cached_path),
    )
