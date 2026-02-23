"""Unit tests for the retrieval module (edgar_tracker.retrieval).

Tests the RateLimiter, CIK resolution (with mocked HTTP), and ticker
candidate generation.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from edgar_tracker.retrieval import (
    RateLimiter,
    resolve_cik,
    _ticker_candidates,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ticker_map() -> dict[str, str]:
    """A small mock ticker-to-CIK mapping."""
    return {
        "AAPL": "0000320193",
        "JPM": "0000019617",
        "MSFT": "0000789019",
        "BRK-A": "0001067983",
        "BRK.A": "0001067983",
        "BRK-B": "0001067983",
        "BRK.B": "0001067983",
        "GOOG": "0001652044",
        "GOOGL": "0001652044",
    }


@pytest.fixture()
def mock_session() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def mock_limiter() -> RateLimiter:
    return RateLimiter(max_rps=100.0)


# ---------------------------------------------------------------------------
# Helper to call resolve_cik with a mocked ticker map
# ---------------------------------------------------------------------------


def _resolve_with_map(
    ticker: str,
    ticker_map: dict[str, str],
    session: MagicMock | None = None,
    limiter: RateLimiter | None = None,
    tmp_path: Path | None = None,
) -> str | None:
    """Call resolve_cik while mocking _get_company_ticker_map to return *ticker_map*."""
    sess = session or MagicMock()
    lim = limiter or RateLimiter(max_rps=100.0)
    cache = tmp_path or Path("/tmp/test_cache")
    with patch(
        "edgar_tracker.retrieval._get_company_ticker_map",
        return_value=ticker_map,
    ):
        return resolve_cik(ticker, sess, lim, cache)


# ---------------------------------------------------------------------------
# Tests for RateLimiter
# ---------------------------------------------------------------------------


class TestRateLimiterSpacing:
    """test_rate_limiter_spacing: verify RateLimiter enforces minimum
    interval between calls."""

    def test_minimum_interval_enforced(self) -> None:
        """Two rapid calls should take at least the minimum interval."""
        max_rps = 5.0  # 200ms minimum interval
        limiter = RateLimiter(max_rps)
        min_interval = 1.0 / max_rps

        start = time.monotonic()
        limiter.wait()
        limiter.wait()
        elapsed = time.monotonic() - start

        # The second call should have waited at least ~min_interval.
        # Use a small tolerance for timing jitter.
        assert elapsed >= min_interval * 0.8, (
            f"Expected at least {min_interval * 0.8:.3f}s, got {elapsed:.3f}s"
        )

    def test_three_calls_spacing(self) -> None:
        """Three rapid calls should take at least 2x the minimum interval."""
        max_rps = 10.0  # 100ms minimum interval
        limiter = RateLimiter(max_rps)
        min_interval = 1.0 / max_rps

        start = time.monotonic()
        limiter.wait()
        limiter.wait()
        limiter.wait()
        elapsed = time.monotonic() - start

        # At least 2 intervals for 3 calls.
        assert elapsed >= min_interval * 1.5, (
            f"Expected at least {min_interval * 1.5:.3f}s, got {elapsed:.3f}s"
        )

    def test_invalid_max_rps(self) -> None:
        """max_rps <= 0 should raise ValueError."""
        with pytest.raises(ValueError):
            RateLimiter(0)
        with pytest.raises(ValueError):
            RateLimiter(-1.0)

    def test_single_call_is_fast(self) -> None:
        """A single call to wait() should return essentially immediately."""
        limiter = RateLimiter(5.0)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        # First call should not sleep.
        assert elapsed < 0.1


# ---------------------------------------------------------------------------
# Tests for resolve_cik
# ---------------------------------------------------------------------------


class TestResolveCikFound:
    """test_resolve_cik_found: mock ticker map, verify CIK returned."""

    def test_direct_match(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("AAPL", ticker_map) == "0000320193"

    def test_case_insensitive(self, ticker_map: dict[str, str]) -> None:
        # _ticker_candidates uppercases the input
        assert _resolve_with_map("aapl", ticker_map) == "0000320193"

    def test_jpm_found(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("JPM", ticker_map) == "0000019617"

    def test_msft_found(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("MSFT", ticker_map) == "0000789019"


class TestResolveCikNotFound:
    """test_resolve_cik_not_found: missing ticker returns None."""

    def test_unknown_ticker(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("ZZZZZ", ticker_map) is None

    def test_empty_ticker(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("", ticker_map) is None

    def test_whitespace_only(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("   ", ticker_map) is None


class TestResolveCikVariant:
    """test_resolve_cik_variant: BRK-A matches BRK.A variant and vice versa."""

    def test_dash_to_dot_variant(self, ticker_map: dict[str, str]) -> None:
        # Looking up BRK-A should work (direct key).
        assert _resolve_with_map("BRK-A", ticker_map) == "0001067983"

    def test_dot_to_dash_variant(self, ticker_map: dict[str, str]) -> None:
        # Looking up BRK.A should also work (direct key or variant).
        assert _resolve_with_map("BRK.A", ticker_map) == "0001067983"

    def test_brk_b_variant(self, ticker_map: dict[str, str]) -> None:
        assert _resolve_with_map("BRK-B", ticker_map) == "0001067983"
        assert _resolve_with_map("BRK.B", ticker_map) == "0001067983"


# ---------------------------------------------------------------------------
# Tests for _ticker_candidates helper
# ---------------------------------------------------------------------------


class TestTickerCandidates:
    """Verify the _ticker_candidates function generates the right variants."""

    def test_simple_ticker(self) -> None:
        candidates = _ticker_candidates("AAPL")
        assert "AAPL" in candidates

    def test_dash_ticker_generates_dot(self) -> None:
        candidates = _ticker_candidates("BRK-A")
        assert "BRK-A" in candidates
        assert "BRK.A" in candidates

    def test_dot_ticker_generates_dash(self) -> None:
        candidates = _ticker_candidates("BRK.A")
        assert "BRK.A" in candidates
        assert "BRK-A" in candidates

    def test_uppercase_normalization(self) -> None:
        candidates = _ticker_candidates("brk-a")
        assert all(c == c.upper() for c in candidates)

    def test_empty_input(self) -> None:
        candidates = _ticker_candidates("")
        assert candidates == []

    def test_no_duplicates(self) -> None:
        candidates = _ticker_candidates("BRK-A")
        assert len(candidates) == len(set(candidates))
