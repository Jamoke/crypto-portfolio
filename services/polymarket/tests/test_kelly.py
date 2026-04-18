"""
Unit tests for fractional-Kelly position sizing.

These tests import from `sizing` (not `polymarket`) so the test run does not
pull in anthropic / redis / prometheus_client. Run from the service directory:

    cd crypto-portfolio/services/polymarket
    python -m pytest tests/test_kelly.py -v
"""
from __future__ import annotations

import os
import sys

# Allow running tests from either the service dir or the repo root.
HERE = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR = os.path.dirname(HERE)
if SERVICE_DIR not in sys.path:
    sys.path.insert(0, SERVICE_DIR)

import pytest

from sizing import kelly_size


class TestNoEdge:
    def test_probability_equals_price_returns_zero(self):
        # No edge: Claude agrees with the market.
        assert kelly_size(p_win=0.5, market_price=0.5, bankroll=1000) == 0.0
        assert kelly_size(p_win=0.7, market_price=0.7, bankroll=1000) == 0.0

    def test_negative_ev_returns_zero(self):
        # p_win < market_price means we're paying more than our estimate.
        assert kelly_size(p_win=0.40, market_price=0.60, bankroll=1000) == 0.0
        assert kelly_size(p_win=0.20, market_price=0.80, bankroll=1000) == 0.0


class TestBoundaryConditions:
    @pytest.mark.parametrize("p", [-0.1, 0.0, 1.0, 1.1])
    def test_invalid_probability_returns_zero(self, p):
        assert kelly_size(p_win=p, market_price=0.5, bankroll=1000) == 0.0

    @pytest.mark.parametrize("price", [-0.1, 0.0, 1.0, 1.1])
    def test_invalid_market_price_returns_zero(self, price):
        assert kelly_size(p_win=0.6, market_price=price, bankroll=1000) == 0.0

    @pytest.mark.parametrize("bankroll", [0, -100])
    def test_non_positive_bankroll_returns_zero(self, bankroll):
        assert kelly_size(p_win=0.6, market_price=0.5, bankroll=bankroll) == 0.0


class TestFormula:
    def test_even_odds_with_edge(self):
        # p=0.6, price=0.5 -> b=1, q=0.4, f* = (0.6 - 0.4)/1 = 0.2
        # Below quarter-Kelly cap (0.25), so full f* applies.
        result = kelly_size(p_win=0.6, market_price=0.5, bankroll=1000)
        assert result == pytest.approx(200.0, abs=0.01)

    def test_uncapped_matches_raw_kelly(self):
        # With max_fraction=1.0, the raw Kelly fraction should apply.
        # p=0.82, price=0.65 -> b=0.53846, q=0.18, f* ≈ 0.4857
        result = kelly_size(p_win=0.82, market_price=0.65, bankroll=800, max_fraction=1.0)
        assert result == pytest.approx(800 * 0.4857, abs=1.0)


class TestQuarterKellyCap:
    def test_cap_activates_when_raw_exceeds_25pct(self):
        # p=0.9, price=0.5 -> b=1, q=0.1, f* = 0.8 (way over 25%)
        # With default 0.25 cap, size should be 250, not 800.
        result = kelly_size(p_win=0.9, market_price=0.5, bankroll=1000)
        assert result == pytest.approx(250.0, abs=0.01)

    def test_custom_cap(self):
        # Same f*=0.8, but with a 10% cap.
        result = kelly_size(p_win=0.9, market_price=0.5, bankroll=1000, max_fraction=0.10)
        assert result == pytest.approx(100.0, abs=0.01)

    def test_below_cap_is_not_raised(self):
        # f* = 0.2 should pass through unchanged at a 0.25 cap.
        result = kelly_size(p_win=0.6, market_price=0.5, bankroll=1000, max_fraction=0.25)
        assert result == pytest.approx(200.0, abs=0.01)


class TestOutputShape:
    def test_result_is_rounded_to_cents(self):
        # Any output should have at most 2 decimal places (USD cents).
        result = kelly_size(p_win=0.6001, market_price=0.5, bankroll=1000)
        # round(x, 2) is what the impl does; confirm no extra precision leaks.
        assert result == round(result, 2)

    def test_result_never_exceeds_bankroll_fraction(self):
        # Sweep a grid; every output must be <= max_fraction * bankroll.
        bankroll = 500.0
        cap = 0.25
        for p in [0.55, 0.7, 0.85, 0.95, 0.99]:
            for price in [0.05, 0.2, 0.4, 0.6, 0.8, 0.95]:
                size = kelly_size(p, price, bankroll, max_fraction=cap)
                assert size <= bankroll * cap + 1e-6, (
                    f"p={p} price={price} size={size} exceeded cap {bankroll * cap}"
                )
