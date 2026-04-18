"""
Position sizing math — pure functions, no side effects, no network I/O.
Split out from polymarket.py so unit tests can import without pulling
in the anthropic/redis/prometheus dependency graph.
"""
from __future__ import annotations


def kelly_size(p_win: float, market_price: float,
               bankroll: float, max_fraction: float = 0.25) -> float:
    """
    Fractional-Kelly position sizing.

    f* = (p*b - q) / b    where b = (1/price - 1), q = 1 - p_win

    Returns the USD position size, capped at max_fraction * bankroll.
    Negative-EV trades return 0.0 (caller should skip).

    Invariants:
      - kelly_size(p_win=p, market_price=p, ...) == 0  (no edge)
      - kelly_size(p_win<=0 or p>=1, ...) == 0
      - kelly_size(market_price<=0 or >=1, ...) == 0
      - output <= max_fraction * bankroll
    """
    if market_price <= 0 or market_price >= 1 or bankroll <= 0:
        return 0.0
    if p_win <= 0 or p_win >= 1:
        return 0.0
    b = (1.0 / market_price) - 1.0
    q = 1.0 - p_win
    f_star = (p_win * b - q) / b
    if f_star <= 0:
        return 0.0
    f_capped = min(f_star, max_fraction)
    return round(bankroll * f_capped, 2)
