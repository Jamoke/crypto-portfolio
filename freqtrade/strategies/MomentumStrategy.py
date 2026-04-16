"""
MomentumStrategy — Trend-following with pullback entry.

Design goals:
  - Only trade in confirmed uptrends (price above EMA 200)
  - Enter on pullbacks, not at the top of moves
  - Minimum 10% profit target; trailing stop lets winners run further
  - 3.33% hard stop → 3:1 risk/reward vs 10% target
  - ADX filter keeps strategy out of choppy, sideways markets

Risk/Reward:
  - Hard stop:          -3.33%
  - Minimum exit:       +10.0% (trailing activates here)
  - Trail from peak:     5.0%  (after 10% reached)
  - Example: price hits +20%, trail exits at +15%

Pairs (config.json): BTC/USDC, ETH/USDC, ARB/USDC, MATIC/USDC, OP/USDC, AAVE/USDC, UNI/USDC
Timeframe: 4h
"""

import json
import logging

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None  # type: ignore

from datetime import datetime
from freqtrade.strategy import IStrategy, IntParameter
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

logger = logging.getLogger(__name__)


class MomentumStrategy(IStrategy):

    INTERFACE_VERSION = 3

    # ── Risk / Reward ─────────────────────────────────────────────────────────
    # Hard stop at 3.33% → 3:1 ratio vs 10% minimum target.
    # 50% ROI is a safety net only — the trailing stop handles real exits.
    minimal_roi = {"0": 0.50}

    stoploss = -0.033                        # 3.33% hard stop
    trailing_stop = True
    trailing_stop_positive = 0.05            # 5% trail from peak
    trailing_stop_positive_offset = 0.10     # Trail activates once +10% is reached
    trailing_only_offset_is_reached = True   # Hard stop applies until +10%

    # ── Timeframe ─────────────────────────────────────────────────────────────
    timeframe = "4h"
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = True      # Never exit at a loss via signal — let stoploss handle it
    ignore_roi_if_entry_signal = False

    startup_candle_count: int = 210  # Need 200 for EMA200 + warmup

    # ── Hyperopt parameters ───────────────────────────────────────────────────
    buy_rsi_low  = IntParameter(35, 50, default=42, space="buy")
    buy_rsi_high = IntParameter(50, 62, default=58, space="buy")
    adx_threshold = IntParameter(20, 35, default=25, space="buy")

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._redis = None
        if REDIS_AVAILABLE:
            try:
                self._redis = redis.from_url("redis://redis:6379", decode_responses=True)
            except Exception as e:
                logger.warning(f"Redis unavailable: {e}")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:

        # ── Trend structure ───────────────────────────────────────────────────
        dataframe["ema_50"]  = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema_200"] = ta.EMA(dataframe, timeperiod=200)

        # ── Momentum ──────────────────────────────────────────────────────────
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)

        macd = ta.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)
        dataframe["macd"]       = macd["macd"]
        dataframe["macdsignal"] = macd["macdsignal"]
        dataframe["macdhist"]   = macd["macdhist"]

        # ── Trend strength ────────────────────────────────────────────────────
        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)

        # ── Volume ────────────────────────────────────────────────────────────
        dataframe["volume_ma"] = ta.SMA(dataframe["volume"], timeperiod=20)

        # ── Pullback quality ──────────────────────────────────────────────────
        # How far price is from EMA50 — negative means below (pullback into support)
        dataframe["dist_ema50_pct"] = (
            (dataframe["close"] - dataframe["ema_50"]) / dataframe["ema_50"]
        )

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Buy when all of:
          1. Price is above EMA200          — macro uptrend confirmed
          2. EMA50 is above EMA200          — medium-term trend bullish
          3. ADX > threshold                — market is trending, not choppy
          4. RSI in pullback zone (42–58)   — pulled back but not breaking down
          5. MACD histogram turning positive — momentum resuming after pullback
          6. Price within 8% of EMA50       — not chasing, entering near support
          7. Volume above average           — real buying, not drift
        """
        dataframe.loc[
            (
                (dataframe["close"] > dataframe["ema_200"])                     # Macro uptrend
                & (dataframe["ema_50"] > dataframe["ema_200"])                  # Medium uptrend
                & (dataframe["adx"] > self.adx_threshold.value)                # Trending market
                & (dataframe["rsi"] > self.buy_rsi_low.value)                  # RSI not oversold
                & (dataframe["rsi"] < self.buy_rsi_high.value)                 # RSI not overbought
                & (dataframe["macdhist"] > 0)                                   # MACD histogram positive
                & (dataframe["macdhist"] > dataframe["macdhist"].shift(1))      # MACD histogram growing
                & (dataframe["dist_ema50_pct"] > -0.08)                        # Within 8% of EMA50
                & (dataframe["dist_ema50_pct"] < 0.12)                         # Not too extended above EMA50
                & (dataframe["volume"] > dataframe["volume_ma"] * 0.8)         # Decent volume
            ),
            "enter_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "entry")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Signal-based exit (supplements trailing stop):
          - RSI above 75: significantly overbought
          - Price crosses below EMA50: trend structure broken
          - MACD crosses below signal: momentum reversing

        Note: exit_profit_only=True means these signals only fire when in profit.
        The trailing stop handles the actual exit in most cases.
        """
        dataframe.loc[
            (
                (dataframe["rsi"] > 75)                                          # Overbought
                | (qtpylib.crossed_below(dataframe["close"], dataframe["ema_50"]))  # Trend break
                | (
                    (dataframe["macd"] < dataframe["macdsignal"])               # MACD bearish cross
                    & (dataframe["rsi"] > 60)                                    # Only when extended
                )
            ),
            "exit_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "exit")
        return dataframe

    def _publish_signal(self, dataframe: DataFrame, pair: str, signal_type: str):
        if self._redis is None or dataframe.empty:
            return
        try:
            last = dataframe.iloc[-1]
            signal = {
                "pair":        pair,
                "signal_type": signal_type,
                "timestamp":   datetime.utcnow().isoformat(),
                "source":      "momentum_strategy",
                "indicators": {
                    "rsi":            round(float(last.get("rsi", 0)), 2),
                    "adx":            round(float(last.get("adx", 0)), 2),
                    "macd":           round(float(last.get("macd", 0)), 6),
                    "macdhist":       round(float(last.get("macdhist", 0)), 6),
                    "ema_50":         round(float(last.get("ema_50", 0)), 4),
                    "ema_200":        round(float(last.get("ema_200", 0)), 4),
                    "dist_ema50_pct": round(float(last.get("dist_ema50_pct", 0)), 4),
                },
                "close_price": round(float(last["close"]), 4),
                "enter_long":  int(last.get("enter_long", 0)),
                "exit_long":   int(last.get("exit_long", 0)),
            }
            key = f"signals:momentum:{pair.replace('/', '_')}"
            self._redis.setex(key, 86400, json.dumps(signal))
        except Exception as e:
            logger.warning(f"Failed to publish signal: {e}")
