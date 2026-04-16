"""
RecoveryMomentumStrategy — Trend-following for recovering and ranging markets.

Where MomentumStrategy requires a confirmed uptrend (price above EMA200 with a
golden cross), this strategy targets the earlier recovery phase:
  - Price returning toward EMA200 from below (up to 8% tolerance below)
  - EMA50 turning upward even while still below EMA200
  - RSI recovering into the 38–62 range (wider than Momentum's 42–58)
  - MACD histogram building momentum (improving, not necessarily positive yet)

This complements MomentumStrategy: Momentum runs in confirmed bulls, Recovery
catches the lead-up phase and ranging periods where Momentum goes flat.

Risk/Reward (identical structure to MomentumStrategy):
  Hard stop:          -3.33%
  Minimum exit:       +10.0%  (trailing activates here)
  Trail from peak:      5.0%  (after 10% reached)
  Example: price hits +18%, trail exits at +13%

Key differences vs MomentumStrategy:
  - EMA200: within 8% below allowed (vs strictly above)
  - EMA cross: replaced with EMA50 slope (rising, not golden cross)
  - RSI band: 38–62 (vs 42–58)
  - ADX threshold: 20 (vs 25)
  - MACD: histogram just needs to be rising (vs positive AND rising)
  - Price/EMA50 gap: -5% to +12% (vs -8% to +12%)
  - Volume: 70% of average (vs 80%)

Pairs (config_recovery.json): BTC/USDC, ETH/USDC, ARB/USDC, OP/USDC, UNI/USDC
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


class RecoveryMomentumStrategy(IStrategy):

    INTERFACE_VERSION = 3

    # ── Risk / Reward ─────────────────────────────────────────────────────────
    # Same structure as MomentumStrategy: 50% ROI is a safety net only.
    # The trailing stop handles real exits.
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
    exit_profit_only = True      # Let stoploss handle losses; signals only exit profits
    ignore_roi_if_entry_signal = False

    startup_candle_count: int = 210  # EMA200 + warmup

    # ── Hyperopt parameters ───────────────────────────────────────────────────
    buy_rsi_low    = IntParameter(30, 45, default=38, space="buy")
    buy_rsi_high   = IntParameter(52, 68, default=62, space="buy")
    adx_threshold  = IntParameter(15, 28, default=20, space="buy")
    ema200_pct_min = IntParameter(-12, 0,  default=-8, space="buy")  # % below EMA200 allowed

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

        # EMA50 slope: positive means EMA50 is rising (recovery underway).
        # Using 6-candle lookback (1 day on 4h) to smooth noise.
        dataframe["ema50_slope"] = (
            dataframe["ema_50"] - dataframe["ema_50"].shift(6)
        ) / dataframe["ema_50"].shift(6)

        # Distance from EMA200 as a percentage — negative means price is below.
        dataframe["ema200_gap"] = (
            (dataframe["close"] - dataframe["ema_200"]) / dataframe["ema_200"]
        )

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
        dataframe["dist_ema50_pct"] = (
            (dataframe["close"] - dataframe["ema_50"]) / dataframe["ema_50"]
        )

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Enter when recovery conditions align:
          1. Price within 8% below EMA200 (or above)  — approaching resistance/support
          2. EMA50 slope positive                      — medium trend turning upward
          3. ADX > threshold                           — some directional movement
          4. RSI in 38–62                              — recovering, not overbought
          5. MACD histogram rising                     — building momentum (can still be negative)
          6. Price within 5% below EMA50 or 12% above — entering near support, not chasing
          7. Volume above 70% of average              — real participation
        """
        ema200_pct = self.ema200_pct_min.value / 100.0

        dataframe.loc[
            (
                (dataframe["ema200_gap"] > ema200_pct)                            # Near or above EMA200
                & (dataframe["ema50_slope"] > 0)                                  # EMA50 rising
                & (dataframe["adx"] > self.adx_threshold.value)                   # Some trend
                & (dataframe["rsi"] > self.buy_rsi_low.value)                     # RSI recovering
                & (dataframe["rsi"] < self.buy_rsi_high.value)                    # RSI not extended
                & (dataframe["macdhist"] > dataframe["macdhist"].shift(1))        # MACD building
                & (dataframe["dist_ema50_pct"] > -0.05)                           # Within 5% below EMA50
                & (dataframe["dist_ema50_pct"] < 0.12)                            # Not overextended above EMA50
                & (dataframe["volume"] > dataframe["volume_ma"] * 0.7)            # Volume present
            ),
            "enter_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "entry")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Signal-based exit (supplements trailing stop, only fires in profit):
          - RSI above 72: significantly overbought
          - Price crosses below EMA50: recovery structure broken
          - MACD histogram turns negative after run: momentum fading

        Note: exit_profit_only=True means these signals only fire when in profit.
        """
        dataframe.loc[
            (
                (dataframe["rsi"] > 72)                                           # Overbought
                | (qtpylib.crossed_below(dataframe["close"], dataframe["ema_50"]))  # Trend break
                | (
                    (dataframe["macd"] < dataframe["macdsignal"])                 # MACD bearish cross
                    & (dataframe["rsi"] > 58)                                     # Only after a meaningful run
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
                "source":      "recovery_momentum_strategy",
                "indicators": {
                    "rsi":            round(float(last.get("rsi", 0)), 2),
                    "adx":            round(float(last.get("adx", 0)), 2),
                    "macd":           round(float(last.get("macd", 0)), 6),
                    "macdhist":       round(float(last.get("macdhist", 0)), 6),
                    "ema_50":         round(float(last.get("ema_50", 0)), 4),
                    "ema_200":        round(float(last.get("ema_200", 0)), 4),
                    "ema200_gap":     round(float(last.get("ema200_gap", 0)), 4),
                    "ema50_slope":    round(float(last.get("ema50_slope", 0)), 6),
                    "dist_ema50_pct": round(float(last.get("dist_ema50_pct", 0)), 4),
                },
                "close_price": round(float(last["close"]), 4),
                "enter_long":  int(last.get("enter_long", 0)),
                "exit_long":   int(last.get("exit_long", 0)),
            }
            key = f"signals:recovery:{pair.replace('/', '_')}"
            self._redis.setex(key, 86400, json.dumps(signal))
        except Exception as e:
            logger.warning(f"Failed to publish signal: {e}")
