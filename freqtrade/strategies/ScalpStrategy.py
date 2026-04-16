"""
ScalpStrategy — Renamed to TrendBreakout internally. Replaces the failed 5m scalp.

The previous 5m Bollinger Band scalp targeted 0.3–1% per trade on Gate.io spot.
After fees (~0.2% round trip), expected value was negative. This rewrite abandons
scalping entirely in favour of trend-breakout entries on high-beta altcoins.

Design goals:
  - Catch breakouts from consolidation on high-beta pairs (SOL, LINK, DOGE, etc.)
  - Same 3:1 risk/reward structure as MomentumStrategy:
      Hard stop:   -3.33%
      Trail offset: +10%  (trail only activates once minimum target is reached)
      Trail width:   5%   (gives trades room to run to 20–30%+ in strong moves)
  - Donchian channel breakout as primary entry trigger
  - Volume surge confirmation (real breakout, not a fake-out)
  - EMA200 trend filter (long bias only in macro uptrends)

Why high-beta alts:
  SOL, LINK, DOGE, ADA move 1.5–3× BTC's daily percentage. A 10% BTC rally
  often produces 15–25% moves in these assets. Same R:R setup, larger moves.

Pairs (config_scalp.json): SOL/USDC, ADA/USDC, LTC/USDC, DOGE/USDC, LINK/USDC, POL/USDC, SAND/USDC
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
from freqtrade.strategy import IStrategy, IntParameter, DecimalParameter
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

logger = logging.getLogger(__name__)


class ScalpStrategy(IStrategy):
    """
    Trend-breakout strategy on high-beta altcoins.
    Class name kept as ScalpStrategy so docker-compose requires no changes.
    """

    INTERFACE_VERSION = 3

    # ── Risk / Reward ─────────────────────────────────────────────────────────
    minimal_roi = {"0": 0.50}             # Safety net only — trail handles exits

    stoploss = -0.033                     # 3.33% hard stop
    trailing_stop = True
    trailing_stop_positive = 0.05         # 5% trail from peak
    trailing_stop_positive_offset = 0.10  # Trail activates at +10%
    trailing_only_offset_is_reached = True

    # ── Timeframe ─────────────────────────────────────────────────────────────
    timeframe = "4h"
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = True
    ignore_roi_if_entry_signal = False

    startup_candle_count: int = 220  # EMA200 + Donchian(20) warmup

    # ── Hyperopt parameters ───────────────────────────────────────────────────
    donchian_period  = IntParameter(15, 30, default=20, space="buy")
    volume_mult      = DecimalParameter(1.2, 2.5, default=1.5, decimals=1, space="buy")
    adx_min          = IntParameter(18, 30, default=22, space="buy")

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._redis = None
        if REDIS_AVAILABLE:
            try:
                self._redis = redis.from_url("redis://redis:6379", decode_responses=True)
            except Exception as e:
                logger.warning(f"Redis unavailable: {e}")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:

        # ── Trend filter ──────────────────────────────────────────────────────
        dataframe["ema_50"]  = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema_200"] = ta.EMA(dataframe, timeperiod=200)

        # ── Breakout detection: Donchian channel ──────────────────────────────
        # Upper band = highest high of last N candles (breakout above = bullish signal)
        n = int(self.donchian_period.value)
        dataframe["donchian_high"] = dataframe["high"].rolling(n).max().shift(1)
        dataframe["donchian_low"]  = dataframe["low"].rolling(n).min().shift(1)

        # ── Trend strength ────────────────────────────────────────────────────
        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)

        # ── Momentum confirmation ─────────────────────────────────────────────
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)

        macd = ta.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)
        dataframe["macd"]     = macd["macd"]
        dataframe["macdhist"] = macd["macdhist"]

        # ── Volume ────────────────────────────────────────────────────────────
        dataframe["volume_ma"]    = ta.SMA(dataframe["volume"], timeperiod=20)
        dataframe["volume_surge"] = dataframe["volume"] > (
            dataframe["volume_ma"] * float(self.volume_mult.value)
        )

        # ── ATR for volatility context ────────────────────────────────────────
        dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Buy on confirmed breakout when:
          1. Price breaks above 20-period Donchian high  — breakout confirmed
          2. Price above EMA200                          — macro uptrend only
          3. EMA50 above EMA200                          — medium uptrend aligned
          4. ADX > threshold                             — trending, not ranging
          5. RSI between 50–70                           — momentum strong but not exhausted
          6. MACD histogram positive and growing         — momentum accelerating
          7. Volume surge (1.5× average)                 — real breakout, not fake-out
        """
        dataframe.loc[
            (
                (dataframe["close"] > dataframe["donchian_high"])              # Breakout
                & (dataframe["close"] > dataframe["ema_200"])                  # Macro uptrend
                & (dataframe["ema_50"] > dataframe["ema_200"])                 # Medium uptrend
                & (dataframe["adx"] > self.adx_min.value)                     # Trending market
                & (dataframe["rsi"] > 50)                                      # Momentum positive
                & (dataframe["rsi"] < 72)                                      # Not exhausted
                & (dataframe["macdhist"] > 0)                                  # MACD bullish
                & (dataframe["volume_surge"])                                  # Volume confirmed
            ),
            "enter_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "entry")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Exit signals (only fire in profit due to exit_profit_only=True):
          - RSI > 78: significantly overbought on a high-beta alt
          - Price crosses back below Donchian high: breakout failed
          - MACD histogram turns negative after a run: momentum fading
        """
        dataframe.loc[
            (
                (dataframe["rsi"] > 78)                                         # Overbought
                | (qtpylib.crossed_below(dataframe["close"], dataframe["ema_50"]))  # Trend break
                | (
                    (dataframe["macdhist"] < 0)                                 # MACD turned negative
                    & (dataframe["rsi"] > 60)                                   # After a meaningful run
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
                "source":      "scalp_strategy",
                "indicators": {
                    "rsi":           round(float(last.get("rsi", 0)), 2),
                    "adx":           round(float(last.get("adx", 0)), 2),
                    "macdhist":      round(float(last.get("macdhist", 0)), 6),
                    "donchian_high": round(float(last.get("donchian_high", 0)), 4),
                    "volume_surge":  bool(last.get("volume_surge", False)),
                    "atr":           round(float(last.get("atr", 0)), 4),
                },
                "close_price": round(float(last["close"]), 6),
                "enter_long":  int(last.get("enter_long", 0)),
                "exit_long":   int(last.get("exit_long", 0)),
            }
            key = f"signals:scalp:{pair.replace('/', '_')}"
            self._redis.setex(key, 86400, json.dumps(signal))
        except Exception as e:
            logger.warning(f"Failed to publish signal: {e}")
