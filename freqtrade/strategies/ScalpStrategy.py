"""
ScalpStrategy — High-frequency 5m scalping strategy.

Entry logic: Bollinger Band mean reversion confirmed by RSI + Stochastic RSI.
Looks for oversold bounces off the lower band with volume confirmation.

Pairs: High-volatility alts (SOL, ADA, LTC, DOGE, LINK, POL, SAND)
Timeframe: 5 minutes
Target: 0.5–1% per trade, tight 2% hard stop

All parameters configurable in config/strategy_config.yaml under
strategies.scalp section.
"""

import yaml
import json
import logging

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None  # type: ignore

from datetime import datetime
from pathlib import Path
from freqtrade.strategy import IStrategy, DecimalParameter, IntParameter
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("/app/config/strategy_config.yaml")


def load_scalp_config() -> dict:
    """Load scalp params from YAML. Falls back to defaults if unavailable."""
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("scalp", {})
    except Exception as e:
        logger.warning(f"Could not load scalp config: {e}. Using defaults.")
    return {}


class ScalpStrategy(IStrategy):
    """
    5-minute Bollinger Band mean-reversion scalp strategy.

    Entry: Price touches or pierces lower BB + RSI oversold + Stoch-RSI recovering
    Exit:  Price reaches BB midline (primary) or RSI overbought (secondary)
    Stop:  2% hard stop, 1% trailing once in profit
    """

    INTERFACE_VERSION = 3

    # Tight ROI — scalps exit quickly
    minimal_roi = {
        "0":  0.010,   # 1.0% immediate target
        "5":  0.007,   # 0.7% after 5 minutes
        "15": 0.005,   # 0.5% after 15 minutes
        "45": 0.003,   # 0.3% after 45 minutes — get out
    }

    stoploss = -0.02           # 2% hard stop
    trailing_stop = True
    trailing_stop_positive = 0.01   # 1% trailing once profitable
    trailing_stop_positive_offset = 0.015
    trailing_only_offset_is_reached = True

    timeframe = "5m"
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    # Need more history for Bollinger + Stoch warmup
    startup_candle_count: int = 40

    # Hyperopt-tunable parameters
    buy_rsi_threshold  = IntParameter(20, 40, default=32, space="buy")
    sell_rsi_threshold = IntParameter(60, 80, default=68, space="sell")
    bb_std             = DecimalParameter(1.5, 2.5, default=2.0, decimals=1, space="buy")

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._scalp_cfg = load_scalp_config()

        # Redis for signal publishing
        self._redis = None
        if REDIS_AVAILABLE:
            try:
                self._redis = redis.from_url("redis://redis:6379", decode_responses=True)
                logger.info("ScalpStrategy connected to Redis")
            except Exception as e:
                logger.warning(f"Redis unavailable for ScalpStrategy: {e}")
        else:
            logger.warning("redis package not installed. Scalp signal publishing disabled.")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """Calculate Bollinger Bands, RSI, Stochastic RSI, volume indicators."""

        cfg = self._scalp_cfg.get("indicators", {})
        bb_period  = cfg.get("bb", {}).get("period", 20)
        rsi_period = cfg.get("rsi", {}).get("period", 14)
        stoch_k    = cfg.get("stoch", {}).get("fastk_period", 14)
        stoch_d    = cfg.get("stoch", {}).get("fastd_period", 3)

        # ── Bollinger Bands ────────────────────────────────────────
        bollinger = qtpylib.bollinger_bands(
            qtpylib.typical_price(dataframe),
            window=bb_period,
            stds=float(self.bb_std.value),
        )
        dataframe["bb_lower"]  = bollinger["lower"]
        dataframe["bb_mid"]    = bollinger["mid"]
        dataframe["bb_upper"]  = bollinger["upper"]
        dataframe["bb_width"]  = (bollinger["upper"] - bollinger["lower"]) / bollinger["mid"]
        dataframe["bb_pct"]    = (dataframe["close"] - bollinger["lower"]) / (bollinger["upper"] - bollinger["lower"])

        # ── RSI ───────────────────────────────────────────────────
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=rsi_period)

        # ── Stochastic Oscillator ─────────────────────────────────
        stoch = ta.STOCH(
            dataframe,
            fastk_period=stoch_k,
            slowk_period=stoch_d,
            slowk_matype=0,
            slowd_period=stoch_d,
            slowd_matype=0,
        )
        dataframe["slowk"] = stoch["slowk"]
        dataframe["slowd"] = stoch["slowd"]

        # ── EMA trend filter ──────────────────────────────────────
        dataframe["ema_21"]  = ta.EMA(dataframe, timeperiod=21)
        dataframe["ema_50"]  = ta.EMA(dataframe, timeperiod=50)

        # ── Volume filter ─────────────────────────────────────────
        dataframe["volume_ma"] = ta.SMA(dataframe["volume"], timeperiod=20)
        dataframe["volume_surge"] = dataframe["volume"] > (dataframe["volume_ma"] * 1.2)

        # ── Candle patterns ───────────────────────────────────────
        # Bullish engulfing / hammer for extra confluence
        dataframe["hammer"] = (
            (dataframe["close"] > dataframe["open"])                              # green candle
            & ((dataframe["open"] - dataframe["low"]) > 2 * dataframe["close"].diff().abs())  # long lower wick
        ).astype(int)

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Buy when:
          1. Price at or below lower Bollinger Band (oversold stretch)
          2. RSI below buy threshold (momentum exhausted)
          3. Stochastic K < 20 but turning up (reversal confirmation)
          4. Volume above average (real move, not drift)
          5. EMA 21 > EMA 50 OR price near major support (not fighting a strong downtrend)
        """
        rsi_buy = self._scalp_cfg.get("indicators", {}).get("rsi", {}).get(
            "buy_threshold", self.buy_rsi_threshold.value
        )

        dataframe.loc[
            (
                (dataframe["close"] <= dataframe["bb_lower"] * 1.005)  # At/below lower BB (5bp buffer)
                & (dataframe["rsi"] < rsi_buy)                          # RSI oversold
                & (dataframe["slowk"] < 25)                             # Stoch oversold
                & (dataframe["slowk"] > dataframe["slowd"])             # Stoch K crossing above D (turning up)
                & (dataframe["volume_surge"])                           # Volume confirmation
                & (dataframe["close"] > dataframe["ema_50"] * 0.97)    # Not in a deep downtrend (3% buffer)
            ),
            "enter_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "entry")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Sell when:
          1. Price reaches BB midline (mean reversion target hit), OR
          2. RSI overbought (momentum exhausted on upside), OR
          3. Stochastic K crosses below D above 75 (reversal at top)
        """
        rsi_sell = self._scalp_cfg.get("indicators", {}).get("rsi", {}).get(
            "sell_threshold", self.sell_rsi_threshold.value
        )

        dataframe.loc[
            (
                (dataframe["close"] >= dataframe["bb_mid"])             # Mean reversion achieved
                | (dataframe["rsi"] > rsi_sell)                         # Overbought
                | (
                    (dataframe["slowk"] > 75)                           # Stoch high
                    & (dataframe["slowk"] < dataframe["slowd"])         # Turning down
                )
            ),
            "exit_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "exit")
        return dataframe

    def _publish_signal(self, dataframe: DataFrame, pair: str, signal_type: str):
        """Publish latest scalp signal to Redis."""
        if self._redis is None or dataframe.empty:
            return
        try:
            last = dataframe.iloc[-1]
            signal = {
                "pair": pair,
                "signal_type": signal_type,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "scalp_strategy",
                "indicators": {
                    "rsi":      round(float(last.get("rsi", 0)), 2),
                    "slowk":    round(float(last.get("slowk", 0)), 2),
                    "slowd":    round(float(last.get("slowd", 0)), 2),
                    "bb_pct":   round(float(last.get("bb_pct", 0)), 4),
                    "bb_width": round(float(last.get("bb_width", 0)), 4),
                },
                "close_price": round(float(last["close"]), 6),
                "enter_long":  int(last.get("enter_long", 0)),
                "exit_long":   int(last.get("exit_long", 0)),
            }
            key = f"signals:scalp:{pair.replace('/', '_')}"
            self._redis.setex(key, 3600, json.dumps(signal))  # 1h TTL for 5m signals
            logger.debug(f"Published scalp signal: {key}")
        except Exception as e:
            logger.warning(f"Failed to publish scalp signal: {e}")
