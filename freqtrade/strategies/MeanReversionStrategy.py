"""
MeanReversionStrategy — Bollinger Band / Z-score mean reversion.

This strategy buys deep pullbacks (price below lower Bollinger Band with
a highly negative z-score) and exits when price reverts to the mean.
It complements MomentumStrategy by performing well in ranging/sideways markets
where momentum strategies tend to chop.

Parameters are driven by strategy_config.yaml (mean_reversion block).
Timeframe: 1h
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


def load_strategy_config() -> dict:
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("mean_reversion", {})
    except Exception as e:
        logger.warning(f"Could not load strategy config: {e}. Using defaults.")
        return {}


class MeanReversionStrategy(IStrategy):
    """
    Bollinger Band + Z-score mean reversion strategy.
    Enters when price is deeply oversold relative to recent range;
    exits when price reverts to the mean.
    """

    INTERFACE_VERSION = 3

    # Conservative ROI — mean reversion targets the midline, not large rallies
    minimal_roi = {
        "0": 0.06,    # 6% take profit
        "120": 0.03,  # 3% after 2 hours
        "480": 0.01,  # 1% after 8 hours
    }

    # Tighter stop than momentum — mean reversion can accelerate against you
    stoploss = -0.08
    trailing_stop = False  # Fixed stop for mean reversion; we exit at midline

    timeframe = "1h"
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    # Need enough candles to compute 50-period z-score + 20-period BB
    startup_candle_count: int = 60

    # Hyperopt parameters
    buy_zscore = DecimalParameter(-3.0, -1.5, default=-2.0, decimals=1, space="buy")
    sell_zscore = DecimalParameter(-0.5, 0.5, default=0.0, decimals=1, space="sell")
    bb_std = DecimalParameter(1.5, 2.5, default=2.0, decimals=1, space="buy")

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._strategy_cfg = load_strategy_config()

        self._redis = None
        if REDIS_AVAILABLE:
            try:
                self._redis = redis.from_url("redis://redis:6379", decode_responses=True)
                logger.info("MeanReversionStrategy connected to Redis")
            except Exception as e:
                logger.warning(f"Redis unavailable: {e}. Signals will not be published.")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        cfg = self._strategy_cfg.get("indicators", {})
        bb_period = cfg.get("bollinger", {}).get("period", 20)
        bb_std_dev = cfg.get("bollinger", {}).get("std_dev", self.bb_std.value)
        zscore_lookback = cfg.get("zscore", {}).get("lookback", 50)

        # Bollinger Bands on typical price
        bollinger = qtpylib.bollinger_bands(
            qtpylib.typical_price(dataframe), window=bb_period, stds=bb_std_dev
        )
        dataframe["bb_lower"] = bollinger["lower"]
        dataframe["bb_mid"] = bollinger["mid"]
        dataframe["bb_upper"] = bollinger["upper"]
        dataframe["bb_width"] = (
            dataframe["bb_upper"] - dataframe["bb_lower"]
        ) / dataframe["bb_mid"]

        # Bollinger %B — how far price is within/outside the bands
        # %B = (price - lower) / (upper - lower); <0 = below lower band
        dataframe["bb_pct_b"] = (
            (dataframe["close"] - dataframe["bb_lower"])
            / (dataframe["bb_upper"] - dataframe["bb_lower"])
        )

        # Z-score of close price over lookback window
        rolling_mean = dataframe["close"].rolling(window=zscore_lookback).mean()
        rolling_std = dataframe["close"].rolling(window=zscore_lookback).std()
        dataframe["zscore"] = (dataframe["close"] - rolling_mean) / rolling_std

        # RSI for additional confirmation
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)

        # Volume MA
        dataframe["volume_ma"] = ta.SMA(dataframe["volume"], timeperiod=20)

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        cfg = self._strategy_cfg.get("indicators", {})
        zscore_entry = cfg.get("zscore", {}).get(
            "entry_threshold", self.buy_zscore.value
        )

        dataframe.loc[
            (
                # Price has fallen below the lower Bollinger Band
                (dataframe["close"] < dataframe["bb_lower"])
                # Z-score confirms deep oversold (typically < -2.0)
                & (dataframe["zscore"] < zscore_entry)
                # RSI not already in freefall (avoid catching a knife)
                & (dataframe["rsi"] > 20)
                # Reasonable volume (not a ghost candle)
                & (dataframe["volume"] > dataframe["volume_ma"] * 0.5)
            ),
            "enter_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "entry")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        cfg = self._strategy_cfg.get("indicators", {})
        zscore_exit = cfg.get("zscore", {}).get(
            "exit_threshold", self.sell_zscore.value
        )

        dataframe.loc[
            (
                # Price has reverted to or above the midline (mean)
                (dataframe["close"] >= dataframe["bb_mid"])
                # Z-score confirms reversion to mean
                | (dataframe["zscore"] >= zscore_exit)
                # Or price is now overbought (extended reversal)
                | (dataframe["rsi"] > 72)
            ),
            "exit_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "exit")
        return dataframe

    def _publish_signal(self, dataframe: DataFrame, pair: str, signal_type: str):
        """Publish latest signal to Redis for Claude analyst and DeFi executor."""
        if self._redis is None or dataframe.empty:
            return

        try:
            last = dataframe.iloc[-1]
            signal = {
                "pair": pair,
                "signal_type": signal_type,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "mean_reversion_strategy",
                "indicators": {
                    "rsi": round(float(last.get("rsi", 0)), 2),
                    "zscore": round(float(last.get("zscore", 0)), 4),
                    "bb_pct_b": round(float(last.get("bb_pct_b", 0)), 4),
                    "bb_lower": round(float(last.get("bb_lower", 0)), 4),
                    "bb_mid": round(float(last.get("bb_mid", 0)), 4),
                    "bb_upper": round(float(last.get("bb_upper", 0)), 4),
                    "bb_width": round(float(last.get("bb_width", 0)), 4),
                },
                "close_price": round(float(last["close"]), 4),
                "enter_long": int(last.get("enter_long", 0)),
                "exit_long": int(last.get("exit_long", 0)),
            }

            key = f"signals:mean_reversion:{pair.replace('/', '_')}"
            self._redis.setex(key, 86400, json.dumps(signal))
            logger.debug(f"Published mean reversion signal: {key}")

        except Exception as e:
            logger.warning(f"Failed to publish signal to Redis: {e}")
