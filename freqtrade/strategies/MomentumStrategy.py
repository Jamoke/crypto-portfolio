"""
MomentumStrategy — Phase 1 baseline strategy for signal generation.

This strategy generates BUY/SELL signals based on:
  - RSI (Relative Strength Index) for momentum
  - EMA crossover (12/26) for trend direction
  - MACD for trend confirmation

Running in DRY-RUN mode. Signals are also published to Redis so
the Claude Analyst and DeFi Executor can consume them.

All parameters are read from strategy_config.yaml at startup.
"""

import yaml
import redis
import json
import logging
from datetime import datetime
from pathlib import Path
from freqtrade.strategy import IStrategy, DecimalParameter, IntParameter
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("/app/config/strategy_config.yaml")


def load_strategy_config() -> dict:
    """Load momentum params from YAML. Falls back to defaults if unavailable."""
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("momentum", {})
    except Exception as e:
        logger.warning(f"Could not load strategy config: {e}. Using defaults.")
        return {}


class MomentumStrategy(IStrategy):
    """
    Momentum-based strategy using RSI + EMA crossover + MACD.
    Parameters are driven by strategy_config.yaml.
    """

    INTERFACE_VERSION = 3

    # Minimal ROI — let stop losses and signals handle exits
    minimal_roi = {
        "0": 0.10,    # 10% take profit
        "60": 0.05,   # 5% after 60 minutes
        "240": 0.03,  # 3% after 4 hours
    }

    # Trailing stop — overridden by risk_config.yaml in production
    stoploss = -0.10
    trailing_stop = True
    trailing_stop_positive = 0.02
    trailing_stop_positive_offset = 0.05
    trailing_only_offset_is_reached = True

    timeframe = "4h"
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    startup_candle_count: int = 30

    # Hyperopt parameters (can be tuned with freqtrade hyperopt)
    buy_rsi = IntParameter(25, 45, default=35, space="buy")
    sell_rsi = IntParameter(60, 80, default=68, space="sell")

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._strategy_cfg = load_strategy_config()
        self._timeframe = self._strategy_cfg.get("timeframe", "4h")

        # Redis connection for publishing signals
        self._redis = None
        try:
            self._redis = redis.from_url("redis://redis:6379", decode_responses=True)
            logger.info("Connected to Redis for signal publishing")
        except Exception as e:
            logger.warning(f"Redis unavailable: {e}. Signals will not be published.")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """Calculate all technical indicators."""

        cfg = self._strategy_cfg.get("indicators", {})
        rsi_period = cfg.get("rsi", {}).get("period", 14)
        ema_fast = cfg.get("ema", {}).get("fast_period", 12)
        ema_slow = cfg.get("ema", {}).get("slow_period", 26)
        macd_fast = cfg.get("macd", {}).get("fast", 12)
        macd_slow = cfg.get("macd", {}).get("slow", 26)
        macd_signal = cfg.get("macd", {}).get("signal", 9)
        bb_period = 20

        # RSI
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=rsi_period)

        # EMA crossover
        dataframe["ema_fast"] = ta.EMA(dataframe, timeperiod=ema_fast)
        dataframe["ema_slow"] = ta.EMA(dataframe, timeperiod=ema_slow)
        dataframe["ema_cross"] = qtpylib.crossed_above(
            dataframe["ema_fast"], dataframe["ema_slow"]
        )

        # MACD
        macd = ta.MACD(
            dataframe,
            fastperiod=macd_fast,
            slowperiod=macd_slow,
            signalperiod=macd_signal,
        )
        dataframe["macd"] = macd["macd"]
        dataframe["macdsignal"] = macd["macdsignal"]
        dataframe["macdhist"] = macd["macdhist"]

        # Bollinger Bands (for additional context)
        bollinger = qtpylib.bollinger_bands(
            qtpylib.typical_price(dataframe), window=bb_period, stds=2
        )
        dataframe["bb_lower"] = bollinger["lower"]
        dataframe["bb_mid"] = bollinger["mid"]
        dataframe["bb_upper"] = bollinger["upper"]
        dataframe["bb_width"] = (
            dataframe["bb_upper"] - dataframe["bb_lower"]
        ) / dataframe["bb_mid"]

        # Volume MA for confirmation
        dataframe["volume_ma"] = ta.SMA(dataframe["volume"], timeperiod=20)

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """Define BUY signal conditions."""

        cfg = self._strategy_cfg.get("indicators", {})
        rsi_buy = self._strategy_cfg.get("indicators", {}).get("rsi", {}).get(
            "buy_threshold", self.buy_rsi.value
        )

        dataframe.loc[
            (
                (dataframe["rsi"] > rsi_buy)           # RSI recovering from oversold
                & (dataframe["rsi"] < 60)               # Not already overbought
                & (dataframe["ema_fast"] > dataframe["ema_slow"])  # Bullish EMA
                & (dataframe["macd"] > dataframe["macdsignal"])    # MACD bullish
                & (dataframe["volume"] > dataframe["volume_ma"])   # Above avg volume
                & (dataframe["close"] > dataframe["bb_mid"])       # Above BB midline
            ),
            "enter_long",
        ] = 1

        # Publish signal to Redis for other services
        self._publish_signal(dataframe, metadata["pair"], "entry")

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """Define SELL signal conditions."""

        cfg = self._strategy_cfg.get("indicators", {})
        rsi_sell = self._strategy_cfg.get("indicators", {}).get("rsi", {}).get(
            "sell_threshold", self.sell_rsi.value
        )

        dataframe.loc[
            (
                (dataframe["rsi"] > rsi_sell)          # Overbought
                | (dataframe["macd"] < dataframe["macdsignal"])  # MACD bearish cross
            ),
            "exit_long",
        ] = 1

        self._publish_signal(dataframe, metadata["pair"], "exit")

        return dataframe

    def _publish_signal(self, dataframe: DataFrame, pair: str, signal_type: str):
        """Publish latest signal to Redis for DeFi executor and Claude analyst."""
        if self._redis is None or dataframe.empty:
            return

        try:
            last = dataframe.iloc[-1]
            signal = {
                "pair": pair,
                "signal_type": signal_type,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "momentum_strategy",
                "indicators": {
                    "rsi": round(float(last.get("rsi", 0)), 2),
                    "macd": round(float(last.get("macd", 0)), 6),
                    "macdsignal": round(float(last.get("macdsignal", 0)), 6),
                    "ema_fast": round(float(last.get("ema_fast", 0)), 4),
                    "ema_slow": round(float(last.get("ema_slow", 0)), 4),
                    "bb_width": round(float(last.get("bb_width", 0)), 4),
                },
                "close_price": round(float(last["close"]), 4),
                "enter_long": int(last.get("enter_long", 0)),
                "exit_long": int(last.get("exit_long", 0)),
            }

            key = f"signals:momentum:{pair.replace('/', '_')}"
            self._redis.setex(key, 86400, json.dumps(signal))  # Expire in 24h
            logger.debug(f"Published signal to Redis: {key}")

        except Exception as e:
            logger.warning(f"Failed to publish signal to Redis: {e}")
