"""
FreqAIStrategy — Machine learning-enhanced trading strategy.
Uses Freqtrade's built-in FreqAI framework with a LightGBM classifier.

Feature set:
  - RSI(14), EMA(20), EMA(50)
  - MACD histogram
  - Volume z-score (50-period)
  - Bollinger %B

Target:
  - &-s_close: 1-period forward return (LightGBM classification)
  - Entry when prediction > 0.6 (bullish confidence)
  - Exit when prediction < 0.4

Training:
  - 30-day rolling training window
  - 7-day backtest period
  - 24h live retrain interval
  - Trained on 1h and 4h timeframes

To backtest:
  docker compose run --rm freqtrade_freqai backtesting \\
    --config /freqtrade/user_data/config_freqai.json \\
    --strategy FreqAIStrategy \\
    --timerange 20240101-20250101 \\
    --freqai-backtest-live-models

Compare results against MomentumStrategy baseline.
"""

import logging
from freqtrade.strategy import IFreqaiStrategy, IntParameter, DecimalParameter
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

logger = logging.getLogger(__name__)


class FreqAIStrategy(IFreqaiStrategy):
    """
    FreqAI LightGBM strategy. Entry/exit is driven by the model's
    prediction score, not hardcoded indicator thresholds.
    """

    # Required by IFreqaiStrategy
    INTERFACE_VERSION = 3

    minimal_roi = {
        "0": 0.10,
        "60": 0.05,
        "240": 0.03,
    }

    stoploss       = -0.10
    trailing_stop  = True
    trailing_stop_positive        = 0.02
    trailing_stop_positive_offset = 0.05
    trailing_only_offset_is_reached = True

    timeframe = "1h"
    process_only_new_candles = True
    use_exit_signal            = True
    exit_profit_only           = False
    ignore_roi_if_entry_signal = False

    # FreqAI requires a minimum number of candles for feature calculation
    startup_candle_count: int = 60

    # Entry/exit thresholds (tunable via hyperopt)
    entry_threshold  = DecimalParameter(0.5, 0.8, default=0.6, decimals=2, space="buy")
    exit_threshold   = DecimalParameter(0.2, 0.5, default=0.4, decimals=2, space="sell")

    def feature_engineering_expand_all(
        self, dataframe: DataFrame, period: int, metadata: dict, **kwargs
    ) -> DataFrame:
        """
        Add features that will be calculated for all requested timeframes
        and shifted candle periods. Called by FreqAI automatically.
        """
        # RSI
        dataframe["%-rsi"] = ta.RSI(dataframe, timeperiod=14)

        # EMA signals
        dataframe["%-ema_20"]      = ta.EMA(dataframe, timeperiod=20)
        dataframe["%-ema_50"]      = ta.EMA(dataframe, timeperiod=50)
        dataframe["%-ema_ratio"]   = (
            dataframe["%-ema_20"] / dataframe["%-ema_50"].replace(0, 1)
        )

        # MACD histogram
        macd = ta.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)
        dataframe["%-macd_hist"] = macd["macdhist"]

        # Bollinger %B
        bollinger = qtpylib.bollinger_bands(
            qtpylib.typical_price(dataframe), window=20, stds=2
        )
        bb_range = (bollinger["upper"] - bollinger["lower"]).replace(0, 1)
        dataframe["%-bb_pct_b"] = (
            (dataframe["close"] - bollinger["lower"]) / bb_range
        )

        # Volume z-score
        vol_mean = dataframe["volume"].rolling(50).mean()
        vol_std  = dataframe["volume"].rolling(50).std().replace(0, 1)
        dataframe["%-volume_zscore"] = (dataframe["volume"] - vol_mean) / vol_std

        # Price momentum (close vs close N periods ago)
        dataframe["%-momentum_5"]  = dataframe["close"].pct_change(5)
        dataframe["%-momentum_14"] = dataframe["close"].pct_change(14)

        return dataframe

    def feature_engineering_expand_basic(
        self, dataframe: DataFrame, metadata: dict, **kwargs
    ) -> DataFrame:
        """
        Add basic features that are calculated once (not expanded across timeframes).
        """
        # Day of week and hour (for intra-week seasonality)
        dataframe["%-day_of_week"] = dataframe["date"].dt.dayofweek
        dataframe["%-hour_of_day"] = dataframe["date"].dt.hour
        return dataframe

    def feature_engineering_standard(
        self, dataframe: DataFrame, metadata: dict, **kwargs
    ) -> DataFrame:
        """
        Add the prediction target label. FreqAI uses this to train.
        &-s_close: future 1-period return (regression) or direction (classification)
        """
        # FreqAI's convention: prefix "&-" for targets
        # For LightGBMClassifier, we want 1 (price up) or 0 (price down/flat)
        future_close = dataframe["close"].shift(-1)
        dataframe["&-s_close"] = (future_close > dataframe["close"]).astype(int)
        return dataframe

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        FreqAI populates indicators via the feature_engineering methods.
        This method runs FreqAI prediction and stores the result.
        """
        dataframe = self.freqai.start(dataframe, metadata, self)
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Enter long when the model predicts price will go up with sufficient confidence.
        """
        # FreqAI stores the prediction in the "&-s_close" column after training
        # and prediction probability in "&-s_close_mean" (for classifiers)
        prediction_col = "&-s_close_mean"
        if prediction_col not in dataframe.columns:
            # Fallback: use raw prediction
            prediction_col = "&-s_close"

        dataframe.loc[
            dataframe[prediction_col] > self.entry_threshold.value,
            "enter_long",
        ] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Exit when model confidence in upward movement drops below threshold.
        """
        prediction_col = "&-s_close_mean"
        if prediction_col not in dataframe.columns:
            prediction_col = "&-s_close"

        dataframe.loc[
            dataframe[prediction_col] < self.exit_threshold.value,
            "exit_long",
        ] = 1
        return dataframe
