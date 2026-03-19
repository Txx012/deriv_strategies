# -*- coding: utf-8 -*-
"""期货趋势策略：MA5/MA20均线交叉，继承BaseStrategy，标准化实现"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order

class FuturesTrendStrategy(BaseStrategy):
    def __init__(self, config: Dict):
        super().__init__(config, strategy_type="futures_trend")
        # 配置参数
        self.symbol = config["symbol"]
        self.instrument_type = "futures"
        self.ma_short = config.get("ma_short", 5)
        self.ma_long = config.get("ma_long", 20)
        self.contract_multiplier = config["contract"]["multiplier"]
        self.margin_rate = config["contract"]["margin_rate"]

    def init(self, data: pd.DataFrame) -> None:
        self.data = data
        self.logger.info(f"期货趋势策略初始化：MA{self.ma_short}/{self.ma_long}交叉 | 标的{self.symbol}")
        # 验证均线指标
        if f"ma{self.ma_short}" not in data.columns or f"ma{self.ma_long}" not in data.columns:
            raise ValueError(f"缺失均线指标：ma{self.ma_short}/ma{self.ma_long}")

    def next(self, bar: pd.Series) -> None:
        current_ma_short = bar[f"ma{self.ma_short}"]
        current_ma_long = bar[f"ma{self.ma_long}"]
        current_pos = self.state.positions.get(self.symbol, 0)
        current_close = bar["close"]

        # 金叉：短期均线上穿长期均线，做多
        if current_ma_short > current_ma_long and current_pos <= 0:
            order = Order(
                symbol=self.symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=current_close,
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            )
            self.send_order(order)
        # 死叉：短期均线下穿长期均线，做空
        elif current_ma_short < current_ma_long and current_pos >= 0:
            order = Order(
                symbol=self.symbol,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=current_close,
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            )
            self.send_order(order)