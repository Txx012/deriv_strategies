# -*- coding: utf-8 -*-
"""期货跨期套利策略：近远月合约价差套利，继承BaseStrategy"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order
from data.data_loader_test import DataLoader

class FuturesArbitrageStrategy(BaseStrategy):
    def __init__(self, config: Dict):
        super().__init__(config, strategy_type="futures_arbitrage")
        # 套利参数
        self.near_symbol = config["near_symbol"]  # 近月合约
        self.far_symbol = config["far_symbol"]    # 远月合约
        self.spread_threshold = config["spread_threshold"]  # 价差阈值
        self.instrument_type = "futures"
        self.near_data: pd.DataFrame = pd.DataFrame()
        self.far_data: pd.DataFrame = pd.DataFrame()

    def init(self, data: Dict[str, pd.DataFrame]) -> None:
        """初始化：加载近远月数据并计算价差"""
        self.near_data = data["near"]
        self.far_data = data["far"]
        self.logger.info(f"跨期套利初始化：{self.near_symbol}/{self.far_symbol} | 价差阈值{self.spread_threshold}")
        # 合并数据计算价差
        self.data = pd.merge(
            self.near_data[["datetime", "close"]].rename(columns={"close": "near_close"}),
            self.far_data[["datetime", "close"]].rename(columns={"close": "far_close"}),
            on="datetime", how="inner"
        )
        self.data["spread"] = self.data["near_close"] - self.data["far_close"]
        self.data["spread_mean"] = self.data["spread"].rolling(window=20).mean()

    def next(self, bar: pd.Series) -> None:
        current_spread = bar["spread"]
        spread_mean = bar["spread_mean"]
        spread_diff = current_spread - spread_mean

        # 价差过大：卖近月，买远月
        if spread_diff > self.spread_threshold:
            # 卖近月
            self.send_order(Order(
                symbol=self.near_symbol,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=bar["near_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
            # 买远月
            self.send_order(Order(
                symbol=self.far_symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=bar["far_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
        # 价差过小：买近月，卖远月
        elif spread_diff < -self.spread_threshold:
            self.send_order(Order(
                symbol=self.near_symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=bar["near_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
            self.send_order(Order(
                symbol=self.far_symbol,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=bar["far_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
        # 价差回归，平仓
        elif abs(spread_diff) < self.spread_threshold / 2:
            # 平近月
            near_pos = self.state.positions.get(self.near_symbol, 0)
            if near_pos != 0:
                self.send_order(Order(
                    symbol=self.near_symbol,
                    instrument_type=self.instrument_type,
                    direction="BUY" if near_pos < 0 else "SELL",
                    price=bar["near_close"],
                    volume=abs(near_pos),
                    order_type="MARKET"
                ))
            # 平远月
            far_pos = self.state.positions.get(self.far_symbol, 0)
            if far_pos != 0:
                self.send_order(Order(
                    symbol=self.far_symbol,
                    instrument_type=self.instrument_type,
                    direction="BUY" if far_pos < 0 else "SELL",
                    price=bar["far_close"],
                    volume=abs(far_pos),
                    order_type="MARKET"
                ))