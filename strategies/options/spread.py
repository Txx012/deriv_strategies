# -*- coding: utf-8 -*-
"""期权垂直价差策略：买入低行权价认购 + 卖出高行权价认购，继承BaseStrategy"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order

class OptionsSpreadStrategy(BaseStrategy):
    def __init__(self, config: Dict):
        super().__init__(config, strategy_type="options_spread")
        # 价差策略参数
        self.option_symbol = config["option_symbol"]
        self.strike_low = config["strike_low"]  # 低行权价
        self.strike_high = config["strike_high"]# 高行权价
        self.expiry = config["expiry"]
        self.instrument_type = "options"
        self.call_low = f"{self.option_symbol}-C-{self.strike_low}"
        self.call_high = f"{self.option_symbol}-C-{self.strike_high}"
        self.spread_profit_limit = config["spread_profit_limit"]
        self.spread_loss_limit = config["spread_loss_limit"]

    def init(self, data: pd.DataFrame) -> None:
        self.data = data
        self.logger.info(f"期权垂直价差初始化：{self.call_low}/{self.call_high} | 价差{self.strike_high-self.strike_low}")
        self.data["spread_price"] = self.data[f"{self.call_low}_close"] - self.data[f"{self.call_high}_close"]

    def next(self, bar: pd.Series) -> None:
        spread_price = bar["spread_price"]
        current_pos = self.state.positions.get(self.call_low, 0) + self.state.positions.get(self.call_high, 0)

        # 开仓：买入低行权价，卖出高行权价
        if current_pos == 0:
            self.send_order(Order(
                symbol=self.call_low,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=bar[f"{self.call_low}_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
            self.send_order(Order(
                symbol=self.call_high,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=bar[f"{self.call_high}_close"],
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
        # 止盈/止损平仓
        else:
            spread_pnl = (spread_price - self.data["spread_price"].iloc[0]) * self.config["contract"]["multiplier"]
            if spread_pnl >= self.spread_profit_limit:
                self._close_all(bar)
                self.logger.info(f"价差策略止盈：{spread_pnl:.2f} ≥ {self.spread_profit_limit:.2f}")
            elif spread_pnl <= -self.spread_loss_limit:
                self._close_all(bar)
                self.logger.info(f"价差策略止损：{spread_pnl:.2f} ≤ -{self.spread_loss_limit:.2f}")

    def _close_all(self, bar: pd.Series):
        """平仓所有头寸"""
        # 平低行权价认购
        call_low_pos = self.state.positions.get(self.call_low, 0)
        if call_low_pos != 0:
            self.send_order(Order(
                symbol=self.call_low,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=bar[f"{self.call_low}_close"],
                volume=abs(call_low_pos),
                order_type="MARKET"
            ))
        # 平高行权价认购
        call_high_pos = self.state.positions.get(self.call_high, 0)
        if call_high_pos != 0:
            self.send_order(Order(
                symbol=self.call_high,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=bar[f"{self.call_high}_close"],
                volume=abs(call_high_pos),
                order_type="MARKET"
            ))