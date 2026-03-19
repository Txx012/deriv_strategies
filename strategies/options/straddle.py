# -*- coding: utf-8 -*-
"""期权跨式策略：同时买入相同行权价的认购+认沽期权，赌波动率上涨，继承BaseStrategy"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order

class OptionsStraddleStrategy(BaseStrategy):
    def __init__(self, config: Dict):
        super().__init__(config, strategy_type="options_straddle")
        # 跨式策略参数
        self.underlying_symbol = config["underlying_symbol"]  # 标的期货/股票
        self.option_symbol = config["option_symbol"]          # 期权标的
        self.strike = config["strike"]                        # 行权价
        self.expiry = config["expiry"]                        # 到期日
        self.instrument_type = "options"
        self.call_symbol = f"{self.option_symbol}-C-{self.strike}"
        self.put_symbol = f"{self.option_symbol}-P-{self.strike}"
        self.days_to_expiry_limit = config.get("days_to_expiry_limit", 7)

    def init(self, data: pd.DataFrame) -> None:
        self.data = data
        self.logger.info(f"期权跨式策略初始化：{self.call_symbol}/{self.put_symbol} | 行权价{self.strike}")
        # 验证期权字段
        required_fields = ["days_to_expiry", "volatility", "close"]
        for field in required_fields:
            if field not in data.columns:
                raise ValueError(f"缺失期权字段：{field}")

    def next(self, bar: pd.Series) -> None:
        current_vol = bar["volatility"]
        current_dte = bar["days_to_expiry"]
        current_pos = self.state.positions.get(self.call_symbol, 0) + self.state.positions.get(self.put_symbol, 0)
        current_close = bar["close"]

        # 波动率偏低+剩余到期日充足，开仓（买入认购+认沽）
        if current_vol < self.config["vol_threshold"] and current_dte > self.days_to_expiry_limit and current_pos == 0:
            # 买入认购
            self.send_order(Order(
                symbol=self.call_symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=current_close,
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
            # 买入认沽
            self.send_order(Order(
                symbol=self.put_symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=current_close,
                volume=self.config["min_order_volume"],
                order_type="MARKET"
            ))
        # 波动率回归/到期日不足，平仓
        elif (current_vol > self.config["vol_threshold"] * 1.5) or (current_dte <= self.days_to_expiry_limit):
            # 平认购
            call_pos = self.state.positions.get(self.call_symbol, 0)
            if call_pos != 0:
                self.send_order(Order(
                    symbol=self.call_symbol,
                    instrument_type=self.instrument_type,
                    direction="SELL",
                    price=current_close,
                    volume=abs(call_pos),
                    order_type="MARKET"
                ))
            # 平认沽
            put_pos = self.state.positions.get(self.put_symbol, 0)
            if put_pos != 0:
                self.send_order(Order(
                    symbol=self.put_symbol,
                    instrument_type=self.instrument_type,
                    direction="SELL",
                    price=current_close,
                    volume=abs(put_pos),
                    order_type="MARKET"
                ))