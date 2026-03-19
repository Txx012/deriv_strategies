# -*- coding: utf-8 -*-
"""期权备兑看涨策略：持有标的期货 + 卖出认购期权，继承BaseStrategy"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order

class OptionsCoveredCallStrategy(BaseStrategy):
    def __init__(self, config: Dict):
        super().__init__(config, strategy_type="options_covered_call")
        # 备兑看涨参数
        self.underlying_fut = config["underlying_fut"]  # 标的期货
        self.option_symbol = config["option_symbol"]    # 期权标的
        self.strike = config["strike"]                  # 行权价（虚值认购）
        self.delta_target = config.get("delta_target", 0.3)  # 目标Delta
        self.instrument_type = "mix"  # 期现混合
        self.call_symbol = f"{self.option_symbol}-C-{self.strike}"
        self.rollover_days = config.get("rollover_days", 3)  # 到期前N天换月

    def init(self, data: Dict[str, pd.DataFrame]) -> None:
        """初始化：加载期货+期权数据"""
        self.fut_data = data["futures"]
        self.opt_data = data["options"]
        self.logger.info(f"备兑看涨策略初始化：{self.underlying_fut} + {self.call_symbol} | Delta目标{self.delta_target}")
        # 合并数据
        self.data = pd.merge(
            self.fut_data[["datetime", "close", "delta"]].rename(columns={"close": "fut_close"}),
            self.opt_data[["datetime", "close", "delta", "days_to_expiry"]].rename(columns={"close": "opt_close"}),
            on="datetime", how="inner"
        )

    def next(self, bar: pd.Series) -> None:
        fut_pos = self.state.positions.get(self.underlying_fut, 0)
        opt_pos = self.state.positions.get(self.call_symbol, 0)
        current_dte = bar["days_to_expiry"]
        current_delta = bar["delta"]

        # 核心规则：持有期货 + 卖出虚值认购，Delta中性
        # 1. 持有标的期货（无期货仓位则开仓）
        if fut_pos == 0:
            self.send_order(Order(
                symbol=self.underlying_fut,
                instrument_type="futures",
                direction="BUY",
                price=bar["fut_close"],
                volume=self.config["fut_lots"],
                order_type="MARKET"
            ))
        # 2. 卖出虚值认购期权（无期权仓位/Delta偏离时开仓）
        if opt_pos >= 0 and abs(current_delta - self.delta_target) > 0.1:
            self.send_order(Order(
                symbol=self.call_symbol,
                instrument_type="options",
                direction="SELL",
                price=bar["opt_close"],
                volume=self.config["opt_lots"],
                order_type="MARKET"
            ))
        # 3. 期权到期前换月平仓
        if current_dte <= self.rollover_days and opt_pos < 0:
            self.send_order(Order(
                symbol=self.call_symbol,
                instrument_type="options",
                direction="BUY",
                price=bar["opt_close"],
                volume=abs(opt_pos),
                order_type="MARKET"
            ))
        # 4. 期货价格大涨，平仓锁定利润
        if bar["fut_close"] > self.strike * 1.05 and fut_pos > 0:
            self.send_order(Order(
                symbol=self.underlying_fut,
                instrument_type="futures",
                direction="SELL",
                price=bar["fut_close"],
                volume=abs(fut_pos),
                order_type="MARKET"
            ))