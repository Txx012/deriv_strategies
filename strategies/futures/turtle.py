# -*- coding: utf-8 -*-
"""海龟交易策略：经典趋势跟踪策略，期货专属，继承BaseStrategy，参数全外置"""
from typing import Dict
import pandas as pd
import numpy as np
from strategies.base_strategy import BaseStrategy, Order
from utils.log_utils import logger

class TurtleStrategy(BaseStrategy):
    """
    经典海龟交易策略实现：
    核心规则：N值（真实波动率）→ 20日高低点突破入场 → 2N止损/4N止盈 → 4单位仓位管理 → 1%风险控制
    所有参数从turtle_config.yaml读取，无硬编码
    """
    def __init__(self, config: Dict):
        # 调用基类初始化，策略类型标识为turtle
        super().__init__(config, strategy_type="turtle")
        # 从配置读取核心参数（无硬编码）
        self.n_period = config["n_period"]
        self.entry_period = config["entry_period"]
        self.stop_loss_multi = config["stop_loss_multi"]
        self.take_profit_multi = config["take_profit_multi"]
        self.max_pos_units = config["max_pos_units"]
        self.risk_ratio = config["risk_ratio"]
        self.single_pos_limit = config["single_pos_limit"]
        # 海龟专属缓存变量
        self.symbol = config["symbol"]
        self.instrument_type = "futures"
        self.contract_multiplier = config["contract"]["multiplier"]
        self.margin_rate = config["contract"]["margin_rate"]
        # 动态变量（逐K线更新）
        self.current_n = 0.0
        self.stop_loss_price = 0.0
        self.take_profit_price = 0.0
        self.pos_units = 0  # 当前持仓单位数（海龟专属）

    def init(self, data: pd.DataFrame) -> None:
        """策略初始化：加载数据，验证指标"""
        self.data = data
        self.logger.info(f"海龟策略初始化：标的{self.symbol} | N周期{self.n_period} | 入场周期{self.entry_period}")
        # 验证核心指标是否存在（由data_cleaner计算）
        required_indicators = ["n_value", "high_20", "low_20", "close"]
        for ind in required_indicators:
            if ind not in self.data.columns:
                self.logger.error(f"海龟策略缺失核心指标：{ind}，请先计算技术指标")
                raise ValueError(f"Missing Turtle indicator: {ind}")
        self.logger.info("海龟策略初始化完成，数据共{}条".format(len(self.data)))

    def next(self, bar: pd.Series) -> None:
        """逐K线执行海龟核心逻辑：入场→止损止盈→仓位管理"""
        # 更新当前N值（海龟核心）
        self.current_n = bar["n_value"]
        if np.isnan(self.current_n):
            return  # 指标未计算完成，跳过

        # 核心变量
        current_close = bar["close"]
        current_high = bar["high_20"]
        current_low = bar["low_20"]
        current_pos = self.state.positions.get(self.symbol, 0)
        account_value = self.state.account_value or self.config["backtest"]["initial_capital"]

        # 1. 计算海龟仓位（按风险比例和N值）
        pos_volume = self._calculate_turtle_volume(account_value)
        if pos_volume <= 0:
            self.logger.warning(f"海龟仓位计算为0，跳过入场：N={self.current_n:.2f}，账户{account_value:.2f}")
            return

        # 2. 海龟入场规则：突破20日高点做多，突破20日低点做空
        self._entry_rule(bar, current_high, current_low, current_pos, pos_volume)

        # 3. 海龟止损止盈规则：2N止损，4N止盈（经典规则）
        self._stop_take_profit_rule(bar, current_close, current_pos)

        # 4. 海龟仓位限制：最大4单位，单一标的仓位上限
        self._position_limit(account_value)

    def _calculate_turtle_volume(self, account_value: float) -> int:
        """
        海龟专属仓位计算：按账户资金、N值、风险比例计算单单位手数
        公式：手数 = (账户净资产 × 风险比例) / (N值 × 合约乘数)
        """
        # 每单位风险金额
        risk_per_unit = account_value * self.risk_ratio
        # 每手风险金额（N值 × 合约乘数 × 止损倍数）
        risk_per_lot = self.current_n * self.contract_multiplier * self.stop_loss_multi
        # 单单位手数
        pos_volume = int(risk_per_unit / risk_per_lot)
        # 最小手数限制
        return max(pos_volume, self.config.get("min_order_volume", 1))

    def _entry_rule(self, bar: pd.Series, high_20: float, low_20: float, current_pos: int, pos_volume: int):
        """海龟入场规则：突破20日高低点，多空分开，不反手"""
        # 做多入场：突破20日高点，当前无多仓/空仓
        if bar["high"] > high_20 and current_pos <= 0:
            # 委托价格：突破高点（市价）
            entry_price = bar["high"]
            # 发送做多订单
            order = Order(
                symbol=self.symbol,
                instrument_type=self.instrument_type,
                direction="BUY",
                price=entry_price,
                volume=pos_volume,
                order_type="MARKET"
            )
            self.send_order(order)
            # 初始化止损止盈价
            self.stop_loss_price = entry_price - self.stop_loss_multi * self.current_n
            self.take_profit_price = entry_price + self.take_profit_multi * self.current_n
            self.pos_units = 1  # 持仓单位数+1
            self.logger.info(f"海龟做多入场：突破20日高点{high_20:.2f}，手数{pos_volume}，止损{self.stop_loss_price:.2f}")

        # 做空入场：突破20日低点，当前无空仓/多仓
        elif bar["low"] < low_20 and current_pos >= 0:
            entry_price = bar["low"]
            order = Order(
                symbol=self.symbol,
                instrument_type=self.instrument_type,
                direction="SELL",
                price=entry_price,
                volume=pos_volume,
                order_type="MARKET"
            )
            self.send_order(order)
            self.stop_loss_price = entry_price + self.stop_loss_multi * self.current_n
            self.take_profit_price = entry_price - self.take_profit_multi * self.current_n
            self.pos_units = 1
            self.logger.info(f"海龟做空入场：突破20日低点{low_20:.2f}，手数{pos_volume}，止损{self.stop_loss_price:.2f}")

    def _stop_take_profit_rule(self, bar: pd.Series, current_close: float, current_pos: int):
        """海龟止损止盈规则：2N止损，4N止盈，触发后平仓"""
        if current_pos == 0:
            return  # 无持仓，跳过

        # 多仓止损止盈
        if current_pos > 0:
            if bar["low"] <= self.stop_loss_price:
                # 触发止损，平仓
                self._close_position("SELL", bar["low"])
                self.logger.info(f"海龟多仓止损：价格{bar['low']:.2f} ≤ 止损价{self.stop_loss_price:.2f}")
            elif bar["high"] >= self.take_profit_price:
                # 触发止盈，平仓
                self._close_position("BUY", bar["high"])
                self.logger.info(f"海龟多仓止盈：价格{bar['high']:.2f} ≥ 止盈价{self.take_profit_price:.2f}")

        # 空仓止损止盈
        elif current_pos < 0:
            if bar["high"] >= self.stop_loss_price:
                self._close_position("BUY", bar["high"])
                self.logger.info(f"海龟空仓止损：价格{bar['high']:.2f} ≥ 止损价{self.stop_loss_price:.2f}")
            elif bar["low"] <= self.take_profit_price:
                self._close_position("SELL", bar["low"])
                self.logger.info(f"海龟空仓止盈：价格{bar['low']:.2f} ≤ 止盈价{self.take_profit_price:.2f}")

    def _close_position(self, direction: str, price: float):
        """平仓辅助方法：根据当前持仓发送反向订单"""
        current_pos = self.state.positions.get(self.symbol, 0)
        close_volume = abs(current_pos)
        # 发送平仓订单
        order = Order(
            symbol=self.symbol,
            instrument_type=self.instrument_type,
            direction=direction,
            price=price,
            volume=close_volume,
            order_type="MARKET"
        )
        self.send_order(order)
        # 重置持仓单位和止损止盈价
        self.pos_units = 0
        self.stop_loss_price = 0.0
        self.take_profit_price = 0.0

    def _position_limit(self, account_value: float):
        """海龟仓位限制：最大4单位，单一标的仓位上限"""
        current_pos = self.state.positions.get(self.symbol, 0)
        if current_pos == 0:
            return
        # 持仓单位数限制
        if self.pos_units >= self.max_pos_units:
            self.logger.warning(f"海龟持仓达最大单位数：{self.pos_units} ≥ {self.max_pos_units}，停止开仓")
        # 单一标的仓位上限（保证金占比）
        pos_margin = abs(current_pos) * self.data["close"].iloc[-1] * self.contract_multiplier * self.margin_rate
        pos_ratio = pos_margin / account_value
        if pos_ratio >= self.single_pos_limit:
            self.logger.warning(f"单一标的仓位达上限：{pos_ratio:.2%} ≥ {self.single_pos_limit:.2%}，强制平仓")
            self._close_position("SELL" if current_pos > 0 else "BUY", self.data["close"].iloc[-1])

# 测试代码
if __name__ == "__main__":
    import yaml
    import os
    from data.data_loader_test import DataLoader

    # 加载配置
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "turtle_config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # 加载并清洗数据
    loader = DataLoader()
    data = loader.load_futures(cfg["symbol"])
    # 初始化海龟策略
    turtle = TurtleStrategy(cfg)
    turtle.init(data)
    # 逐K线运行
    for idx, bar in data.iterrows():
        turtle.next(bar)
    # 打印策略信息
    print(turtle.get_strategy_info())