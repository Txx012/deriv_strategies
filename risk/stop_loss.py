# -*- coding: utf-8 -*-
"""通用止损止盈风控：策略未实现止损时自动生效，支持固定比例/移动止损"""
import pandas as pd
from typing import Dict, Optional
from strategies.base_strategy import BaseStrategy, Order
from utils.log_utils import logger

class StopLossManager:
    def __init__(self, backtest_config: Dict):
        self.cfg = backtest_config
        # 通用止损止盈参数（从配置读取）
        self.default_stop_loss = self.cfg.get("default_stop_loss", 0.05)  # 固定5%止损
        self.default_take_profit = self.cfg.get("default_take_profit", 0.10)  # 固定10%止盈
        self.trailing_stop = self.cfg.get("trailing_stop", True)  # 开启移动止损
        self.trailing_step = self.cfg.get("trailing_step", 0.02)  # 移动止损步长2%
        self.multiplier = self.cfg["multiplier"]
        self.logger = logger
        # 动态止损价缓存（{symbol: stop_price}）
        self.trailing_stop_prices: Dict[str, float] = {}

    def check_stop_loss(self, positions: Dict[str, int], bar: pd.Series,
                        strategy: BaseStrategy, account: Dict) -> None:
        """
        检查并执行止损止盈（直接调用策略下单方法平仓）
        :param positions: 当前持仓
        :param bar: 当前K线数据
        :param strategy: 策略实例（用于下单）
        :param account: 账户信息
        """
        if not positions:
            return  # 无持仓，跳过
        for symbol, vol in positions.items():
            if vol == 0:
                continue
            # 获取标的最新价和成本价
            latest_price = bar.get(f"{symbol}_close", bar["close"])
            cost_price = self._get_avg_cost_price(strategy, symbol)
            if cost_price <= 0 or latest_price <= 0:
                continue
            # 计算收益率
            ret = (latest_price - cost_price) / cost_price if vol > 0 else (cost_price - latest_price) / cost_price
            # 1. 固定比例止损止盈
            if self._check_fixed_stop_take_profit(ret, vol):
                self._close_position(strategy, symbol, vol, latest_price)
                continue
            # 2. 移动止损（如果开启）
            if self.trailing_stop and self._check_trailing_stop(latest_price, vol, symbol):
                self._close_position(strategy, symbol, vol, latest_price)
                del self.trailing_stop_prices[symbol]  # 平仓后清空止损价

    def _get_avg_cost_price(self, strategy: BaseStrategy, symbol: str) -> float:
        """获取标的平均持仓成本价"""
        filled_orders = [o for o in strategy.state.filled_orders if o.symbol == symbol]
        if not filled_orders:
            return 0.0
        # 计算平均成交价（按手数加权）
        total_volume = sum(abs(o.volume) for o in filled_orders)
        avg_price = sum(o.filled_price * abs(o.volume) for o in filled_orders) / total_volume
        return avg_price

    def _check_fixed_stop_take_profit(self, ret: float, volume: int) -> bool:
        """检查固定比例止损止盈是否触发"""
        if volume > 0:  # 多仓
            if ret <= -self.default_stop_loss:
                self.logger.warning(f"多仓固定止损触发：收益率{ret:.2%} ≤ -{self.default_stop_loss:.2%}")
                return True
            elif ret >= self.default_take_profit:
                self.logger.info(f"多仓固定止盈触发：收益率{ret:.2%} ≥ {self.default_take_profit:.2%}")
                return True
        else:  # 空仓
            if ret <= -self.default_stop_loss:
                self.logger.warning(f"空仓固定止损触发：收益率{ret:.2%} ≤ -{self.default_stop_loss:.2%}")
                return True
            elif ret >= self.default_take_profit:
                self.logger.info(f"空仓固定止盈触发：收益率{ret:.2%} ≥ {self.default_take_profit:.2%}")
                return True
        return False

    def _check_trailing_stop(self, latest_price: float, volume: int, symbol: str) -> bool:
        """检查移动止损是否触发，自动更新止损价"""
        # 初始化移动止损价
        if symbol not in self.trailing_stop_prices:
            self.trailing_stop_prices[symbol] = latest_price * (1 - self.trailing_step) if volume > 0 else latest_price * (1 + self.trailing_step)
            return False
        current_stop_price = self.trailing_stop_prices[symbol]
        # 多仓移动止损：新高后上移止损价，跌破止损价触发
        if volume > 0:
            if latest_price > (current_stop_price / (1 - self.trailing_step)):
                # 创近期新高，更新止损价
                new_stop_price = latest_price * (1 - self.trailing_step)
                self.trailing_stop_prices[symbol] = new_stop_price
                self.logger.debug(f"多仓移动止损价更新：{current_stop_price:.2f} → {new_stop_price:.2f}")
            elif latest_price < current_stop_price:
                self.logger.warning(f"多仓移动止损触发：价格{latest_price:.2f} < 止损价{current_stop_price:.2f}")
                return True
        # 空仓移动止损：新低后下移止损价，突破止损价触发
        else:
            if latest_price < (current_stop_price / (1 + self.trailing_step)):
                new_stop_price = latest_price * (1 + self.trailing_step)
                self.trailing_stop_prices[symbol] = new_stop_price
                self.logger.debug(f"空仓移动止损价更新：{current_stop_price:.2f} → {new_stop_price:.2f}")
            elif latest_price > current_stop_price:
                self.logger.warning(f"空仓移动止损触发：价格{latest_price:.2f} > 止损价{current_stop_price:.2f}")
                return True
        return False

    def _close_position(self, strategy: BaseStrategy, symbol: str, volume: int, price: float):
        """触发止损止盈，执行平仓（调用策略标准化下单方法）"""
        instr_type = "options" if any(c in symbol for c in ["C", "P"]) else "futures"
        direction = "SELL" if volume > 0 else "BUY"
        # 生成标准化平仓订单
        close_order = Order(
            symbol=symbol,
            instrument_type=instr_type,
            direction=direction,
            price=price,
            volume=abs(volume),
            order_type="MARKET"
        )
        strategy.send_order(close_order)