# -*- coding: utf-8 -*-
"""回测核心引擎：所有策略通用，实现订单执行/资金计算/净值更新/风控对接"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union
from strategies.base_strategy import BaseStrategy, Order
from risk.position import PositionManager
from risk.stop_loss import StopLossManager
from risk.slippage_fee import SlippageFeeCalculator
from utils.log_utils import logger
from utils.time_utils import convert_time_format

class BacktestEngine:
    def __init__(self, backtest_config: Dict, strategy: BaseStrategy):
        """
        初始化回测引擎
        :param backtest_config: 通用回测配置（backtest_config.yaml）
        :param strategy: 策略实例（继承BaseStrategy）
        """
        # 配置
        self.cfg = backtest_config
        self.strategy = strategy
        self.initial_capital = self.cfg["initial_capital"]
        # 核心回测变量
        self.account = {
            "capital": self.initial_capital,  # 净资产
            "cash": self.initial_capital,     # 可用资金
            "margin": 0.0,                    # 占用保证金
            "daily_pnl": 0.0,                 # 当日盈亏
            "total_pnl": 0.0,                 # 总盈亏
            "cum_return": 0.0,                # 累计收益率
            "drawdown": 0.0,                  # 最大回撤
            "max_capital": self.initial_capital  # 历史最大净资产
        }
        # 复用风控组件
        self.pos_manager = PositionManager(self.cfg)
        self.stop_loss_manager = StopLossManager(self.cfg)
        self.slippage_fee_calc = SlippageFeeCalculator(self.cfg)
        # 回测结果
        self.results: List[Dict] = []
        self.logger = logger
        # 确保结果保存目录存在
        self._init_result_dir()

    def _init_result_dir(self):
        """初始化结果保存目录"""
        if self.cfg["save_result"] and not os.path.exists(self.cfg["result_path"]):
            os.makedirs(self.cfg["result_path"])
            self.logger.info(f"创建回测结果目录：{self.cfg['result_path']}")

    def _calculate_margin(self, order: Order, price: float) -> float:
        """
        计算保证金
        :param order: 订单对象
        :param price: 成交价格
        :return: 占用保证金
        """
        multiplier = self.cfg["multiplier"][order.instrument_type]
        margin_rate = self.cfg["margin_rate"][order.instrument_type]
        # 保证金 = 手数 × 成交价格 × 合约乘数 × 保证金比例
        margin = abs(order.volume) * price * multiplier * margin_rate
        return margin

    def _execute_order(self, order: Order) -> Optional[Order]:
        """
        执行订单：计算滑点/手续费/保证金，更新资金/持仓，返回已成交订单
        :param order: 待执行订单
        :return: 已成交订单（None表示执行失败）
        """
        # 1. 计算滑点后的成交价格
        filled_price = self.slippage_fee_calc.calculate_slippage(order.price, order.direction)
        if filled_price <= 0:
            self.logger.warning(f"订单执行失败：成交价格无效{filled_price}")
            return None
        # 2. 计算手续费
        fee = self.slippage_fee_calc.calculate_commission(
            filled_price, order.volume, order.instrument_type
        )
        # 3. 计算保证金
        margin = self._calculate_margin(order, filled_price)
        # 4. 资金检查：可用资金是否足够（手续费+保证金）
        if self.account["cash"] < (fee + margin):
            self.logger.warning(f"订单执行失败：可用资金不足，需{fee+margin:.2f}，剩余{self.account['cash']:.2f}")
            return None
        # 5. 风控检查：仓位是否超限
        if not self.pos_manager.check_position_limit(
            self.strategy.state.positions, order.symbol, order.volume, self.account["capital"]
        ):
            self.logger.warning(f"订单执行失败：仓位超限")
            return None
        # 6. 执行订单：更新资金/保证金/持仓
        self.account["cash"] -= (fee + margin)
        self.account["margin"] += margin
        self.strategy.update_position(order.symbol, order.volume if order.direction == "BUY" else -order.volume)
        # 7. 更新订单状态
        order.status = "FILLED"
        order.filled_price = filled_price
        order.fee = fee
        order.margin = margin
        order.filled_time = convert_time_format(pd.Timestamp.now()).strftime("%Y-%m-%d %H:%M:%S")
        self.strategy.state.filled_orders.append(order)
        self.logger.info(f"订单成交：{order.symbol} {order.direction} {order.volume}手 @ {filled_price:.2f}，手续费{fee:.2f}")
        return order

    def _update_account(self, bar: pd.Series):
        """
        更新账户状态：计算浮盈/净资产/累计收益/最大回撤
        :param bar: 当前K线
        """
        # 计算浮盈（按最新价计算持仓市值变化）
        unrealized_pnl = 0.0
        multiplier_fut = self.cfg["multiplier"]["futures"]
        multiplier_opt = self.cfg["multiplier"]["options"]
        for symbol, pos in self.strategy.state.positions.items():
            if pos == 0:
                continue
            # 获取最新价（期货/期权区分）
            if "options" in symbol.lower() or any(c in symbol for c in ["C", "P"]):
                latest_price = bar.get(f"{symbol}_close", bar["close"])
                multiplier = multiplier_opt
            else:
                latest_price = bar["close"]
                multiplier = multiplier_fut
            # 持仓成本（简化：取平均成交价，实际可优化为逐笔计算）
            cost_price = np.mean([o.filled_price for o in self.strategy.state.filled_orders if o.symbol == symbol])
            # 浮盈 = 持仓手数 × (最新价 - 成本价) × 合约乘数
            unrealized_pnl += pos * (latest_price - cost_price) * multiplier

        # 更新账户净资产：可用资金 + 保证金 + 浮盈
        self.account["capital"] = self.account["cash"] + self.account["margin"] + unrealized_pnl
        # 更新当日盈亏
        self.account["daily_pnl"] = unrealized_pnl - self.account.get("prev_unrealized_pnl", 0)
        self.account["prev_unrealized_pnl"] = unrealized_pnl
        # 更新总盈亏
        self.account["total_pnl"] = self.account["capital"] - self.initial_capital
        # 更新累计收益率
        self.account["cum_return"] = self.account["total_pnl"] / self.initial_capital
        # 更新历史最大净资产和最大回撤
        self.account["max_capital"] = max(self.account["max_capital"], self.account["capital"])
        self.account["drawdown"] = (self.account["max_capital"] - self.account["capital"]) / self.account["max_capital"]
        # 更新策略状态
        self.strategy.update_strategy_state(bar, self.account["capital"], self.account["daily_pnl"])

    def _record_result(self, bar: pd.Series):
        """记录单K线回测结果（用于后续分析/绘图）"""
        result = {
            "datetime": bar["datetime"],
            "capital": round(self.account["capital"], 2),
            "cash": round(self.account["cash"], 2),
            "margin": round(self.account["margin"], 2),
            "daily_pnl": round(self.account["daily_pnl"], 2),
            "total_pnl": round(self.account["total_pnl"], 2),
            "cum_return": round(self.account["cum_return"], 4),
            "drawdown": round(self.account["drawdown"], 4),
            "positions": self.strategy.state.positions.copy(),
            "filled_orders": len(self.strategy.state.filled_orders)
        }
        self.results.append(result)

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        运行回测核心循环
        :param data: 清洗后的标准化数据（带技术指标）
        :return: 回测结果DataFrame
        """
        self.logger.info(f"开始回测：策略{self.strategy.strategy_type} | 初始资金{self.initial_capital:,.2f}元 | 时间{data['datetime'].iloc[0]}至{data['datetime'].iloc[-1]}")
        # 策略初始化
        self.strategy.init(data)
        # 逐K线执行回测
        for idx, bar in data.iterrows():
            self.logger.debug(f"处理K线：{bar['datetime'].strftime('%Y-%m-%d')}")
            # 1. 策略执行：生成订单
            self.strategy.next(bar)
            # 2. 执行所有未成交订单
            for order in self.strategy.state.orders:
                self._execute_order(order)
            # 3. 通用止损止盈（策略外补充）
            self.stop_loss_manager.check_stop_loss(
                self.strategy.state.positions, bar, self.strategy, self.account
            )
            # 4. 更新账户状态
            self._update_account(bar)
            # 5. 记录回测结果
            self._record_result(bar)
        # 回测结束：计算绩效指标
        result_df = pd.DataFrame(self.results)
        self.logger.info(f"回测完成：最终净资产{self.account['capital']:,.2f}元 | 总收益{self.account['total_pnl']:,.2f}元 | 累计收益率{self.account['cum_return']:.2%} | 最大回撤{self.account['drawdown']:.2%}")
        # 保存结果
        if self.cfg["save_result"]:
            save_path = os.path.join(self.cfg["result_path"], f"{self.strategy.strategy_type}_backtest.csv")
            result_df.to_csv(save_path, index=False, encoding="utf-8")
            self.logger.info(f"回测结果已保存至：{save_path}")
        return result_df