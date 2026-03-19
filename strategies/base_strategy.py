# -*- coding: utf-8 -*-
"""策略基类：定义标准化接口，所有期货/期权策略继承此类，实现核心逻辑即可"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import pandas as pd
from utils.log_utils import logger

# 标准化订单类（期货/期权通用，所有策略统一下单格式）
@dataclass
class Order:
    symbol: str          # 交易标的代码（如rb2405/IO2405-C-4000）
    instrument_type: str # 品种类型：futures(期货) / options(期权)
    direction: str       # 交易方向：BUY(买入) / SELL(卖出) / LONG(做多) / SHORT(做空)
    price: float         # 委托价格
    volume: int          # 委托手数
    order_type: str = "LIMIT"  # 订单类型：LIMIT(限价) / MARKET(市价)
    status: str = "PENDING"    # 订单状态：PENDING(待执行) / FILLED(已成交) / CANCELLED(已撤销)
    create_time: str = ""      # 订单创建时间
    filled_time: str = ""      # 成交时间

# 策略状态类（记录策略运行过程中的核心状态）
@dataclass
class StrategyState:
    positions: Dict[str, int] = field(default_factory=dict)  # 持仓：{symbol: 手数，多正空负}
    orders: List[Order] = field(default_factory=list)        # 未成交订单
    filled_orders: List[Order] = field(default_factory=list) # 已成交订单
    account_value: float = 0.0                               # 账户净资产
    daily_pnl: float = 0.0                                   # 当日盈亏
    total_pnl: float = 0.0                                   # 总盈亏
    current_bar: pd.Series = field(default_factory=pd.Series)# 当前K线

class BaseStrategy(ABC):
    """
    策略抽象基类：所有策略必须实现 __init__/init/next 三个核心方法
    通用方法（下单/更新持仓/更新状态）已实现，所有策略复用
    """
    def __init__(self, config: Dict, strategy_type: str):
        """
        初始化策略（所有策略必须调用super().__init__）
        :param config: 策略配置（yaml加载的字典）
        :param strategy_type: 策略类型：futures/turtle/options/straddle等
        """
        self.config = config  # 外置配置（无硬编码）
        self.strategy_type = strategy_type  # 策略类型标识
        self.state = StrategyState()        # 策略运行状态
        self.data: pd.DataFrame = pd.DataFrame()  # 策略运行数据
        self.logger = logger  # 日志

    @abstractmethod
    def init(self, data: pd.DataFrame) -> None:
        """
        策略初始化：加载数据、计算指标、初始化参数（仅运行一次）
        :param data: 清洗后的标准化数据（带技术指标）
        """
        pass

    @abstractmethod
    def next(self, bar: pd.Series) -> None:
        """
        逐K线执行策略核心逻辑（回测/实盘的核心循环）
        :param bar: 当前K线数据（pd.Series，含datetime/close/指标等）
        """
        pass

    def send_order(self, order: Order) -> None:
        """
        通用下单方法（所有策略复用，对接回测/实盘引擎）
        :param order: 标准化Order对象
        """
        # 订单合法性检查
        if order.volume < self.config.get("min_order_volume", 1):
            self.logger.warning(f"委托手数不足最小限制：{order.volume} < {self.config['min_order_volume']}")
            return
        if order.price <= 0:
            self.logger.warning(f"委托价格无效：{order.price}")
            return
        # 记录订单
        order.create_time = bar["datetime"].strftime("%Y-%m-%d %H:%M:%S") if not order.create_time else order.create_time
        self.state.orders.append(order)
        self.logger.info(f"发送订单：{order.direction} {order.symbol} {order.volume}手 @ {order.price}")

    def update_position(self, symbol: str, volume: int) -> None:
        """
        通用更新持仓方法（多仓正，空仓负，平仓为反向手数）
        :param symbol: 标的代码
        :param volume: 持仓变动手数（+开多/-开空/平仓为反向）
        """
        current_pos = self.state.positions.get(symbol, 0)
        new_pos = current_pos + volume
        if new_pos == 0:
            # 平仓，删除持仓记录
            del self.state.positions[symbol]
            self.logger.info(f"平仓完成：{symbol}，原持仓{current_pos}手")
        else:
            # 更新持仓
            self.state.positions[symbol] = new_pos
            self.logger.info(f"更新持仓：{symbol}，原{current_pos}手，变动{volume}手，现{new_pos}手")

    def update_strategy_state(self, bar: pd.Series, account_value: float, daily_pnl: float) -> None:
        """
        更新策略运行状态（回测引擎调用，所有策略复用）
        :param bar: 当前K线
        :param account_value: 最新账户净资产
        :param daily_pnl: 当日盈亏
        """
        self.state.current_bar = bar
        self.state.account_value = account_value
        self.state.daily_pnl = daily_pnl
        self.state.total_pnl += daily_pnl
        # 清空未成交订单（当日未成交自动撤销，可根据策略修改）
        self.state.orders = []

    def get_strategy_info(self) -> Dict:
        """获取策略当前信息（用于回测报告/实盘监控）"""
        return {
            "strategy_type": self.strategy_type,
            "current_positions": self.state.positions,
            "account_value": round(self.state.account_value, 2),
            "total_pnl": round(self.state.total_pnl, 2),
            "daily_pnl": round(self.state.daily_pnl, 2),
            "current_datetime": self.state.current_bar.get("datetime", "").strftime("%Y-%m-%d")
        }