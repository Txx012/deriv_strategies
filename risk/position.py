# -*- coding: utf-8 -*-
"""仓位管理风控：检查总仓位上限、单一标的仓位上限，适配期货/期权/混合策略"""
import numpy as np
from typing import Dict, Optional
from utils.log_utils import logger

class PositionManager:
    def __init__(self, backtest_config: Dict):
        self.cfg = backtest_config
        # 从配置读取仓位限制
        self.total_pos_limit = self.cfg.get("total_pos_limit", 0.8)  # 总仓位≤80%
        self.single_pos_limit = self.cfg.get("single_pos_limit", 0.2)  # 单一标的≤20%
        self.min_order_volume = self.cfg.get("min_order_volume", 1)
        self.max_order_volume = self.cfg.get("max_order_volume", 100)
        self.margin_rate = self.cfg["margin_rate"]
        self.multiplier = self.cfg["multiplier"]
        self.logger = logger

    def calculate_position_ratio(self, positions: Dict[str, int], symbol: str,
                                 add_volume: int, account_capital: float,
                                 latest_price: float) -> float:
        """
        计算调仓后的仓位占比（保证金/账户净资产）
        :param positions: 当前持仓{symbol: volume}
        :param symbol: 待交易标的
        :param add_volume: 拟变动手数（正=开仓，负=平仓）
        :param account_capital: 账户净资产
        :param latest_price: 标的最新价
        :return: 调仓后仓位占比
        """
        if account_capital <= 0:
            return 1.0  # 资金为0，直接返回100%仓位
        # 判断标的类型（期货/期权）
        instr_type = "options" if any(c in symbol for c in ["C", "P"]) or "option" in symbol.lower() else "futures"
        # 计算拟变动仓位的保证金
        add_margin = abs(add_volume) * latest_price * self.multiplier[instr_type] * self.margin_rate[instr_type]
        # 计算当前总保证金
        current_margin = sum(
            abs(vol) * latest_price * self.multiplier["futures" if "fut" in s else "options"] * self.margin_rate["futures" if "fut" in s else "options"]
            for s, vol in positions.items()
        )
        # 调仓后总仓位占比
        total_margin = current_margin + add_margin
        pos_ratio = total_margin / account_capital
        return pos_ratio

    def check_position_limit(self, positions: Dict[str, int], symbol: str,
                             add_volume: int, account_capital: float,
                             latest_price: Optional[float] = None) -> bool:
        """
        仓位限制总检查：手数限制+单一标的+总仓位
        :return: True=通过，False=不通过
        """
        latest_price = latest_price or self._get_default_price(symbol)
        # 1. 手数合法性检查
        if abs(add_volume) < self.min_order_volume or abs(add_volume) > self.max_order_volume:
            self.logger.warning(f"手数超出限制：{add_volume}，范围[{self.min_order_volume}, {self.max_order_volume}]")
            return False
        # 2. 计算调仓后仓位占比
        total_pos_ratio = self.calculate_position_ratio(positions, symbol, add_volume, account_capital, latest_price)
        # 3. 总仓位上限检查
        if total_pos_ratio > self.total_pos_limit:
            self.logger.warning(f"总仓位超出上限：{total_pos_ratio:.2%} > {self.total_pos_limit:.2%}")
            return False
        # 4. 单一标的仓位上限检查
        single_pos_ratio = self._calculate_single_pos_ratio(symbol, add_volume, latest_price, account_capital)
        if single_pos_ratio > self.single_pos_limit:
            self.logger.warning(f"单一标的仓位超出上限：{single_pos_ratio:.2%} > {self.single_pos_limit:.2%}")
            return False
        return True

    def _calculate_single_pos_ratio(self, symbol: str, add_volume: int,
                                    latest_price: float, account_capital: float) -> float:
        """计算单一标的调仓后的仓位占比"""
        if account_capital <= 0:
            return 1.0
        instr_type = "options" if any(c in symbol for c in ["C", "P"]) else "futures"
        single_margin = abs(add_volume) * latest_price * self.multiplier[instr_type] * self.margin_rate[instr_type]
        return single_margin / account_capital

    def _get_default_price(self, symbol: str) -> float:
        """获取默认价格（无最新价时使用，避免计算失败）"""
        return 1000.0 if "fut" in symbol.lower() else 100.0  # 期货默认1000，期权默认100