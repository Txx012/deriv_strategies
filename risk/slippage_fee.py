# -*- coding: utf-8 -*-
"""滑点和手续费计算器：适配期货/期权不同规则，支持比例/固定/混合模式"""
from typing import Optional, Dict
from utils.log_utils import logger

class SlippageFeeCalculator:
    def __init__(self, backtest_config: Dict):
        self.cfg = backtest_config
        # 滑点配置
        self.slippage_type = self.cfg.get("slippage_type", "ratio")  # ratio/fixed
        self.slippage_value = self.cfg.get("slippage_value", 0.0001)  # 比例0.01%/固定值0.1
        # 手续费配置
        self.commission_type = self.cfg.get("commission_type", "mix")  # ratio/fixed/mix
        self.commission_ratio = self.cfg.get("commission_ratio", 0.0003)  # 比例0.03%
        self.commission_fixed = self.cfg.get("commission_fixed", 5)  # 固定5元/笔（期权常用）
        self.min_commission = self.cfg.get("min_commission", 1)  # 最低手续费1元
        # 合约乘数
        self.multiplier = self.cfg["multiplier"]
        self.logger = logger

    def calculate_slippage(self, original_price: float, direction: str) -> float:
        """
        计算滑点后的成交价格
        :param original_price: 委托价格
        :param direction: 交易方向 BUY/SELL
        :return: 滑点后成交价格
        """
        if original_price <= 0:
            return original_price
        if self.slippage_type == "ratio":
            # 比例滑点：买入加价，卖出减价
            slippage = original_price * self.slippage_value
            filled_price = original_price + slippage if direction == "BUY" else original_price - slippage
        else:
            # 固定值滑点：买入加价，卖出减价
            slippage = self.slippage_value
            filled_price = original_price + slippage if direction == "BUY" else original_price - slippage
        # 价格不能为负
        filled_price = max(filled_price, 0.0001)
        self.logger.debug(f"滑点计算：委托价{original_price:.2f} → 成交价{filled_price:.2f}（{direction}，{self.slippage_type}滑点{self.slippage_value}）")
        return filled_price

    def calculate_commission(self, filled_price: float, volume: int,
                             instrument_type: str = "futures") -> float:
        """
        计算手续费（保底最低手续费）
        :param filled_price: 成交价格
        :param volume: 成交手数
        :param instrument_type: 品种类型 futures/options
        :return: 最终手续费（元）
        """
        if volume <= 0 or filled_price <= 0:
            return 0.0
        multiplier = self.multiplier[instrument_type]
        # 计算合约总价值
        contract_value = filled_price * volume * multiplier
        # 按类型计算手续费
        if self.commission_type == "ratio":
            commission = contract_value * self.commission_ratio
        elif self.commission_type == "fixed":
            commission = volume * self.commission_fixed
        else:  # mix 混合模式（期货按比例，期权按固定）
            if instrument_type == "futures":
                commission = contract_value * self.commission_ratio
            else:
                commission = volume * self.commission_fixed
        # 最低手续费保底
        commission = max(commission, self.min_commission)
        self.logger.debug(f"手续费计算：{instrument_type} {volume}手，合约价值{contract_value:.2f} → 手续费{commission:.2f}元")
        return commission