# -*- coding: utf-8 -*-
"""回测报告工具：支持多策略对比报告生成+批量导出+指标排序"""
import pandas as pd
import numpy as np
import os
import yaml
from typing import List, Dict, Optional
from backtest.performance import PerformanceAnalyzer
from utils.log_utils import logger

class BacktestReportGenerator:
    def __init__(self, backtest_config_path: str = "config/backtest_config.yaml"):
        """初始化报告生成器"""
        with open(backtest_config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)
        self.result_dir = self.cfg["result_path"]
        os.makedirs(self.result_dir, exist_ok=True)
        self.all_strategies_metrics: List[Dict] = []  # 多策略指标汇总
        self.logger = logger

    def add_strategy_result(self, result_df: pd.DataFrame, strategy_name: str):
        """添加单个策略的回测结果，自动计算指标"""
        try:
            analyzer = PerformanceAnalyzer(result_df, self.cfg, strategy_name)
            metrics = analyzer.calculate_metrics()
            self.all_strategies_metrics.append(metrics)
            logger.info(f"已添加策略结果：{strategy_name}")
        except Exception as e:
            logger.error(f"添加策略{strategy_name}结果失败：{e}")

    def generate_compare_report(self, sort_by: str = "夏普比率", ascending: bool = False) -> str:
        """
        生成多策略对比报告（Excel）
        :param sort_by: 排序字段（夏普比率/年化收益率/最大回撤等）
        :param ascending: 是否升序（False=降序，即夏普比率越高越靠前）
        :return: 报告保存路径
        """
        if not self.all_strategies_metrics:
            raise ValueError("未添加任何策略结果，无法生成对比报告")
        # 转换为DataFrame并排序
        compare_df = pd.DataFrame(self.all_strategies_metrics)
        # 百分比字段格式化（用于展示）
        pct_cols = ["累计收益率", "年化收益率", "月化收益率", "胜率", "最大回撤", "年化波动率"]
        for col in pct_cols:
            if col in compare_df.columns:
                compare_df[col + "_展示"] = compare_df[col].apply(lambda x: f"{x:.2%}")
        # 按指定字段排序
        if sort_by in compare_df.columns:
            compare_df = compare_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
        # 保存对比报告
        report_path = os.path.join(self.result_dir, "多策略对比报告.xlsx")
        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
            compare_df.to_excel(writer, sheet_name="策略对比", index=False)
        logger.info(f"多策略对比报告已生成（按{sort_by}{'升序' if ascending else '降序'}），路径：{report_path}")
        # 打印对比摘要
        self._print_compare_summary(compare_df)
        return report_path

    def _print_compare_summary(self, compare_df: pd.DataFrame):
        """打印多策略对比摘要（控制台）"""
        logger.info("\n" + "="*60)
        logger.info("                    多策略回测结果对比摘要")
        logger.info("="*60)
        summary_cols = ["策略名称", "最终资金(元)", "年化收益率_展示", "最大回撤_展示", "夏普比率", "胜率_展示"]
        for col in summary_cols:
            if col not in compare_df.columns:
                summary_cols.remove(col)
        # 格式化打印
        for idx, row in compare_df[summary_cols].iterrows():
            row_str = f"第{idx+1}名 | "
            for k, v in row.items():
                row_str += f"{k:<15}: {v:>10} | "
            logger.info(row_str[:-3])
        logger.info("="*60 + "\n")

# 测试代码
if __name__ == "__main__":
    generator = BacktestReportGenerator()
    # 生成测试策略1结果
    df1 = pd.DataFrame({
        "datetime": pd.date_range(start="2023-01-01", periods=100, freq="D"),
        "capital": np.cumsum(np.random.randn(100)*80 + 600) + 1000000
    })
    generator.add_strategy_result(df1, "海龟策略")
    # 生成测试策略2结果
    df2 = pd.DataFrame({
        "datetime": pd.date_range(start="2023-01-01", periods=100, freq="D"),
        "capital": np.cumsum(np.random.randn(100)*100 + 400) + 1000000
    })
    generator.add_strategy_result(df2, "均线趋势策略")
    # 生成对比报告
    generator.generate_compare_report(sort_by="夏普比率")