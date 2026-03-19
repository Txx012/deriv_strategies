# -*- coding: utf-8 -*-
"""海龟策略运行示例：完整流程（加载配置→数据→清洗→回测→分析→导出报告）"""
import os
import yaml
import sys
# 添加项目根目录到Python路径（解决导入问题）
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data.data_loader_test import DataLoader
from strategies.futures.turtle import TurtleStrategy
from backtest.engine import BacktestEngine
from backtest.performance import PerformanceAnalyzer
from utils.log_utils import logger

def main():
    # ===================== 1. 加载配置 =====================
    project_root = os.path.dirname(os.path.dirname(__file__))
    turtle_cfg_path = os.path.join(project_root, "config", "turtle_config.yaml")
    backtest_cfg_path = os.path.join(project_root, "config", "backtest_config.yaml")
    # 加载海龟策略配置
    with open(turtle_cfg_path, "r", encoding="utf-8") as f:
        turtle_cfg = yaml.safe_load(f)
    # 加载通用回测配置
    with open(backtest_cfg_path, "r", encoding="utf-8") as f:
        backtest_cfg = yaml.safe_load(f)
    # 合并配置（海龟配置覆盖通用配置）
    if turtle_cfg["backtest"]["inherit"] and turtle_cfg["backtest"]["override"]:
        backtest_cfg.update(turtle_cfg["backtest"]["override"])
    logger.info("配置加载完成，开始初始化组件")

    # ===================== 2. 初始化数据组件 =====================
    data_loader = DataLoader()
    # 加载期货数据
    fut_data = data_loader.load_futures(
        symbol=turtle_cfg["symbol"],
        start_date=backtest_cfg["start_date"],
        end_date=backtest_cfg["end_date"]
    )
    # 数据清洗+计算技术指标（海龟需要N值/高低点）
    logger.info("数据加载并清洗完成，有效数据条数：{}".format(len(fut_data)))

    # ===================== 3. 初始化策略和回测引擎 =====================
    turtle_strategy = TurtleStrategy(turtle_cfg)
    backtest_engine = BacktestEngine(backtest_config=backtest_cfg, strategy=turtle_strategy)

    # ===================== 4. 运行回测 =====================
    logger.info("开始运行海龟策略回测...")
    backtest_result = backtest_engine.run(data=fut_data)
    if backtest_result.empty:
        logger.error("回测结果为空，程序终止")
        return

    # ===================== 5. 绩效分析+绘图+导出报告 =====================
    analyzer = PerformanceAnalyzer(
        result_df=backtest_result,
        backtest_config=backtest_cfg,
        strategy_name="海龟交易策略"
    )
    analyzer.calculate_metrics()  # 计算指标
    analyzer.plot_charts(save_fig=True)  # 绘制图表
    analyzer.export_report()  # 导出Excel报告
    logger.info("海龟策略回测全流程完成！")

if __name__ == "__main__":
    main()