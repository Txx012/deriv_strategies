# -*- coding: utf-8 -*-
"""期货均线趋势策略运行示例：MA5/MA20金叉做多，死叉做空"""
import os
import yaml
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data.data_loader_test import DataLoader
from strategies.futures.trend import FuturesTrendStrategy
from backtest.engine import BacktestEngine
from backtest.performance import PerformanceAnalyzer
from utils.log_utils import logger

def main():
    # 加载配置
    project_root = os.path.dirname(os.path.dirname(__file__))
    backtest_cfg_path = os.path.join(project_root, "config", "backtest_config.yaml")
    with open(backtest_cfg_path, "r", encoding="utf-8") as f:
        backtest_cfg = yaml.safe_load(f)
    # 自定义趋势策略配置
    trend_cfg = {
        "symbol": "rb2405",
        "ma_short": 5,
        "ma_long": 20,
        "min_order_volume": 1,
        "max_order_volume": 10,
        "contract": {
            "multiplier": 10,
            "margin_rate": 0.12
        },
        "backtest": backtest_cfg
    }

    # 数据加载+清洗
    data_loader = DataLoader()
    fut_data = data_loader.load_futures(
        symbol=trend_cfg["symbol"],
        start_date=backtest_cfg["start_date"],
        end_date=backtest_cfg["end_date"]
    )

    # 初始化策略和回测引擎
    trend_strategy = FuturesTrendStrategy(trend_cfg)
    backtest_engine = BacktestEngine(backtest_config=backtest_cfg, strategy=trend_strategy)

    # 运行回测
    logger.info("开始运行均线趋势策略回测...")
    backtest_result = backtest_engine.run(data=fut_data)
    if backtest_result.empty:
        logger.error("回测结果为空")
        return

    # 绩效分析
    analyzer = PerformanceAnalyzer(backtest_result, backtest_cfg, "MA5-MA20趋势策略")
    analyzer.calculate_metrics()
    analyzer.plot_charts()
    analyzer.export_report()
    logger.info("均线趋势策略回测完成！")

if __name__ == "__main__":
    main()