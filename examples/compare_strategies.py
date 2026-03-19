# -*- coding: utf-8 -*-
"""多策略对比回测示例：同时运行海龟+均线趋势策略，生成对比报告"""
import os
import yaml
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data.data_loader_test import DataLoader
from strategies.futures.turtle import TurtleStrategy
from strategies.futures.trend import FuturesTrendStrategy
from backtest.engine import BacktestEngine
from backtest.report import BacktestReportGenerator
from utils.log_utils import logger

def load_common_data_and_config(symbol: str, backtest_cfg: dict):
    """加载多策略通用数据和清洗"""
    data_loader = DataLoader()
    fut_data = data_loader.load_futures(symbol, backtest_cfg["start_date"], backtest_cfg["end_date"])
    return fut_data

def main():
    # 加载配置
    project_root = os.path.dirname(os.path.dirname(__file__))
    turtle_cfg_path = os.path.join(project_root, "config", "turtle_config.yaml")
    backtest_cfg_path = os.path.join(project_root, "config", "backtest_config.yaml")
    with open(turtle_cfg_path, "r", encoding="utf-8") as f:
        turtle_cfg = yaml.safe_load(f)
    with open(backtest_cfg_path, "r", encoding="utf-8") as f:
        backtest_cfg = yaml.safe_load(f)
    symbol = turtle_cfg["symbol"]

    # 加载通用数据
    common_data = load_common_data_and_config(symbol, backtest_cfg)
    logger.info(f"多策略通用数据加载完成，标的：{symbol}，数据条数：{len(common_data)}")

    # 初始化多策略报告生成器
    report_generator = BacktestReportGenerator(backtest_config_path=backtest_cfg_path)

    # ===================== 策略1：海龟交易策略 =====================
    turtle_strategy = TurtleStrategy(turtle_cfg)
    turtle_engine = BacktestEngine(backtest_cfg, turtle_strategy)
    turtle_result = turtle_engine.run(common_data)
    report_generator.add_strategy_result(turtle_result, "海龟交易策略")

    # ===================== 策略2：MA5-MA20趋势策略 =====================
    trend_cfg = {
        "symbol": symbol,
        "ma_short": 5,
        "ma_long": 20,
        "min_order_volume": 1,
        "contract": {"multiplier": 10, "margin_rate": 0.12}
    }
    trend_strategy = FuturesTrendStrategy(trend_cfg)
    trend_engine = BacktestEngine(backtest_cfg, trend_strategy)
    trend_result = trend_engine.run(common_data)
    report_generator.add_strategy_result(trend_result, "MA5-MA20均线趋势策略")

    # ===================== 生成多策略对比报告 =====================
    report_generator.generate_compare_report(sort_by="夏普比率")
    logger.info("多策略对比回测全流程完成！")

if __name__ == "__main__":
    main()