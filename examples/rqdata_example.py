#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data import DataProviderFactory
from data.exceptions import QueryError

def main():
    try:
        rq_provider = DataProviderFactory.create_provider("ricequant")
        print("✅ RiceQuant数据源初始化成功\n")
    except Exception as e:
        print(f"❌ 数据源初始化失败: {e}")
        return

############################################################ ricequant_provider测试 #####################################################
    # 场景1：查询RiceQuant数据源（日行情，对齐接口格式）
    try:
        print("=== 场景1: 查询RiceQuant数据源 ===")
        rq_daily = rq_provider.get_data(
            table_name="AShareEODPrices",
            fields=["windcode", "trade_date", "open", "high", "low", "close", "volume"],
            instruments=["600519.SH", "000001.SZ"],
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        print(f"RiceQuant日行情查询结果形状: {rq_daily.shape}")
        print("前5条数据:")
        print(rq_daily.head())
    except QueryError as e:
        print(f"❌ 场景8失败: {e}\n")

    # 场景2：查询期权数据
    try:
        print("=== 查询期权合约列表 ===")
        contracts_df = rq_provider.get_data(
            table_name="OptionContracts",
            instruments=["CU"],       # 标的代码，如 'CU' 表示铜期货
            start_date="20230101",    # 查询日期
            option_type=None,         # 'C' 或 'P'
            maturity="2303",          # 到期月份 'YYMM'
            strike=None               # 行权价
        )
        print(contracts_df)

        print("=== 查询期权日行情 ===")
        option_price_df = rq_provider.get_data(
            table_name="OptionEODPrices",
            instruments=contracts_df["windcode"].tolist(),
            start_date="20230101",
            end_date="20230110"
        )
        print(option_price_df.head())

        print("=== 查询期权希腊字母 ===")
        greeks_df = rq_provider.get_data(
            table_name="OptionGreeks",
            instruments=contracts_df["windcode"].tolist(),
            start_date="20230101",
            end_date="20230110",
            fields=None
        )
        print(greeks_df.head())

        print("=== 查询期权衍生指标 ===")
        indicators_df = rq_provider.get_data(
            table_name="OptionIndicators",
            instruments=["CU"],
            maturity="2303",
            start_date="20230101",
            end_date="20230110"
        )
        print(indicators_df.head())
    except Exception as e:
        print("查询失败：", e)

         


if __name__ == "__main__":
    main()