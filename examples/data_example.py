#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data import DataProviderFactory
from data.exceptions import QueryError

def main():
    # 初始化数据源
    try:
        dolphindb_provider = DataProviderFactory.create_provider("dolphindb")
        print("✅ DolphinDB数据源初始化成功\n")
        oracle_provider = DataProviderFactory.create_provider("oracle")
        print("✅ Oracle数据源初始化成功\n")
        # wind_provider = DataProviderFactory.create_provider("wind")
        # print("✅ Wind数据源初始化成功\n")
        rq_provider = DataProviderFactory.create_provider("ricequant")
        print("✅ RiceQuant数据源初始化成功\n")
    except Exception as e:
        print(f"❌ 数据源初始化失败: {e}")
        return

    ##################################################### dolphindb_provider测试 #####################################################
    # 场景1: dolphindb 查询沪市股票L2数据
    try:
        print("=== 场景1: 查询沪市股票L2数据 ===")
        sh_l2_data = dolphindb_provider.get_data(
            table_name="StockL2Snap",  # 表名（配置中定义）
            instruments=["600519.SH"],  # 对齐Wind参数名：带市场后缀的标的
            start_date="20230103",  # 对齐Wind日期格式：yyyymmdd
            end_date="20230105",
            fields=["windcode", "trade_date", "last_price", "pre_close", "offer_price", "bid_price"]  # 对齐Wind字段名
        )
        print(f"沪市L2查询结果形状: {sh_l2_data.shape}")
        print("前5条数据:")
        print(sh_l2_data.head())
        print()
    except QueryError as e:
        print(f"❌ 场景1失败: {e}\n")

    # 场景2: dolphindb 查询深市股票L2数据
    try:
        print("=== 场景2: 查询深市股票L2数据 ===")
        sz_l2_data = dolphindb_provider.get_data(
            table_name="StockL2Snap",
            instruments=["000001.SZ", "002594.SZ"],  # 深市标的，自动解析市场为SZ
            start_date="20230103",
            end_date="20230105",
            fields=["windcode", "trade_date", "last_price", "pre_close", "offer_price", "bid_price"]  # 对齐Wind字段名
        )
        print(f"深市L2查询结果形状: {sz_l2_data.shape}")
        print("前5条数据:")
        print(sz_l2_data.head())
        print()
    except QueryError as e:
        print(f"❌ 场景2失败: {e}\n")

    # 场景3：查询期货L2数据（CFE市场）
    try:
        print("=== 场景3: 查询期货L2数据 ===")
        future_l2_data = dolphindb_provider.get_data(
            table_name="FutureL2",
            fields=["windcode", "trade_date", "last_price", "pre_close", "offer_price", "bid_price"],
            instruments=["IF2301.CFE", "IC2301.CFE"],  # 期货标的，带.CFE后缀
            start_date="20230103",
            end_date="20230103"
        )
        print(f"期货L2查询结果形状: {future_l2_data.shape}")
        print("前5条数据:")
        print(future_l2_data.head())
        print()
    except QueryError as e:
        print(f"❌ 场景3失败: {e}\n")

    # 场景4：查询 DayLine 数据（自动解析市场）
    ################################################# 注意：DayLine表端口不同，需要重新连接dolphindb #############################################
    try:
        print("=== 场景4: 查询 DayLine 数据 ===")
        day_data = dolphindb_provider.get_data(
            table_name="DayLine",
            instruments=["000001.SZ"],  # 自动解析市场
            start_date="20230103",
            end_date="20230105",
            fields=["windcode", "trade_date", "open", "high", "low", "close", "volume"]
        )
        print(f"DayLine 查询结果形状：{day_data.shape}")
        print("前5条数据:")
        print(day_data.head())
        print()
    except QueryError as e:
        print(f"❌ 场景4失败: {e}\n")

    # 场景5：查询 MinuteLine 数据（自动解析市场）
    try:
        print("=== 场景5: 查询 MinuteLine 数据 ===")
        minute_data = dolphindb_provider.get_data(
            table_name="MinuteLine",
            instruments=["000001.SZ"],  # 自动解析市场
            start_date="20230103",
            end_date="20230105",
            fields=["windcode", "trade_date", "time", "close", "volume"]
        )
        print(f"MinuteLine 查询结果形状：{minute_data.shape}")
        print("前5条数据:")
        print(minute_data.head())
        print()
    except QueryError as e:
        print(f"❌ 场景5失败: {e}\n")

    # 场景6：仅返回SQL（调试用，不执行查询）
    try:
        print("=== 场景6: 获取生成的SQL语句 ===")
        sql = dolphindb_provider.get_data(
            table_name="StockL2Snap",
            fields=["windcode", "last_price", "trade_date"],
            instruments=["600000.SH"],
            start_date="20230103",
            end_date="20230103",
            return_sql=True  # 仅返回SQL，不执行
        )
        print("生成的DolphinDB SQL:")
        print(sql)
    except QueryError as e:
        print(f"❌ 场景6失败: {e}\n")

    ##################################################### oracle_provider测试 #####################################################
    # 场景7：查询Oracle数据源（测试连接）
    try:
        print("=== 场景7: 查询Oracle数据源 ===")
        oracle_data = oracle_provider.get_data(
            table_name="AShareCalendar",
            fields=["trade_date", "exchange"],
            start_date="20230103",
            end_date="20230103"
        )
        print(f"Oracle查询结果形状: {oracle_data.shape}")
        print("前5条数据:")
        print(oracle_data.head())
    except QueryError as e:
        print(f"❌ 场景6失败: {e}\n")
    
    # ##################################################### wind_provider测试 #####################################################
    # # 场景7：查询Wind数据源(日行情，对齐接口格式)
    # try:
    #     print("=== 场景7: 查询Wind数据源 ===")
    #     df_daily = wind_provider.get_data(
    #         table_name="daily_quote",
    #         fields=["windcode", "trade_date", "open", "high", "low", "close", "volume"],
    #         instruments=["600519.SH", "000001.SZ"],
    #         start_date="2024-01-01",
    #         end_date="2024-01-31",
    #         custom_conditions={"PriceAdj": "F"}  # 不复权
    #     )
    #     print(f"Wind日行情查询结果形状: {df_daily.shape}")
    #     print("前5条数据:")
    #     print(df_daily.head())
    # except QueryError as e:
    #     print(f"❌ 场景7失败: {e}\n")

    ################################################################### ricequant_provider测试 #####################################################
    # 场景8：查询RiceQuant数据源（日行情，对齐接口格式）
    try:
        print("=== 场景8: 查询RiceQuant数据源 ===")
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
         


if __name__ == "__main__":
    main()