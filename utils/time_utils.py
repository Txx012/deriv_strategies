# -*- coding: utf-8 -*-
"""时间工具类：统一时间格式转换/交易日判断/日期范围生成/时间差计算"""
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Optional, List, Union
import warnings
warnings.filterwarnings("ignore")

def convert_time_format(time_str: Union[str, datetime, pd.Timestamp],
                        target_format: str = "%Y-%m-%d %H:%M:%S") -> Union[str, datetime]:
    """
    统一时间格式转换：支持str/PD.Timestamp/datetime互转
    :param time_str: 原始时间（任意格式）
    :param target_format: 目标格式（%Y-%m-%d/%Y-%m-%d %H:%M:%S），传None返回datetime对象
    :return: 转换后时间（str/datetime）
    """
    # 处理空值
    if pd.isna(time_str) or time_str == "" or time_str is None:
        return ""
    # 转换为datetime对象
    if isinstance(time_str, pd.Timestamp):
        dt = time_str.to_pydatetime()
    elif isinstance(time_str, datetime):
        dt = time_str
    elif isinstance(time_str, date):
        dt = datetime.combine(time_str, datetime.min.time())
    else:
        # 字符串自动识别格式
        try:
            dt = pd.to_datetime(time_str).to_pydatetime()
        except Exception:
            raise ValueError(f"无法识别的时间格式：{time_str}")
    # 返回指定格式字符串或datetime对象
    return dt.strftime(target_format) if target_format else dt

def generate_date_range(start_date: str, end_date: str,
                        freq: str = "D", include_end: bool = True) -> List[str]:
    """
    生成日期范围列表（标准化格式%Y-%m-%d）
    :param start_date: 开始日期（任意格式）
    :param end_date: 结束日期（任意格式）
    :param freq: 频率 D=日/W=周/M=月
    :param include_end: 是否包含结束日期
    :return: 日期列表["2023-01-01", ...]
    """
    start = convert_time_format(start_date, target_format=None)
    end = convert_time_format(end_date, target_format=None)
    if start > end:
        raise ValueError(f"开始日期{start_date}晚于结束日期{end_date}")
    # 生成日期范围
    date_series = pd.date_range(start=start, end=end, freq=freq, inclusive="both" if include_end else "left")
    # 转换为标准化字符串
    date_list = [d.strftime("%Y-%m-%d") for d in date_series]
    return date_list

def is_trading_day(dt: Union[str, datetime], exchange: str = "SHFE") -> bool:
    """
    判断是否为交易日（简易版，支持上期所/沪深交易所）
    :param dt: 待判断时间
    :param exchange: 交易所 SHFE=上期所/SSE=上交所
    :return: True=交易日，False=非交易日
    """
    dt = convert_time_format(dt, target_format=None)
    # 周末非交易日
    if dt.weekday() in [5, 6]:
        return False
    # 简易节假日（可扩展为读取节假日文件）
    holidays = [
        "2024-01-01", "2024-02-10", "2024-02-11", "2024-02-12", "2024-04-04",
        "2024-05-01", "2024-06-10", "2024-09-15", "2024-10-01", "2024-10-02",
        "2024-10-03", "2024-10-04", "2024-10-05"
    ]
    if dt.strftime("%Y-%m-%d") in holidays:
        return False
    return True

def calculate_days_between(start_time: Union[str, datetime], end_time: Union[str, datetime],
                           trading_day_only: bool = False, exchange: str = "SHFE") -> int:
    """
    计算两个时间的天数差
    :param trading_day_only: 是否仅计算交易日
    :return: 天数差
    """
    start = convert_time_format(start_time, target_format=None)
    end = convert_time_format(end_time, target_format=None)
    if start > end:
        start, end = end, start
    # 计算自然日差
    days = (end - start).days
    if not trading_day_only:
        return days
    # 计算交易日差
    trading_days = 0
    current = start
    while current <= end:
        if is_trading_day(current, exchange):
            trading_days += 1
        current += timedelta(days=1)
    return trading_days

# 测试代码
if __name__ == "__main__":
    print(convert_time_format("20230101", "%Y-%m-%d"))
    print(generate_date_range("2023-01-01", "2023-01-05"))
    print(is_trading_day("2024-10-01"))
    print(calculate_days_between("2023-01-01", "2023-01-10", trading_day_only=True))