# -*- coding: utf-8 -*-
"""数学工具类：策略通用指标计算，无业务逻辑，纯数学计算"""
import pandas as pd
import numpy as np
from typing import List, Optional
import warnings
warnings.filterwarnings("ignore")

def calculate_ma(series: pd.Series, period: int, ma_type: str = "sma") -> pd.Series:
    """
    计算移动平均
    :param series: 价格序列（pd.Series）
    :param period: 周期
    :param ma_type: 类型 sma=简单移动平均/ema=指数移动平均
    :return: 移动平均序列
    """
    if period < 1 or len(series) < period:
        return pd.Series(np.nan, index=series.index)
    if ma_type.lower() == "ema":
        return series.ewm(span=period, adjust=False).mean()
    else:  # sma
        return series.rolling(window=period).mean()

def calculate_true_range(df: pd.DataFrame) -> pd.Series:
    """
    计算真实波动率TR（海龟策略核心）
    TR = max(今日最高价-今日最低价, |今日最高价-昨日收盘价|, |今日最低价-昨日收盘价|)
    :param df: 含high/low/close的DataFrame
    :return: TR序列
    """
    if not all(col in df.columns for col in ["high", "low", "close"]):
        raise ValueError("计算TR需要字段：high/low/close")
    tr1 = df["high"] - df["low"]
    tr2 = abs(df["high"] - df["close"].shift(1))
    tr3 = abs(df["low"] - df["close"].shift(1))
    tr = pd.Series(np.maximum(np.maximum(tr1, tr2), tr3), index=df.index)
    return tr.fillna(0)

def calculate_volatility(return_series: pd.Series, period: int = 20,
                         annualize: bool = True, trading_days: int = 252) -> pd.Series:
    """
    计算收益率波动率（年化/非年化）
    :param return_series: 收益率序列
    :param period: 滚动周期
    :param annualize: 是否年化
    :param trading_days: 年交易日数
    :return: 波动率序列
    """
    if period < 1 or len(return_series) < period:
        return pd.Series(np.nan, index=return_series.index)
    vol = return_series.rolling(window=period).std()
    if annualize:
        vol = vol * np.sqrt(trading_days)
    return vol

def calculate_max_drawdown(series: pd.Series) -> float:
    """
    计算最大回撤（单值，非序列）
    :param series: 净资产/收益率序列
    :return: 最大回撤值（负表示回撤）
    """
    cummax = series.cummax()
    drawdown = (series - cummax) / cummax
    return drawdown.min()

def calculate_sharpe(return_series: pd.Series, risk_free_rate: float = 0.02,
                     trading_days: int = 252) -> float:
    """
    计算夏普比率（单值）
    :param return_series: 日收益率序列
    :param risk_free_rate: 年化无风险利率
    :return: 夏普比率
    """
    daily_rf = risk_free_rate / trading_days
    excess_return = return_series - daily_rf
    sharpe = excess_return.mean() / excess_return.std() * np.sqrt(trading_days)
    return sharpe if not np.isnan(sharpe) else 0.0

# 测试代码
if __name__ == "__main__":
    # 生成测试数据
    test_series = pd.Series(np.cumsum(np.random.randn(100)) + 100)
    test_df = pd.DataFrame({
        "high": test_series + np.random.randn(100)*2,
        "low": test_series - np.random.randn(100)*2,
        "close": test_series
    })
    # 测试指标
    print("SMA20：", calculate_ma(test_series, 20).tail(1).values[0])
    print("TR：", calculate_true_range(test_df).tail(1).values[0])
    print("年化波动率：", calculate_volatility(test_series.pct_change(), 20).tail(1).values[0])
    print("最大回撤：", calculate_max_drawdown(test_series))
    print("夏普比率：", calculate_sharpe(test_series.pct_change()))