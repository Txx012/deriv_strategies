'''计算希腊字母'''
import pandas as pd
import numpy as np
from tqdm import tqdm
from joblib import Parallel, delayed
from scipy.stats import norm
from scipy.optimize import brentq, newton
from itertools import product
import warnings
warnings.filterwarnings('ignore')
import numba
import math


class GreekCalculator:
    """期权计算希腊字母计算器 - 基于Black-Scholes模型的高性能实现
    入参: 期权行情数据(dataframe), 标的行情数据(dataframe), 无风险利率(float)
    出参: 包含希腊字母的dataframe(Delta, Gamma, Vega, Theta, Rho, IV)"""
    def __init__(self, option_data: pd.DataFrame, underlying_data: pd.DataFrame, risk_free_rate: float):
        self.option_data = option_data
        self.underlying_data = underlying_data
        self.risk_free_rate = risk_free_rate

    @numba.njit
    def normal_pdf_high_precision(self, x):
        """高精度PDF实现（误差<1e-7）"""
        a1 = 0.254829592
        a2 = -0.284496736
        a3 = 1.421413741
        a4 = -1.453152027
        a5 = 1.061405429
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x) / np.sqrt(2.0)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - ((((a5 * t + a4) * t
                        + a3) * t + a2) * t + a1) * t * np.exp(-x * x)
        return (1.0 / np.sqrt(2.0 * np.pi)) * np.exp(-0.5 * x * x) * (1.0 + sign * y)
    @numba.njit
    def bs_price_single(self, S_i, K_i, T_i, r, sigma_i, option_type_i):
        """为单个期权定价的Numba兼容函数"""
        if S_i <= 1e-12 or K_i <= 1e-12 or T_i <= 1e-12 or sigma_i <= 1e-12:
            return np.nan
        log_sk = np.log(S_i / K_i)
        sqrt_T = np.sqrt(T_i)
        d1 = (log_sk + (r + 0.5 * sigma_i ** 2) * T_i) / (sigma_i * sqrt_T + 1e-12)
        d2 = d1 - sigma_i * sqrt_T
        exp_term = np.exp(-r * T_i)
        if option_type_i == 0:  # Call
            price = S_i * self.norm_cdf_high_precision(d1) - K_i * exp_term * self.norm_cdf_high_precision(d2)
        else:  # Put
            price = K_i * exp_term * self.norm_cdf_high_precision(-d2) - S_i * self.norm_cdf_high_precision(-d1)
        return price
    @numba.njit
    def objective_function(self, sigma, S_i, K_i, T_i, r, market_price_i, option_type_i):
        return self.bs_price_single(S_i, K_i, T_i, r, sigma, option_type_i) - market_price_i
    def solve_iv(self, S, K, T, r, market_price, option_type):
        option_type_int = np.where(option_type == 'C', 0, np.where(option_type == 'P', 1, None))
        valid_mask = (S > 1e-10) & (K > 1e-10) & (T > 1e-10) & (market_price > 1e-10)
        iv = np.empty_like(S)
        for i in range(len(S)):
            if valid_mask[i]:
                S_i = S[i]
                K_i = K[i]
                T_i = T[i]
                mp_i = market_price[i]
                opt_type = option_type_int[i]
                lower = 1e-6
                upper = 10.0    # 阈值上限改成 10.0
                try:
                    def obj(sig):
                        return self.objective_function(sig, S_i, K_i, T_i, r, mp_i, opt_type)
                    fa = obj(lower)
                    fb = obj(upper)
                    if fa * fb > 0:
                        iv[i] = 0
                        continue
                    iv[i] = brentq(obj, lower, upper, xtol=1e-6, maxiter=100)
                except:
                    iv[i] = np.nan
            else:
                iv[i] = np.nan
        return iv
    
    def calculate_greeks(self) -> pd.DataFrame:
        """计算希腊字母的主函数"""
        merged = pd.merge(self.option_data, self.underlying_data, on='datetime', how='inner')
        S = merged['underlying_price'].values
        K = merged['strike'].values
        T = merged['time_to_expiry'].values / 365.0
        r = self.risk_free_rate
        market_price = merged['option_price'].values
        option_type = merged['option_type'].values
        iv = self.solve_iv(S, K, T, r, market_price, option_type)
        d1 = (np.log(S / K) + (r + 0.5 * iv ** 2) * T) / (iv * np.sqrt(T) + 1e-12)
        d2 = d1 - iv * np.sqrt(T)
        N_d1, N_d2 = norm.cdf(d1), norm.cdf(d2)
        merged['Delta'] = np.where(option_type == 'C', N_d1, N_d1 - 1)
        merged['Gamma'] = norm.pdf(d1) / (S * iv * np.sqrt(T) + 1e-12)
        merged['Vega'] = S * norm.pdf(d1) * np.sqrt(T)
        merged['Theta'] = np.where(option_type == 'C', (- S * norm.pdf(d1) * iv / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * N_d2) / 365,
                                        (- S * norm.pdf(d1) * iv / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * (1 - N_d2)) / 365)
        merged['Rho'] = np.where(option_type == 'C', K * T * np.exp(-r * T) * N_d2 / 100, -K * T * np.exp(-r * T) * (1 - N_d2) / 100)
        merged['IV'] = iv
        return merged[['datetime', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho', 'IV']]
    
        
    