import pandas as pd
import os
import sys
from joblib import Parallel, delayed
from tqdm import tqdm
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data import DataProviderFactory




def get_option_contract():
    """GET 期权合约信息 From Oracle"""
    data_provider = DataProviderFactory.create_provider("oracle")
    option_contract = data_provider.get_data(
        table_name="OPTIONCONTRACT",
        fields=["windcode", "underlying_code", "option_type", "maturity_date", "strike_price", "first_trading_date"],
        instruments=None,  # 获取所有期权合约
        start_date=None,
        end_date=None,
        return_sql=False,
        ignore_date_field=True  # 忽略日期字段检查
    )
    return option_contract

print("期权合约信息获取成功，数据量：", len(get_option_contract()))