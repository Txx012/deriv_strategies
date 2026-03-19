from typing import List, Optional, Union, Dict
import pandas as pd
import rqdatac as rq
from .base_provider import BaseDataProvider
from .exceptions import QueryError, DatabaseTypeNotSupportedError, InvalidParameterError


class RiceQuantDataProvider(BaseDataProvider):
    """RiceQuant（米筐）数据源实现"""

    def __init__(self):
        super().__init__(source_type="ricequant")
        # 校验数据源类型配置
        if self.connection_config.get("type") != "ricequant":
            raise DatabaseTypeNotSupportedError(self.connection_config.get("type"))
        
        # 初始化RiceQuant连接配置
        self.username = self.connection_config.get("user", "license")
        self.password = self.connection_config.get("password", "")
        self._conn = None  # 缓存连接状态
        self._init_rq_connection()
        self._extra_params = {} 

        # RiceQuant字段映射特殊处理：兼容Wind字段名习惯
        self.RQ_FIELD_ALIAS = {
            "trade_date": "date",
            "windcode": "order_book_id",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "total_turnover",
            "pre_close": "prev_close"
        }

    def _init_rq_connection(self):
        """初始化RiceQuant连接"""
        try:
            # 初始化连接（支持空密码/默认license模式）
            rq.init(self.username, self.password)
            self._conn = rq  # 将rq实例作为连接缓存
            print("✅ RiceQuant连接成功")
        except Exception as e:
            raise QueryError(f"RiceQuant连接失败：{str(e)}")

    def _get_connection(self) -> object:
        """获取RiceQuant连接（实现抽象方法）"""
        if not self._conn or not self._is_conn_alive():
            self._init_rq_connection()
        return self._conn

    def _is_conn_alive(self) -> bool:
        """检查RiceQuant连接有效性（实现抽象方法）"""
        try:
            # 执行简单查询验证连接
            self._conn.get_securities_count()
            return True
        except:
            return False

    def _format_date_condition(self, field: str, start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> str:
        """格式化日期条件（RiceQuant用Python参数处理，此处仅做格式校验）"""
        # RiceQuant的日期格式为 'YYYY-MM-DD'，需转换输入的yyyymmdd格式
        def convert_date(date_str):
            if not date_str:
                return None
            if len(date_str) == 8:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            return date_str

        self.start_date_conv = convert_date(start_date)
        self.end_date_conv = convert_date(end_date)
        return ""  # RiceQuant无需拼接SQL条件，返回空字符串

    def _format_in_condition(self, field: str, values: List[str]) -> str:
        """格式化IN条件（RiceQuant用Python列表处理）"""
        # 转换Wind格式代码到RiceQuant格式（600000.SH → 600000.XSHG）
        self.instrument_list = [
            ins.replace(".SH", ".XSHG").replace(".SZ", ".XSHE") 
            for ins in values
        ]
        return ""  # RiceQuant无需拼接SQL条件，返回空字符串

    def _execute_query(self, sql: str) -> pd.DataFrame:
        """执行RiceQuant查询（重写实现，适配API调用）"""
        # RiceQuant不使用SQL，此处sql参数仅用于日志
        try:
            # 根据表名路由到不同的RiceQuant API
            table_name = self._current_table_name
            rq_conn = self._get_connection()

            # 构建查询参数
            query_kwargs = {
                "order_book_ids": self.instrument_list if hasattr(self, "instrument_list") else None,
                "start_date": self.start_date_conv if hasattr(self, "start_date_conv") else None,
                "end_date": self.end_date_conv if hasattr(self, "end_date_conv") else None,
            }

            # 核心：根据表名调用对应的RiceQuant API
            if table_name == "AShareEODPrices":
                # A股日行情数据
                df = rq_conn.get_price(**query_kwargs)
            # elif table_name == "MinuteLine":
            #     # 分钟线数据
            #     df = rq_conn.get_price(frequency="1m", **query_kwargs)
            elif table_name == "MinuteLine":
                df = rq_conn.get_price(
                    order_book_ids=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    frequency="1m",
                    fields=query_kwargs.get("fields"))
                if "price" in df.columns:
                    df.drop(columns="price", inplace=True)
            # === 新增：期权数据接口支持 ===
            elif table_name == "OptionContracts":
                # 获取标的代码：如果传入的是列表，取第一个元素；如果是字符串则直接用
                raw_underlying = query_kwargs.get("order_book_ids")
                underlying_str = raw_underlying[0] if isinstance(raw_underlying, list) else raw_underlying
                
                # 去除可能存在的后缀（如 CU2303.CFE 变为 CU），因为 underlying 通常指品种
                # 或者确保你传入的就是品种代码
                if underlying_str and "." in underlying_str:
                    underlying_str = underlying_str.split('.')[0]
                # 如果包含数字（如 CU2303），有些接口需要纯品种代码 'CU'
                import re
                underlying_code = re.sub(r'\d+', '', underlying_str)

                # 筛选期权合约
                contracts = rq_conn.options.get_contracts(
                    underlying=underlying_code,  # 确保这里是字符串 'CU'
                    option_type=self._extra_params.get("option_type"),
                    maturity=self._extra_params.get("maturity"),
                    strike=self._extra_params.get("strike"),
                    trading_date=query_kwargs.get("start_date")
                )
                # 注意：rq 返回的是 list，需要转换成 DataFrame 并映射字段
                df = pd.DataFrame(contracts, columns=["order_book_id"])
            elif table_name == "OptionEODPrices":
                # 期权日行情（使用通用 get_price）
                df = rq_conn.get_price(
                    order_book_ids=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    frequency="1d",
                    fields=query_kwargs.get("fields"))
            elif table_name == "OptionMinute":
                # 期权分钟行情
                df = rq_conn.get_price(
                    order_book_ids=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    frequency="1m",
                    fields=query_kwargs.get("fields"))
            elif table_name == "OptionGreeks":
                # 期权希腊字母
                df = rq_conn.options.get_greeks(
                    order_book_ids=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    fields=query_kwargs.get("fields"),
                    model=query_kwargs.get("model", "implied_forward"),
                    price_type=query_kwargs.get("price_type", "close"),
                    frequency=query_kwargs.get("frequency", "1d"),
                    market=query_kwargs.get("market", "cn"))
            elif table_name == "OptionContractProperty":
                # ETF期权合约属性
                df = rq_conn.options.get_contract_property(
                    order_book_ids=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    fields=query_kwargs.get("fields"))
            elif table_name == "OptionDominantMonth":
                # 期权主力月份
                series = rq_conn.options.get_dominant_month(
                    underlying_symbol=query_kwargs["order_book_ids"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    rule=query_kwargs.get("rule", 0),
                    rank=query_kwargs.get("rank", 1),
                    market=query_kwargs.get("market", "cn"))
                df = series.reset_index(name="dominant")
                df.rename(columns={"index": "date"}, inplace=True)
            elif table_name == "OptionIndicators":
                # 期权衍生指标
                df = rq_conn.options.get_indicators(
                    underlying_symbols=query_kwargs["order_book_ids"],
                    maturity=query_kwargs["maturity"],
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"],
                    fields=query_kwargs.get("fields"),
                    market=query_kwargs.get("market", "cn"))

            elif table_name == "DayLine":
                # 日线数据（兼容自定义表名）
                df = rq_conn.get_price(frequency="1d", **query_kwargs)
            elif table_name == "AShareCalendar":
                # 交易日历
                df = rq_conn.get_trading_dates(
                    start_date=query_kwargs["start_date"],
                    end_date=query_kwargs["end_date"]
                )
                df = pd.DataFrame(df, columns=["trade_date"])
            else:
                raise QueryError(f"RiceQuant不支持查询表：{table_name}")

            # 字段映射：对齐Wind字段名规范
            df = self._map_rq_fields_to_wind(df, table_name)
            
            # 确保日期字段统一为trade_date
            if "date" in df.columns:
                df.rename(columns={"date": "trade_date"}, inplace=True)
            
            return df.reset_index(drop=False)

        except Exception as e:
            error_msg = f"RiceQuant查询失败：{str(e)}，请求表：{self._current_table_name}"
            raise QueryError(error_msg)

    def _map_rq_fields_to_wind(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """将RiceQuant字段映射为Wind规范字段名"""
        if df.empty:
            return df
        
        # 获取当前表的字段映射配置
        table_field_map = self.field_mappings.get(table_name, {})
        
        # 反向映射：RQ字段 → Wind业务别名
        reverse_map = {}
        for wind_alias, rq_field in self.RQ_FIELD_ALIAS.items():
            if wind_alias in table_field_map:
                reverse_map[rq_field] = wind_alias

        # 重命名列
        df.rename(columns=reverse_map, inplace=True)
        
        # 只保留配置中定义的字段
        valid_fields = [f for f in table_field_map.keys() if f in df.columns]
        return df[valid_fields]

    def get_data(
            self,
            table_name: str,
            fields: Optional[List[str]] = None,
            instruments: Optional[List[str]] = None,
            return_sql: bool = False,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            custom_conditions: Optional[Dict[str, Union[str, int, float, List]]] = None,
            ignore_date_field: bool = False,
            **kwargs
    ) -> Union[pd.DataFrame, str]:
        """重写统一查询接口，适配RiceQuant API"""
        self._current_table_name = table_name
        self._extra_params = kwargs
        
        # 1. 校验表配置
        if table_name not in self.field_mappings:
            raise InvalidParameterError(f"RiceQuant未配置表 '{table_name}' 的字段映射")
        
        # 2. 处理标的代码
        if instruments:
            self._format_in_condition("windcode", instruments)
        
        # 3. 处理日期条件
        if not ignore_date_field and (start_date or end_date):
            self._format_date_condition("trade_date", start_date, end_date)
        
        # 4. 构建模拟SQL（用于return_sql=True场景）
        mock_sql = f"""
        SELECT {', '.join(fields) if fields else '*'}
        FROM RiceQuant.{table_name}
        WHERE instrument IN ({', '.join(self.instrument_list) if hasattr(self, 'instrument_list') else ''})
        AND trade_date BETWEEN '{self.start_date_conv}' AND '{self.end_date_conv}'
        """.strip()

        if return_sql:
            return mock_sql
        
        # 5. 执行查询
        return self._execute_query(mock_sql)

    def close_connection(self):
        """关闭RiceQuant连接（实现抽象方法）"""
        if self._conn:
            try:
                # RiceQuant没有显式关闭连接的API，重置连接状态即可
                self._conn = None
                print("✅ RiceQuant连接已重置")
            except Exception as e:
                print(f"⚠️ 关闭RiceQuant连接时发生错误: {e}")