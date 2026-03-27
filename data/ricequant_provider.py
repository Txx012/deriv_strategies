# ricequant_provider.py
from typing import List, Optional, Union, Dict, Any
import pandas as pd
import rqdatac as rq
from .base_provider import BaseDataProvider
from .exceptions import QueryError, DatabaseTypeNotSupportedError, InvalidParameterError


class RiceQuantDataProvider(BaseDataProvider):
    """RiceQuant（米筐）数据源实现 - 对齐官方文档"""

    def __init__(self):
        super().__init__(source_type="ricequant")
        if self.connection_config.get("type") != "ricequant":
            raise DatabaseTypeNotSupportedError(self.connection_config.get("type"))

        self.username = self.connection_config.get("user", "license")
        self.password = self.connection_config.get("password", "")
        self._conn = None
        self._extra_params = {}
        self._current_table_name = None
        self._query_fields = []
        self._query_instruments = []
        self._query_start_date = None
        self._query_end_date = None

        # 日期格式转换映射
        self.DATE_FORMAT_IN = "%Y%m%d"      # 输入格式 yyyymmdd
        self.DATE_FORMAT_OUT = "%Y-%m-%d"   # RiceQuant 格式

        self._init_rq_connection()

        # 市场后缀映射（Wind格式 -> RiceQuant格式），从配置中读取
        self.MARKET_SUFFIX_MAP = self.connection_config.get("market_suffix_map", {
            ".SH": ".XSHG",
            ".SZ": ".XSHE",
            ".HK": ".XHKG",
            ".CFE": ".CFE",
            ".CF": ".CFE",
        })

        # 反向映射（RiceQuant格式 -> Wind格式），用于导出时转换
        self.REVERSE_MARKET_SUFFIX_MAP = {v: k for k, v in self.MARKET_SUFFIX_MAP.items()}

        # 表名对应的默认频率
        self.TABLE_FREQ = {
            "AShareEODPrices": "1d",
            "DayLine": "1d",
            "MinuteLine": "1m",
            "OptionEODPrices": "1d",
            "OptionMinute": "1m",
        }

    def _init_rq_connection(self):
        """初始化RiceQuant连接"""
        try:
            rq.init(self.username, self.password)
            self._conn = rq
            print("✅ RiceQuant连接成功")
        except Exception as e:
            raise QueryError(f"RiceQuant连接失败：{str(e)}")

    def _get_connection(self) -> object:
        """获取RiceQuant连接"""
        if not self._conn:
            self._init_rq_connection()
        return self._conn

    def _is_conn_alive(self) -> bool:
        """检查连接有效性"""
        try:
            self._conn.get_securities_count()
            return True
        except:
            return False

    def _convert_wind_code_to_rq(self, windcode: str) -> str:
        """将Wind格式代码转换为RiceQuant格式"""
        if not isinstance(windcode, str):
            return windcode
        for suffix, rq_suffix in self.MARKET_SUFFIX_MAP.items():
            if windcode.endswith(suffix):
                return windcode[:-len(suffix)] + rq_suffix
        return windcode

    def _convert_rq_code_to_wind(self, rq_code: str) -> str:
        """将RiceQuant格式代码转换回Wind格式"""
        if not isinstance(rq_code, str):
            return rq_code
        for rq_suffix, wind_suffix in self.REVERSE_MARKET_SUFFIX_MAP.items():
            if rq_code.endswith(rq_suffix):
                return rq_code[:-len(rq_suffix)] + wind_suffix
        return rq_code

    def _convert_instruments(self, instruments: List[str]) -> List[str]:
        """转换标的列表（Wind格式 -> RiceQuant格式）"""
        return [self._convert_wind_code_to_rq(instr) for instr in instruments]

    def _convert_instruments_to_wind(self, instruments: List[str]) -> List[str]:
        """转换标的列表（RiceQuant格式 -> Wind格式）"""
        return [self._convert_rq_code_to_wind(instr) for instr in instruments]

    def _convert_date(self, date_str: Optional[str]) -> Optional[str]:
        """转换日期格式 yyyymmdd -> YYYY-MM-DD"""
        if not date_str:
            return None
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    def _get_fields_mapping(self, table_name: str, fields: List[str]) -> Dict[str, str]:
        """获取字段映射：业务字段 -> 数据库字段"""
        table_map = self.field_mappings.get(table_name, {})
        return {field: table_map.get(field, field) for field in fields}

    def _map_result_fields(self, df: pd.DataFrame, table_name: str, fields: List[str]) -> pd.DataFrame:
        """将RiceQuant返回的字段映射为业务字段名"""
        if df.empty:
            return pd.DataFrame(columns=fields)

        table_map = self.field_mappings.get(table_name, {})
        # 构建反向映射：RiceQuant字段 -> 业务字段
        reverse_map = {v: k for k, v in table_map.items() if v in df.columns}
        df.rename(columns=reverse_map, inplace=True)

        # 分钟线特殊处理：将 time 列重命名为 trade_date（如果需要）
        if table_name == "MinuteLine" and "time" in df.columns and "trade_date" not in df.columns:
            df.rename(columns={"time": "trade_date"}, inplace=True)

        # 将 windcode/order_book_id 从 RiceQuant 格式转换回 Wind 格式
        code_column = None
        if "windcode" in df.columns:
            code_column = "windcode"
        elif "order_book_id" in df.columns:
            code_column = "order_book_id"

        if code_column:
            df[code_column] = df[code_column].apply(self._convert_rq_code_to_wind)
            # 如果是 order_book_id，同时重命名为 windcode
            if code_column == "order_book_id":
                df.rename(columns={"order_book_id": "windcode"}, inplace=True)

        # 只保留需要的字段
        valid_fields = [f for f in fields if f in df.columns]
        return df[valid_fields]

    def _get_price_data(self, table_name: str, instruments: List[str],
                        start_date: Optional[str], end_date: Optional[str],
                        fields: List[str], **kwargs) -> pd.DataFrame:
        """通用 get_price 调用"""
        rq_conn = self._get_connection()

        # 转换标的代码
        rq_instruments = self._convert_instruments(instruments) if instruments else None

        # 转换日期
        start = self._convert_date(start_date)
        end = self._convert_date(end_date)

        # 频率
        frequency = kwargs.get("frequency", self.TABLE_FREQ.get(table_name, "1d"))

        # 获取字段映射
        field_map = self._get_fields_mapping(table_name, fields)

        # 构建 RiceQuant 字段列表，排除索引字段
        excluded_fields = {'order_book_id', 'date', 'time', 'datetime'}
        rq_fields = []
        for biz_field, db_field in field_map.items():
            if db_field not in excluded_fields:
                rq_fields.append(db_field)
        rq_fields = list(set(rq_fields))

        # 其他参数
        adjust_type = kwargs.get("adjust_type", "none")

        # 调用 get_price
        df = rq_conn.get_price(
            order_book_ids=rq_instruments,
            start_date=start,
            end_date=end,
            frequency=frequency,
            fields=rq_fields if rq_fields else None,
            adjust_type=adjust_type,
        )

        # 重置索引，将 order_book_id 和 date/time 变为列
        if df is not None and not df.empty:
            df = df.reset_index()

        # 字段映射
        if df is not None and not df.empty:
            df = self._map_result_fields(df, table_name, fields)

        return df if df is not None else pd.DataFrame(columns=fields)

    def _get_trading_dates(self, start_date: Optional[str], end_date: Optional[str],
                           fields: List[str]) -> pd.DataFrame:
        """获取交易日历"""
        rq_conn = self._get_connection()
        start = self._convert_date(start_date)
        end = self._convert_date(end_date)

        dates = rq_conn.get_trading_dates(start_date=start, end_date=end)
        df = pd.DataFrame(dates, columns=["trade_date"])
        return df[fields] if fields else df

    def _get_instruments_info(self, instruments: List[str], fields: List[str]) -> pd.DataFrame:
        """获取合约详细信息"""
        rq_conn = self._get_connection()
        rq_instruments = self._convert_instruments(instruments)

        results = []
        for code in rq_instruments:
            instr = rq_conn.instruments(code)
            if instr:
                data = {
                    "windcode": code,
                    "symbol": getattr(instr, "symbol", ""),
                    "abbrev_symbol": getattr(instr, "abbrev_symbol", ""),
                    "listed_date": getattr(instr, "listed_date", None),
                    "de_listed_date": getattr(instr, "de_listed_date", None),
                    "exchange": getattr(instr, "exchange", ""),
                    "type": getattr(instr, "type", ""),
                    "round_lot": getattr(instr, "round_lot", 1),
                    "sector_code": getattr(instr, "sector_code", ""),
                    "industry_code": getattr(instr, "industry_code", ""),
                }
                results.append(data)

        df = pd.DataFrame(results)
        return self._map_result_fields(df, "instruments", fields) if not df.empty else pd.DataFrame(columns=fields)

    def _get_option_contracts(self, underlying: str, start_date: Optional[str],
                              fields: List[str]) -> pd.DataFrame:
        """获取期权合约"""
        rq_conn = self._get_connection()
        rq_underlying = self._convert_wind_code_to_rq(underlying)

        contracts = rq_conn.options.get_contracts(
            underlying=rq_underlying,
            trading_date=self._convert_date(start_date),
            option_type=self._extra_params.get("option_type"),
            maturity=self._extra_params.get("maturity"),
            strike=self._extra_params.get("strike_price"),
        )

        df = pd.DataFrame(contracts, columns=["order_book_id"])
        df["windcode"] = df["order_book_id"]
        return self._map_result_fields(df, "OptionContracts", fields) if not df.empty else pd.DataFrame(columns=fields)

    def _get_option_greeks(self, instruments: List[str], start_date: Optional[str],
                           end_date: Optional[str], fields: List[str]) -> pd.DataFrame:
        """获取期权希腊字母"""
        rq_conn = self._get_connection()
        rq_instruments = self._convert_instruments(instruments)
        start = self._convert_date(start_date)
        end = self._convert_date(end_date)

        df = rq_conn.options.get_greeks(
            order_book_ids=rq_instruments,
            start_date=start,
            end_date=end,
            model=self._extra_params.get("model", "implied_forward"),
            price_type=self._extra_params.get("price_type", "close"),
        )

        if df is not None and not df.empty:
            df = df.reset_index()
            df = self._map_result_fields(df, "OptionGreeks", fields)

        return df if df is not None else pd.DataFrame(columns=fields)

    def _execute_query(self, sql: str) -> pd.DataFrame:
        """执行查询（重写实现，适配各表路由）"""
        table_name = self._current_table_name
        fields = self._query_fields
        instruments = self._query_instruments
        start_date = self._query_start_date
        end_date = self._query_end_date
        extra = self._extra_params

        # 根据表名路由到不同API
        if table_name in ["AShareEODPrices", "DayLine", "MinuteLine", "OptionEODPrices", "OptionMinute"]:
            return self._get_price_data(table_name, instruments, start_date, end_date, fields, **extra)

        elif table_name == "AShareCalendar":
            return self._get_trading_dates(start_date, end_date, fields)

        elif table_name == "instruments":
            return self._get_instruments_info(instruments, fields)

        elif table_name == "OptionContracts":
            underlying = instruments[0] if instruments else None
            if not underlying:
                raise QueryError("期权合约查询需要传入 underlying_code")
            return self._get_option_contracts(underlying, start_date, fields)

        elif table_name == "OptionGreeks":
            if not instruments:
                raise QueryError("期权希腊字母查询需要传入合约代码")
            return self._get_option_greeks(instruments, start_date, end_date, fields)

        elif table_name == "OptionDominantMonth":
            rq_conn = self._get_connection()
            rq_underlying = self._convert_instruments(instruments)
            start = self._convert_date(start_date)
            end = self._convert_date(end_date)

            series = rq_conn.options.get_dominant_month(
                underlying_symbol=rq_underlying,
                start_date=start,
                end_date=end,
                rule=extra.get("rule", 0),
                rank=extra.get("rank", 1),
            )
            df = series.reset_index(name="dominant")
            df.rename(columns={"index": "trade_date"}, inplace=True)
            return self._map_result_fields(df, table_name, fields)

        elif table_name == "current_snapshot":
            rq_conn = self._get_connection()
            rq_instruments = self._convert_instruments(instruments)
            result = rq_conn.current_snapshot(rq_instruments)
            if isinstance(result, list):
                df = pd.DataFrame([{
                    "windcode": getattr(x, "order_book_id", ""),
                    "last": getattr(x, "last", None),
                    "open": getattr(x, "open", None),
                    "high": getattr(x, "high", None),
                    "low": getattr(x, "low", None),
                    "volume": getattr(x, "volume", None),
                    "total_turnover": getattr(x, "total_turnover", None),
                    "prev_close": getattr(x, "prev_close", None),
                    "limit_up": getattr(x, "limit_up", None),
                    "limit_down": getattr(x, "limit_down", None),
                } for x in result])
                return self._map_result_fields(df, table_name, fields)
            return pd.DataFrame(columns=fields)

        else:
            raise QueryError(f"RiceQuant不支持查询表：{table_name}")

    # ========== 实现基类抽象方法 ==========
    def _format_date_condition(self, field: str, start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> str:
        self._query_start_date = start_date
        self._query_end_date = end_date
        return ""

    def _format_in_condition(self, field: str, values: List[str]) -> str:
        self._query_instruments = values
        return ""

    def get_data(
            self,
            table_name: str,
            fields: Optional[List[str]] = None,
            instruments: Optional[List[str]] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            return_sql: bool = False,
            ignore_date_field: bool = False,
            **kwargs
    ) -> Union[pd.DataFrame, str]:
        self._current_table_name = table_name
        self._query_fields = fields or []
        self._query_start_date = start_date
        self._query_end_date = end_date
        self._query_instruments = instruments or []
        self._extra_params = kwargs

        if not ignore_date_field:
            self._format_date_condition("trade_date", start_date, end_date)
        if instruments:
            self._format_in_condition("windcode", instruments)

        if table_name not in self.field_mappings and table_name not in ["instruments", "current_snapshot"]:
            raise InvalidParameterError(f"RiceQuant未配置表 '{table_name}' 的字段映射")

        mock_sql = f"RiceQuant.{table_name}("
        if instruments:
            mock_sql += f"instruments={instruments}, "
        if start_date:
            mock_sql += f"start_date={start_date}, "
        if end_date:
            mock_sql += f"end_date={end_date}, "
        if fields:
            mock_sql += f"fields={fields}"
        mock_sql += ")"

        if return_sql:
            return mock_sql

        return self._execute_query(mock_sql)

    def close_connection(self):
        if self._conn:
            self._conn = None
            print("✅ RiceQuant连接已重置")