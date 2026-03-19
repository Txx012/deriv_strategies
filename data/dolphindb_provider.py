from abc import ABC
from typing import List, Optional, Dict, Union, Tuple  # 补充Tuple导入
import dolphindb
import pandas as pd
from .base_provider import BaseDataProvider
from .exceptions import QueryError, DatabaseTypeNotSupportedError
import atexit
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DolphinDBDataProvider(BaseDataProvider, ABC):
    """DolphinDB数据源实现（对齐Wind接口格式，仅保留有效表）"""
    # 表名-端口映射关系（核心配置，与YAML表名对应）
    TABLE_PORT_MAP = {
        "StockL2Snap": 8902,  # 原有表，对应port: 8902
        "FutureL2": 8902,  # 原有表，对应port: 8902
        "DayLine": 9903,  # 新增表，默认对应port_kl: 9903（将从YAML读取覆盖）
        "MinuteLine": 9903  # 新增表，默认对应port_kl: 9903（将从YAML读取覆盖）
    }
    def __init__(self):
        super().__init__(source_type="dolphindb")
        # 校验数据库类型
        if self.connection_config.get("type") != "dolphindb":
            raise DatabaseTypeNotSupportedError(self.connection_config.get("type"))
        # 读取DolphinDB专属配置
        self.dfs_path = self.connection_config.get("dfs_path", "dfs://")
        self.ip = self.connection_config.get("ip")
        # self.port = self.connection_config.get("port")
        self.default_port = self.connection_config.get("port", 8902)  # 对应YAML port: 8902
        self.kl_port = self.connection_config.get("port_kl", 9903)  # 关键：读取YAML中的port_kl: 9903
        self.username = self.connection_config.get("username")
        self.password = self.connection_config.get("password")
        self._conn = None  # 连接缓存
        self._conn_lock = threading.Lock()  # 连接锁，用于线程安全
        self._last_used_time = 0  # 记录连接最后使用时间
        self._max_idle_time = 300  # 最大空闲时间（秒），超过则关闭连接

        # 注册退出时清理连接
        atexit.register(self.close_connection)

        # 市场后缀映射（对齐Wind标的格式）
        self.MARKET_SUFFIX_MAP = {
            ".SH": "SH",
            ".SZ": "SZ",
            ".CFE": "CFE",  # 期货市场后缀
            ".CSI":"CSI"
        }
        self.TABLE_PORT_MAP["MinuteLine"] = self.kl_port
        self.TABLE_PORT_MAP["DayLine"] = self.kl_port
        for table in ["StockL2Snap", "FutureL2"]:
            self.TABLE_PORT_MAP[table] = self.default_port

    def close_connection(self):
        """关闭连接"""
        with self._conn_lock:
            if self._conn:
                try:
                    self._conn.close()
                    # print("✅ DolphinDB连接已关闭")
                except Exception as e:
                    print(f"⚠️ 关闭连接时发生错误: {e}")
                finally:
                    self._conn = None

    def _get_connection(self, port: int) -> dolphindb.Session:
        """建立DolphinDB连接（自动重连）"""
        port = int(port)
        if not self._conn or not self._is_conn_alive():
            try:
                conn = dolphindb.session()
                conn.connect(self.ip, port, self.username, self.password)
                self._conn = conn
                # print("✅ DolphinDB连接成功")
            except Exception as e:
                raise QueryError(f"DolphinDB连接失败：{str(e)}")
        return self._conn

    def _is_conn_alive(self) -> bool:
        """检查连接有效性"""
        try:
            if self._conn:
                self._conn.run("1+1")  # 执行简单测试
                return True
            return False
        except:
            return False

    def _parse_instruments(self, instruments: List[str]) -> Tuple[List[str], Optional[str]]:
        """解析标的列表（对齐Wind格式：600000.SH → 纯代码+市场）"""
        if not instruments:
            return [], None

        pure_codes = []
        markets = set()

        for instr in instruments:
            # 提取市场后缀（如 .SH/.SZ/.CFE）
            matched_suffix = None
            for suffix in self.MARKET_SUFFIX_MAP.keys():
                if instr.endswith(suffix):
                    matched_suffix = suffix
                    break

            if not matched_suffix:
                raise QueryError(f"标的格式错误：{instr}，支持后缀：{list(self.MARKET_SUFFIX_MAP.keys())}")

            # 提取纯代码（去除后缀）
            pure_code = instr[:-len(matched_suffix)]
            pure_codes.append(pure_code)

            # 记录市场（股票需统一市场，期货无需）
            market = self.MARKET_SUFFIX_MAP[matched_suffix]
            markets.add(market)

        # 股票表（StockL2Snap）需确保所有标的属于同一市场
        if len(markets) > 1 and self._current_table_name == "StockL2Snap":
            raise QueryError(f"股票L2查询不支持跨市场：{markets}，请按市场分批次查询")

        # 返回纯代码列表和唯一市场（无则返回None）
        return pure_codes, markets.pop() if markets else None

    def _convert_date_format(self, date_str: str) -> str:
        """日期格式转换：yyyymmdd → YYYY.MM.DD（DolphinDB兼容格式）"""
        if not date_str:
            return ""
        try:
            # 校验日期长度（必须8位）
            if len(date_str) != 8:
                raise ValueError("日期长度错误")
            # 拆分年、月、日
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            # 转换为 YYYY.MM.DD
            return f"{year}.{month}.{day}"
        except Exception:
            raise QueryError(f"日期格式错误：{date_str}，请使用 yyyymmdd 格式（如 20230101）")

    def _format_date_condition(self, field: str, start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> str:
        """格式化日期条件（兼容外部yyyymmdd格式）"""
        # 转换日期格式
        start_date_conv = self._convert_date_format(start_date) if start_date else None
        end_date_conv = self._convert_date_format(end_date) if end_date else None

        if start_date_conv and end_date_conv:
            return f"date({field}) >= {start_date_conv} and date({field}) <= {end_date_conv}"
        elif start_date_conv:
            return f"date({field}) >= {start_date_conv}"
        elif end_date_conv:
            return f"date({field}) <= {end_date_conv}"
        return ""

    def _format_in_condition(self, field: str, values: List[str]) -> str:
        """格式化IN条件（字符串用单引号）"""
        values_str = "'" + "', '".join(values) + "'"
        return f"{field} in ({values_str})"

    def _get_qualified_table_name(self, table_name: str, market: Optional[str] = None) -> str:
        """获取DFS表完整路径（仅保留有效表）"""
        if table_name == "StockL2Snap":

            if not market:
                raise QueryError("股票L2查询必须指定市场（通过instruments后缀自动提取）")
            if market.upper() == "SZ":
                return f"loadTable('{self.dfs_path}StockL2', 'SZSnap')"
            elif market.upper() == "SH":
                return f"loadTable('{self.dfs_path}StockL2', 'SHSnap')"
            else:
                raise QueryError(f"股票L2不支持市场类型：{market}（仅支持SZ/SH）")
        elif table_name == "FutureL2":
            return f"loadTable('{self.dfs_path}FutureL2', 'CFESnap')"
        elif table_name == "DayLine":
            if not market:
                raise QueryError("DayLine查询必须指定市场（通过instruments后缀自动提取）")
            my_market = market.upper()
            if my_market not in ["SH", "SZ", "BJ", "CSI", "CF", "CZC", "DCE", "GF", "SHF", "NEEQ", "HK", "Nasdaq", "Nyse", "GI"]:
                raise QueryError(f"DayLine不支持市场类型：{my_market}（仅支持SH/SZ/BJ/CSI/CF/CZC/DCE/GF/SHF/NEEQ/HK/Nasdaq/Nyse/GI）")
            return f"loadTable('dfs://DayLine', '{my_market}DayLine')"
        elif table_name == "MinuteLine":
            if not market:
                raise QueryError("MinuteLine查询必须指定市场（通过instruments后缀自动提取）")
            my_market = market.upper()
            if my_market not in ["SH", "SZ", "BJ", "CSI", "CF", "CZC", "DCE", "GF", "SHF", "NEEQ", "HK", "Nasdaq", "Nyse", "GI"]:
                raise QueryError(f"MinuteLine不支持市场类型：{my_market}（仅支持SH/SZ/BJ/CSI/CF/CZC/DCE/GF/SHF/NEEQ/HK/Nasdaq/Nyse/GI）")
            return f"loadTable('{self.dfs_path}MinuteLine', '{my_market}MinuteLine')"
        else:
            raise QueryError(f"未配置或不存在的表：{table_name}（支持表：StockL2Snap、FutureL2、DayLine、MinuteLine）")

    def get_data(
            self,
            table_name: str,
            fields: Optional[List[str]] = None,
            instruments: Optional[List[str]] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            return_sql: bool = False, **kwargs
    ) -> Union[pd.DataFrame, str]:
        """统一查询接口（空结果保留列结构）"""
        self._current_table_name = table_name

        # 1. 校验表配置
        if table_name not in self.TABLE_PORT_MAP:
            raise QueryError(f"不支持的表名：{table_name}，支持表：{list(self.TABLE_PORT_MAP.keys())}")
        target_port = self.TABLE_PORT_MAP[table_name]
        if table_name not in self.field_mappings:
            raise QueryError(f"未配置表 '{table_name}' 的字段映射（支持表：{list(self.field_mappings.keys())}）")
        table_field_map = self.field_mappings[table_name]

        # 2. 校验日期字段
        if self.DATE_ALIAS not in table_field_map:
            raise QueryError(f"表 '{table_name}' 缺少日期字段映射（{self.DATE_ALIAS}）")
        actual_date_field = table_field_map[self.DATE_ALIAS]

        # 3. 处理查询字段（核心修改：添加 AS 重命名，记录业务别名）
        if not fields:
            fields = list(table_field_map.keys())  # fields 是业务别名列表
        invalid_fields = [f for f in fields if f not in table_field_map]
        if invalid_fields:
            raise QueryError(f"无效字段：{invalid_fields}，支持字段：{list(table_field_map.keys())}")

        # 🌟 关键：构建 "数据库字段 AS 业务别名" 格式（对齐 Wind 逻辑）
        db_fields = [f"{table_field_map[f]} AS \"{f}\"" for f in fields]
        select_clause = ", ".join(db_fields)
        # 记录业务别名（用于空结果时构建列）
        business_fields = fields.copy()

        # 4. 解析标的和市场
        pure_codes = []
        market = None
        if instruments:
            pure_codes, market = self._parse_instruments(instruments)

        # 5. 构建查询条件
        where_clauses = []
        if pure_codes:
            sec_field = table_field_map.get("windcode")
            if not sec_field:
                raise QueryError(f"表 '{table_name}' 缺少标的字段映射（windcode）")
            where_clauses.append(self._format_in_condition(sec_field, pure_codes))
        if start_date or end_date:
            where_clauses.append(self._format_date_condition(actual_date_field, start_date, end_date))
        where_clause = "where " + " and ".join(where_clauses) if where_clauses else ""

        # 6. 构建SQL
        qualified_table = self._get_qualified_table_name(table_name, market)
        sql = f"select {select_clause} from {qualified_table} {where_clause}".strip()

        # 7. 按需返回SQL或执行查询（传递业务别名用于空结果列结构）
        if return_sql:
            return sql
        return self._execute_query(sql, business_fields, target_port)  # 传递查询的字段名

    def _execute_query(self, sql: str, business_fields: List[str], port: int) -> pd.DataFrame:
        """执行查询（空结果保留列结构）"""
        try:
            # conn = self._get_connection()
            conn = self._get_connection(port)
            # print(f"[dolphindb] 执行SQL：\n{sql}\n")

            result = conn.run(sql)
            result['trade_date'] = result.get('DateTime') or result.get('TradeDate') or result.get('trade_date')

            # 类型1：已是pandas DataFrame
            if isinstance(result, pd.DataFrame):
                if result.empty:
                    logger.warning("查询无结果，返回空结构DataFrame")
                    return pd.DataFrame(columns=business_fields)  # 用查询字段构建空DataFrame
                return result

            # 类型2：DolphinDB原生Table（有toDF()方法）
            elif hasattr(result, "toDF"):
                df = result.toDF()
                if df.empty:
                    logger.warning("查询无结果，返回空结构DataFrame")
                    return pd.DataFrame(columns=business_fields)
                return df

            # 类型3：多个Table（list/tuple包裹）
            elif isinstance(result, (list, tuple)) and len(result) > 0:
                valid_tables = [item for item in result if hasattr(item, "toDF")]
                if not valid_tables:
                    logger.warning("查询返回多个结果，但无有效Table")
                    return pd.DataFrame(columns=business_fields)
                df_list = [item.toDF() for item in valid_tables]
                df = pd.concat(df_list, ignore_index=True)
                if df.empty:
                    return pd.DataFrame(columns=business_fields)
                return df

            # 类型4：真正的scalar结果
            else:
                logger.warning(f"查询返回单个值：{result}")
                return pd.DataFrame({"result": [result]})

        except Exception as e:
            error_msg = f"查询失败：{str(e)}，SQL：\n{sql}\n"
            error_msg += f"提示：1. 检查SQL语法/表名/字段名；2. 检查DolphinDB服务状态；3. 确认客户端版本兼容"
            self.close_connection()
            raise QueryError(error_msg)