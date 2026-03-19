from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union
import pandas as pd
from .config_loader import ConfigLoader
from .exceptions import InvalidParameterError, QueryError


class BaseDataProvider(ABC):
    """数据提供器抽象基类（所有数据源必须继承）"""

    def __init__(self, source_type: str = "oracle"):
        self.source_type = source_type
        self.config_loader = ConfigLoader()
        self.source_config = self.config_loader.get_data_source_config(source_type)
        self.field_mappings = self.config_loader.get_field_mappings(source_type)
        self.connection_config = self.source_config.get("connection", {})
        self._conn = None  # 数据库连接缓存
        self.DATE_ALIAS = "trade_date"  # 统一约定的日期字段业务别名

    @abstractmethod
    def _get_connection(self) -> object:
        """建立数据库连接（子类实现）"""
        pass

    @abstractmethod
    def _is_conn_alive(self) -> bool:
        """检查连接有效性（子类实现）"""
        pass

    @abstractmethod
    def _format_date_condition(self, field: str, start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> str:
        """格式化日期条件（子类实现）"""
        pass

    @abstractmethod
    def _format_in_condition(self, field: str, values: List[str]) -> str:
        """格式化IN条件（子类实现）"""
        pass

    def get_data(
            self,
            table_name: str,
            fields: Optional[List[str]] = None,
            instruments: Optional[List[str]] = None,
            return_sql: bool = False,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            custom_conditions: Optional[Dict[str, Union[str, int, float, List[Union[str, int, float]]]]] = None,
            ignore_date_field: bool = False,  # 新增：忽略日期字段检查
            **kwargs,
    ) -> Union[pd.DataFrame, str]:
        """统一数据获取接口（支持无日期字段表）"""
        # 1. 校验表配置
        if table_name not in self.field_mappings:
            raise InvalidParameterError(f"数据源 '{self.source_type}' 未配置表 '{table_name}' 的字段映射")
        table_field_map = self.field_mappings[table_name]

        # 2. 校验并获取日期字段实际名称（新增ignore_date_field参数控制）
        actual_date_field = None
        if not ignore_date_field:
            if self.DATE_ALIAS not in table_field_map:
                raise InvalidParameterError(
                    f"表 '{table_name}' 的字段映射缺少日期字段配置（需添加 '{self.DATE_ALIAS}' 别名）"
                )
            actual_date_field = table_field_map[self.DATE_ALIAS]

        # 3. 处理查询字段
        if not fields:
            fields = list(table_field_map.keys())
        invalid_fields = [f for f in fields if f not in table_field_map]
        if invalid_fields:
            raise InvalidParameterError(
                f"无效字段：{invalid_fields}，表 '{table_name}' 支持字段：{list(table_field_map.keys())}"
            )
        db_fields = [f"{table_field_map[f]} AS \"{f}\"" for f in fields]
        select_clause = ", ".join(db_fields)

        # 4. 构建查询条件
        where_clauses = []

        # 4.1 标的代码过滤
        if instruments:
            formatted_ins = self._format_instruments(instruments)
            instrument_db_field = None
            for alias, db_field in table_field_map.items():
                if db_field == "S_INFO_WINDCODE":
                    instrument_db_field = db_field
                    break
            if not instrument_db_field:
                raise InvalidParameterError(f"表 '{table_name}' 不支持标的代码过滤（缺少 S_INFO_WINDCODE 映射）")
            where_clauses.append(self._format_in_condition(instrument_db_field, formatted_ins))

        # 4.2 日期条件（仅当ignore_date_field=False且有日期参数时生效）
        if not ignore_date_field and (start_date or end_date) and actual_date_field:
            where_clauses.append(self._format_date_condition(actual_date_field, start_date, end_date))

        # 4.3 自定义条件
        if custom_conditions:
            for field_alias, value in custom_conditions.items():
                if field_alias not in table_field_map:
                    raise InvalidParameterError(
                        f"自定义条件字段 '{field_alias}' 无效，表 '{table_name}' 支持字段：{list(table_field_map.keys())}"
                    )
                db_field = table_field_map[field_alias]
                if isinstance(value, list):
                    where_clauses.append(self._format_in_condition(db_field, [str(v) for v in value]))
                else:
                    if isinstance(value, str):
                        where_clauses.append(f"{db_field} = '{value}'")
                    else:
                        where_clauses.append(f"{db_field} = {value}")

        # 4.4 其他关键字参数条件
        for key, value in kwargs.items():
            if key in table_field_map:
                db_key = table_field_map[key]
                if not ignore_date_field and key == self.DATE_ALIAS:
                    date_clause = self._format_date_condition(db_key, start_date=value, end_date=value)
                    if date_clause:
                        where_clauses.append(date_clause)
                else:
                    where_clauses.append(f"{db_key} = '{value}'")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # 5. 构建SQL
        if '.' in table_name:
            qualified_table_name = table_name
        else:
            qualified_table_name = f"wind_filesync.{table_name}"

        # 排序：有日期字段时按日期排序，否则按标的代码排序
        order_by_clause = ""
        if not ignore_date_field and actual_date_field:
            order_by_clause = f"ORDER BY {actual_date_field} ASC"
        elif "windcode" in table_field_map:
            order_by_clause = f"ORDER BY {table_field_map['windcode']} ASC"

        sql = f"""
            SELECT {select_clause}
            FROM {qualified_table_name}
            {where_clause}
            {order_by_clause}
        """.strip()

        # 6. 返回SQL或执行查询
        if return_sql:
            return sql
        data = self._execute_query(sql)

        return data

    def _format_instruments(self, instruments: List[str]) -> List[str]:
        """格式化标的代码（Wind专用，其他数据库可重写）"""
        return [ins.replace(".XSHG", ".SH").replace(".XSHE", ".SZ") for ins in instruments]

    def _execute_query(self, sql: str) -> pd.DataFrame:
        """执行SQL查询"""
        try:
            conn = self._get_connection()
            print(f"[{self.source_type}] 执行SQL：\n{sql}")  # 打印执行的SQL（便于调试）
            return pd.read_sql(sql, conn)
        except Exception as e:
            raise QueryError(f"查询失败：{str(e)}，SQL：\n{sql}")

    def close_connection(self):
        pass