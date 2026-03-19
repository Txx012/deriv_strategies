import cx_Oracle
from typing import List, Optional
from .base_provider import BaseDataProvider
from .exceptions import QueryError, DatabaseTypeNotSupportedError

class OracleDataProvider(BaseDataProvider):
    """Oracle数据源实现"""

    def __init__(self):
        super().__init__(source_type="oracle")
        # 校验数据库类型
        if self.connection_config.get("type") != "oracle":
            raise DatabaseTypeNotSupportedError(self.connection_config.get("type"))

    def _get_connection(self) -> cx_Oracle.Connection:
        """建立Oracle连接"""
        if not self._conn or not self._is_conn_alive():
            try:
                connection_string = self.connection_config.get("connection_string")
                if not connection_string:
                    raise QueryError("Oracle配置缺少 'connection_string'")
                self._conn = cx_Oracle.connect(connection_string)
                print("Oracle连接成功")
            except cx_Oracle.Error as e:
                raise QueryError(f"Oracle连接失败：{str(e)}")
            except Exception as e:
                raise QueryError(f"Oracle连接异常：{str(e)}")
        return self._conn

    def _is_conn_alive(self) -> bool:
        """检查Oracle连接有效性"""
        try:
            with self._conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM DUAL")
                return True
        except:
            return False

    def _format_date_condition(self, field: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:
        """格式化日期条件（适配 yyyymmdd 字符串格式，无需TO_DATE转换）"""
        if start_date and end_date:
            return f"{field} BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            return f"{field} >= '{start_date}'"
        elif end_date:
            return f"{field} <= '{end_date}'"
        return ""

    def _format_in_condition(self, field: str, values: List[str]) -> str:
        """格式化Oracle IN条件"""
        values_str = "'" + "', '".join(values) + "'"
        return f"{field} IN ({values_str})"