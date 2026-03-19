class DataError(Exception):
    """所有数据操作相关异常的基类"""
    pass


class ConfigError(DataError):
    """配置加载或解析异常"""
    def __init__(self, message: str = "配置错误"):
        super().__init__(message)


class QueryError(DataError):
    """SQL查询执行异常"""
    def __init__(self, message: str = "查询失败"):
        super().__init__(message)


class InvalidParameterError(DataError):
    """输入参数无效异常"""
    def __init__(self, message: str = "参数无效"):
        super().__init__(message)


class DataSourceNotFoundError(DataError):
    """指定的数据源未在配置中定义"""
    def __init__(self, source_type: str):
        super().__init__(f"数据源 '{source_type}' 未找到，支持的数据源：{['wind']}")


class DatabaseTypeNotSupportedError(DataError):
    """数据库类型不支持异常"""
    def __init__(self, db_type: str):
        super().__init__(f"数据库类型 '{db_type}' 不支持，支持的类型：['oracle']")