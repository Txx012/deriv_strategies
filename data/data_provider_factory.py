from typing import Type, Dict
from .base_provider import BaseDataProvider
# from .wind_provider import WindDataProvider
from .oracle_provider import OracleDataProvider
from .dolphindb_provider import DolphinDBDataProvider
from .ricequant_provider import RiceQuantDataProvider
from .exceptions import DataSourceNotFoundError  # 只保留自定义异常

class DataProviderFactory:
    """数据提供器工厂类"""

    # 注册所有支持的数据源
    _REGISTERED_PROVIDERS: Dict[str, Type[BaseDataProvider]] = {
        # "wind": WindDataProvider,
        "oracle": OracleDataProvider,
        "dolphindb": DolphinDBDataProvider,
        "ricequant": RiceQuantDataProvider
    }

    @staticmethod
    def create_provider(source_type: str = "oracle") -> BaseDataProvider:
        """创建指定类型的数据源实例"""
        if source_type not in DataProviderFactory._REGISTERED_PROVIDERS:
            raise DataSourceNotFoundError(
                f"未注册数据源：{source_type}，支持的数据源：{list(DataProviderFactory._REGISTERED_PROVIDERS.keys())}"
            )
        provider_class = DataProviderFactory._REGISTERED_PROVIDERS[source_type]
        return provider_class()

    @staticmethod
    def register_provider(source_type: str, provider_class: Type[BaseDataProvider]) -> None:
        """扩展接口：动态注册新数据源"""
        # 直接使用内置异常，无需自定义
        if not issubclass(provider_class, BaseDataProvider):
            raise TypeError("新数据源必须继承 BaseDataProvider 抽象类")  # 内置TypeError
        if source_type in DataProviderFactory._REGISTERED_PROVIDERS:
            raise ValueError(f"数据源 '{source_type}' 已存在，不可重复注册")  # 内置ValueError
        DataProviderFactory._REGISTERED_PROVIDERS[source_type] = provider_class
        print(f"✅ 成功注册新数据源：{source_type}")