import yaml
import os
from typing import Dict, Optional
from .exceptions import ConfigError, DataSourceNotFoundError  # 补充导入

class ConfigLoader:
    """配置加载器（单例模式，适配 yaml 文件夹路径）"""
    _instance: Optional["ConfigLoader"] = None

    def __new__(cls, config_path: Optional[str] = None) -> "ConfigLoader":
        if cls._instance is None:
            # 自动定位配置文件：项目根目录/yaml/data_config.yaml
            if not config_path:
                # 获取项目根目录（data 文件夹的父目录）
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                config_path = os.path.join(project_root, "yaml", "data_config.yaml")
            cls._instance = super().__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, config_path: str) -> None:
        """加载YAML配置文件"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                loaded_config = yaml.safe_load(f) or {}
                self.data_sources = loaded_config.get("data_sources", {})
        except FileNotFoundError:
            raise ConfigError(f"配置文件未找到：{config_path}\n请确认 yaml 文件夹下存在 data_config.yaml")
        except yaml.YAMLError as e:
            raise ConfigError(f"YAML配置解析失败：{str(e)}")
        except Exception as e:
            raise ConfigError(f"配置加载异常：{str(e)}，路径：{config_path}")

    def get_data_source_config(self, source_type: str = "wind") -> Dict:
        """获取指定数据源的完整配置"""
        source_config = self.data_sources.get(source_type)
        if not source_config:
            raise DataSourceNotFoundError(source_type)
        source_config["source_type"] = source_type
        return source_config

    def get_field_mappings(self, source_type: str = "wind", table_name: Optional[str] = None) -> Dict:
        """获取指定数据源的字段映射"""
        source_config = self.get_data_source_config(source_type)
        field_mappings = source_config.get("field_mappings", {})
        if table_name:
            return field_mappings.get(table_name, {})
        return field_mappings