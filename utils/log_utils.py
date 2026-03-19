# -*- coding: utf-8 -*-
"""全局日志工具：统一配置日志格式，支持控制台+文件输出，所有模块复用同一个logger"""
import logging
import os
from datetime import datetime
from typing import Optional

def setup_logger(name: Optional[str] = "FuturesOptionsStrategy",
                 log_level: int = logging.INFO,
                 log_dir: str = "logs") -> logging.Logger:
    """
    配置全局logger
    :param name: logger名称
    :param log_level: 日志级别 logging.INFO/logging.DEBUG
    :param log_dir: 日志文件保存目录
    :return: 配置好的logger
    """
    # 创建日志目录
    os.makedirs(log_dir, exist_ok=True)
    # 日志文件名称（按日期）
    log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d')}_{name.lower()}.log")
    # 避免重复添加处理器
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    # 日志格式
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # 1. 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    console_handler.setLevel(log_level)
    # 2. 文件处理器（按大小切割，保留5个文件，每个50MB）
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=50*1024*1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(log_level)
    # 添加处理器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    logger.propagate = False
    return logger

# 全局默认logger（所有模块直接导入使用）
logger = setup_logger(name="FuturesOptionsStrategy", log_level=logging.INFO)

# 测试代码
if __name__ == "__main__":
    logger.debug("这是DEBUG级日志（详细调试信息）")
    logger.info("这是INFO级日志（正常运行信息）")
    logger.warning("这是WARNING级日志（警告信息）")
    logger.error("这是ERROR级日志（错误信息）")
    logger.critical("这是CRITICAL级日志（严重错误信息）")