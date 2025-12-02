"""
Simple config-driven logging setup for IbexDB
"""

import logging
import os
from typing import Optional

_logger_initialized = False


def setup_logging(log_level: Optional[str] = None, log_format: Optional[str] = None) -> None:
    """
    Setup logging configuration (call once at startup)

    Args:
        log_level: DEBUG, INFO, WARNING, ERROR (from config or env)
        log_format: Log message format
    """
    global _logger_initialized
    if _logger_initialized:
        return

    # Get log level from parameter, env, or default to INFO
    level_str = log_level or os.environ.get("IBEX_LOG_LEVEL", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    # Simple format for Lambda (CloudWatch handles timestamps)
    fmt = log_format or os.environ.get("IBEX_LOG_FORMAT", "%(levelname)s - %(name)s - %(message)s")

    logging.basicConfig(level=level, format=fmt, force=True)  # Override any existing config

    _logger_initialized = True


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance"""
    return logging.getLogger(name)
