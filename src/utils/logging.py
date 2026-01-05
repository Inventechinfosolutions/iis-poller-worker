"""
Logging configuration and utilities for the poller worker service.
Provides structured logging with proper formatting and context.
"""

import sys
import logging
from typing import Any, Dict
import structlog
from structlog.stdlib import LoggerFactory

from src.config.settings import settings


def configure_logging():
    """Configure structured logging for the application."""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if settings.log_format == "json" 
            else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level.upper())
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


class LogContext:
    """Context manager for adding structured logging context."""
    
    def __init__(self, logger: structlog.BoundLogger, **context):
        self.logger = logger
        self.context = context
    
    def __enter__(self):
        return self.logger.bind(**self.context)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def log_function_call(logger: structlog.BoundLogger, func_name: str, **kwargs):
    """Decorator for logging function calls."""
    def decorator(func):
        async def async_wrapper(*args, **func_kwargs):
            with LogContext(logger, function=func_name, **kwargs):
                logger.info("Function called", args=args, kwargs=func_kwargs)
                try:
                    result = await func(*args, **func_kwargs)
                    logger.info("Function completed successfully")
                    return result
                except Exception as e:
                    logger.error("Function failed", error=str(e), exc_info=True)
                    raise
        return async_wrapper
    return decorator
