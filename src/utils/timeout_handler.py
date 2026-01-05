"""
Timeout handler for operations.
Prevents operations from hanging indefinitely.
"""

import asyncio
from typing import TypeVar, Callable, Optional
from functools import wraps
import structlog

from src.config.settings import settings

logger = structlog.get_logger(__name__)

T = TypeVar('T')


async def with_timeout(
    func: Callable[..., T],
    timeout_seconds: float,
    *args,
    operation_name: str = "operation",
    **kwargs
) -> T:
    """
    Execute function with timeout.
    
    Args:
        func: Function to execute
        timeout_seconds: Timeout in seconds
        *args: Function arguments
        operation_name: Name for logging
        **kwargs: Function keyword arguments
        
    Returns:
        Function result
        
    Raises:
        asyncio.TimeoutError: If operation times out
    """
    try:
        if asyncio.iscoroutinefunction(func):
            return await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=timeout_seconds
            )
        else:
            # For sync functions, run in executor
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(None, lambda: func(*args, **kwargs)),
                timeout=timeout_seconds
            )
    except asyncio.TimeoutError:
        logger.error("Operation timed out", operation=operation_name, timeout=timeout_seconds)
        raise


def timeout(seconds: float, operation_name: str = "operation"):
    """
    Decorator for adding timeout to async functions.
    
    Usage:
        @timeout(30.0, "connect_to_source")
        async def connect():
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await with_timeout(func, seconds, *args, operation_name=operation_name, **kwargs)
        return wrapper
    return decorator

