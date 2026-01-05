"""
Retry handler with exponential backoff for production-ready error handling.
Supports configurable retries, delays, and DLQ integration.
"""

import asyncio
import time
from typing import Callable, TypeVar, Optional, Dict, Any
from datetime import datetime
import structlog
from functools import wraps

from src.config.settings import settings
from src.utils.monitoring import metrics_collector

logger = structlog.get_logger(__name__)

T = TypeVar('T')


class RetryHandler:
    """
    Production-ready retry handler with exponential backoff.
    Supports configurable retries, delays, and error tracking.
    """
    
    def __init__(
        self,
        max_retries: Optional[int] = None,
        initial_delay: Optional[float] = None,
        backoff_multiplier: Optional[float] = None,
        max_delay: Optional[float] = None
    ):
        """
        Initialize retry handler.
        
        Args:
            max_retries: Maximum number of retry attempts (default: from settings)
            initial_delay: Initial delay in seconds (default: from settings)
            backoff_multiplier: Exponential backoff multiplier (default: from settings)
            max_delay: Maximum delay cap in seconds (default: from settings)
        """
        self.max_retries = max_retries or settings.max_retries
        self.initial_delay = initial_delay or settings.retry_delay_seconds
        self.backoff_multiplier = backoff_multiplier or settings.retry_backoff_multiplier
        self.max_delay = max_delay or settings.max_retry_delay_seconds
    
    async def execute_with_retry(
        self,
        func: Callable[..., T],
        *args,
        operation_name: str = "operation",
        context: Optional[Dict[str, Any]] = None,
        retryable_exceptions: tuple = (Exception,),
        **kwargs
    ) -> T:
        """
        Execute a function with retry logic and exponential backoff.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            operation_name: Name of the operation (for logging)
            context: Additional context for logging (e.g., job_id, org_id)
            retryable_exceptions: Tuple of exceptions that should trigger retry
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function execution
            
        Raises:
            Exception: If all retries are exhausted
        """
        context = context or {}
        last_exception = None
        attempt = 0
        
        while attempt <= self.max_retries:
            try:
                if attempt > 0:
                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Retrying {operation_name}", attempt=attempt,
                                 max_retries=self.max_retries, delay_seconds=delay, **context)
                    await asyncio.sleep(delay)
                    metrics_collector.record_retry(operation_name, attempt)
                
                # Execute the function
                start_time = time.time()
                result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
                duration = time.time() - start_time
                
                if attempt > 0:
                    logger.info(
                        f"{operation_name} succeeded after retry",
                        attempt=attempt,
                        duration_seconds=duration,
                        **context
                    )
                
                return result
                
            except retryable_exceptions as e:
                last_exception = e
                attempt += 1
                
                error_type = type(e).__name__
                error_msg = str(e)
                
                logger.warning(
                    f"{operation_name} failed",
                    attempt=attempt,
                    max_retries=self.max_retries,
                    error_type=error_type,
                    error=error_msg,
                    **context
                )
                
                metrics_collector.record_error(operation_name, error_type)
                
                if attempt > self.max_retries:
                    logger.error(
                        f"{operation_name} failed after all retries",
                        total_attempts=attempt,
                        error_type=error_type,
                        error=error_msg,
                        **context
                    )
                    metrics_collector.record_failure(operation_name, error_type)
                    break
        
        # All retries exhausted
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff. Simple and clear."""
        if attempt == 0:
            return 0
        delay = self.initial_delay * (self.backoff_multiplier ** (attempt - 1))
        return min(delay, self.max_delay)


def retry_on_failure(
    max_retries: Optional[int] = None,
    initial_delay: Optional[float] = None,
    backoff_multiplier: Optional[float] = None,
    max_delay: Optional[float] = None,
    retryable_exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying async functions with exponential backoff.
    
    Usage:
        @retry_on_failure(max_retries=3, initial_delay=1.0)
        async def my_function():
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            handler = RetryHandler(
                max_retries=max_retries,
                initial_delay=initial_delay,
                backoff_multiplier=backoff_multiplier,
                max_delay=max_delay
            )
            return await handler.execute_with_retry(
                func,
                *args,
                operation_name=func.__name__,
                retryable_exceptions=retryable_exceptions,
                **kwargs
            )
        return wrapper
    return decorator


# Global retry handler instance
retry_handler = RetryHandler()

