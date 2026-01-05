"""
Circuit breaker pattern for handling failing connections.
Prevents cascading failures by stopping requests to failing services.
"""

import asyncio
from enum import Enum
from typing import Callable, TypeVar, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

logger = structlog.get_logger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Simple circuit breaker implementation.
    Opens circuit after failure threshold, closes after recovery.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        success_threshold: int = 2
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before trying again
            success_threshold: Number of successes to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        # Per-connection state: connection_key -> state
        self._states: dict = defaultdict(lambda: CircuitState.CLOSED)
        self._failure_counts: dict = defaultdict(int)
        self._success_counts: dict = defaultdict(int)
        self._last_failure_time: dict = {}
        self._lock = asyncio.Lock()
    
    async def call(
        self,
        connection_key: str,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """
        Execute function with circuit breaker protection.
        
        Args:
            connection_key: Unique identifier for the connection
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        async with self._lock:
            state = self._states[connection_key]
            now = datetime.utcnow()
            
            # Check if circuit should transition from OPEN to HALF_OPEN
            if state == CircuitState.OPEN:
                last_failure = self._last_failure_time.get(connection_key)
                if last_failure and (now - last_failure).total_seconds() >= self.recovery_timeout:
                    self._states[connection_key] = CircuitState.HALF_OPEN
                    self._success_counts[connection_key] = 0
                    logger.info("Circuit breaker half-open", connection_key=connection_key)
                else:
                    raise Exception(f"Circuit breaker is OPEN for {connection_key}")
        
        # Execute function
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            await self._record_success(connection_key)
            return result
        except Exception as e:
            await self._record_failure(connection_key)
            raise
    
    async def _record_success(self, connection_key: str):
        """Record successful call."""
        async with self._lock:
            state = self._states[connection_key]
            
            if state == CircuitState.HALF_OPEN:
                self._success_counts[connection_key] += 1
                if self._success_counts[connection_key] >= self.success_threshold:
                    self._states[connection_key] = CircuitState.CLOSED
                    self._failure_counts[connection_key] = 0
                    logger.info("Circuit breaker closed", connection_key=connection_key)
            elif state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_counts[connection_key] = 0
    
    async def _record_failure(self, connection_key: str):
        """Record failed call."""
        async with self._lock:
            state = self._states[connection_key]
            self._failure_counts[connection_key] += 1
            self._last_failure_time[connection_key] = datetime.utcnow()
            
            if state == CircuitState.HALF_OPEN:
                # Any failure in half-open goes back to open
                self._states[connection_key] = CircuitState.OPEN
                logger.warning("Circuit breaker opened (from half-open)", connection_key=connection_key)
            elif state == CircuitState.CLOSED:
                if self._failure_counts[connection_key] >= self.failure_threshold:
                    self._states[connection_key] = CircuitState.OPEN
                    logger.error("Circuit breaker opened", connection_key=connection_key,
                               failures=self._failure_counts[connection_key])
    
    def get_state(self, connection_key: str) -> CircuitState:
        """Get current circuit state."""
        return self._states.get(connection_key, CircuitState.CLOSED)
    
    def is_open(self, connection_key: str) -> bool:
        """Check if circuit is open."""
        return self.get_state(connection_key) == CircuitState.OPEN


# Global circuit breaker instance
circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60,
    success_threshold=2
)

