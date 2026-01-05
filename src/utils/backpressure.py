"""
Simple backpressure management for Kafka consumer.
Prevents overwhelming the system when processing is slow.
"""

import asyncio
from typing import Optional
import structlog

from src.config.settings import settings

logger = structlog.get_logger(__name__)


class BackpressureManager:
    """
    Simple backpressure manager.
    Pauses consumption when too many jobs are in progress.
    """
    
    def __init__(self, max_in_flight: int = None):
        """
        Initialize backpressure manager.
        
        Args:
            max_in_flight: Maximum jobs in flight (default: max_concurrent_jobs)
        """
        self.max_in_flight = max_in_flight or settings.max_concurrent_jobs
        self._in_flight_count = 0
        self._lock = asyncio.Lock()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Start unpaused
    
    async def acquire(self) -> bool:
        """
        Acquire permission to process a job.
        Returns True if allowed, False if backpressure active.
        """
        async with self._lock:
            if self._in_flight_count >= self.max_in_flight:
                self._pause_event.clear()  # Pause consumption
                logger.warning("Backpressure active",
                             in_flight=self._in_flight_count,
                             max_in_flight=self.max_in_flight)
                return False
            
            self._in_flight_count += 1
            return True
    
    async def release(self):
        """Release a job slot."""
        async with self._lock:
            if self._in_flight_count > 0:
                self._in_flight_count -= 1
            
            if self._in_flight_count < self.max_in_flight:
                self._pause_event.set()  # Resume consumption
    
    async def wait_if_paused(self):
        """Wait if backpressure is active."""
        await self._pause_event.wait()
    
    def get_stats(self) -> dict:
        """Get backpressure statistics."""
        return {
            "in_flight": self._in_flight_count,
            "max_in_flight": self.max_in_flight,
            "is_paused": not self._pause_event.is_set()
        }


# Global backpressure manager
backpressure_manager = BackpressureManager()

