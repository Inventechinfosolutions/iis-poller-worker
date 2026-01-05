"""
Rate limiter for controlling request rate per organization.
Prevents overwhelming the system with too many concurrent requests.
"""

import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

from src.config.settings import settings

logger = structlog.get_logger(__name__)


class RateLimiter:
    """
    Simple rate limiter using token bucket algorithm.
    Limits requests per organization to prevent system overload.
    """
    
    def __init__(
        self,
        max_requests_per_minute: int = 60,
        max_concurrent_requests: int = 10
    ):
        """
        Initialize rate limiter.
        
        Args:
            max_requests_per_minute: Maximum requests per minute per org
            max_concurrent_requests: Maximum concurrent requests per org
        """
        self.max_requests_per_minute = max_requests_per_minute
        self.max_concurrent_requests = max_concurrent_requests
        
        # Track requests per org: org_id -> list of timestamps
        self._request_timestamps: Dict[str, list] = defaultdict(list)
        
        # Track concurrent requests per org: org_id -> count
        self._concurrent_requests: Dict[str, int] = defaultdict(int)
        
        # Semaphores per org for concurrency control
        self._semaphores: Dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(max_concurrent_requests)
        )
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    async def acquire(self, org_id: str) -> bool:
        """
        Acquire permission to make a request.
        Returns True if allowed, False if rate limited.
        
        Args:
            org_id: Organization identifier
            
        Returns:
            True if request allowed, False if rate limited
        """
        async with self._lock:
            now = datetime.utcnow()
            cutoff = now - timedelta(minutes=1)
            
            # Clean old timestamps
            timestamps = self._request_timestamps[org_id]
            timestamps[:] = [ts for ts in timestamps if ts > cutoff]
            
            # Check rate limit
            if len(timestamps) >= self.max_requests_per_minute:
                logger.warning("Rate limit exceeded", org_id=org_id,
                             requests=len(timestamps),
                             max_requests=self.max_requests_per_minute)
                return False
            
            # Check concurrent limit
            if self._concurrent_requests[org_id] >= self.max_concurrent_requests:
                logger.warning("Concurrent limit exceeded", org_id=org_id,
                             concurrent=self._concurrent_requests[org_id],
                             max_concurrent=self.max_concurrent_requests)
                return False
            
            # Allow request
            timestamps.append(now)
            self._concurrent_requests[org_id] += 1
            return True
    
    async def release(self, org_id: str):
        """Release a request slot."""
        async with self._lock:
            if self._concurrent_requests[org_id] > 0:
                self._concurrent_requests[org_id] -= 1
    
    async def get_semaphore(self, org_id: str) -> asyncio.Semaphore:
        """Get semaphore for an organization."""
        return self._semaphores[org_id]
    
    def get_stats(self, org_id: str) -> Dict[str, int]:
        """Get rate limiting stats for an organization."""
        async def _get_stats():
            async with self._lock:
                now = datetime.utcnow()
                cutoff = now - timedelta(minutes=1)
                timestamps = self._request_timestamps[org_id]
                timestamps[:] = [ts for ts in timestamps if ts > cutoff]
                
                return {
                    "requests_last_minute": len(timestamps),
                    "concurrent_requests": self._concurrent_requests[org_id],
                    "max_requests_per_minute": self.max_requests_per_minute,
                    "max_concurrent_requests": self.max_concurrent_requests
                }
        
        # For sync access, return current state
        return {
            "requests_last_minute": len(self._request_timestamps[org_id]),
            "concurrent_requests": self._concurrent_requests[org_id],
            "max_requests_per_minute": self.max_requests_per_minute,
            "max_concurrent_requests": self.max_concurrent_requests
        }


# Global rate limiter instance
rate_limiter = RateLimiter(
    max_requests_per_minute=settings.max_concurrent_jobs * 6,  # 6x concurrent jobs per minute
    max_concurrent_requests=settings.max_concurrent_jobs
)

