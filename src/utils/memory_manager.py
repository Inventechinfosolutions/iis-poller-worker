"""
Memory management utilities for tracking and limiting memory usage.
Prevents memory leaks and ensures system stability.
"""

import gc
import sys
from typing import Dict, Any
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


class MemoryManager:
    """Manages memory usage and cleanup."""
    
    def __init__(self, max_memory_mb: int = 1024, cleanup_interval_minutes: int = 30):
        """
        Initialize memory manager.
        
        Args:
            max_memory_mb: Maximum memory usage in MB
            cleanup_interval_minutes: Interval for cleanup in minutes
        """
        self.max_memory_mb = max_memory_mb
        self.cleanup_interval_minutes = cleanup_interval_minutes
        self.last_cleanup = datetime.utcnow()
    
    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            # Fallback if psutil not available
            return 0.0
    
    def should_cleanup(self) -> bool:
        """Check if cleanup is needed."""
        now = datetime.utcnow()
        return (now - self.last_cleanup).total_seconds() >= (self.cleanup_interval_minutes * 60)
    
    def cleanup(self):
        """Perform memory cleanup."""
        try:
            # Force garbage collection
            collected = gc.collect()
            self.last_cleanup = datetime.utcnow()
            memory_mb = self.get_memory_usage_mb()
            
            logger.info("Memory cleanup performed",
                       collected_objects=collected,
                       memory_usage_mb=memory_mb,
                       max_memory_mb=self.max_memory_mb)
            
            # Warn if memory usage is high
            if memory_mb > self.max_memory_mb * 0.8:
                logger.warning("High memory usage",
                             memory_usage_mb=memory_mb,
                             max_memory_mb=self.max_memory_mb,
                             usage_percent=(memory_mb / self.max_memory_mb) * 100)
            
        except Exception as e:
            logger.error("Error during memory cleanup", error=str(e))
    
    def check_memory_limit(self) -> bool:
        """Check if memory limit is exceeded."""
        memory_mb = self.get_memory_usage_mb()
        if memory_mb > self.max_memory_mb:
            logger.error("Memory limit exceeded",
                        memory_usage_mb=memory_mb,
                        max_memory_mb=self.max_memory_mb)
            return False
        return True


# Global memory manager instance
memory_manager = MemoryManager()

