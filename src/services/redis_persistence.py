"""
Redis persistence layer for file tracking and connection statistics.
Simple, production-ready implementation.
"""

import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import structlog

from src.clients.redis_client import redis_client
from src.config.redis_settings import redis_settings
from src.config.settings import settings
from src.models.schemas import FileEvent, SourceConfig
from src.utils.connection_key import generate_connection_key

logger = structlog.get_logger(__name__)


class RedisPersistence:
    """
    Redis persistence for file tracking and connection statistics.
    Simple, efficient, production-ready.
    """
    
    # Redis key prefixes
    FILE_TRACKING_PREFIX = "poller:file:"
    CONNECTION_STATS_PREFIX = "poller:conn:"
    ORG_STATS_PREFIX = "poller:org:"
    
    def __init__(self):
        self._initialized = False
    
    async def initialize(self):
        """Initialize Redis persistence."""
        try:
            await redis_client.initialize()
            self._initialized = True
            logger.info("Redis persistence initialized")
        except Exception as e:
            logger.error("Failed to initialize Redis persistence", error=str(e))
            # Continue without Redis (graceful degradation)
            self._initialized = False
    
    def _get_file_key(self, org_id: str, file_event: FileEvent) -> str:
        """Generate Redis key for file tracking."""
        import hashlib
        key_string = f"{file_event.source_type.value}:{file_event.file_path}:{file_event.file_name}"
        file_hash = hashlib.md5(key_string.encode()).hexdigest()
        # Support global vs org-scoped file tracking
        # - org: poller:file:{org_id}:{hash}
        # - global: poller:file:global:{hash}
        scope = (settings.file_tracking_scope or "org").lower()
        scope_key = "global" if scope == "global" else org_id
        return f"{self.FILE_TRACKING_PREFIX}{scope_key}:{file_hash}"
    
    def _get_connection_key(self, org_id: str, source_config: SourceConfig) -> str:
        """Generate Redis key for connection statistics."""
        conn_key = generate_connection_key(org_id, source_config)
        return f"{self.CONNECTION_STATS_PREFIX}{org_id}:{conn_key}"
    
    async def is_file_processed(self, org_id: str, file_event: FileEvent) -> bool:
        """Check if file is already processed (from Redis)."""
        if not self._initialized:
            return False
        
        try:
            key = self._get_file_key(org_id, file_event)
            return await redis_client.exists(key)
        except Exception as e:
            logger.error("Error checking file in Redis", error=str(e))
            return False
    
    async def mark_file_processed(self, org_id: str, file_event: FileEvent, success: bool = True):
        """Mark file as processed in Redis."""
        if not self._initialized:
            return
        
        try:
            key = self._get_file_key(org_id, file_event)
            ttl_seconds = redis_settings.file_tracking_ttl_days * 24 * 60 * 60
            
            data = {
                "org_id": org_id,
                "file_name": file_event.file_name,
                "file_path": file_event.file_path,
                "source_type": file_event.source_type.value,
                "processed_at": datetime.utcnow().isoformat(),
                "success": success
            }
            
            if file_event.checksum:
                data["checksum"] = file_event.checksum
            
            await redis_client.set(key, data, ttl_seconds=ttl_seconds)
        except Exception as e:
            logger.error("Error marking file in Redis", error=str(e))
    
    async def save_connection_stats(
        self,
        org_id: str,
        source_config: SourceConfig,
        job_id: str,
        success_count: int,
        failed_count: int,
        skipped_count: int
    ):
        """Save connection statistics to Redis."""
        if not self._initialized:
            return
        
        try:
            key = self._get_connection_key(org_id, source_config)
            ttl_seconds = redis_settings.connection_stats_ttl_days * 24 * 60 * 60
            
            # Get existing stats
            existing = await redis_client.get_hash(key)
            if existing:
                total_processed = int(existing.get("total_processed", 0)) + success_count + failed_count + skipped_count
                total_success = int(existing.get("total_success", 0)) + success_count
                total_failed = int(existing.get("total_failed", 0)) + failed_count
                total_skipped = int(existing.get("total_skipped", 0)) + skipped_count
                first_processed_at = existing.get("first_processed_at")
            else:
                total_processed = success_count + failed_count + skipped_count
                total_success = success_count
                total_failed = failed_count
                total_skipped = skipped_count
                first_processed_at = datetime.utcnow().isoformat()
            
            stats = {
                "connection_key": generate_connection_key(org_id, source_config),
                "org_id": org_id,
                "source_type": source_config.source_type.value,
                "last_processed_at": datetime.utcnow().isoformat(),
                "first_processed_at": first_processed_at,
                "total_processed": total_processed,
                "total_success": total_success,
                "total_failed": total_failed,
                "total_skipped": total_skipped,
                "last_job_id": job_id
            }
            
            await redis_client.set_hash(key, stats, ttl_seconds=ttl_seconds)
        except Exception as e:
            logger.error("Error saving connection stats to Redis", error=str(e))
    
    async def get_connection_stats(self, org_id: str, source_config: SourceConfig) -> Optional[Dict[str, Any]]:
        """Get connection statistics from Redis."""
        if not self._initialized:
            return None
        
        try:
            key = self._get_connection_key(org_id, source_config)
            return await redis_client.get_hash(key)
        except Exception as e:
            logger.error("Error getting connection stats from Redis", error=str(e))
            return None
    
    async def get_all_connection_stats(self, org_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all connection statistics for an organization."""
        if not self._initialized:
            return {}
        
        try:
            # Use pattern matching to find all connection stats for org
            pattern = f"{self.CONNECTION_STATS_PREFIX}{org_id}:*"
            keys = []
            
            # Simple implementation: scan for keys
            if redis_client.client:
                cursor = 0
                while True:
                    cursor, batch = await redis_client.client.scan(cursor, match=pattern, count=100)
                    keys.extend(batch)
                    if cursor == 0:
                        break
            
            result = {}
            for key in keys:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                conn_key = key_str.replace(f"{self.CONNECTION_STATS_PREFIX}{org_id}:", "")
                stats = await redis_client.get_hash(key_str)
                if stats:
                    result[conn_key] = stats
            
            return result
        except Exception as e:
            logger.error("Error getting all connection stats from Redis", error=str(e))
            return {}
    
    async def close(self):
        """Close Redis persistence."""
        await redis_client.close()
        self._initialized = False


# Global Redis persistence instance
redis_persistence = RedisPersistence()

