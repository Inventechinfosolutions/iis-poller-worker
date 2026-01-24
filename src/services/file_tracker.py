"""
File tracker service to track processed files and avoid reprocessing.
Uses Redis for persistence (with in-memory fallback).
Tracks files by org_id, source_type, and file_path/checksum.
Also tracks connection-level statistics: last processed time, success/failed counts.
"""

import hashlib
from typing import Set, Optional, Dict, Any
from datetime import datetime
import structlog
from collections import defaultdict
from dataclasses import dataclass, field

from src.models.schemas import SourceType, FileEvent, SourceConfig
from src.utils.connection_key import generate_connection_key
from src.services.redis_persistence import redis_persistence
from src.config.settings import settings
from src.clients.database_client import database_client
from src.models.file_events import FileEvent as FileEventModel
from sqlalchemy import select, and_, or_

logger = structlog.get_logger(__name__)


@dataclass
class ConnectionStatistics:
    """Statistics for a connection."""
    connection_key: str
    org_id: str
    source_type: str
    last_processed_at: Optional[datetime] = None
    total_processed: int = 0
    total_success: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    first_processed_at: Optional[datetime] = None
    last_job_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "connection_key": self.connection_key,
            "org_id": self.org_id,
            "source_type": self.source_type,
            "last_processed_at": self.last_processed_at.isoformat() if self.last_processed_at else None,
            "first_processed_at": self.first_processed_at.isoformat() if self.first_processed_at else None,
            "total_processed": self.total_processed,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
            "last_job_id": self.last_job_id
        }


class FileTracker:
    """
    Tracks processed files to avoid reprocessing.
    Also tracks connection-level statistics (last processed, success/failed counts).
    Uses in-memory storage (can be extended to use database/Redis).
    """
    
    def __init__(self):
        # Track processed files by org_id -> source_type -> file_key -> processed_at
        self._processed_files: Dict[str, Dict[str, Dict[str, datetime]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # Track by checksum for duplicate detection
        self._processed_checksums: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        # Track connection statistics: org_id -> connection_key -> ConnectionStatistics
        self._connection_stats: Dict[str, Dict[str, ConnectionStatistics]] = defaultdict(dict)
    
    def _generate_file_key(self, file_path: str, file_name: str, source_type: SourceType) -> str:
        """
        Generate a unique key for a file.
        
        Args:
            file_path: Path to the file
            file_name: Name of the file
            source_type: Type of source
            
        Returns:
            Unique file key string
        """
        # Use file_path + file_name + source_type for unique identification
        key_string = f"{source_type.value}:{file_path}:{file_name}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _generate_connection_key(self, org_id: str, source_config: SourceConfig) -> str:
        """Generate connection key using shared utility (DRY principle)."""
        return generate_connection_key(org_id, source_config)
    
    async def is_file_processed(self, org_id: str, file_event: FileEvent) -> bool:
        """
        Check if a file has already been processed.
        Checks Redis first, then in-memory cache, then database (fallback).
        
        Args:
            org_id: Organization identifier
            file_event: FileEvent object
            
        Returns:
            True if file has been processed, False otherwise
        """
        # Resolve scope: org vs global
        effective_org_id = org_id
        if (settings.file_tracking_scope or "org").lower() == "global":
            effective_org_id = "global"

        # 1. Check Redis first (fast, persistent storage)
        if redis_persistence._initialized:
            redis_result = await redis_persistence.is_file_processed(effective_org_id, file_event)
            if redis_result:
                return True
        
        # 2. Check in-memory cache (fast)
        source_type = file_event.source_type.value
        file_key = self._generate_file_key(
            file_event.file_path,
            file_event.file_name,
            file_event.source_type
        )
        
        if file_key in self._processed_files[effective_org_id][source_type]:
            logger.debug("File already processed (by key)",
                        org_id=effective_org_id,
                        file_name=file_event.file_name,
                        file_path=file_event.file_path,
                        source_type=source_type)
            return True
        
        # 3. Check by checksum in memory cache
        if file_event.checksum:
            if file_event.checksum in self._processed_checksums[effective_org_id]:
                logger.debug("File already processed (by checksum)",
                            org_id=effective_org_id,
                            file_name=file_event.file_name,
                            checksum=file_event.checksum)
                return True
        
        # 4. Fallback: Check database (slower but persistent - protects against Redis flush)
        if database_client._initialized:
            db_result = await self._check_database_for_file(effective_org_id, file_event)
            if db_result:
                logger.info("File found in database (Redis was likely flushed), skipping reprocessing",
                           org_id=effective_org_id,
                           file_name=file_event.file_name,
                           file_path=file_event.file_path,
                           source_type=source_type)
                # Re-populate Redis cache for faster future lookups
                await redis_persistence.mark_file_processed(effective_org_id, file_event, success=True)
                return True
        
        return False
    
    async def _check_database_for_file(self, org_id: str, file_event: FileEvent) -> bool:
        """
        Check database to see if file was already processed.
        This is a fallback when Redis is flushed.
        
        For OneDrive: checks by original_file_path from metadata
        For all sources: checks by checksum from metadata
        """
        try:
            async with database_client.get_session() as session:
                # For OneDrive: check by original_file_path in metadata
                if file_event.source_type == SourceType.ONEDRIVE:
                    original_path = (file_event.metadata or {}).get("original_file_path") or file_event.file_path
                    if original_path and original_path.startswith("onedrive://"):
                        # Query all OneDrive files with matching file_name, then check metadata in Python
                        # (More reliable across different database JSON implementations)
                        stmt = select(FileEventModel).where(
                            and_(
                                FileEventModel.source_type == SourceType.ONEDRIVE.value,
                                FileEventModel.file_name == file_event.file_name
                            )
                        )
                        result = await session.execute(stmt)
                        for db_file in result.scalars():
                            metadata = db_file.event_metadata or {}
                            db_original_path = metadata.get("original_file_path")
                            if db_original_path == original_path:
                                return True
                
                # For all sources: check by checksum in metadata (if available)
                if file_event.checksum:
                    stmt = select(FileEventModel).where(
                        and_(
                            FileEventModel.source_type == file_event.source_type.value,
                            FileEventModel.file_name == file_event.file_name
                        )
                    )
                    result = await session.execute(stmt)
                    for db_file in result.scalars():
                        metadata = db_file.event_metadata or {}
                        db_checksum = metadata.get("checksum")
                        if db_checksum == file_event.checksum:
                            return True
                
                # Fallback: check by file_path/object_key and file_name
                stmt = select(FileEventModel).where(
                    and_(
                        FileEventModel.source_type == file_event.source_type.value,
                        FileEventModel.file_name == file_event.file_name,
                        or_(
                            FileEventModel.object_key == file_event.file_path,
                            FileEventModel.object_key.like(f"%{file_event.file_name}%")
                        )
                    )
                ).limit(1)
                result = await session.execute(stmt)
                return result.scalar_one_or_none() is not None
                
        except Exception as e:
            logger.error("Error checking database for file",
                        org_id=org_id,
                        file_name=file_event.file_name,
                        error=str(e))
            return False
    
    async def mark_file_as_processed(self, org_id: str, file_event: FileEvent, success: bool = True):
        """
        Mark a file as processed.
        Saves to Redis (persistent) and in-memory cache (fast lookup).
        
        Args:
            org_id: Organization identifier
            file_event: FileEvent object
            success: Whether the file was successfully processed (default: True)
        """
        # Resolve scope: org vs global
        effective_org_id = org_id
        if (settings.file_tracking_scope or "org").lower() == "global":
            effective_org_id = "global"

        # Save to Redis first (persistent storage)
        if redis_persistence._initialized:
            await redis_persistence.mark_file_processed(effective_org_id, file_event, success)
        
        # Also save to in-memory cache (fast lookup)
        source_type = file_event.source_type.value
        file_key = self._generate_file_key(
            file_event.file_path,
            file_event.file_name,
            file_event.source_type
        )
        
        processed_time = datetime.utcnow()
        
        # Track by file key
        self._processed_files[effective_org_id][source_type][file_key] = processed_time
        
        # Track by checksum if available
        if file_event.checksum:
            self._processed_checksums[effective_org_id][file_event.checksum] = processed_time
        
        logger.debug("File marked as processed",
                    org_id=effective_org_id,
                    file_name=file_event.file_name,
                    file_path=file_event.file_path,
                    source_type=source_type,
                    file_key=file_key,
                    success=success)
    
    async def record_connection_statistics(
        self,
        org_id: str,
        source_config: SourceConfig,
        job_id: str,
        success_count: int,
        failed_count: int,
        skipped_count: int
    ):
        """
        Record connection-level statistics.
        Saves to Redis (persistent) and in-memory cache.
        
        Args:
            org_id: Organization identifier
            source_config: Source configuration
            job_id: Job identifier
            success_count: Number of successfully processed files
            failed_count: Number of failed files
            skipped_count: Number of skipped files
        """
        # Save to Redis first (persistent storage)
        if redis_persistence._initialized:
            await redis_persistence.save_connection_stats(
                org_id, source_config, job_id,
                success_count, failed_count, skipped_count
            )
        
        # Also update in-memory cache
        connection_key = self._generate_connection_key(org_id, source_config)
        source_type = source_config.source_type.value
        now = datetime.utcnow()
        
        # Get or create connection statistics
        if connection_key not in self._connection_stats[org_id]:
            self._connection_stats[org_id][connection_key] = ConnectionStatistics(
                connection_key=connection_key,
                org_id=org_id,
                source_type=source_type
            )
        
        stats = self._connection_stats[org_id][connection_key]
        
        # Update statistics
        stats.last_processed_at = now
        stats.last_job_id = job_id
        stats.total_processed += (success_count + failed_count + skipped_count)
        stats.total_success += success_count
        stats.total_failed += failed_count
        stats.total_skipped += skipped_count
        
        if stats.first_processed_at is None:
            stats.first_processed_at = now
        
        logger.info("Connection statistics updated",
                   org_id=org_id,
                   connection_key=connection_key,
                   source_type=source_type,
                   job_id=job_id,
                   success=success_count,
                   failed=failed_count,
                   skipped=skipped_count,
                   total_processed=stats.total_processed)
    
    async def get_connection_statistics(
        self,
        org_id: str,
        source_config: SourceConfig
    ) -> Optional[ConnectionStatistics]:
        """
        Get statistics for a specific connection.
        Checks Redis first, then in-memory cache.
        
        Args:
            org_id: Organization identifier
            source_config: Source configuration
            
        Returns:
            ConnectionStatistics object or None if not found
        """
        # Check Redis first
        if redis_persistence._initialized:
            redis_stats = await redis_persistence.get_connection_stats(org_id, source_config)
            if redis_stats:
                # Convert Redis dict to ConnectionStatistics
                return ConnectionStatistics(
                    connection_key=redis_stats.get("connection_key", ""),
                    org_id=redis_stats.get("org_id", org_id),
                    source_type=redis_stats.get("source_type", ""),
                    last_processed_at=datetime.fromisoformat(redis_stats["last_processed_at"]) if redis_stats.get("last_processed_at") else None,
                    first_processed_at=datetime.fromisoformat(redis_stats["first_processed_at"]) if redis_stats.get("first_processed_at") else None,
                    total_processed=int(redis_stats.get("total_processed", 0)),
                    total_success=int(redis_stats.get("total_success", 0)),
                    total_failed=int(redis_stats.get("total_failed", 0)),
                    total_skipped=int(redis_stats.get("total_skipped", 0)),
                    last_job_id=redis_stats.get("last_job_id")
                )
        
        # Fallback to in-memory cache
        connection_key = self._generate_connection_key(org_id, source_config)
        
        if org_id in self._connection_stats and connection_key in self._connection_stats[org_id]:
            return self._connection_stats[org_id][connection_key]
        
        return None
    
    async def get_all_connection_statistics(self, org_id: str) -> Dict[str, ConnectionStatistics]:
        """
        Get all connection statistics for an organization.
        Checks Redis first, then in-memory cache.
        
        Args:
            org_id: Organization identifier
            
        Returns:
            Dictionary of connection_key -> ConnectionStatistics
        """
        result = {}
        
        # Check Redis first
        if redis_persistence._initialized:
            redis_stats = await redis_persistence.get_all_connection_stats(org_id)
            for conn_key, stats_dict in redis_stats.items():
                result[conn_key] = ConnectionStatistics(
                    connection_key=stats_dict.get("connection_key", conn_key),
                    org_id=stats_dict.get("org_id", org_id),
                    source_type=stats_dict.get("source_type", ""),
                    last_processed_at=datetime.fromisoformat(stats_dict["last_processed_at"]) if stats_dict.get("last_processed_at") else None,
                    first_processed_at=datetime.fromisoformat(stats_dict["first_processed_at"]) if stats_dict.get("first_processed_at") else None,
                    total_processed=int(stats_dict.get("total_processed", 0)),
                    total_success=int(stats_dict.get("total_success", 0)),
                    total_failed=int(stats_dict.get("total_failed", 0)),
                    total_skipped=int(stats_dict.get("total_skipped", 0)),
                    last_job_id=stats_dict.get("last_job_id")
                )
        
        # Merge with in-memory cache
        if org_id in self._connection_stats:
            for conn_key, stats in self._connection_stats[org_id].items():
                if conn_key not in result:
                    result[conn_key] = stats
        
        return result
    
    def get_processed_count(self, org_id: str, source_type: Optional[SourceType] = None) -> int:
        """
        Get count of processed files for an org.
        
        Args:
            org_id: Organization identifier
            source_type: Optional source type filter
            
        Returns:
            Count of processed files
        """
        if org_id not in self._processed_files:
            return 0
        
        if source_type:
            return len(self._processed_files[org_id][source_type.value])
        else:
            # Count all source types
            total = 0
            for source_files in self._processed_files[org_id].values():
                total += len(source_files)
            return total
    
    def clear_processed_files(self, org_id: Optional[str] = None, source_type: Optional[SourceType] = None):
        """
        Clear processed files tracking.
        
        Args:
            org_id: Optional organization identifier (if None, clears all)
            source_type: Optional source type filter
        """
        if org_id is None:
            self._processed_files.clear()
            self._processed_checksums.clear()
            logger.info("Cleared all processed files tracking")
        elif source_type is None:
            if org_id in self._processed_files:
                del self._processed_files[org_id]
            if org_id in self._processed_checksums:
                del self._processed_checksums[org_id]
            logger.info("Cleared processed files for org", org_id=org_id)
        else:
            source_type_str = source_type.value
            if org_id in self._processed_files and source_type_str in self._processed_files[org_id]:
                del self._processed_files[org_id][source_type_str]
            logger.info("Cleared processed files for org and source type",
                       org_id=org_id,
                       source_type=source_type_str)


# Global file tracker instance
file_tracker = FileTracker()

