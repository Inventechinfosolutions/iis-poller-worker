"""
Service for managing file_events in the database.
Handles saving file events when they are published to the queue.
"""

from typing import Optional
from datetime import datetime
from urllib.parse import urlparse
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.clients.database_client import database_client
from src.models.file_events import FileEvent as FileEventModel
from src.models.schemas import FileEvent as FileEventSchema
from src.models.poller_jobs import PollerJob

logger = structlog.get_logger(__name__)


class FileEventService:
    """Service for managing file events in the database."""
    
    async def create_file_event(
        self, 
        file_event: FileEventSchema, 
        poller_job_id: Optional[str] = None
    ) -> Optional[FileEventModel]:
        """Create a file event record in the database."""
        if not database_client._initialized:
            logger.warning("Database not initialized, skipping file event tracking")
            return None
        
        try:
            # poller_job_id can be either the UUID id or the string job_id
            # We need to resolve it to the actual UUID id from poller_jobs table
            if not poller_job_id:
                # Try to get job_id from file_event
                poller_job_id = file_event.job_id if hasattr(file_event, 'job_id') else None
            
            if not poller_job_id:
                logger.warning("poller_job_id is required for file event")
                return None
            
            # Extract object_key - prioritize file_path, then file_url, then file_name
            # object_key should be the file path/name (with or without bucket prefix depending on source)
            object_key = None
            
            # First, try to use file_path (most reliable source)
            # Use file_path as-is since it should already be the correct path
            if file_event.file_path:
                object_key = file_event.file_path.strip('/')
            
            # If no object_key from file_path, try to extract from file_url
            if not object_key and file_event.file_url:
                # Handle S3 URL: s3://bucket-name/object-key
                if file_event.file_url.startswith('s3://'):
                    parts = file_event.file_url[5:].split('/', 1)
                    if len(parts) > 1:
                        object_key = parts[1]  # Everything after bucket name
                    else:
                        object_key = parts[0] if parts else None
                # Handle HTTP/HTTPS URL: http://endpoint/bucket-name/object-key
                elif file_event.file_url.startswith(('http://', 'https://')):
                    parsed = urlparse(file_event.file_url)
                    path_parts = parsed.path.strip('/').split('/', 1)
                    if len(path_parts) > 1:
                        object_key = path_parts[1]  # Everything after bucket name
                    elif len(path_parts) == 1:
                        object_key = path_parts[0]
            
            # Final fallback to file_name
            if not object_key:
                object_key = file_event.file_name
            
            if not object_key:
                logger.warning("Could not determine object_key for file event", 
                             file_path=file_event.file_path,
                             file_url=file_event.file_url,
                             file_name=file_event.file_name)
                return None
            
            # Prepare metadata - include all available information
            event_metadata = file_event.metadata or {}
            # Add additional fields to metadata if not already present
            if file_event.checksum and 'checksum' not in event_metadata:
                event_metadata['checksum'] = file_event.checksum
            if file_event.event_id and 'event_id' not in event_metadata:
                event_metadata['event_id'] = file_event.event_id
            if file_event.discovered_at and 'discovered_at' not in event_metadata:
                event_metadata['discovered_at'] = file_event.discovered_at.isoformat()
            
            # Resolve poller_job_id to the actual UUID id
            # If it's already a UUID, use it; otherwise look up by job_id
            async with database_client.get_session() as session:
                # Try to find the poller job by job_id (string) or id (UUID)
                stmt = select(PollerJob).where(
                    (PollerJob.job_id == poller_job_id) | (PollerJob.id == poller_job_id)
                )
                result = await session.execute(stmt)
                poller_job = result.scalar_one_or_none()
                
                if not poller_job:
                    logger.warning("Poller job not found", 
                                 poller_job_id=poller_job_id,
                                 job_id=file_event.job_id if hasattr(file_event, 'job_id') else None)
                    return None
                
                # Use the UUID id from the poller_job record
                actual_poller_job_id = poller_job.id
                
                new_file_event = FileEventModel(
                    poller_job_id=actual_poller_job_id,
                    object_key=object_key,
                    file_name=file_event.file_name,
                    file_size=file_event.file_size,
                    file_type=file_event.file_type,
                    source_type=file_event.source_type.value if file_event.source_type else None,
                    file_url=file_event.file_url,
                    event_metadata=event_metadata,
                    processed_at=file_event.processed_at or datetime.utcnow()
                )
                
                session.add(new_file_event)
                await session.commit()
                
                logger.info("Created file event in database", 
                           poller_job_id=str(actual_poller_job_id),
                           job_id=poller_job.job_id,
                           object_key=object_key,
                           file_name=file_event.file_name,
                           file_size=file_event.file_size,
                           file_type=file_event.file_type,
                           source_type=file_event.source_type.value if file_event.source_type else None)
                
                return new_file_event
                
        except Exception as e:
            logger.error("Failed to create file event", 
                        error=str(e),
                        poller_job_id=poller_job_id,
                        file_name=file_event.file_name if file_event else None,
                        exc_info=True)
            return None
    


# Global service instance
file_event_service = FileEventService()

