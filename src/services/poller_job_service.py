"""
Service for managing poller_jobs in the database.
Handles CRUD operations and pagination for job tracking.
"""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import structlog
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.clients.database_client import database_client
from src.models.poller_jobs import PollerJob, JobStatus, JobPriority
from src.models.schemas import PollingJob as PollingJobSchema

logger = structlog.get_logger(__name__)


class PollerJobService:
    """Service for managing poller jobs in the database."""
    
    async def create_or_update_job(self, job: PollingJobSchema, status: JobStatus = JobStatus.PENDING) -> Optional[PollerJob]:
        """Create or update a poller job record."""
        if not database_client._initialized:
            logger.warning("Database not initialized, skipping job tracking")
            return None
        
        try:
            async with database_client.get_session() as session:
                # Check if job already exists
                stmt = select(PollerJob).where(PollerJob.job_id == job.job_id)
                result = await session.execute(stmt)
                existing_job = result.scalar_one_or_none()
                
                if existing_job:
                    # Update existing job
                    existing_job.org_id = job.org_id
                    existing_job.connection_list = [conn.model_dump() for conn in job.connection_list] if job.connection_list else []
                    existing_job.file_pattern = job.file_pattern
                    # Map priority string to enum (handle both lowercase and uppercase)
                    priority_str = job.priority.upper() if job.priority else "NORMAL"
                    existing_job.priority = JobPriority[priority_str] if priority_str in [p.name for p in JobPriority] else JobPriority.NORMAL
                    existing_job.job_metadata = job.metadata
                    existing_job.job_status = status
                    existing_job.updated_at = datetime.utcnow()
                    
                    await session.commit()
                    logger.info("Updated poller job in database", job_id=job.job_id, status=status.value)
                    return existing_job
                else:
                    # Create new job
                    new_job = PollerJob(
                        job_id=job.job_id,
                        org_id=job.org_id,
                        connection_list=[conn.model_dump() for conn in job.connection_list] if job.connection_list else [],
                        file_pattern=job.file_pattern,
                        priority=JobPriority[job.priority.upper()] if job.priority and job.priority.upper() in [p.name for p in JobPriority] else JobPriority.NORMAL,
                        job_metadata=job.metadata,
                        job_status=status,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    session.add(new_job)
                    await session.commit()
                    logger.info("Created poller job in database", job_id=job.job_id, status=status.value)
                    return new_job
                    
        except Exception as e:
            logger.error("Failed to create/update poller job", job_id=job.job_id, error=str(e))
            return None
    
    async def update_job_status(
        self, 
        job_id: str, 
        status: JobStatus, 
        error_message: Optional[str] = None,
        processed_file_count: Optional[int] = None
    ) -> bool:
        """Update job status and related fields."""
        if not database_client._initialized:
            return False
        
        try:
            async with database_client.get_session() as session:
                stmt = select(PollerJob).where(PollerJob.job_id == job_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()
                
                if not job:
                    logger.warning("Job not found for status update", job_id=job_id)
                    return False
                
                job.job_status = status
                job.updated_at = datetime.utcnow()
                
                if status == JobStatus.PROCESSING and not job.start_time:
                    job.start_time = datetime.utcnow()
                
                if status in [JobStatus.COMPLETED, JobStatus.FAILED] and not job.end_time:
                    job.end_time = datetime.utcnow()
                
                if error_message:
                    job.error_message = error_message
                
                if processed_file_count is not None:
                    job.processed_file_count = processed_file_count
                
                await session.commit()
                logger.info("Updated job status", job_id=job_id, status=status.value)
                return True
                
        except Exception as e:
            logger.error("Failed to update job status", job_id=job_id, error=str(e))
            return False
    
    async def increment_file_count(self, job_id: str) -> bool:
        """Increment processed file count for a job."""
        if not database_client._initialized:
            return False
        
        try:
            async with database_client.get_session() as session:
                stmt = select(PollerJob).where(PollerJob.job_id == job_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()
                
                if job:
                    job.processed_file_count += 1
                    job.updated_at = datetime.utcnow()
                    await session.commit()
                    return True
                return False
                
        except Exception as e:
            logger.error("Failed to increment file count", job_id=job_id, error=str(e))
            return False
    
    async def increment_retry_count(self, job_id: str) -> bool:
        """Increment retry count for a job."""
        if not database_client._initialized:
            return False
        
        try:
            async with database_client.get_session() as session:
                stmt = select(PollerJob).where(PollerJob.job_id == job_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()
                
                if job:
                    job.retries_count += 1
                    job.job_status = JobStatus.RETRYING
                    job.updated_at = datetime.utcnow()
                    await session.commit()
                    return True
                return False
                
        except Exception as e:
            logger.error("Failed to increment retry count", job_id=job_id, error=str(e))
            return False
    
    async def get_jobs_paginated(
        self,
        org_id: Optional[str] = None,
        status: Optional[JobStatus] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Tuple[List[PollerJob], int]:
        """
        Get jobs with pagination.
        Returns (jobs_list, total_count).
        """
        if not database_client._initialized:
            logger.warning("Database not initialized, returning empty results")
            return [], 0
        
        try:
            async with database_client.get_session() as session:
                # Build base query
                base_query = select(PollerJob)
                count_query = select(func.count()).select_from(PollerJob)
                
                # Apply filters
                if org_id:
                    base_query = base_query.where(PollerJob.org_id == org_id)
                    count_query = count_query.where(PollerJob.org_id == org_id)
                
                if status:
                    base_query = base_query.where(PollerJob.job_status == status)
                    count_query = count_query.where(PollerJob.job_status == status)
                
                # Get total count
                count_result = await session.execute(count_query)
                total_count = count_result.scalar() or 0
                
                # Apply pagination
                offset = (page - 1) * page_size
                base_query = base_query.order_by(desc(PollerJob.created_at))
                base_query = base_query.offset(offset).limit(page_size)
                
                # Execute query
                result = await session.execute(base_query)
                jobs = result.scalars().all()
                
                logger.info("Retrieved jobs with pagination", 
                           total=total_count, 
                           page=page, 
                           page_size=page_size,
                           returned=len(jobs))
                
                return list(jobs), total_count
                
        except Exception as e:
            logger.error("Failed to get jobs with pagination", error=str(e))
            return [], 0
    
    async def get_job_by_id(self, job_id: str) -> Optional[PollerJob]:
        """Get a job by job_id."""
        if not database_client._initialized:
            return None
        
        try:
            async with database_client.get_session() as session:
                stmt = select(PollerJob).where(PollerJob.job_id == job_id)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
                
        except Exception as e:
            logger.error("Failed to get job by id", job_id=job_id, error=str(e))
            return None


# Global service instance
poller_job_service = PollerJobService()

