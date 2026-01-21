"""
Health check API endpoints for the poller worker service.
Provides REST endpoints for health checks and metrics.
"""

from typing import Dict, Any, Optional, List
import structlog
from fastapi import FastAPI, APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import generate_latest
import os

from src.config.settings import settings
from src.utils.monitoring import health_checker, metrics_collector
from src.clients.database_client import database_client
from src.services.poller_job_service import poller_job_service
from src.services.file_event_service import file_event_service
from src.models.poller_jobs import JobStatus

logger = structlog.get_logger(__name__)

# Get API prefix from environment (default: poller-worker/api/v1)
api_prefix = os.getenv("API_PREFIX", "poller-worker/api/v1")

# Create router with prefix (similar to jd-service pattern)
router = APIRouter(prefix=f"/{api_prefix}", tags=["poller-worker"])

# Create FastAPI app for health checks
app = FastAPI(
    title="Poller Worker Health API",
    description="Health check and metrics endpoints for poller worker service",
    version=settings.version,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Include router
app.include_router(router)


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: float
    uptime: float
    checks: Dict[str, Any]


@router.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Poller Worker Health API",
        "version": settings.version,
        "docs": "/docs",
        "health": f"/{api_prefix}/health",
        "ready": f"/{api_prefix}/ready",
        "metrics": f"/{api_prefix}/metrics",
        "jobs": f"/{api_prefix}/api/jobs",
        "job_by_id": f"/{api_prefix}/api/jobs/{{job_id}}",
        "file_events": f"/{api_prefix}/api/file-events",
        "file_event_by_id": f"/{api_prefix}/api/file-events/{{event_id}}"
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        health_status = await health_checker.check_health()
        return HealthResponse(**health_status)
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=500, detail="Health check failed")


@router.get("/ready")
async def readiness_check():
    """Readiness check endpoint for Kubernetes."""
    try:
        health_status = await health_checker.check_health()
        
        # Check critical components
        kafka_healthy = health_status["checks"].get("kafka", {}).get("status") == "healthy"
        service_healthy = health_status["checks"].get("service", {}).get("status") == "healthy"
        
        if kafka_healthy and service_healthy:
            return {"status": "ready", "message": "Service is ready to accept requests"}
        else:
            return Response(
                content='{"status": "not_ready", "message": "Service is not ready"}',
                status_code=503,
                media_type="application/json"
            )
    except Exception as e:
        logger.error("Readiness check failed", error=str(e))
        return Response(
            content='{"status": "not_ready", "message": "Readiness check failed"}',
            status_code=503,
            media_type="application/json"
        )


@router.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    try:
        return Response(
            content=generate_latest(),
            media_type="text/plain"
        )
    except Exception as e:
        logger.error("Metrics endpoint failed", error=str(e))
        raise HTTPException(status_code=500, detail="Metrics endpoint failed")


class PollerJobResponse(BaseModel):
    """Poller job response model."""
    id: str
    job_id: str
    org_id: str
    connection_list: List[Dict[str, Any]]
    file_pattern: Optional[str]
    priority: str
    job_metadata: Optional[Dict[str, Any]]
    job_status: str
    processed_file_count: int
    retries_count: int
    error_message: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    created_at: str
    updated_at: str


class PaginatedJobsResponse(BaseModel):
    """Paginated jobs response model."""
    jobs: List[PollerJobResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


@router.get("/api/jobs", response_model=PaginatedJobsResponse)
async def get_jobs(
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    status: Optional[str] = Query(None, description="Filter by job status (PENDING, PROCESSING, COMPLETED, FAILED, RETRYING)"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page (max 100)")
):
    """
    Get poller jobs with pagination.
    Default page size is 10. Maximum page size is 100.
    """
    try:
        # Parse status if provided
        job_status = None
        if status:
            try:
                job_status = JobStatus[status.upper()]
            except KeyError:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid status: {status}. Valid values: {', '.join([s.name for s in JobStatus])}"
                )
        
        # Get jobs with pagination
        jobs, total_count = await poller_job_service.get_jobs_paginated(
            org_id=org_id,
            status=job_status,
            page=page,
            page_size=page_size
        )
        
        # Calculate total pages
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
        
        # Convert to response models
        job_responses = []
        for job in jobs:
            job_responses.append(PollerJobResponse(
                id=str(job.id),
                job_id=job.job_id,
                org_id=job.org_id,
                connection_list=job.connection_list,
                file_pattern=job.file_pattern,
                priority=job.priority.value if job.priority else "NORMAL",
                job_metadata=job.job_metadata,
                job_status=job.job_status.value if job.job_status else "PENDING",
                processed_file_count=job.processed_file_count,
                retries_count=job.retries_count,
                error_message=job.error_message,
                start_time=job.start_time.isoformat() if job.start_time else None,
                end_time=job.end_time.isoformat() if job.end_time else None,
                created_at=job.created_at.isoformat() if job.created_at else "",
                updated_at=job.updated_at.isoformat() if job.updated_at else ""
            ))
        
        return PaginatedJobsResponse(
            jobs=job_responses,
            total=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get jobs", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve jobs: {str(e)}")


@router.get("/api/jobs/{job_id}", response_model=PollerJobResponse)
async def get_job_by_id(job_id: str):
    """Get a specific job by job_id."""
    try:
        job = await poller_job_service.get_job_by_id(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail=f"Job with id {job_id} not found")
        
        return PollerJobResponse(
            id=str(job.id),
            job_id=job.job_id,
            org_id=job.org_id,
            connection_list=job.connection_list,
            file_pattern=job.file_pattern,
            priority=job.priority.value if job.priority else "NORMAL",
            job_metadata=job.job_metadata,
            job_status=job.job_status.value if job.job_status else "PENDING",
            processed_file_count=job.processed_file_count,
            retries_count=job.retries_count,
            error_message=job.error_message,
            start_time=job.start_time.isoformat() if job.start_time else None,
            end_time=job.end_time.isoformat() if job.end_time else None,
            created_at=job.created_at.isoformat() if job.created_at else "",
            updated_at=job.updated_at.isoformat() if job.updated_at else ""
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get job", job_id=job_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve job: {str(e)}")


class FileEventResponse(BaseModel):
    """File event response model."""
    id: str
    poller_job_id: str
    object_key: str
    file_name: Optional[str]
    file_size: Optional[int]
    file_type: Optional[str]
    source_type: Optional[str]
    file_url: Optional[str]
    event_metadata: Optional[Dict[str, Any]]
    processed_at: Optional[str]


class PaginatedFileEventsResponse(BaseModel):
    """Paginated file events response model."""
    file_events: List[FileEventResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


@router.get("/api/file-events", response_model=PaginatedFileEventsResponse)
async def get_file_events(
    poller_job_id: Optional[str] = Query(None, description="Filter by poller job ID (UUID)"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page (max 100)")
):
    """
    Get file events with pagination.
    Default page size is 10. Maximum page size is 100.
    """
    try:
        if not database_client._initialized:
            raise HTTPException(status_code=503, detail="Database not initialized")
        
        async with database_client.get_session() as session:
            from sqlalchemy import select, func, desc
            from src.models.file_events import FileEvent as FileEventModel
            
            # Build base query
            base_query = select(FileEventModel)
            count_query = select(func.count()).select_from(FileEventModel)
            
            # Apply filters
            if poller_job_id:
                base_query = base_query.where(FileEventModel.poller_job_id == poller_job_id)
                count_query = count_query.where(FileEventModel.poller_job_id == poller_job_id)
            
            # Get total count
            count_result = await session.execute(count_query)
            total_count = count_result.scalar() or 0
            
            # Apply pagination
            offset = (page - 1) * page_size
            base_query = base_query.order_by(desc(FileEventModel.id))
            base_query = base_query.offset(offset).limit(page_size)
            
            # Execute query
            result = await session.execute(base_query)
            file_events = result.scalars().all()
            
            # Convert to response models
            event_responses = []
            for event in file_events:
                event_responses.append(FileEventResponse(
                    id=str(event.id),
                    poller_job_id=str(event.poller_job_id),
                    object_key=event.object_key,
                    file_name=event.file_name,
                    file_size=event.file_size,
                    file_type=event.file_type,
                    source_type=event.source_type,
                    file_url=event.file_url,
                    event_metadata=event.event_metadata,
                    processed_at=event.processed_at.isoformat() if event.processed_at else None
                ))
            
            # Calculate total pages
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            
            return PaginatedFileEventsResponse(
                file_events=event_responses,
                total=total_count,
                page=page,
                page_size=page_size,
                total_pages=total_pages
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get file events", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve file events: {str(e)}")


@router.get("/api/file-events/{event_id}", response_model=FileEventResponse)
async def get_file_event_by_id(event_id: str):
    """Get a specific file event by id (UUID)."""
    try:
        if not database_client._initialized:
            raise HTTPException(status_code=503, detail="Database not initialized")
        
        async with database_client.get_session() as session:
            from sqlalchemy import select
            from src.models.file_events import FileEvent as FileEventModel
            import uuid
            
            try:
                event_uuid = uuid.UUID(event_id)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format: {event_id}")
            
            stmt = select(FileEventModel).where(FileEventModel.id == event_uuid)
            result = await session.execute(stmt)
            event = result.scalar_one_or_none()
            
            if not event:
                raise HTTPException(status_code=404, detail=f"File event with id {event_id} not found")
            
            return FileEventResponse(
                id=str(event.id),
                poller_job_id=str(event.poller_job_id),
                object_key=event.object_key,
                file_name=event.file_name,
                file_size=event.file_size,
                file_type=event.file_type,
                source_type=event.source_type,
                file_url=event.file_url,
                event_metadata=event.event_metadata,
                processed_at=event.processed_at.isoformat() if event.processed_at else None
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get file event", event_id=event_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve file event: {str(e)}")


@router.get("/api/file-events/processed", response_model=List[str])
async def get_processed_file_events(
    org_id: Optional[str] = Query(None, description="Organization ID (required when scope=org). Ignored when scope=global."),
    object_keys: str = Query(..., description="Comma-separated list of object keys to check"),
    scope: str = Query("global", description="Tracking scope: org (only this org) or global (any org)")
):
    """
    Check if a list of object keys have already been processed by the poller worker for a given organization.
    Returns a list of object keys that have been processed.
    
    scope=org:
      Only returns files processed for the specified org_id.
      Files processed for different organizations are considered as not processed.

    scope=global:
      Returns files processed for ANY organization (cross-org de-dupe).
      This matches: if Org 1 already processed File X, Org 2 should skip File X.
    """
    try:
        if not database_client._initialized:
            raise HTTPException(status_code=503, detail="Database not initialized")

        scope_norm = (scope or "org").lower().strip()
        if scope_norm not in {"org", "global"}:
            raise HTTPException(status_code=400, detail="Invalid scope. Use scope=org or scope=global")

        if scope_norm == "org" and not org_id:
            raise HTTPException(status_code=400, detail="org_id is required when scope=org")
        
        keys_to_check = [key.strip() for key in object_keys.split(',') if key.strip()]
        if not keys_to_check:
            return []

        async with database_client.get_session() as session:
            from sqlalchemy import select
            from src.models.file_events import FileEvent as FileEventModel
            from src.models.poller_jobs import PollerJob
            
            # Query file_events table for processed files.
            # If scope=org, filter by PollerJob.org_id.
            stmt = (
                select(FileEventModel.object_key)
                .distinct()
                .join(PollerJob, FileEventModel.poller_job_id == PollerJob.id)
                .where(
                    FileEventModel.object_key.in_(keys_to_check),
                    FileEventModel.processed_at.isnot(None)  # Only files that have been processed
                )
            )
            if scope_norm == "org":
                stmt = stmt.where(PollerJob.org_id == org_id)
            result = await session.execute(stmt)
            processed_keys = result.scalars().all()
            
            logger.info(
                "Checked processed file events",
                org_id=org_id,
                scope=scope_norm,
                total_keys_checked=len(keys_to_check),
                processed_keys_count=len(processed_keys),
                processed_keys=list(processed_keys),
                message=(
                    f"scope={scope_norm}. "
                    + (f"Only org_id={org_id} is considered." if scope_norm == "org" else "Any org is considered (global de-dupe).")
                ),
            )
            
            return processed_keys
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check processed file events", org_id=org_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to check processed file events: {str(e)}")
