"""
Poller worker service - main orchestration logic.
Simple, production-ready implementation following SOLID principles.
"""

import asyncio
import uuid
import mimetypes
from datetime import datetime
from typing import Dict, Any, List
import structlog

from src.models.schemas import PollingJob, FileEvent, SourceConfig
from src.models.poller_jobs import JobStatus
from src.services.kafka_consumer import polling_queue_consumer
from src.services.kafka_producer import file_event_queue_producer
from src.services.source_connector import source_connector
from src.services.file_tracker import file_tracker
from src.services.retry_handler import retry_handler
from src.services.dlq_producer import dlq_producer
from src.services.redis_persistence import redis_persistence
from src.services.poller_job_service import poller_job_service
from src.services.file_event_service import file_event_service
from src.utils.monitoring import metrics_collector
from src.utils.rate_limiter import rate_limiter
from src.utils.circuit_breaker import circuit_breaker
from src.utils.timeout_handler import with_timeout
from src.utils.secrets_manager import SecretsManager
from src.utils.memory_manager import memory_manager
from src.utils.backpressure import backpressure_manager
from src.utils.tracing import tracer
from src.config.settings import settings
from src.clients.minio_client import MinIOClient

logger = structlog.get_logger(__name__)


class PollerWorker:
    """
    Main poller worker service.
    Orchestrates the 5-step polling process.
    """
    
    def __init__(self):
        self._running = False
        self._active_jobs = set()
        self._active_jobs_lock = asyncio.Lock()  # Fix race condition
        self._shutdown_event = asyncio.Event()
        self._job_semaphore = asyncio.Semaphore(settings.max_concurrent_jobs)  # Concurrency control
    
    async def initialize(self):
        """Initialize the poller worker."""
        try:
            logger.info("Initializing poller worker")
            
            # Initialize Kafka consumer
            await polling_queue_consumer.initialize()
            polling_queue_consumer.set_message_handler(self._handle_polling_job)
            
            # Initialize Kafka producer
            await file_event_queue_producer.initialize()
            
            # Initialize DLQ producer
            await dlq_producer.initialize()
            
            # Initialize Redis persistence
            await redis_persistence.initialize()
            
            logger.info("Poller worker initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize poller worker", error=str(e))
            raise
    
    async def start(self):
        """Start the poller worker."""
        if self._running:
            logger.warning("Poller worker is already running")
            return
        
        try:
            logger.info("Starting poller worker")
            self._running = True
            self._shutdown_event.clear()
            
            # Start Kafka consumer (Step 1: Listen to Kafka topic)
            await polling_queue_consumer.start()
            
        except Exception as e:
            logger.error("Failed to start poller worker", error=str(e))
            raise
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the poller worker."""
        if not self._running:
            return
        
        try:
            logger.info("Stopping poller worker")
            self._running = False
            self._shutdown_event.set()
            
            # Wait for active jobs to complete (with timeout)
            async with self._active_jobs_lock:
                active_jobs = list(self._active_jobs)
            
            if active_jobs:
                logger.info("Waiting for active jobs to complete", 
                           active_jobs=len(active_jobs))
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_jobs, return_exceptions=True),
                        timeout=settings.worker_timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for jobs, cancelling",
                                 active_jobs=len(active_jobs))
                    for task in active_jobs:
                        if not task.done():
                            task.cancel()
            
            # Stop Kafka consumer
            await polling_queue_consumer.stop()
            
            # Stop DLQ producer
            await dlq_producer.stop()
            
            # Close Redis persistence
            await redis_persistence.close()
            
            logger.info("Poller worker stopped")
            
        except Exception as e:
            logger.error("Error stopping poller worker", error=str(e))
    
    async def _handle_polling_job(self, job: PollingJob):
        """Handle polling job with retry, rate limiting, backpressure, tracing, and DLQ support."""
        context = self._get_job_context(job)
        
        # Sanitize context for logging (remove any sensitive data)
        safe_context = SecretsManager.sanitize_dict(context)
        
        # Create/update job in database (PENDING status)
        await poller_job_service.create_or_update_job(job, JobStatus.PENDING)
        
        # Start tracing
        with tracer.trace("handle_polling_job", job_id=job.job_id, org_id=job.org_id):
            logger.info("Handling polling job", **safe_context)
            
            # Backpressure check
            if not await backpressure_manager.acquire():
                logger.warning("Backpressure active, skipping job", **safe_context)
                metrics_collector.record_job_processed("backpressure", "multi_connection", 0)
                return
            
            # Rate limiting check
            if not await rate_limiter.acquire(job.org_id):
                logger.warning("Rate limit exceeded, skipping job", **safe_context)
                metrics_collector.record_job_processed("rate_limited", "multi_connection", 0)
                await backpressure_manager.release()
                return
            
            # Concurrency control (semaphore)
            async with self._job_semaphore:
                # Track active job (with lock to prevent race condition)
                task = asyncio.current_task()
                async with self._active_jobs_lock:
                    if task:
                        self._active_jobs.add(task)
                
                try:
                    # Update status to PROCESSING
                    await poller_job_service.update_job_status(job.job_id, JobStatus.PROCESSING)
                    
                    await retry_handler.execute_with_retry(
                        self._process_polling_job,
                        job,
                        operation_name="process_polling_job",
                        context=safe_context,
                        retryable_exceptions=(Exception,)
                    )
                    
                    logger.info("Job completed successfully", **safe_context)
                except Exception as e:
                    await self._handle_job_failure(job, e)
                    raise
                finally:
                    # Remove from active jobs (with lock)
                    async with self._active_jobs_lock:
                        if task:
                            self._active_jobs.discard(task)
                    await rate_limiter.release(job.org_id)
                    await backpressure_manager.release()
    
    def _get_job_context(self, job: PollingJob) -> Dict[str, Any]:
        """Get logging context for a job."""
        return {
            "job_id": job.job_id,
            "org_id": job.org_id,
            "connection_count": len(job.connection_list)
        }
    
    def _convert_connection_list_to_dict(self, connection_list: List[SourceConfig]) -> List[Dict[str, Any]]:
        """Convert SourceConfig list to dict format expected by resume parser."""
        result = []
        for conn in connection_list:
            # If it's already a dict, use it as-is (shouldn't happen but handle it)
            if isinstance(conn, dict):
                result.append(conn)
                continue
            
            # Use model_dump to get all fields, including any extra fields from the original message
            conn_dict = conn.model_dump(exclude_none=True, mode='json')
            
            # Ensure source_type is a string value
            if "source_type" in conn_dict and hasattr(conn_dict["source_type"], 'value'):
                conn_dict["source_type"] = conn_dict["source_type"].value
            
            # For MySQL sources, ensure field names match what resume parser expects
            # Master-service sends: host, port, database, username, password, table
            # SourceConfig model has: database_host, database_port, database_name, database_user, database_password, database_table
            # If the original message had host/port/etc, they should be preserved in model_dump
            # But if only database_* fields exist, convert them
            if conn_dict.get("source_type") == "mysql":
                # Convert database_* fields to expected format if original format not present
                if "host" not in conn_dict and conn.database_host:
                    conn_dict["host"] = conn.database_host
                if "port" not in conn_dict and conn.database_port:
                    conn_dict["port"] = conn.database_port
                if "database" not in conn_dict and conn.database_name:
                    conn_dict["database"] = conn.database_name
                if "username" not in conn_dict and conn.database_user:
                    conn_dict["username"] = conn.database_user
                if "password" not in conn_dict and conn.database_password:
                    conn_dict["password"] = conn.database_password
                if "table" not in conn_dict and conn.database_table:
                    conn_dict["table"] = conn.database_table
                
                # Remove database_* fields to avoid confusion
                conn_dict.pop("database_host", None)
                conn_dict.pop("database_port", None)
                conn_dict.pop("database_name", None)
                conn_dict.pop("database_user", None)
                conn_dict.pop("database_password", None)
                conn_dict.pop("database_table", None)
            
            result.append(conn_dict)
        return result
    
    async def _handle_job_failure(self, job: PollingJob, error: Exception):
        """Handle job failure by sending to DLQ and updating database."""
        context = self._get_job_context(job)
        logger.error("Job failed, sending to DLQ", error=str(error), error_type=type(error).__name__, **context)
        
        # Update job status to FAILED in database
        await poller_job_service.update_job_status(
            job.job_id,
            JobStatus.FAILED,
            error_message=str(error)
        )
        
        await dlq_producer.publish_failed_job(
            job=job,
            error=error,
            retry_count=settings.max_retries,
            failure_reason=f"Job failed after {settings.max_retries} retries: {str(error)}",
            additional_metadata={**context, "priority": job.priority}
        )
    
    async def _process_polling_job(self, job: PollingJob):
        """Process polling job through all connections with validation and tracing."""
        context = self._get_job_context(job)
        job_start_time = datetime.utcnow()
        stats = {"published": 0, "failed": 0, "skipped": 0}
        
        # Validate job data
        from src.utils.data_sanitizer import data_sanitizer
        validation = data_sanitizer.validate_sanitized_data(job.model_dump(), data_type='job')
        if not validation['is_valid']:
            logger.error("Job validation failed", errors=validation['errors'], **context)
            raise ValueError(f"Invalid job data: {validation['errors']}")
        
        # Trace job processing
        with tracer.trace("process_polling_job", job_id=job.job_id, org_id=job.org_id):
            logger.info("Processing polling job", priority=job.priority, **context)
        
        try:
            # Check memory before processing
            if not memory_manager.check_memory_limit():
                logger.error("Memory limit exceeded, skipping job", **context)
                raise Exception("Memory limit exceeded")
            
            # Perform cleanup if needed
            if memory_manager.should_cleanup():
                memory_manager.cleanup()
            
            for idx, source_config in enumerate(job.connection_list, 1):
                connection_stats = await self._process_connection(job, source_config, idx)
                stats["published"] += connection_stats["published"]
                stats["failed"] += connection_stats["failed"]
                stats["skipped"] += connection_stats["skipped"]
            
            await self._log_job_completion(job, stats, job_start_time)
            
            # Update job status to COMPLETED with final file count
            await poller_job_service.update_job_status(
                job.job_id, 
                JobStatus.COMPLETED,
                processed_file_count=stats["published"]
            )
            
            metrics_collector.record_job_processed("completed", "multi_connection", 
                                                  (datetime.utcnow() - job_start_time).total_seconds())
        except Exception as e:
            duration = (datetime.utcnow() - job_start_time).total_seconds()
            logger.error("Error processing job", error=str(e), duration=duration, **context, exc_info=True)
            
            # Update job status to FAILED
            await poller_job_service.update_job_status(
                job.job_id,
                JobStatus.FAILED,
                error_message=str(e)
            )
            
            metrics_collector.record_job_processed("failed", "multi_connection", duration)
            raise
    
    async def _process_connection(self, job: PollingJob, source_config: SourceConfig, index: int) -> Dict[str, int]:
        """Process a single connection. Returns stats."""
        source_type = source_config.source_type.value
        context = {**self._get_job_context(job), "connection_index": index, "source_type": source_type}
        stats = {"published": 0, "failed": 0, "skipped": 0}
        
        logger.info("Processing connection", **context)
        
        try:
            # Connect
            if not await self._connect_to_source(source_config, context):
                return stats
            
            # List files
            files = await self._list_files_from_source(source_config, job.file_pattern, context)
            if not files:
                await source_connector.disconnect(source_config)
                return stats
            
            # Process files
            stats = await self._process_files(job, files, source_config, context)
            
            # Record statistics
            await file_tracker.record_connection_statistics(
                org_id=job.org_id,
                source_config=source_config,
                job_id=job.job_id,
                success_count=stats["published"],
                failed_count=stats["failed"],
                skipped_count=stats["skipped"]
            )
            
            await self._log_connection_completion(context, stats, source_config)
            await source_connector.disconnect(source_config)
            
        except Exception as e:
            logger.error("Error processing connection", error=str(e), **context)
            await self._safe_disconnect(source_config)
        
        return stats
    
    async def _connect_to_source(self, source_config: SourceConfig, context: Dict[str, Any]) -> bool:
        """Connect to source with circuit breaker and timeout. Returns True if successful."""
        from src.utils.connection_key import generate_connection_key
        
        logger.info("Connecting to source", **context)
        
        # Check circuit breaker
        connection_key = generate_connection_key(context["org_id"], source_config)
        if circuit_breaker.is_open(connection_key):
            logger.warning("Circuit breaker is open, skipping connection", 
                         connection_key=connection_key, **context)
            return False
        
        try:
            # Connect with timeout and circuit breaker
            connected = await circuit_breaker.call(
                connection_key,
                with_timeout,
                source_connector.connect,
                settings.connection_timeout,
                source_config,
                operation_name="connect_to_source"
            )
            
            if not connected:
                logger.error("Connection failed", **context)
            return connected
        except asyncio.TimeoutError:
            logger.error("Connection timeout", timeout=settings.connection_timeout, **context)
            return False
        except Exception as e:
            logger.error("Connection error", error=str(e), **context)
            return False
    
    async def _list_files_from_source(self, source_config: SourceConfig, file_pattern: str, 
                                     context: Dict[str, Any]) -> list:
        """List files from source with timeout."""
        logger.info("Listing files", file_pattern=file_pattern, **context)
        try:
            files = await with_timeout(
                source_connector.list_files,
                settings.read_timeout,
                source_config,
                file_pattern,
                operation_name="list_files"
            )
            if files:
                logger.info("Files discovered", file_count=len(files), **context)
            else:
                logger.warning("No files found", **context)
            return files or []
        except asyncio.TimeoutError:
            logger.error("List files timeout", timeout=settings.read_timeout, **context)
            return []
    
    async def _process_files(self, job: PollingJob, files: list, source_config: SourceConfig,
                           context: Dict[str, Any]) -> Dict[str, int]:
        """Process files in batches and publish to queue. Returns stats."""
        stats = {"published": 0, "failed": 0, "skipped": 0}
        logger.info("Processing files", file_count=len(files), **context)
        
        # Filter out skipped files first
        files_to_process = []
        for file_event in files:
            if await self._should_skip_file(job, file_event):
                stats["skipped"] += 1
            else:
                files_to_process.append(file_event)
        
        # Sort files to ensure consistent processing order
        # Sort by file_path first, then file_name for predictable ordering
        files_to_process.sort(key=lambda f: (f.file_path or "", f.file_name or ""))
        
        logger.info("Files sorted for processing", 
                   total_files=len(files_to_process),
                   skipped=stats["skipped"],
                   **context)
        
        # Process only ONE batch per job execution (maximum batch_size per batch)
        # Use batch_size from job if provided, otherwise fall back to settings
        max_batch_size = job.batch_size if job.batch_size is not None else settings.max_file_batch_size
        if job.batch_size is not None:
            logger.info("Using batch_size from job", batch_size=job.batch_size, **context)
        else:
            logger.info("Using default batch_size from settings", batch_size=settings.max_file_batch_size, **context)
        total_files = len(files_to_process)
        
        if total_files == 0:
            logger.info("No files to process", **context)
            return stats
        
        # Process only the first batch (files 0 to max_batch_size)
        batch_start = 0
        batch_end = min(max_batch_size, total_files)
        batch_files = files_to_process[batch_start:batch_end]
        
        # Calculate which batch number this is (for logging)
        # Count how many files have already been processed to determine batch number
        total_processed = stats["skipped"]  # Files already processed
        batch_number = (total_processed // max_batch_size) + 1
        
        # Log file names in this batch for clarity
        file_names_in_batch = [f.file_name for f in batch_files]
        file_paths_in_batch = [f.file_path for f in batch_files]
        
        total_batches = (total_files + max_batch_size - 1) // max_batch_size  # Ceiling division
        
        logger.info("Processing single file batch (one batch per job)", 
                   batch_number=batch_number,
                   file_range=f"{batch_start + 1}-{batch_end}",
                   batch_start=batch_start + 1,
                   batch_end=batch_end,
                   batch_size=len(batch_files),
                   total_files=total_files,
                   total_batches=total_batches,
                   remaining_files=total_files - len(batch_files),
                   file_names=file_names_in_batch,
                   file_paths=file_paths_in_batch,
                   **context)
        
        try:
            # Prepare batch: set job_id and processed_at for all files
            for file_event in batch_files:
                file_event.job_id = job.job_id
                file_event.processed_at = datetime.utcnow()

            # If nxtworkforce flag is set, first upload the file bytes to local MinIO (env.local MINIO_* creds)
            # and publish file events pointing to the uploaded object.
            nxtworkforce_enabled = bool((job.metadata or {}).get("nxtworkforce"))
            if nxtworkforce_enabled:
                local_bucket = settings.nxtworkforce_save_bucket
                local_minio = MinIOClient(
                    {
                        "endpoint": settings.minio_endpoint,
                        "access_key": settings.minio_access_key,
                        "secret_key": settings.minio_secret_key,
                        "minio_secure": settings.minio_secure,
                    }
                )

                for file_event in batch_files:
                    file_bytes = await source_connector.read_file(source_config, file_event.file_path)
                    if not file_bytes:
                        raise RuntimeError(f"Empty file bytes for {file_event.file_path}")

                    # Generate UUID for object key (without extension)
                    original_file_name = file_event.file_name or (
                        file_event.file_path.split("/")[-1] if file_event.file_path else "file"
                    )
                    
                    # Extract file extension for metadata (but don't include in object key)
                    file_extension = ""
                    if original_file_name and "." in original_file_name:
                        file_extension = "." + original_file_name.rsplit(".", 1)[-1]
                    
                    # Generate UUID (without extension) for object key and database ID
                    file_uuid = str(uuid.uuid4())
                    # Use UUID only as object key (no extension)
                    dest_object_key = file_uuid

                    # Determine MIME type from file extension
                    mime_type, _ = mimetypes.guess_type(original_file_name)
                    if not mime_type:
                        # Fallback to common types based on extension
                        extension_lower = file_extension.lower() if file_extension else ""
                        mime_type_map = {
                            ".pdf": "application/pdf",
                            ".doc": "application/msword",
                            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            ".txt": "text/plain",
                            ".rtf": "application/rtf",
                        }
                        mime_type = mime_type_map.get(extension_lower, "application/octet-stream")

                    # Prepare metadata to store with the object in MinIO
                    # Only include specific keys: Filename, Id, Mimetype, Content-Type, Entityid, Entitytype, Referenceid, Referencetype
                    # Use double prefix format: X-Amz-Meta-X-Amz-Meta-{key}
                    minio_metadata = {
                        "X-Amz-Meta-Filename": original_file_name,
                        "X-Amz-Meta-Id": file_uuid,
                        "X-Amz-Meta-Mimetype": mime_type,
                        "X-Amz-Meta-Content-Type": mime_type,
                    }
                    
                    # Extract specific metadata from file_event.metadata or job.metadata if available
                    # These should already be in the correct format (Entityid, Entitytype, Referenceid, Referencetype)
                    # Map of allowed metadata keys to preserve
                    allowed_keys = ["Entityid", "Entitytype", "Referenceid", "Referencetype"]
                    
                    # Check file_event.metadata first, then job.metadata
                    sources = []
                    if file_event.metadata:
                        sources.append(file_event.metadata)
                    if job.metadata:
                        sources.append(job.metadata)
                    
                    for key in allowed_keys:
                        value = None
                        # Check all sources for the key (with various prefix formats)
                        for source in sources:
                            value = (
                                source.get(f"X-Amz-Meta-{key}") or
                                source.get(f"X-Amz-Meta-X-Amz-Meta-{key}") or
                                source.get(key)
                            )
                            if value is not None:
                                break
                        
                        if value is not None:
                            # Ensure double prefix format
                            minio_metadata[f"X-Amz-Meta-{key}"] = str(value)

                    uploaded = await local_minio.put_object_bytes(
                        local_bucket,
                        dest_object_key,
                        file_bytes,
                        content_type=mime_type,  # Use proper MIME type
                        metadata=minio_metadata,
                    )
                    if not uploaded:
                        raise RuntimeError(
                            f"Failed to upload to local MinIO: s3://{local_bucket}/{dest_object_key}"
                        )

                    file_event.file_path = dest_object_key
                    file_event.file_url = f"s3://{local_bucket}/{dest_object_key}"
                    file_event.metadata = {
                        **(file_event.metadata or {}),
                        "nxtworkforce_saved": True,
                        "nxtworkforce_bucket": local_bucket,
                        "nxtworkforce_object_key": dest_object_key,
                        "nxtworkforce_uuid": file_uuid,  # Store UUID without extension for database ID
                    }
            
            # Publish batch to Kafka
            # Include connection_list in metadata so resume parser can extract MySQL config
            # Convert to dict format expected by resume parser
            batch_metadata = {
                **(job.metadata or {}), 
                "org_id": job.org_id,
                "connection_list": self._convert_connection_list_to_dict(job.connection_list) if job.connection_list else []
            }
            # Include API keys in metadata if provided
            if job.api_keys:
                batch_metadata["api_keys"] = job.api_keys
            await retry_handler.execute_with_retry(
                file_event_queue_producer.publish_file_events_batch,
                batch_files,
                metadata=batch_metadata,
                operation_name="publish_file_events_batch",
                context={**context, "batch_number": batch_number, "batch_size": len(batch_files)},
                retryable_exceptions=(Exception,)
            )
            
            # Mark all files in batch as processed
            for file_event in batch_files:
                await file_tracker.mark_file_as_processed(job.org_id, file_event, success=True)
                
                # Save file event to database
                await file_event_service.create_file_event(file_event, poller_job_id=job.job_id)
                
                # Increment processed file count in database
                await poller_job_service.increment_file_count(job.job_id)
                
                stats["published"] += 1
                metrics_collector.record_file_processed("success", file_event.file_type)
            
            logger.info("File batch processed and saved successfully", 
                       batch_number=batch_number,
                       file_range=f"{batch_start + 1}-{batch_end}",
                       batch_size=len(batch_files),
                       files_processed=len(batch_files),
                       remaining_files=total_files - len(batch_files),
                       **context)
            
        except Exception as e:
            # Mark all files in batch as failed
            for file_event in batch_files:
                stats["failed"] += 1
                metrics_collector.record_file_processed("failed", file_event.file_type)
                logger.error("Failed to publish file batch", 
                           batch_number=batch_number,
                           file_name=file_event.file_name, 
                           error=str(e), **context)
        
        return stats
    
    async def _should_skip_file(self, job: PollingJob, file_event: FileEvent) -> bool:
        """Check if file should be skipped (already processed)."""
        is_processed = await file_tracker.is_file_processed(job.org_id, file_event)
        if is_processed:
            logger.debug("File already processed, skipping", 
                        file_name=file_event.file_name,
                        file_type=file_event.file_type,
                        org_id=job.org_id)
            return True
        
        # Process all files regardless of extension
        # File extension filtering is disabled to allow all file types from MinIO
        return False
    
    async def _publish_file(self, job: PollingJob, file_event: FileEvent, context: Dict[str, Any]):
        """Publish file event to queue with retry."""
        file_event.job_id = job.job_id
        file_event.processed_at = datetime.utcnow()
        
        # Include connection_list in metadata so resume parser can extract MySQL config
        # Convert to dict format expected by resume parser
        file_metadata = {
            **(job.metadata or {}), 
            "org_id": job.org_id,
            "connection_list": self._convert_connection_list_to_dict(job.connection_list) if job.connection_list else []
        }
        # Include API keys in metadata if provided
        if job.api_keys:
            file_metadata["api_keys"] = job.api_keys
        await retry_handler.execute_with_retry(
            file_event_queue_producer.publish_file_event,
            file_event,
            metadata=file_metadata,
            operation_name="publish_file_event",
            context={**context, "file_name": file_event.file_name, "event_id": file_event.event_id},
            retryable_exceptions=(Exception,)
        )
    
    async def _log_connection_completion(self, context: Dict[str, Any], stats: Dict[str, int], 
                                  source_config: SourceConfig):
        """Log connection completion with statistics."""
        conn_stats = await file_tracker.get_connection_statistics(context["org_id"], source_config)
        stats_info = {}
        if conn_stats:
            stats_info = {
                "last_processed_at": conn_stats.last_processed_at.isoformat() if conn_stats.last_processed_at else None,
                "total_processed": conn_stats.total_processed,
                "total_success": conn_stats.total_success,
                "total_failed": conn_stats.total_failed,
                "total_skipped": conn_stats.total_skipped
            }
        
        logger.info("Connection completed", published=stats["published"], 
                   failed=stats["failed"], skipped=stats["skipped"],
                   connection_stats=stats_info, **context)
    
    async def _log_job_completion(self, job: PollingJob, stats: Dict[str, int], start_time: datetime):
        """Log job completion with summary."""
        duration = (datetime.utcnow() - start_time).total_seconds()
        all_stats = await file_tracker.get_all_connection_statistics(job.org_id)
        connection_summary = [
            {
                "connection_key": key,
                "source_type": stat.source_type,
                "last_processed_at": stat.last_processed_at.isoformat() if stat.last_processed_at else None,
                "total_success": stat.total_success,
                "total_failed": stat.total_failed,
                "total_skipped": stat.total_skipped,
                "total_processed": stat.total_processed
            }
            for key, stat in all_stats.items()
        ]
        
        logger.info("Job completed", total_published=stats["published"],
                   total_failed=stats["failed"], total_skipped=stats["skipped"],
                   duration=duration, connection_statistics=connection_summary,
                   **self._get_job_context(job))
    
    async def _safe_disconnect(self, source_config: SourceConfig):
        """Safely disconnect from source, ignoring errors."""
        try:
            await source_connector.disconnect(source_config)
        except Exception:
            pass


# Global poller worker instance
poller_worker = PollerWorker()

