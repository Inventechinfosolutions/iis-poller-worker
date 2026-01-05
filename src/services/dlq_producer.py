"""
Dead Letter Queue (DLQ) producer for failed messages.
Handles publishing failed jobs to DLQ after all retries are exhausted.
"""

import json
from typing import Optional, Dict, Any
from datetime import datetime
import structlog
from confluent_kafka import Producer, KafkaException

from src.config.kafka_settings import kafka_settings
from src.config.settings import settings
from src.models.schemas import PollingJob
from src.utils.monitoring import metrics_collector
from src.utils.secrets_manager import SecretsManager

logger = structlog.get_logger(__name__)


class DLQProducer:
    """
    Dead Letter Queue producer for failed polling jobs.
    Publishes jobs that failed after all retries to DLQ for manual inspection.
    """
    
    def __init__(self):
        self.producer: Optional[Producer] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the DLQ producer."""
        try:
            logger.info("Initializing DLQ producer",
                       bootstrap_servers=kafka_settings.bootstrap_servers,
                       dlq_topic=settings.dlq_topic)
            
            # Create producer with production-ready settings
            producer_config = {
                "bootstrap.servers": kafka_settings.bootstrap_servers,
                "client.id": f"{kafka_settings.client_id}-dlq",
                "acks": "all",  # Wait for all replicas
                "retries": 5,  # Retry failed sends
                "max.in.flight.requests.per.connection": 1,  # Ensure ordering
                "enable.idempotence": True,  # Prevent duplicates
                "compression.type": "snappy",  # Compression for efficiency
                "linger.ms": 10,  # Batch messages for efficiency
                "batch.size": 16384,  # 16KB batch size
            }
            
            self.producer = Producer(producer_config)
            self._initialized = True
            
            logger.info("DLQ producer initialized successfully",
                       dlq_topic=settings.dlq_topic)
            
        except Exception as e:
            logger.error("Failed to initialize DLQ producer", error=str(e))
            raise
    
    async def publish_failed_job(
        self,
        job: PollingJob,
        error: Exception,
        retry_count: int,
        failure_reason: str,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish a failed job to the Dead Letter Queue.
        
        Args:
            job: The failed PollingJob
            error: The exception that caused the failure
            retry_count: Number of retry attempts made
            failure_reason: Human-readable failure reason
            additional_metadata: Additional metadata to include
            
        Returns:
            True if published successfully, False otherwise
        """
        if not self._initialized:
            logger.error("DLQ producer not initialized")
            return False
        
        try:
            # Sanitize job data to remove credentials before sending to DLQ
            job_data = job.model_dump()
            sanitized_job = SecretsManager.sanitize_dict(job_data)
            
            # Create DLQ message with failure details (credentials sanitized)
            dlq_message = {
                "original_job": sanitized_job,
                "failure_info": {
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "failure_reason": failure_reason,
                    "retry_count": retry_count,
                    "failed_at": datetime.utcnow().isoformat(),
                    "max_retries": settings.max_retries
                },
                "metadata": {
                    **(additional_metadata or {}),
                    "dlq_topic": settings.dlq_topic,
                    "original_topic": kafka_settings.polling_queue_topic
                }
            }
            
            # Serialize message
            message_json = json.dumps(dlq_message, default=str)
            message_key = f"{job.job_id}_dlq_{datetime.utcnow().timestamp()}"
            
            # Publish to DLQ
            def delivery_callback(err, msg):
                if err:
                    logger.error("Failed to publish to DLQ",
                               job_id=job.job_id,
                               error=str(err))
                    metrics_collector.record_dlq_publish("failed")
                else:
                    logger.info("Job published to DLQ",
                              job_id=job.job_id,
                              topic=msg.topic(),
                              partition=msg.partition(),
                              offset=msg.offset())
                    metrics_collector.record_dlq_publish("success")
            
            self.producer.produce(
                topic=settings.dlq_topic,
                key=message_key.encode('utf-8'),
                value=message_json.encode('utf-8'),
                headers={
                    'job_id': job.job_id.encode('utf-8'),
                    'org_id': job.org_id.encode('utf-8'),
                    'error_type': type(error).__name__.encode('utf-8'),
                    'retry_count': str(retry_count).encode('utf-8'),
                    'failed_at': datetime.utcnow().isoformat().encode('utf-8')
                },
                callback=delivery_callback
            )
            
            # Flush to ensure message is sent
            self.producer.flush(timeout=10)
            
            logger.warning("Failed job sent to DLQ",
                         job_id=job.job_id,
                         org_id=job.org_id,
                         retry_count=retry_count,
                         error_type=type(error).__name__,
                         dlq_topic=settings.dlq_topic)
            
            return True
            
        except Exception as e:
            logger.error("Error publishing to DLQ",
                        job_id=job.job_id,
                        error=str(e),
                        exc_info=True)
            metrics_collector.record_dlq_publish("error")
            return False
    
    async def stop(self):
        """Stop the DLQ producer."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                logger.info("DLQ producer stopped")
            except Exception as e:
                logger.error("Error stopping DLQ producer", error=str(e))


# Global DLQ producer instance
dlq_producer = DLQProducer()

