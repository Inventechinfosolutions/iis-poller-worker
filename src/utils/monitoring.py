"""
Monitoring and metrics utilities for the poller worker service.
Provides Prometheus metrics and health checks.
"""

import time
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import structlog

logger = structlog.get_logger(__name__)

# Prometheus metrics
POLLER_JOBS_TOTAL = Counter(
    'poller_jobs_total',
    'Total number of polling jobs processed',
    ['status', 'source_type']
)

POLLER_JOB_DURATION = Histogram(
    'poller_job_duration_seconds',
    'Time spent processing polling jobs',
    ['source_type']
)

POLLER_FILES_PROCESSED = Counter(
    'poller_files_processed_total',
    'Total number of files processed',
    ['status', 'file_type']
)

POLLER_FILES_PUBLISHED = Counter(
    'poller_files_published_total',
    'Total number of files published to queue',
    ['status']
)

POLLER_ACTIVE_JOBS = Gauge(
    'poller_active_jobs',
    'Number of active polling jobs'
)

POLLER_KAFKA_MESSAGES_CONSUMED = Counter(
    'poller_kafka_messages_consumed_total',
    'Total number of Kafka messages consumed',
    ['topic', 'status']
)

POLLER_KAFKA_MESSAGES_PUBLISHED = Counter(
    'poller_kafka_messages_published_total',
    'Total number of Kafka messages published',
    ['topic', 'status']
)

POLLER_CONNECTION_ERRORS = Counter(
    'poller_connection_errors_total',
    'Total number of connection errors',
    ['source_type', 'error_type']
)

POLLER_RETRIES = Counter(
    'poller_retries_total',
    'Total number of retry attempts',
    ['operation', 'attempt']
)

POLLER_ERRORS = Counter(
    'poller_errors_total',
    'Total number of errors',
    ['operation', 'error_type']
)

POLLER_FAILURES = Counter(
    'poller_failures_total',
    'Total number of failures after retries',
    ['operation', 'error_type']
)

POLLER_DLQ_PUBLISHES = Counter(
    'poller_dlq_publishes_total',
    'Total number of messages published to DLQ',
    ['status']
)

POLLER_CONSUMER_LAG = Gauge(
    'poller_consumer_lag',
    'Kafka consumer lag (messages behind)',
    ['topic', 'partition']
)


class MetricsCollector:
    """Collects and exposes application metrics."""
    
    def __init__(self, port: int = 9093):
        self.port = port
        self.start_time = time.time()
        self._server_started = False
    
    def start_metrics_server(self):
        """Start the Prometheus metrics server."""
        if not self._server_started:
            try:
                start_http_server(self.port)
                logger.info("Prometheus metrics server started", port=self.port)
                logger.info("Health check available at", url=f"http://localhost:{self.port}/metrics")
                self._server_started = True
            except Exception as e:
                logger.warning("Failed to start metrics server", port=self.port, error=str(e))
    
    def record_job_processed(self, status: str, source_type: str, duration: float):
        """Record polling job metrics."""
        POLLER_JOBS_TOTAL.labels(status=status, source_type=source_type).inc()
        POLLER_JOB_DURATION.labels(source_type=source_type).observe(duration)
    
    def record_file_processed(self, status: str, file_type: str):
        """Record file processing metrics."""
        POLLER_FILES_PROCESSED.labels(status=status, file_type=file_type).inc()
    
    def record_file_published(self, status: str):
        """Record file publishing metrics."""
        POLLER_FILES_PUBLISHED.labels(status=status).inc()
    
    def update_active_jobs(self, count: int):
        """Update active jobs metric."""
        POLLER_ACTIVE_JOBS.set(count)
    
    def record_kafka_message_consumed(self, topic: str, success: bool = True):
        """Record Kafka message consumed metrics."""
        status = "success" if success else "error"
        POLLER_KAFKA_MESSAGES_CONSUMED.labels(topic=topic, status=status).inc()
    
    def record_kafka_message_published(self, topic: str, success: bool = True):
        """Record Kafka message published metrics."""
        status = "success" if success else "error"
        POLLER_KAFKA_MESSAGES_PUBLISHED.labels(topic=topic, status=status).inc()
    
    def record_connection_error(self, source_type: str, error_type: str):
        """Record connection error metrics."""
        POLLER_CONNECTION_ERRORS.labels(source_type=source_type, error_type=error_type).inc()
    
    def record_retry(self, operation: str, attempt: int):
        """Record retry attempt metrics."""
        POLLER_RETRIES.labels(operation=operation, attempt=str(attempt)).inc()
    
    def record_error(self, operation: str, error_type: str):
        """Record error metrics."""
        POLLER_ERRORS.labels(operation=operation, error_type=error_type).inc()
    
    def record_failure(self, operation: str, error_type: str):
        """Record failure metrics (after all retries)."""
        POLLER_FAILURES.labels(operation=operation, error_type=error_type).inc()
    
    def record_dlq_publish(self, status: str):
        """Record DLQ publish metrics."""
        POLLER_DLQ_PUBLISHES.labels(status=status).inc()
    
    def record_consumer_lag(self, topic: str, partition: int, lag: int):
        """Record Kafka consumer lag metric."""
        POLLER_CONSUMER_LAG.labels(topic=topic, partition=partition).set(lag)


class HealthChecker:
    """Health check utilities for the service."""
    
    def __init__(self):
        self.start_time = time.time()
    
    async def check_health(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        health_status = {
            "status": "healthy",
            "timestamp": time.time(),
            "uptime": time.time() - self.start_time,
            "checks": {}
        }
        
        # Check basic service health
        health_status["checks"]["service"] = {"status": "healthy", "message": "Service is running"}
        
        # Check Kafka connectivity (consumer)
        try:
            from confluent_kafka import Producer, Consumer
            from src.config.kafka_settings import kafka_settings
            
            # Test producer
            producer = Producer({"bootstrap.servers": kafka_settings.bootstrap_servers})
            metadata = producer.list_topics(timeout=5)
            producer.flush(timeout=1)
            
            health_status["checks"]["kafka"] = {
                "status": "healthy", 
                "message": "Kafka connection successful",
                "topics": len(metadata.topics),
                "bootstrap_servers": kafka_settings.bootstrap_servers
            }
        except Exception as e:
            health_status["checks"]["kafka"] = {"status": "unhealthy", "message": f"Kafka error: {str(e)}"}
            health_status["status"] = "unhealthy"
        
        # Check Redis connectivity
        try:
            from src.clients.redis_client import redis_client
            
            # Test Redis connection
            if redis_client and hasattr(redis_client, 'ping'):
                await redis_client.ping()
                health_status["checks"]["redis"] = {
                    "status": "healthy", 
                    "message": "Redis connection successful"
                }
            else:
                health_status["checks"]["redis"] = {"status": "warning", "message": "Redis client not initialized"}
        except Exception as e:
            health_status["checks"]["redis"] = {"status": "warning", "message": f"Redis not available: {str(e)}"}
        
        # Determine overall health
        critical_checks = ["service", "kafka"]
        unhealthy_checks = [check for check in critical_checks if health_status["checks"].get(check, {}).get("status") != "healthy"]
        
        if unhealthy_checks:
            health_status["status"] = "unhealthy"
        elif any(check.get("status") == "unhealthy" for check in health_status["checks"].values()):
            health_status["status"] = "degraded"
        
        return health_status


# Global instances
metrics_collector = MetricsCollector()
health_checker = HealthChecker()

