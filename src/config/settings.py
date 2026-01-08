"""
Configuration management for the poller worker service.
All settings are loaded from environment variables with proper validation.
"""

from typing import List
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
import os

# Load environment variables
# Check for ENV_FILE environment variable, otherwise default to .env
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file)


class PollerWorkerConfig(BaseSettings):
    """Main application configuration for poller worker."""
    
    # App settings
    name: str = Field("iis-poller-worker", env="APP_NAME")
    version: str = Field("1.0.0", env="APP_VERSION")
    worker_timeout: int = Field(300, env="WORKER_TIMEOUT")
    health_api_port: int = Field(3009, env="HEALTH_API_PORT")  # Port for health check API
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_client_id: str = Field("poller-worker", env="KAFKA_CLIENT_ID")
    kafka_group_id: str = Field("poller-worker-group", env="KAFKA_GROUP_ID")
    
    # Kafka Topics
    polling_queue_topic: str = Field("polling-queue", env="POLLING_QUEUE_TOPIC")
    file_event_queue_topic: str = Field("file-event-queue", env="FILE_EVENT_QUEUE_TOPIC")
    dlq_topic: str = Field("polling-queue-dlq", env="DLQ_TOPIC")  # Dead Letter Queue
    
    # Poller Configuration
    poll_interval: int = Field(5, env="POLL_INTERVAL")  # seconds
    max_concurrent_jobs: int = Field(10, env="MAX_CONCURRENT_JOBS")
    batch_size: int = Field(100, env="BATCH_SIZE")
    max_file_batch_size: int = Field(10, env="MAX_FILE_BATCH_SIZE")  # Maximum number of files per batch (processes up to this many)
    
    # Rate Limiting Configuration
    max_requests_per_minute: int = Field(60, env="MAX_REQUESTS_PER_MINUTE")  # Per org
    max_concurrent_per_org: int = Field(5, env="MAX_CONCURRENT_PER_ORG")  # Per org
    
    # Circuit Breaker Configuration
    circuit_breaker_failure_threshold: int = Field(5, env="CIRCUIT_BREAKER_FAILURE_THRESHOLD")
    circuit_breaker_recovery_timeout: int = Field(60, env="CIRCUIT_BREAKER_RECOVERY_TIMEOUT")  # seconds
    circuit_breaker_success_threshold: int = Field(2, env="CIRCUIT_BREAKER_SUCCESS_THRESHOLD")
    
    # Retry Configuration
    max_retries: int = Field(3, env="MAX_RETRIES")  # Number of retry attempts
    retry_delay_seconds: int = Field(1, env="RETRY_DELAY_SECONDS")  # Initial delay in seconds
    retry_backoff_multiplier: float = Field(2.0, env="RETRY_BACKOFF_MULTIPLIER")  # Exponential backoff multiplier
    max_retry_delay_seconds: int = Field(60, env="MAX_RETRY_DELAY_SECONDS")  # Maximum delay cap
    
    # Source Connection Configuration
    default_source_type: str = Field("dummy", env="DEFAULT_SOURCE_TYPE")  # dummy, s3, ftp, etc.
    connection_timeout: int = Field(30, env="CONNECTION_TIMEOUT")
    read_timeout: int = Field(60, env="READ_TIMEOUT")
    
    # File Processing Configuration
    max_file_size: int = Field(104857600, env="MAX_FILE_SIZE")  # 100MB
    
    @property
    def supported_file_extensions(self) -> List[str]:
        """Get supported file extensions as a list from environment variable."""
        ext_str = os.getenv('SUPPORTED_FILE_EXTENSIONS', ".csv,.xlsx,.xls,.txt")
        if isinstance(ext_str, str) and ext_str.strip():
            extensions = [ext.strip() for ext in ext_str.split(',') if ext.strip()]
            return extensions if extensions else [".csv", ".xlsx", ".xls", ".txt"]
        return [".csv", ".xlsx", ".xls", ".txt"]
    
    # Logging Configuration
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("json", env="LOG_FORMAT")
    
    # Metrics Configuration
    metrics_port: int = Field(9093, env="METRICS_PORT")
    
    # Dummy Data Configuration
    dummy_data_path: str = Field("./dummy_data", env="DUMMY_DATA_PATH")
    
    # MinIO/S3 Configuration
    minio_endpoint: str = Field("localhost:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(False, env="MINIO_SECURE")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


# Global configuration instance
settings = PollerWorkerConfig()

