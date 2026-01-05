"""
Kafka configuration settings for poller worker service.
"""

from pydantic_settings import BaseSettings
from typing import Dict, Any
import os


class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""
    
    # Kafka connection settings
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "poller-worker")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "poller-worker-group")
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    
    # Topic names
    polling_queue_topic: str = "polling-queue"
    file_event_queue_topic: str = "file-event-queue"
    
    # Consumer/Producer settings
    max_retries: int = 3
    retry_delay: int = 5  # seconds
    timeout: int = 30000  # milliseconds
    delivery_timeout: int = 120000  # milliseconds
    
    @property
    def producer_config(self) -> Dict[str, Any]:
        """Producer configuration with dynamic values and production features."""
        # Note: Only valid confluent-kafka Python properties are used
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": "all",  # Wait for all replicas
            "retries": 5,  # More retries for reliability
            "batch.size": 32768,  # Larger batches for better throughput
            "linger.ms": 10,  # Wait for batching
            "compression.type": "snappy",  # Faster compression
            "max.in.flight.requests.per.connection": 1,  # Ensure ordering
            "enable.idempotence": True,  # Prevent duplicates
            "request.timeout.ms": 30000,  # Request timeout
            "delivery.timeout.ms": 120000,  # Delivery timeout
        }
    
    @property
    def consumer_config(self) -> Dict[str, Any]:
        """Consumer configuration with dynamic values and production features."""
        # Note: Only valid confluent-kafka Python properties are used
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": self.client_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit for reliability
            "session.timeout.ms": 30000,  # Session timeout
            "heartbeat.interval.ms": 10000,  # Heartbeat frequency
            "max.poll.interval.ms": 300000,  # Max processing time
        }
    
    @property
    def topic_configs(self) -> Dict[str, Dict[str, Any]]:
        """Topic configuration with dynamic values."""
        return {
            self.polling_queue_topic: {
                "num_partitions": 3,
                "replication_factor": self.replication_factor,
                "config": {
                    "retention.ms": 604800000,  # 7 days
                    "compression.type": "gzip"
                }
            },
            self.file_event_queue_topic: {
                "num_partitions": 3,
                "replication_factor": self.replication_factor,
                "config": {
                    "retention.ms": 604800000,  # 7 days
                    "compression.type": "gzip"
                }
            }
        }
    
    class Config:
        env_file = ".env"
        env_prefix = "KAFKA_"


# Global Kafka settings instance
kafka_settings = KafkaSettings()

