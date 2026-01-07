"""
Kafka producer service for file event queue.
Handles publishing file events to Kafka topics.
"""

import asyncio
import json
import uuid
from typing import Dict, Any, Optional
from datetime import datetime
import structlog
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from src.config.kafka_settings import kafka_settings
from src.models.schemas import FileEvent
from src.utils.monitoring import metrics_collector

logger = structlog.get_logger(__name__)


class FileEventQueueProducer:
    """
    Kafka producer service for file event queue operations.
    Handles publishing file events to Kafka topics.
    """
    
    def __init__(self):
        self.producer: Optional[Producer] = None
        self.admin_client: Optional[AdminClient] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the Kafka producer."""
        try:
            logger.info("Initializing file event queue producer", 
                       bootstrap_servers=kafka_settings.bootstrap_servers)
            
            # Initialize producer
            self.producer = Producer(kafka_settings.producer_config)
            
            # Initialize admin client for topic management
            self.admin_client = AdminClient({
                "bootstrap.servers": kafka_settings.bootstrap_servers
            })
            
            # Ensure topics exist
            await self._ensure_topics_exist()
            
            self._initialized = True
            logger.info("File event queue producer initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize file event queue producer", error=str(e))
            raise
    
    async def _ensure_topics_exist(self):
        """Ensure all required topics exist."""
        try:
            # Get existing topics
            metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            
            # Create missing topics
            topics_to_create = []
            for topic_name, config in kafka_settings.topic_configs.items():
                if topic_name not in existing_topics:
                    topic = NewTopic(
                        topic=topic_name,
                        num_partitions=config["num_partitions"],
                        replication_factor=config["replication_factor"],
                        config=config["config"]
                    )
                    topics_to_create.append(topic)
                    logger.info("Topic will be created", topic=topic_name)
            
            if topics_to_create:
                # Create topics
                fs = self.admin_client.create_topics(topics_to_create)
                
                # Wait for topic creation to complete
                for topic, f in fs.items():
                    try:
                        f.result()
                        logger.info("Topic created successfully", topic=topic)
                    except Exception as e:
                        if e.args[0].code() == KafkaException.TOPIC_ALREADY_EXISTS:
                            logger.info("Topic already exists", topic=topic)
                        else:
                            logger.error("Failed to create topic", topic=topic, error=str(e))
                            raise
            
        except Exception as e:
            logger.error("Failed to ensure topics exist", error=str(e))
            raise
    
    async def publish_file_event(self, file_event: FileEvent,
                                metadata: Optional[Dict[str, Any]] = None):
        """
        Publish a file event to Kafka.
        
        Args:
            file_event: FileEvent object
            metadata: Additional metadata
        """
        if not self._initialized:
            raise RuntimeError("File event queue producer not initialized")
        
        try:
            # Create message payload
            message = {
                "event_id": file_event.event_id,
                "job_id": file_event.job_id,
                "source_type": file_event.source_type.value,
                "file_path": file_event.file_path,
                "file_name": file_event.file_name,
                "file_size": file_event.file_size,
                "file_type": file_event.file_type,
                "file_url": file_event.file_url,
                "checksum": file_event.checksum,
                "metadata": {**(file_event.metadata or {}), **(metadata or {})},
                "discovered_at": file_event.discovered_at.isoformat(),
                "processed_at": file_event.processed_at.isoformat() if file_event.processed_at else None
            }
            
            # Note: For large files, we don't include file_content in the message
            # Instead, we use file_url or file_path for the parser to fetch
            
            # Serialize message
            message_json = json.dumps(message, default=str)
            
            # Determine partition key for consistent partitioning
            # Use org_id for better distribution across partitions
            partition_key = f"{file_event.job_id}:{file_event.source_type.value}"
            
            topic_name = kafka_settings.file_event_queue_topic
            logger.info("Publishing file event to Kafka", 
                       event_id=file_event.event_id,
                       job_id=file_event.job_id,
                       file_name=file_event.file_name,
                       topic=topic_name,
                       message_size=len(message_json))
            
            # Publish message
            await self._publish_message(
                topic=topic_name,
                key=partition_key,
                value=message_json,
                headers={"event_id": file_event.event_id, "job_id": file_event.job_id}
            )
            
            logger.info("File event published to Kafka successfully", 
                       event_id=file_event.event_id,
                       job_id=file_event.job_id,
                       file_name=file_event.file_name,
                       topic=topic_name)
            
            # Record metrics
            metrics_collector.record_kafka_message_published(
                kafka_settings.file_event_queue_topic, success=True
            )
            metrics_collector.record_file_published("success")
            
        except Exception as e:
            logger.error("Failed to publish file event", 
                        event_id=file_event.event_id,
                        job_id=file_event.job_id,
                        error=str(e))
            metrics_collector.record_kafka_message_published(
                kafka_settings.file_event_queue_topic, success=False
            )
            metrics_collector.record_file_published("error")
            raise
    
    async def publish_file_events_batch(self, file_events: list,
                                       metadata: Optional[Dict[str, Any]] = None):
        """
        Publish a batch of file events to Kafka as a single message.
        
        Args:
            file_events: List of FileEvent objects
            metadata: Additional metadata to include in all events
        """
        if not self._initialized:
            raise RuntimeError("File event queue producer not initialized")
        
        if not file_events:
            logger.warning("Empty file events batch, skipping")
            return
        
        try:
            # Extract connection_list from metadata if available (for top-level inclusion)
            connection_list = None
            if metadata and "connection_list" in metadata:
                connection_list = metadata.get("connection_list")
            
            # Create batch message payload
            batch_message = {
                "batch_id": str(uuid.uuid4()),
                "batch_size": len(file_events),
                "job_id": file_events[0].job_id if file_events else None,
                "connection_list": connection_list,  # Include connection_list at top level for easy extraction
                "events": []
            }
            
            # Add each file event to the batch with validation
            for idx, file_event in enumerate(file_events):
                # Validate required fields - skip if both file_path and file_name are missing
                if not file_event.file_path and not file_event.file_name:
                    logger.error("Skipping invalid file event - missing file_path and file_name",
                               event_id=file_event.event_id,
                               job_id=file_event.job_id,
                               index=idx)
                    continue
                
                # Ensure we have at least file_name or file_path
                file_name = file_event.file_name or file_event.file_path or "unknown"
                file_path = file_event.file_path or file_event.file_name or "unknown"
                
                # Extract bucket_name from metadata or file_url
                bucket_name = None
                if file_event.metadata and "bucket" in file_event.metadata:
                    bucket_name = file_event.metadata["bucket"]
                elif file_event.file_url:
                    # Extract from s3://bucket-name/path
                    if file_event.file_url.startswith("s3://"):
                        parts = file_event.file_url[5:].split("/", 1)
                        if parts:
                            bucket_name = parts[0]
                
                # Ensure metadata has bucket_name for parser
                event_metadata = {**(file_event.metadata or {}), **(metadata or {})}
                if bucket_name and "bucket" not in event_metadata:
                    event_metadata["bucket"] = bucket_name
                if bucket_name and "bucket_name" not in event_metadata:
                    event_metadata["bucket_name"] = bucket_name
                
                # Ensure file_url is set for MinIO/S3 sources
                file_url = file_event.file_url
                if not file_url and bucket_name and file_path:
                    file_url = f"s3://{bucket_name}/{file_path}"
                
                event_data = {
                    "event_id": file_event.event_id or f"event_{idx}_{datetime.utcnow().timestamp()}",
                    "job_id": file_event.job_id or batch_message["job_id"],
                    "source_type": file_event.source_type.value if file_event.source_type else "minio",
                    "file_path": file_path,
                    "file_name": file_name,
                    "file_size": file_event.file_size or 0,
                    "file_type": file_event.file_type or "",
                    "file_url": file_url,
                    "checksum": file_event.checksum,
                    "metadata": event_metadata,
                    "discovered_at": file_event.discovered_at.isoformat() if file_event.discovered_at else datetime.utcnow().isoformat(),
                    "processed_at": file_event.processed_at.isoformat() if file_event.processed_at else datetime.utcnow().isoformat()
                }
                
                # Final validation - ensure critical fields are not None
                if (not event_data["file_path"] or event_data["file_path"] == "unknown" or
                    not event_data["file_name"] or event_data["file_name"] == "unknown" or
                    not event_data["file_url"]):
                    logger.error("Skipping invalid event data - missing critical fields",
                               event_data={k: v for k, v in event_data.items() if k != "metadata"},
                               metadata_keys=list(event_data.get("metadata", {}).keys()),
                               index=idx)
                    continue
                
                batch_message["events"].append(event_data)
            
            # Update batch_size to reflect actual number of events added
            batch_message["batch_size"] = len(batch_message["events"])
            
            if batch_message["batch_size"] == 0:
                logger.error("No valid file events in batch, skipping publish",
                           original_count=len(file_events))
                return
            
            # Final validation: Ensure no event has None values for critical fields
            valid_events = []
            for event in batch_message["events"]:
                if (event.get("file_path") and event.get("file_name") and 
                    event.get("file_url") and event.get("metadata")):
                    valid_events.append(event)
                else:
                    logger.error("Removing event with None values from batch",
                               event_id=event.get("event_id"),
                               has_file_path=bool(event.get("file_path")),
                               has_file_name=bool(event.get("file_name")),
                               has_file_url=bool(event.get("file_url")),
                               has_metadata=bool(event.get("metadata")))
            
            if len(valid_events) == 0:
                logger.error("No valid events after final validation, skipping publish",
                           original_count=len(file_events))
                return
            
            batch_message["events"] = valid_events
            batch_message["batch_size"] = len(valid_events)
            
            # Serialize batch message
            message_json = json.dumps(batch_message, default=str)
            
            # Determine partition key for consistent partitioning
            # Use job_id for better distribution across partitions
            partition_key = f"{file_events[0].job_id}:batch"
            
            topic_name = kafka_settings.file_event_queue_topic
            logger.info("Publishing file events batch to Kafka", 
                       batch_id=batch_message["batch_id"],
                       batch_size=len(file_events),
                       job_id=file_events[0].job_id,
                       topic=topic_name,
                       message_size=len(message_json))
            
            # Publish batch message
            await self._publish_message(
                topic=topic_name,
                key=partition_key,
                value=message_json,
                headers={
                    "message_type": "batch",
                    "batch_id": batch_message["batch_id"],
                    "job_id": file_events[0].job_id,
                    "batch_size": str(batch_message["batch_size"]),
                    "original_batch_size": str(len(file_events))
                }
            )
            
            logger.info("File events batch published to Kafka successfully", 
                       batch_id=batch_message["batch_id"],
                       batch_size=batch_message["batch_size"],
                       original_count=len(file_events),
                       job_id=file_events[0].job_id,
                       topic=topic_name,
                       event_ids=[e["event_id"] for e in batch_message["events"][:5]])  # Log first 5 event IDs
            
            # Record metrics
            metrics_collector.record_kafka_message_published(
                kafka_settings.file_event_queue_topic, success=True
            )
            # Record file published metric for each file in batch
            for _ in range(len(file_events)):
                metrics_collector.record_file_published("success")
            
        except Exception as e:
            logger.error("Failed to publish file events batch", 
                        batch_size=len(file_events),
                        job_id=file_events[0].job_id if file_events else None,
                        error=str(e))
            metrics_collector.record_kafka_message_published(
                kafka_settings.file_event_queue_topic, success=False
            )
            # Record file published metric for each file in batch
            for _ in range(len(file_events)):
                metrics_collector.record_file_published("error")
            raise
    
    async def _publish_message(self, topic: str, key: str, value: str, 
                             headers: Optional[Dict[str, str]] = None):
        """Publish a message to Kafka topic."""
        try:
            # Create delivery callback for logging
            def delivery_callback(err, msg):
                if err is not None:
                    logger.error("Message delivery failed", 
                               topic=topic, key=key, error=str(err))
                else:
                    logger.debug("Message delivered successfully", 
                               topic=topic, key=key, offset=msg.offset())
            
            # Publish message
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                callback=delivery_callback
            )
            
            # Flush to ensure message is sent immediately
            # This blocks until the message is delivered or timeout occurs
            # flush() will raise an exception if delivery fails
            self.producer.flush(timeout=10)
            
        except Exception as e:
            logger.error("Failed to publish message", topic=topic, key=key, error=str(e))
            raise
    
    async def close(self):
        """Close the Kafka producer."""
        try:
            if self.producer:
                # Flush any remaining messages
                self.producer.flush(timeout=10)
                
                # Close producer
                self.producer = None
            
            if self.admin_client:
                self.admin_client = None
            
            self._initialized = False
            logger.info("File event queue producer closed")
            
        except Exception as e:
            logger.error("Error closing file event queue producer", error=str(e))


# Global Kafka producer instance
file_event_queue_producer = FileEventQueueProducer()

