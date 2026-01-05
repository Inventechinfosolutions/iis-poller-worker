"""
Kafka consumer service for polling queue.
Handles consuming polling jobs from Kafka.
"""

import asyncio
import json
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
import structlog
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient

from src.config.kafka_settings import kafka_settings
from src.models.schemas import PollingJob, JobStatus
from src.utils.monitoring import metrics_collector

logger = structlog.get_logger(__name__)


class PollingQueueConsumer:
    """
    Kafka consumer service for polling queue operations.
    Handles consuming polling jobs and processing them.
    """
    
    def __init__(self):
        self.consumer: Optional[Consumer] = None
        self.admin_client: Optional[AdminClient] = None
        self._initialized = False
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._consumer_tasks: list = []
        self._message_handler: Optional[callable] = None
    
    async def initialize(self):
        """Initialize the Kafka consumer."""
        try:
            logger.info("Initializing polling queue consumer", 
                       bootstrap_servers=kafka_settings.bootstrap_servers,
                       group_id=kafka_settings.group_id)
            
            # Initialize consumer with production-ready settings
            # Note: max.poll.records is not supported in confluent-kafka Python library
            self.consumer = Consumer(kafka_settings.consumer_config)
            
            # Initialize admin client
            self.admin_client = AdminClient({
                "bootstrap.servers": kafka_settings.bootstrap_servers
            })
            
            # Subscribe to polling queue topic
            topics = [kafka_settings.polling_queue_topic]
            self.consumer.subscribe(topics)
            
            self._initialized = True
            logger.info("Polling queue consumer initialized successfully", 
                       topics=topics)
            
        except Exception as e:
            logger.error("Failed to initialize polling queue consumer", error=str(e))
            raise
    
    def set_message_handler(self, handler: callable):
        """Set the message handler function."""
        self._message_handler = handler
    
    async def start(self):
        """Start consuming messages from Kafka."""
        if not self._initialized:
            raise RuntimeError("Polling queue consumer not initialized")
        
        if self._running:
            logger.warning("Polling queue consumer is already running")
            return
        
        try:
            logger.info("Starting polling queue consumer")
            self._running = True
            self._shutdown_event.clear()
            
            # Start consumer task
            consumer_task = asyncio.create_task(self._consume_messages())
            self._consumer_tasks.append(consumer_task)
            
            # Start periodic health check task
            health_check_task = asyncio.create_task(self._periodic_health_check())
            self._consumer_tasks.append(health_check_task)
            
            # Start consumer lag monitoring task
            lag_monitor_task = asyncio.create_task(self._monitor_consumer_lag())
            self._consumer_tasks.append(lag_monitor_task)
            
            logger.info("Polling queue consumer started successfully")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error("Failed to start polling queue consumer", error=str(e))
            raise
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop consuming messages from Kafka."""
        if not self._running:
            return
        
        try:
            logger.info("Stopping polling queue consumer")
            self._running = False
            self._shutdown_event.set()
            
            # Cancel all tasks
            for task in self._consumer_tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            if self._consumer_tasks:
                await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
            
            self._consumer_tasks.clear()
            
            # Close consumer
            if self.consumer:
                self.consumer.close()
                self.consumer = None
            
            logger.info("Polling queue consumer stopped")
            
        except Exception as e:
            logger.error("Error stopping polling queue consumer", error=str(e))
    
    async def _consume_messages(self):
        """Main message consumption loop."""
        logger.info("Starting message consumption loop", 
                   group_id=kafka_settings.group_id)
        
        poll_count = 0
        message_count = 0
        
        while self._running and not self._shutdown_event.is_set():
            try:
                # Poll for messages with timeout
                msg = self.consumer.poll(timeout=1.0)
                poll_count += 1
                
                # Log polling status every 60 polls
                if poll_count % 60 == 0:
                    logger.info("Consumer actively polling", 
                               poll_count=poll_count,
                               messages_processed=message_count,
                               group_id=kafka_settings.group_id)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition", 
                                   partition=msg.partition(), 
                                   topic=msg.topic())
                        continue
                    else:
                        logger.error("Kafka consumer error", error=str(msg.error()))
                        metrics_collector.record_kafka_message_consumed(
                            kafka_settings.polling_queue_topic, success=False
                        )
                        continue
                
                # Process message
                message_count += 1
                logger.info("Message received from Kafka", 
                           message_number=message_count,
                           topic=msg.topic(),
                           partition=msg.partition(),
                           offset=msg.offset())
                
                # Check backpressure before processing
                from src.utils.backpressure import backpressure_manager
                await backpressure_manager.wait_if_paused()
                
                await self._process_message(msg)
                
                # Commit offset after successful processing
                # This ensures we don't reprocess messages if worker crashes
                try:
                    self.consumer.commit(message=msg, asynchronous=False)
                    logger.debug("Offset committed successfully", 
                               topic=msg.topic(), 
                               partition=msg.partition(),
                               offset=msg.offset())
                except Exception as commit_error:
                    logger.error("Failed to commit offset",
                               topic=msg.topic(),
                               partition=msg.partition(),
                               offset=msg.offset(),
                               error=str(commit_error))
                    # Don't raise - message was processed, just commit failed
                    # Will be retried on next commit
                
            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                break
            except Exception as e:
                logger.error("Error in message consumption loop", 
                           error=str(e),
                           error_type=type(e).__name__)
                metrics_collector.record_kafka_message_consumed(
                    kafka_settings.polling_queue_topic, success=False
                )
                await asyncio.sleep(1)  # Brief pause before retry
        
        logger.info("Message consumption loop ended", 
                   total_polls=poll_count,
                   total_messages=message_count)
    
    async def _process_message(self, msg):
        """Process a single Kafka message."""
        try:
            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None
            headers = {k: v.decode('utf-8') for k, v in msg.headers()} if msg.headers() else {}
            
            logger.info("Processing Kafka message", 
                       topic=topic, 
                       key=key, 
                       headers=headers,
                       value_length=len(value) if value else 0)
            
            # Check if value is empty or None
            if not value or value.strip() == "":
                logger.error("Empty or null message value", 
                           topic=topic, 
                           key=key)
                return
            
            # Parse message value
            try:
                message_data = json.loads(value)
                logger.info("Successfully parsed JSON message", 
                           topic=topic,
                           message_keys=list(message_data.keys()) if isinstance(message_data, dict) else "not_dict")
            except json.JSONDecodeError as json_err:
                logger.error("Failed to parse JSON message", 
                           topic=topic,
                           key=key,
                           value_preview=value[:200] if value else "None",
                           error=str(json_err))
                return
            
            # Route message based on topic
            if topic == kafka_settings.polling_queue_topic:
                await self._process_polling_job(message_data, headers)
            else:
                logger.warning("Unknown topic", topic=topic)
            
            # Record metrics
            metrics_collector.record_kafka_message_consumed(
                kafka_settings.polling_queue_topic, success=True
            )
            
        except Exception as e:
            logger.error("Failed to process Kafka message", 
                        topic=msg.topic(), 
                        key=msg.key().decode('utf-8') if msg.key() else None,
                        error=str(e),
                        traceback=traceback.format_exc())
            metrics_collector.record_kafka_message_consumed(
                kafka_settings.polling_queue_topic, success=False
            )
    
    async def _process_polling_job(self, message_data: Dict[str, Any], headers: Dict[str, str]):
        """Process a polling job message."""
        try:
            # Parse job from message
            logger.debug("Parsing job from message", message_data=message_data)
            job = PollingJob(**message_data)
            
            # Get source types from connection_list (new format)
            source_types = [conn.source_type.value for conn in job.connection_list] if job.connection_list else []
            source_type_str = ", ".join(source_types) if source_types else "unknown"
            
            logger.info("Processing polling job", 
                       job_id=job.job_id,
                       org_id=job.org_id,
                       connection_count=len(job.connection_list) if job.connection_list else 0,
                       source_types=source_type_str,
                       priority=job.priority)
            
            # Call message handler if set
            if self._message_handler:
                logger.info("Calling message handler", job_id=job.job_id)
                try:
                    await self._message_handler(job)
                    logger.info("Message handler completed successfully", job_id=job.job_id)
                except Exception as handler_error:
                    logger.error("Error in message handler", 
                               job_id=job.job_id,
                               error=str(handler_error),
                               traceback=traceback.format_exc())
                    raise
            else:
                logger.warning("No message handler set for polling job", job_id=job.job_id)
            
        except Exception as e:
            logger.error("Failed to process polling job", 
                        job_id=message_data.get("job_id"),
                        error=str(e),
                        traceback=traceback.format_exc())
    
    async def _periodic_health_check(self):
        """Periodic health check for Kafka consumer."""
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.consumer:
                    logger.debug("Polling queue consumer health check", 
                               group_id=kafka_settings.group_id)
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in health check", error=str(e))
                await asyncio.sleep(60)  # Wait 1 minute before retry
    
    async def _monitor_consumer_lag(self):
        """
        Monitor Kafka consumer lag.
        This is critical for detecting when processing falls behind.
        Simple and easy to understand.
        """
        logger.info("Starting consumer lag monitoring")
        
        while self._running and not self._shutdown_event.is_set():
            try:
                if not self.consumer:
                    await asyncio.sleep(30)
                    continue
                
                # Get assigned partitions
                assignment = self.consumer.assignment()
                
                if not assignment:
                    logger.debug("No partitions assigned yet")
                    await asyncio.sleep(30)
                    continue
                
                # Check lag for each partition
                for partition in assignment:
                    try:
                        # Get high watermark (latest offset available)
                        low, high = self.consumer.get_watermark_offsets(partition, timeout=5)
                        
                        # Get committed offset (where we are)
                        committed = self.consumer.committed([partition], timeout=5)
                        committed_offset = committed[partition].offset if committed[partition] else low
                        
                        # Calculate lag: how many messages behind
                        lag = high - committed_offset if committed_offset >= 0 else 0
                        
                        # Record metric for Prometheus
                        metrics_collector.record_consumer_lag(
                            topic=partition.topic,
                            partition=partition.partition,
                            lag=lag
                        )
                        
                        # Log warning if lag is high (configurable threshold)
                        lag_threshold = 10000  # 10,000 messages - can be made configurable
                        if lag > lag_threshold:
                            logger.warning("High consumer lag detected",
                                        topic=partition.topic,
                                        partition=partition.partition,
                                        lag=lag,
                                        committed_offset=committed_offset,
                                        high_watermark=high,
                                        threshold=lag_threshold)
                        else:
                            logger.debug("Consumer lag normal",
                                       topic=partition.topic,
                                       partition=partition.partition,
                                       lag=lag)
                    
                    except Exception as e:
                        logger.error("Error checking lag for partition",
                                   topic=partition.topic,
                                   partition=partition.partition,
                                   error=str(e))
                
                # Check every 30 seconds
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                logger.info("Consumer lag monitoring cancelled")
                break
            except Exception as e:
                logger.error("Error in consumer lag monitoring", error=str(e))
                await asyncio.sleep(60)  # Wait longer on error
    
    async def close(self):
        """Close the Kafka consumer."""
        try:
            await self.stop()
            
            if self.admin_client:
                self.admin_client = None
            
            self._initialized = False
            logger.info("Polling queue consumer closed")
            
        except Exception as e:
            logger.error("Error closing polling queue consumer", error=str(e))


# Global Kafka consumer instance
polling_queue_consumer = PollingQueueConsumer()

