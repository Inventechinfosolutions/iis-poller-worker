"""
Test script that publishes a real job to Kafka.
Run this AFTER starting the poller worker service.
"""

import json
import sys
from datetime import datetime
import uuid
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POLLING_QUEUE_TOPIC = "polling-queue"

def create_test_job():
    """Create a test polling job."""
    job = {
        "job_id": f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}",
        "source_config": {
            "source_type": "dummy",
            "path": "./dummy_data"
        },
        "file_pattern": "*.csv",
        "priority": "normal",
        "metadata": {
            "test": True,
            "created_at": datetime.utcnow().isoformat(),
            "description": "Test job for 5-step flow verification"
        },
        "created_at": datetime.utcnow().isoformat()
    }
    return job

def publish_job_to_kafka():
    """Publish job to Kafka polling queue."""
    try:
        print("=" * 70)
        print("📤 Publishing Test Job to Kafka")
        print("=" * 70)
        print(f"\nKafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Topic: {POLLING_QUEUE_TOPIC}\n")
        
        # Create producer
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "test-job-publisher"
        })
        
        # Create test job
        job = create_test_job()
        
        # Serialize job
        job_json = json.dumps(job, indent=2)
        
        print("📋 Job Details:")
        print(job_json)
        print("\n" + "-" * 70 + "\n")
        
        # Publish to Kafka
        def delivery_callback(err, msg):
            if err is not None:
                print(f"❌ Failed to deliver message: {err}")
                sys.exit(1)
            else:
                print(f"✅ Message delivered successfully!")
                print(f"   Topic: {msg.topic()}")
                print(f"   Partition: {msg.partition()}")
                print(f"   Offset: {msg.offset()}")
        
        producer.produce(
            topic=POLLING_QUEUE_TOPIC,
            key=job["job_id"],
            value=job_json,
            headers={"job_id": job["job_id"], "priority": job["priority"]},
            callback=delivery_callback
        )
        
        # Flush to ensure message is sent
        producer.flush(timeout=10)
        
        print("\n" + "=" * 70)
        print("✅ Job Published Successfully!")
        print("=" * 70)
        print(f"\nJob ID: {job['job_id']}")
        print(f"\n💡 Now check your poller worker logs to see:")
        print("   Step 1: Message received from Kafka")
        print("   Step 2: Job parsed")
        print("   Step 3: Connected to source")
        print("   Step 4: Files discovered")
        print("   Step 5: Files published to file-event-queue")
        print("\n")
        
        return job
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Make sure Kafka is running")
        print("   2. Check KAFKA_BOOTSTRAP_SERVERS is correct")
        print("   3. Verify the topic exists (or will be auto-created)")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    publish_job_to_kafka()

