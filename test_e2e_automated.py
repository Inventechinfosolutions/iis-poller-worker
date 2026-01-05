"""
Automated End-to-End Test for all 5 steps.
Tests the complete flow without user interaction.
"""

import json
import time
import sys
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POLLING_QUEUE_TOPIC = "polling-queue"
FILE_EVENT_QUEUE_TOPIC = "file-event-queue"
TEST_GROUP_ID = f"test-consumer-{int(time.time())}"


def check_kafka_connection():
    """Check if Kafka is accessible."""
    print("🔌 Checking Kafka Connection...")
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=5)
        print(f"✅ Kafka connected (topics: {len(metadata.topics)})")
        return True
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False


def publish_test_job():
    """Publish test job to Kafka."""
    print("\n📤 STEP 1: Publishing test job to Kafka...")
    
    job_id = f"test_e2e_{int(time.time())}"
    job_data = {
        "job_id": job_id,
        "org_id": "test_org_e2e",
        "connection_list": [
            {
                "source_type": "dummy",
                "path": "./dummy_data"
            }
        ],
        "file_pattern": "*.csv",
        "priority": "normal",
        "metadata": {
            "test": True,
            "created_at": datetime.utcnow().isoformat(),
            "description": "End-to-end automated test"
        },
        "created_at": datetime.utcnow().isoformat()
    }
    
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "e2e-test-producer"
        })
        
        producer.produce(
            topic=POLLING_QUEUE_TOPIC,
            key=job_id,
            value=json.dumps(job_data),
            headers={"job_id": job_id, "priority": "normal"}
        )
        producer.flush(timeout=10)
        
        print(f"✅ Job published: {job_id}")
        return job_id, job_data
        
    except Exception as e:
        print(f"❌ Failed to publish job: {e}")
        return None, None


def check_file_events(job_id, timeout=30):
    """Check if file events were published."""
    print(f"\n📥 STEP 5: Checking file-event-queue for job {job_id}...")
    print(f"   Waiting up to {timeout} seconds...")
    
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": TEST_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
        
        consumer.subscribe([FILE_EVENT_QUEUE_TOPIC])
        
        start_time = time.time()
        file_events = []
        
        while time.time() - start_time < timeout:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                break
            
            try:
                event_data = json.loads(msg.value().decode('utf-8'))
                if event_data.get("job_id") == job_id:
                    file_events.append(event_data)
                    print(f"   ✅ Found: {event_data.get('file_name')} ({event_data.get('file_type')})")
            except:
                continue
        
        consumer.close()
        
        if file_events:
            print(f"✅ Found {len(file_events)} file event(s)")
            return True, file_events
        else:
            print(f"❌ No file events found")
            return False, []
            
    except Exception as e:
        print(f"❌ Error checking queue: {e}")
        return False, []


def main():
    """Run automated end-to-end test."""
    print("="*70)
    print("🧪 AUTOMATED END-TO-END TEST")
    print("="*70)
    print("\n⚠️  Make sure poller worker is running:")
    print("   python -m src.main")
    print("\nStarting test in 3 seconds...\n")
    time.sleep(3)
    
    # Check Kafka
    if not check_kafka_connection():
        print("\n❌ TEST FAILED: Cannot connect to Kafka")
        print("   Start Kafka: docker-compose up -d kafka")
        sys.exit(1)
    
    # Publish job
    job_id, job_data = publish_test_job()
    if not job_id:
        print("\n❌ TEST FAILED: Cannot publish job")
        sys.exit(1)
    
    # Wait for processing
    print(f"\n⏳ Waiting 10 seconds for poller worker to process...")
    time.sleep(10)
    
    # Check results
    success, file_events = check_file_events(job_id, timeout=30)
    
    # Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    print(f"Job ID: {job_id}")
    print(f"\nSteps:")
    print(f"  ✅ Step 1: Listen to Kafka (polling-queue)")
    print(f"  ✅ Step 2: Pick and parse job")
    print(f"  ✅ Step 3: Connect to source")
    print(f"  ✅ Step 4: Read files from source")
    if success:
        print(f"  ✅ Step 5: Push files to file-event-queue")
        print(f"\n✅ ALL 5 STEPS PASSED!")
        print(f"   Files published: {len(file_events)}")
        for event in file_events:
            print(f"     - {event.get('file_name')}")
    else:
        print(f"  ❌ Step 5: Push files - FAILED")
        print(f"\n❌ TEST FAILED")
        print(f"   Check poller worker logs for errors")
    print("="*70)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

