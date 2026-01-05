"""
Test script to publish a MinIO polling job to Kafka.
Tests pulling files from MinIO storage.
"""

import json
import uuid
from datetime import datetime
from confluent_kafka import Producer
import sys

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POLLING_QUEUE_TOPIC = "polling-queue"

def create_minio_job(endpoint: str, access_key: str, secret_key: str, 
                     bucket_name: str, path: str = "", file_pattern: str = "*"):
    """Create a MinIO polling job."""
    job_id = f"minio_test_{int(datetime.utcnow().timestamp())}"
    
    job = {
        "job_id": job_id,
        "org_id": "test_org_minio",
        "connection_list": [
            {
                "source_type": "minio",
                "endpoint": endpoint,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "path": path,
                "connection_params": {
                    "secure": endpoint.startswith("https://")
                }
            }
        ],
        "file_pattern": file_pattern,
        "priority": "normal",
        "metadata": {
            "test": True,
            "created_at": datetime.utcnow().isoformat(),
            "description": "Test job for MinIO file pulling"
        },
        "created_at": datetime.utcnow().isoformat()
    }
    
    return job

def publish_job(job):
    """Publish job to Kafka."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })
    
    try:
        # Serialize job to JSON
        value = json.dumps(job).encode('utf-8')
        
        # Publish to Kafka
        producer.produce(
            POLLING_QUEUE_TOPIC,
            key=job["job_id"],
            value=value,
            headers={
                "job_id": job["job_id"],
                "priority": job["priority"]
            }
        )
        
        # Wait for delivery
        producer.flush(timeout=10)
        
        print("=" * 70)
        print("📤 Publishing MinIO Test Job to Kafka")
        print("=" * 70)
        print(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Topic: {POLLING_QUEUE_TOPIC}")
        print()
        print("📋 Job Details:")
        print(json.dumps(job, indent=2))
        print()
        print("-" * 70)
        print("✅ Message delivered successfully!")
        print("=" * 70)
        print()
        print(f"✅ Job Published Successfully!")
        print(f"Job ID: {job['job_id']}")
        print()
        print("💡 Now check your poller worker logs to see:")
        print("  Step 1: Message received from Kafka")
        print("  Step 2: Job parsed")
        print("  Step 3: Connected to MinIO")
        print("  Step 4: Files discovered from MinIO")
        print("  Step 5: Files published to file-event-queue")
        print()
        print("🔍 To check file events:")
        print("  node scripts/check_file_event_queue.js")
        
    except Exception as e:
        print(f"❌ Error publishing job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # MinIO connection details
    # NOTE: Port 9000 is for API, 9001 is for Web UI
    endpoint = "localhost:9000"  # API port (not 9001 which is web UI)
    access_key = "minioadmin"
    secret_key = "minioadmin123"
    bucket_name = "default-bucket"
    path = ""  # Empty for root, or specify path like "folder/subfolder"
    file_pattern = "*.pdf"  # File pattern to match
    
    # Clean endpoint (remove http:// or https://)
    if endpoint.startswith(("http://", "https://")):
        endpoint = endpoint.split("://", 1)[1]
    
    print("=" * 70)
    print("🧪 MINIO TEST JOB CREATOR")
    print("=" * 70)
    print()
    print("MinIO Configuration:")
    print(f"  Endpoint: {endpoint}")
    print(f"  Access Key: {access_key}")
    print(f"  Secret Key: {'*' * len(secret_key)}")
    print(f"  Bucket: {bucket_name}")
    print(f"  Path: {path or '(root)'}")
    print(f"  File Pattern: {file_pattern}")
    print()
    
    # Create and publish job
    job = create_minio_job(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        path=path,
        file_pattern=file_pattern
    )
    
    publish_job(job)

