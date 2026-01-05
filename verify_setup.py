"""
Quick verification script to check if everything is ready for end-to-end test.
"""

import sys
import os

def check_dummy_data():
    """Check if dummy data exists."""
    print("📁 Checking dummy data...")
    dummy_path = "./dummy_data"
    if os.path.exists(dummy_path):
        csv_files = [f for f in os.listdir(dummy_path) if f.endswith('.csv')]
        if csv_files:
            print(f"   ✅ Found {len(csv_files)} CSV file(s)")
            return True
        else:
            print(f"   ❌ No CSV files in {dummy_path}")
            return False
    else:
        print(f"   ❌ Directory not found: {dummy_path}")
        return False


def check_kafka():
    """Check if Kafka is accessible."""
    print("🔌 Checking Kafka...")
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        metadata = admin.list_topics(timeout=5)
        print(f"   ✅ Kafka connected")
        return True
    except Exception as e:
        print(f"   ❌ Kafka not accessible: {e}")
        print(f"   💡 Start Kafka: docker-compose up -d kafka")
        return False


def check_imports():
    """Check if all modules can be imported."""
    print("📦 Checking Python modules...")
    try:
        from src.services.poller_worker import poller_worker
        from src.services.kafka_consumer import polling_queue_consumer
        from src.services.kafka_producer import file_event_queue_producer
        from src.services.source_connector import source_connector
        from src.services.file_tracker import file_tracker
        from src.services.redis_persistence import redis_persistence
        print("   ✅ All modules importable")
        return True
    except Exception as e:
        print(f"   ❌ Import error: {e}")
        return False


def main():
    """Run all checks."""
    print("="*70)
    print("🔍 SETUP VERIFICATION")
    print("="*70)
    print()
    
    checks = [
        ("Dummy Data", check_dummy_data),
        ("Kafka Connection", check_kafka),
        ("Python Modules", check_imports),
    ]
    
    results = []
    for name, check_func in checks:
        result = check_func()
        results.append((name, result))
        print()
    
    print("="*70)
    print("📊 VERIFICATION SUMMARY")
    print("="*70)
    
    all_passed = True
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")
        if not result:
            all_passed = False
    
    print()
    if all_passed:
        print("✅ All checks passed! Ready for end-to-end test.")
        print("\n📝 Next steps:")
        print("   1. Start poller worker: python -m src.main")
        print("   2. Run test: python test_e2e_automated.py")
    else:
        print("❌ Some checks failed. Fix issues before testing.")
        print("\n💡 Common fixes:")
        print("   - Start Kafka: docker-compose up -d kafka")
        print("   - Check dummy_data folder exists with CSV files")
    
    print("="*70)
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

