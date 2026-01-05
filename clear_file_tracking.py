"""
Script to clear file tracking data from Redis.
Use this when you want to reprocess files that were already processed.
"""

import asyncio
from src.clients.redis_client import redis_client
from src.config.redis_settings import redis_settings
import structlog

logger = structlog.get_logger(__name__)

async def clear_file_tracking(org_id: str = None):
    """Clear file tracking data from Redis."""
    await redis_client.initialize()
    
    try:
        # Clear file tracking (poller:file: prefix)
        if org_id:
            file_pattern = f"poller:file:{org_id}:*"
        else:
            file_pattern = "poller:file:*"
        logger.info("Clearing file tracking data", pattern=file_pattern)
        
        # Clear connection stats (poller:conn: prefix)
        if org_id:
            stats_pattern = f"poller:conn:{org_id}:*"
        else:
            stats_pattern = "poller:conn:*"
        logger.info("Clearing connection statistics", pattern=stats_pattern)
        
        deleted_count = 0
        
        # Clear file tracking
        cursor = 0
        while True:
            cursor, keys = await redis_client.client.scan(cursor, match=file_pattern, count=100)
            if keys:
                for key in keys:
                    key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                    await redis_client.delete(key_str)
                    deleted_count += 1
            if cursor == 0:
                break
        
        # Clear connection stats
        cursor = 0
        while True:
            cursor, keys = await redis_client.client.scan(cursor, match=stats_pattern, count=100)
            if keys:
                for key in keys:
                    key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                    await redis_client.delete(key_str)
                    deleted_count += 1
            if cursor == 0:
                break
        
        logger.info("File tracking cleared", deleted_count=deleted_count, org_id=org_id or "all")
        print(f"✅ Cleared {deleted_count} entries (file tracking + connection stats)")
        
    except Exception as e:
        logger.error("Error clearing file tracking", error=str(e))
        print(f"❌ Error: {e}")
    finally:
        await redis_client.close()

if __name__ == "__main__":
    import sys
    org_id = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(clear_file_tracking(org_id))

