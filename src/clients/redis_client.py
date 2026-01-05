"""
Simple Redis client for data persistence.
Production-ready with connection pooling and error handling.
"""

import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import structlog
import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from src.config.redis_settings import redis_settings

logger = structlog.get_logger(__name__)


class RedisClient:
    """
    Simple Redis client for data persistence.
    Handles file tracking and connection statistics.
    """
    
    def __init__(self):
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize Redis connection pool."""
        try:
            logger.info("Initializing Redis client",
                       host=redis_settings.redis_host,
                       port=redis_settings.redis_port,
                       db=redis_settings.redis_db)
            
            # Create connection pool
            self.pool = ConnectionPool(
                host=redis_settings.redis_host,
                port=redis_settings.redis_port,
                password=redis_settings.redis_password,
                db=redis_settings.redis_db,
                max_connections=redis_settings.redis_max_connections,
                socket_keepalive=redis_settings.redis_socket_keepalive,
                socket_connect_timeout=redis_settings.redis_socket_connect_timeout,
                socket_timeout=redis_settings.redis_socket_timeout,
                retry_on_timeout=redis_settings.redis_retry_on_timeout,
                health_check_interval=redis_settings.redis_health_check_interval
            )
            
            # Create Redis client
            self.client = redis.Redis(connection_pool=self.pool, decode_responses=False)
            
            # Test connection
            await self.client.ping()
            
            self._initialized = True
            logger.info("Redis client initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize Redis client", error=str(e))
            raise
    
    async def close(self):
        """Close Redis connection."""
        if self.client:
            await self.client.close()
        if self.pool:
            await self.pool.aclose()
        self._initialized = False
        logger.info("Redis client closed")
    
    async def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Set key-value with optional TTL."""
        if not self._initialized:
            return False
        
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            elif not isinstance(value, (bytes, str)):
                value = str(value)
            
            if ttl_seconds:
                await self.client.setex(key, ttl_seconds, value)
            else:
                await self.client.set(key, value)
            
            return True
        except Exception as e:
            logger.error("Redis set error", key=key, error=str(e))
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value by key."""
        if not self._initialized:
            return None
        
        try:
            value = await self.client.get(key)
            if value is None:
                return None
            
            # Try to parse as JSON, fallback to string
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value.decode('utf-8') if isinstance(value, bytes) else value
        except Exception as e:
            logger.error("Redis get error", key=key, error=str(e))
            return None
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self._initialized:
            return False
        
        try:
            return bool(await self.client.exists(key))
        except Exception as e:
            logger.error("Redis exists error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key."""
        if not self._initialized:
            return False
        
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.error("Redis delete error", key=key, error=str(e))
            return False
    
    async def set_hash(self, name: str, mapping: Dict[str, Any], ttl_seconds: Optional[int] = None) -> bool:
        """Set hash field-value pairs."""
        if not self._initialized:
            return False
        
        try:
            # Convert values to strings/bytes
            hash_mapping = {}
            for k, v in mapping.items():
                if isinstance(v, (dict, list)):
                    hash_mapping[k] = json.dumps(v)
                else:
                    hash_mapping[k] = str(v)
            
            await self.client.hset(name, mapping=hash_mapping)
            
            if ttl_seconds:
                await self.client.expire(name, ttl_seconds)
            
            return True
        except Exception as e:
            logger.error("Redis set_hash error", name=name, error=str(e))
            return False
    
    async def get_hash(self, name: str) -> Optional[Dict[str, Any]]:
        """Get all hash field-value pairs."""
        if not self._initialized:
            return None
        
        try:
            data = await self.client.hgetall(name)
            if not data:
                return None
            
            result = {}
            for k, v in data.items():
                key = k.decode('utf-8') if isinstance(k, bytes) else k
                value = v.decode('utf-8') if isinstance(v, bytes) else v
                
                # Try to parse as JSON
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    result[key] = value
            
            return result
        except Exception as e:
            logger.error("Redis get_hash error", name=name, error=str(e))
            return None
    
    async def hash_exists(self, name: str, key: str) -> bool:
        """Check if hash field exists."""
        if not self._initialized:
            return False
        
        try:
            return bool(await self.client.hexists(name, key))
        except Exception as e:
            logger.error("Redis hash_exists error", name=name, key=key, error=str(e))
            return False
    
    async def ping(self) -> bool:
        """Check Redis connection."""
        if not self._initialized:
            return False
        
        try:
            await self.client.ping()
            return True
        except Exception as e:
            logger.error("Redis ping error", error=str(e))
            return False


# Global Redis client instance
redis_client = RedisClient()

