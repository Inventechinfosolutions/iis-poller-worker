"""
Redis configuration settings for data persistence.
Simple, production-ready Redis configuration.
"""

from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
import os

# Load environment variables with proper path resolution
env_file = os.getenv("ENV_FILE", ".env")
# Resolve path relative to project root (apps/poller-worker)
if not os.path.isabs(env_file):
    # Get the project root directory (apps/poller-worker)
    current_dir = os.path.dirname(os.path.abspath(__file__))  # src/config
    project_root = os.path.dirname(os.path.dirname(current_dir))  # apps/poller-worker
    env_file = os.path.join(project_root, env_file)
load_dotenv(env_file)


class RedisSettings(BaseSettings):
    """Redis configuration for data persistence."""
    
    # Redis Connection
    redis_host: str = Field("localhost", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")
    redis_password: Optional[str] = Field(None, env="REDIS_PASSWORD")
    redis_db: int = Field(0, env="REDIS_DB")
    
    # Connection Pool
    redis_connection_pool_size: int = Field(20, env="REDIS_CONNECTION_POOL_SIZE")
    redis_socket_keepalive: bool = Field(True, env="REDIS_SOCKET_KEEPALIVE")
    redis_retry_on_timeout: bool = Field(True, env="REDIS_RETRY_ON_TIMEOUT")
    redis_health_check_interval: int = Field(30, env="REDIS_HEALTH_CHECK_INTERVAL")
    
    # Data Persistence
    file_tracking_ttl_days: int = Field(30, env="FILE_TRACKING_TTL_DAYS")  # 30 days TTL
    connection_stats_ttl_days: int = Field(90, env="CONNECTION_STATS_TTL_DAYS")  # 90 days TTL
    
    # Performance
    redis_max_connections: int = Field(50, env="REDIS_MAX_CONNECTIONS")
    redis_socket_connect_timeout: int = Field(5, env="REDIS_SOCKET_CONNECT_TIMEOUT")
    redis_socket_timeout: int = Field(5, env="REDIS_SOCKET_TIMEOUT")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


# Global Redis settings instance
redis_settings = RedisSettings()

