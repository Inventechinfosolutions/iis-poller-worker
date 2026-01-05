"""
Database client for MySQL connection and table management.
Handles automatic table creation using SQLAlchemy.
"""

import asyncio
from typing import Optional
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.config.settings import settings
from src.models.base import Base
from src.models.poller_jobs import PollerJob  # noqa: F401  Ensure model is registered with Base.metadata

logger = structlog.get_logger(__name__)


class DatabaseClient:
    """
    Database client for MySQL operations.
    Handles connection pooling and automatic table creation.
    """
    
    def __init__(self):
        self.engine = None
        self.session_factory = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connection and create tables if needed."""
        try:
            # Get database URL from settings
            database_url = self._get_database_url()
            
            if not database_url:
                logger.warning("Database URL not configured, skipping database initialization")
                return
            
            logger.info("Initializing database connection",
                       database_url=database_url.split('@')[1] if '@' in database_url else 'configured')
            
            # Use production-grade connection settings for MySQL
            engine_kwargs = {
                'pool_size': getattr(settings, 'mysql_pool_size', 5),
                'max_overflow': getattr(settings, 'mysql_max_overflow', 10),
                'pool_timeout': getattr(settings, 'mysql_pool_timeout', 30),
                'pool_recycle': getattr(settings, 'mysql_pool_recycle', 3600),
                'pool_pre_ping': getattr(settings, 'mysql_pool_pre_ping', True),
                'echo': False
            }
            
            # Add MySQL-specific connection arguments
            if 'mysql' in database_url:
                engine_kwargs['connect_args'] = {
                    'charset': 'utf8mb4',
                }
            
            self.engine = create_async_engine(
                database_url,
                **engine_kwargs
            )
            
            self.session_factory = sessionmaker(
                self.engine, class_=AsyncSession, expire_on_commit=False
            )
            
            # Create tables with proper MySQL settings
            # This will only create tables that don't exist (safe to call multiple times)
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            self._initialized = True
            logger.info("Database initialized successfully, tables created/verified")
            
        except Exception as e:
            logger.error("Failed to initialize database", error=str(e))
            # Don't raise - allow service to continue even if DB fails
            # Database is optional for the poller worker
    
    def _get_database_url(self) -> Optional[str]:
        """Get database URL from settings or environment variables."""
        import os
        
        # Check for explicit database URL
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            return database_url
        
        # Build from individual components
        mysql_host = getattr(settings, 'mysql_host', os.getenv('MYSQL_HOST', 'localhost'))
        mysql_port = getattr(settings, 'mysql_port', int(os.getenv('MYSQL_PORT', '3306')))
        mysql_user = getattr(settings, 'mysql_user', os.getenv('MYSQL_USER', 'root'))
        mysql_password = getattr(settings, 'mysql_password', os.getenv('MYSQL_PASSWORD', ''))
        mysql_database = getattr(settings, 'mysql_database', os.getenv('MYSQL_DATABASE', 'poller_db'))
        
        # If no password, check if we should skip
        if not mysql_password and mysql_user == 'root':
            # Try to connect without password (local dev)
            pass
        
        # Build MySQL URL
        password_part = f":{mysql_password}" if mysql_password else ""
        return f"mysql+aiomysql://{mysql_user}{password_part}@{mysql_host}:{mysql_port}/{mysql_database}?charset=utf8mb4"
    
    async def close(self):
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.session_factory = None
            self._initialized = False
            logger.info("Database connection closed")
    
    def get_session(self) -> AsyncSession:
        """Get a database session."""
        if not self._initialized:
            raise RuntimeError("Database client not initialized")
        return self.session_factory()


# Global database client instance
database_client = DatabaseClient()

