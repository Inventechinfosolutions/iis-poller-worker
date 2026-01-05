"""
Database migration utility for automatic table creation.
Checks if database exists and creates tables if needed.
"""

import os
import asyncio
import pymysql
import structlog
from pathlib import Path
from typing import Optional

from src.config.database_settings import database_settings

logger = structlog.get_logger(__name__)


class DatabaseMigration:
    """Handles automatic database table creation."""
    
    def __init__(self):
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connection and create tables if needed."""
        if not database_settings.auto_create_tables:
            logger.info("Auto table creation is disabled", 
                       auto_create_tables=database_settings.auto_create_tables)
            return
        
        try:
            logger.info("Initializing database migration",
                       host=database_settings.mysql_host,
                       port=database_settings.mysql_port,
                       database=database_settings.mysql_database)
            
            # Check if database exists
            if not await self._database_exists():
                logger.warning("Database does not exist, skipping table creation",
                             database=database_settings.mysql_database)
                return
            
            # Check if table already exists
            if await self._table_exists():
                logger.info("Table poller_jobs already exists, skipping creation")
                return
            
            # Create table
            await self._create_tables()
            
            self._initialized = True
            logger.info("Database migration completed successfully")
            
        except Exception as e:
            logger.error("Failed to initialize database migration", error=str(e))
            # Don't raise - allow service to continue even if DB migration fails
            # This is important for services that might not need the database
    
    async def _database_exists(self) -> bool:
        """Check if the database exists."""
        try:
            # Connect without specifying database
            connection = pymysql.connect(
                host=database_settings.mysql_host,
                port=database_settings.mysql_port,
                user=database_settings.mysql_user,
                password=database_settings.mysql_password,
                charset='utf8mb4'
            )
            
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                        "WHERE SCHEMA_NAME = %s",
                        (database_settings.mysql_database,)
                    )
                    result = cursor.fetchone()
                    return result is not None
            finally:
                connection.close()
                
        except Exception as e:
            logger.warning("Could not check if database exists", error=str(e))
            return False
    
    async def _table_exists(self) -> bool:
        """Check if poller_jobs table exists."""
        try:
            connection = pymysql.connect(
                host=database_settings.mysql_host,
                port=database_settings.mysql_port,
                user=database_settings.mysql_user,
                password=database_settings.mysql_password,
                database=database_settings.mysql_database,
                charset='utf8mb4'
            )
            
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'poller_jobs'",
                        (database_settings.mysql_database,)
                    )
                    result = cursor.fetchone()
                    return result[0] > 0 if result else False
            finally:
                connection.close()
                
        except Exception as e:
            logger.warning("Could not check if table exists", error=str(e))
            return False
    
    async def _create_tables(self):
        """Create poller_jobs table from migration file."""
        try:
            # Get the migration file path
            project_root = Path(__file__).parent.parent.parent
            migration_file = project_root / "migrations" / "001_create_poller_jobs_table.sql"
            
            if not migration_file.exists():
                logger.error("Migration file not found", path=str(migration_file))
                return
            
            # Read migration SQL
            with open(migration_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Connect to database
            connection = pymysql.connect(
                host=database_settings.mysql_host,
                port=database_settings.mysql_port,
                user=database_settings.mysql_user,
                password=database_settings.mysql_password,
                database=database_settings.mysql_database,
                charset='utf8mb4'
            )
            
            try:
                with connection.cursor() as cursor:
                    # Execute SQL (split by semicolon for multiple statements)
                    statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
                    
                    for statement in statements:
                        if statement:
                            cursor.execute(statement)
                    
                    connection.commit()
                    logger.info("Table poller_jobs created successfully")
                    
            finally:
                connection.close()
                
        except Exception as e:
            logger.error("Failed to create tables", error=str(e))
            raise


# Global database migration instance
database_migration = DatabaseMigration()

