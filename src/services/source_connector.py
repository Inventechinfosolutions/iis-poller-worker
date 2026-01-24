"""
Source connector service for connecting to various data sources.
Handles connection to different source types (dummy, S3, FTP, etc.).
"""

import os
import hashlib
import asyncio
from typing import List, Optional, Dict, Any
from pathlib import Path
import structlog
from datetime import datetime

from src.models.schemas import SourceType, SourceConfig, FileEvent
from src.config.settings import settings
from src.utils.monitoring import metrics_collector

logger = structlog.get_logger(__name__)


class SourceConnector:
    """
    Source connector for various data sources.
    Handles connection and file reading from different source types.
    Supports multiple instances of each source type (e.g., multiple MinIO servers, multiple MySQL databases).
    """
    
    def __init__(self, max_connections: int = 100):
        """
        Initialize source connector with connection pool limits.
        
        Args:
            max_connections: Maximum number of concurrent connections
        """
        self._connections: Dict[str, Any] = {}
        self._max_connections = max_connections
        self._connection_semaphore = asyncio.Semaphore(max_connections)
    
    def get_active_connections_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)
    
    def get_connection_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        return {
            "active_connections": len(self._connections),
            "max_connections": self._max_connections,
            "available_slots": self._max_connections - len(self._connections)
        }
    
    def _generate_connection_key(self, source_type: SourceType, source_config: SourceConfig) -> str:
        """
        Generate a unique connection key for a source configuration.
        Supports multiple instances of the same source type.
        
        Args:
            source_type: Type of source
            source_config: Source configuration
            
        Returns:
            Unique connection key string
        """
        if source_type == SourceType.MINIO:
            # For MinIO: use endpoint + access_key to support multiple MinIO instances
            endpoint = source_config.endpoint or settings.minio_endpoint
            access_key = source_config.access_key or settings.minio_access_key
            # Create hash of endpoint + access_key for unique identification
            key_parts = f"{endpoint}_{access_key}"
            import hashlib
            key_hash = hashlib.md5(key_parts.encode()).hexdigest()[:8]
            return f"minio_{endpoint.replace(':', '_').replace('/', '_')}_{key_hash}"
        
        elif source_type == SourceType.MYSQL:
            # For MySQL: use host + port + database_name to support multiple MySQL instances
            host = source_config.database_host or settings.mysql_host
            port = source_config.database_port or settings.mysql_port
            database = source_config.database_name or settings.mysql_database
            # Create hash for unique identification
            key_parts = f"{host}_{port}_{database}"
            import hashlib
            key_hash = hashlib.md5(key_parts.encode()).hexdigest()[:8]
            return f"mysql_{host.replace('.', '_')}_{port}_{database}_{key_hash}"
        
        elif source_type == SourceType.S3:
            # For S3: use endpoint + access_key
            endpoint = source_config.endpoint or "default"
            access_key = source_config.access_key or "default"
            import hashlib
            key_parts = f"{endpoint}_{access_key}"
            key_hash = hashlib.md5(key_parts.encode()).hexdigest()[:8]
            return f"s3_{endpoint.replace(':', '_').replace('/', '_')}_{key_hash}"
        
        else:
            # For other sources: use endpoint or default
            endpoint = source_config.endpoint or "default"
            return f"{source_type.value}_{endpoint.replace(':', '_').replace('/', '_')}"
    
    async def connect(self, source_config: SourceConfig) -> bool:
        """
        Connect to the source based on configuration.
        Uses connection pool limits to prevent resource exhaustion.
        
        Args:
            source_config: Source configuration
            
        Returns:
            True if connection successful, False otherwise
        """
        # Acquire semaphore to limit concurrent connections
        async with self._connection_semaphore:
            try:
                source_type = source_config.source_type
                connection_key = self._generate_connection_key(source_type, source_config)
                
                # Check if connection already exists
                if connection_key in self._connections:
                    logger.debug("Reusing existing connection", 
                               source_type=source_type.value,
                               connection_key=connection_key)
                    return True
                
                logger.info("Connecting to source", 
                           source_type=source_type.value,
                           endpoint=source_config.endpoint,
                           active_connections=len(self._connections),
                           max_connections=self._max_connections)
                
                start_time = datetime.utcnow()
                
                if source_type == SourceType.DUMMY:
                    success = await self._connect_dummy(source_config)
                elif source_type == SourceType.S3:
                    success = await self._connect_s3(source_config)
                elif source_type == SourceType.MINIO:
                    success = await self._connect_minio(source_config)
                elif source_type == SourceType.MYSQL:
                    success = await self._connect_mysql(source_config)
                elif source_type == SourceType.FTP:
                    success = await self._connect_ftp(source_config)
                elif source_type == SourceType.SFTP:
                    success = await self._connect_sftp(source_config)
                elif source_type == SourceType.LOCAL:
                    success = await self._connect_local(source_config)
                elif source_type == SourceType.HTTP:
                    success = await self._connect_http(source_config)
                elif source_type == SourceType.ONEDRIVE:
                    success = await self._connect_onedrive(source_config)
                else:
                    logger.error("Unsupported source type", source_type=source_type.value)
                    metrics_collector.record_connection_error(source_type.value, "unsupported_type")
                    return False
                
                duration = (datetime.utcnow() - start_time).total_seconds()
                
                if success:
                    # Only update connection metadata if connection doesn't already exist
                    # (specific connect methods like _connect_minio already store full connection data)
                    if connection_key not in self._connections:
                        # Store basic connection info for simple sources
                        self._connections[connection_key] = {
                            "config": source_config,
                            "connected_at": datetime.utcnow(),
                            "source_type": source_type
                        }
                    else:
                        # Connection already stored by specific connect method (e.g., _connect_minio)
                        # Just update metadata if needed
                        existing = self._connections[connection_key]
                        if "connected_at" not in existing:
                            existing["connected_at"] = datetime.utcnow()
                        if "source_type" not in existing:
                            existing["source_type"] = source_type
                    
                    logger.info("Successfully connected to source", 
                               source_type=source_type.value,
                               duration=duration,
                               connection_key=connection_key)
                    return True
                else:
                    logger.error("Failed to connect to source", 
                               source_type=source_type.value)
                    metrics_collector.record_connection_error(source_type.value, "connection_failed")
                    return False
                    
            except Exception as e:
                logger.error("Error connecting to source", 
                            source_type=source_config.source_type.value,
                            error=str(e))
                metrics_collector.record_connection_error(
                    source_config.source_type.value, type(e).__name__
                )
                return False
    
    async def list_files(self, source_config: SourceConfig, 
                        file_pattern: Optional[str] = None) -> List[FileEvent]:
        """
        List files from the source.
        
        Args:
            source_config: Source configuration
            file_pattern: Optional file pattern to match
            
        Returns:
            List of FileEvent objects
        """
        try:
            source_type = source_config.source_type
            logger.info("Listing files from source", 
                       source_type=source_type.value,
                       file_pattern=file_pattern)
            
            if source_type == SourceType.DUMMY:
                files = await self._list_files_dummy(source_config, file_pattern)
            elif source_type == SourceType.S3:
                files = await self._list_files_s3(source_config, file_pattern)
            elif source_type == SourceType.MINIO:
                files = await self._list_files_minio(source_config, file_pattern)
            elif source_type == SourceType.MYSQL:
                files = await self._list_files_mysql(source_config, file_pattern)
            elif source_type == SourceType.FTP:
                files = await self._list_files_ftp(source_config, file_pattern)
            elif source_type == SourceType.SFTP:
                files = await self._list_files_sftp(source_config, file_pattern)
            elif source_type == SourceType.LOCAL:
                files = await self._list_files_local(source_config, file_pattern)
            elif source_type == SourceType.HTTP:
                files = await self._list_files_http(source_config, file_pattern)
            elif source_type == SourceType.ONEDRIVE:
                files = await self._list_files_onedrive(source_config, file_pattern)
            else:
                logger.error("Unsupported source type", source_type=source_type.value)
                return []
            
            logger.info("Files listed from source", 
                       source_type=source_type.value,
                       file_count=len(files))
            
            return files
            
        except Exception as e:
            logger.error("Error listing files from source", 
                        source_type=source_config.source_type.value,
                        error=str(e))
            return []
    
    async def read_file(self, source_config: SourceConfig, 
                       file_path: str,
                       file_event_metadata: Optional[Dict[str, Any]] = None) -> Optional[bytes]:
        """
        Read file content from source.
        
        Args:
            source_config: Source configuration
            file_path: Path to the file
            file_event_metadata: Optional metadata from file event (used for OneDrive to get drive/item IDs)
            
        Returns:
            File content as bytes, or None if error
        """
        try:
            source_type = source_config.source_type
            logger.debug("Reading file from source", 
                        source_type=source_type.value,
                        file_path=file_path)
            
            if source_type == SourceType.DUMMY:
                content = await self._read_file_dummy(source_config, file_path)
            elif source_type == SourceType.S3:
                content = await self._read_file_s3(source_config, file_path)
            elif source_type == SourceType.MINIO:
                content = await self._read_file_minio(source_config, file_path)
            elif source_type == SourceType.MYSQL:
                content = await self._read_file_mysql(source_config, file_path)
            elif source_type == SourceType.FTP:
                content = await self._read_file_ftp(source_config, file_path)
            elif source_type == SourceType.SFTP:
                content = await self._read_file_sftp(source_config, file_path)
            elif source_type == SourceType.LOCAL:
                content = await self._read_file_local(source_config, file_path)
            elif source_type == SourceType.HTTP:
                content = await self._read_file_http(source_config, file_path)
            elif source_type == SourceType.ONEDRIVE:
                content = await self._read_file_onedrive(source_config, file_path, file_event_metadata)
            else:
                logger.error("Unsupported source type", source_type=source_type.value)
                return None
            
            if content:
                logger.debug("File read successfully", 
                            source_type=source_type.value,
                            file_path=file_path,
                            size=len(content))
            
            return content
            
        except Exception as e:
            logger.error("Error reading file from source", 
                        source_type=source_config.source_type.value,
                        file_path=file_path,
                        error=str(e))
            return None
    
    async def disconnect(self, source_config: SourceConfig):
        """
        Disconnect from source and release connection pool slot.
        Supports multiple instances - disconnects the specific instance based on connection key.
        """
        try:
            source_type = source_config.source_type
            connection_key = self._generate_connection_key(source_type, source_config)
            
            if connection_key in self._connections:
                connection = self._connections[connection_key]
                
                # Clean up connection resources
                if source_type == SourceType.MYSQL and connection.get("engine"):
                    # Dispose MySQL engine
                    try:
                        await connection["engine"].dispose()
                        logger.info("MySQL engine disposed", connection_key=connection_key)
                    except Exception as e:
                        logger.warning("Error disposing MySQL engine", 
                                     connection_key=connection_key,
                                     error=str(e))
                
                del self._connections[connection_key]
                logger.info("Disconnected from source", 
                           source_type=source_type.value,
                           connection_key=connection_key,
                           remaining_connections=len(self._connections))
            else:
                logger.warning("Connection not found for disconnection", 
                             source_type=source_type.value,
                             connection_key=connection_key)
            
        except Exception as e:
            logger.error("Error disconnecting from source", 
                        source_type=source_config.source_type.value,
                        error=str(e))
    
    def get_active_connections(self) -> Dict[str, Any]:
        """
        Get information about all active connections.
        Useful for monitoring and debugging multiple instances.
        
        Returns:
            Dictionary with connection information grouped by source type
        """
        connections_info = {
            "total": len(self._connections),
            "by_type": {},
            "details": {}
        }
        
        for connection_key, connection_data in self._connections.items():
            # Extract source type from connection key
            source_type = connection_key.split('_')[0]
            
            if source_type not in connections_info["by_type"]:
                connections_info["by_type"][source_type] = 0
            connections_info["by_type"][source_type] += 1
            
            # Store connection details
            connections_info["details"][connection_key] = {
                "source_type": source_type,
                "endpoint": connection_data.get("endpoint") or connection_data.get("host"),
                "bucket": connection_data.get("bucket"),
                "database": connection_data.get("database"),
                "connected_at": connection_data.get("connected_at").isoformat() if connection_data.get("connected_at") else None
            }
        
        return connections_info
    
    async def disconnect_all(self, source_type: Optional[SourceType] = None):
        """
        Disconnect all connections, optionally filtered by source type.
        Useful for cleanup and shutdown.
        
        Args:
            source_type: Optional source type to disconnect only specific type
        """
        try:
            if source_type:
                # Disconnect only specific source type
                keys_to_remove = [
                    key for key in self._connections.keys() 
                    if key.startswith(f"{source_type.value}_")
                ]
            else:
                # Disconnect all
                keys_to_remove = list(self._connections.keys())
            
            for connection_key in keys_to_remove:
                connection = self._connections[connection_key]
                
                # Clean up MySQL engines
                if connection.get("engine"):
                    try:
                        await connection["engine"].dispose()
                    except Exception as e:
                        logger.warning("Error disposing engine", 
                                     connection_key=connection_key,
                                     error=str(e))
                
                del self._connections[connection_key]
            
            logger.info("Disconnected connections", 
                       source_type=source_type.value if source_type else "all",
                       disconnected_count=len(keys_to_remove),
                       remaining_connections=len(self._connections))
            
        except Exception as e:
            logger.error("Error disconnecting all connections", error=str(e))
    
    # Dummy source implementation
    async def _connect_dummy(self, source_config: SourceConfig) -> bool:
        """Connect to dummy source (uses local dummy data files)."""
        try:
            dummy_path = source_config.path or settings.dummy_data_path
            path = Path(dummy_path)
            
            if not path.exists():
                logger.warning("Dummy data path does not exist, creating it", path=dummy_path)
                path.mkdir(parents=True, exist_ok=True)
            
            self._connections["dummy_default"] = {"path": path}
            return True
            
        except Exception as e:
            logger.error("Error connecting to dummy source", error=str(e))
            return False
    
    async def _list_files_dummy(self, source_config: SourceConfig, 
                               file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from dummy source."""
        files = []
        try:
            dummy_path = source_config.path or settings.dummy_data_path
            path = Path(dummy_path)
            
            if not path.exists():
                logger.warning("Dummy data path does not exist", path=dummy_path)
                return files
            
            # Get all CSV files (or match pattern)
            pattern = file_pattern or "*.csv"
            for file_path in path.glob(pattern):
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    file_name = file_path.name
                    file_type = file_path.suffix.lower()
                    
                    # Calculate checksum
                    checksum = self._calculate_checksum(file_path)
                    
                    file_event = FileEvent(
                        event_id=f"dummy_{file_name}_{datetime.utcnow().timestamp()}",
                        job_id="",  # Will be set by caller
                        source_type=SourceType.DUMMY,
                        file_path=str(file_path),
                        file_name=file_name,
                        file_size=file_size,
                        file_type=file_type,
                        file_url=None,
                        checksum=checksum,
                        metadata={"dummy_source": True}
                    )
                    
                    files.append(file_event)
            
            logger.info("Listed files from dummy source", 
                       path=dummy_path,
                       file_count=len(files))
            
        except Exception as e:
            logger.error("Error listing files from dummy source", error=str(e))
        
        return files
    
    async def _read_file_dummy(self, source_config: SourceConfig, 
                              file_path: str) -> Optional[bytes]:
        """Read file from dummy source."""
        try:
            import aiofiles
            path = Path(file_path)
            if path.exists() and path.is_file():
                async with aiofiles.open(path, 'rb') as f:
                    return await f.read()
            else:
                logger.warning("File not found in dummy source", file_path=file_path)
                return None
                
        except Exception as e:
            logger.error("Error reading file from dummy source", 
                        file_path=file_path,
                        error=str(e))
            return None
    
    # Placeholder implementations for other source types
    async def _connect_s3(self, source_config: SourceConfig) -> bool:
        """Connect to S3 source (placeholder)."""
        logger.warning("S3 connection not implemented yet")
        return False
    
    async def _list_files_s3(self, source_config: SourceConfig, 
                             file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from S3 source (placeholder)."""
        logger.warning("S3 file listing not implemented yet")
        return []
    
    async def _read_file_s3(self, source_config: SourceConfig, 
                            file_path: str) -> Optional[bytes]:
        """Read file from S3 source (placeholder)."""
        logger.warning("S3 file reading not implemented yet")
        return None
    
    async def _connect_ftp(self, source_config: SourceConfig) -> bool:
        """Connect to FTP source (placeholder)."""
        logger.warning("FTP connection not implemented yet")
        return False
    
    async def _list_files_ftp(self, source_config: SourceConfig, 
                              file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from FTP source (placeholder)."""
        logger.warning("FTP file listing not implemented yet")
        return []
    
    async def _read_file_ftp(self, source_config: SourceConfig, 
                            file_path: str) -> Optional[bytes]:
        """Read file from FTP source (placeholder)."""
        logger.warning("FTP file reading not implemented yet")
        return None
    
    async def _connect_sftp(self, source_config: SourceConfig) -> bool:
        """Connect to SFTP source (placeholder)."""
        logger.warning("SFTP connection not implemented yet")
        return False
    
    async def _list_files_sftp(self, source_config: SourceConfig, 
                              file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from SFTP source (placeholder)."""
        logger.warning("SFTP file listing not implemented yet")
        return []
    
    async def _read_file_sftp(self, source_config: SourceConfig, 
                             file_path: str) -> Optional[bytes]:
        """Read file from SFTP source (placeholder)."""
        logger.warning("SFTP file reading not implemented yet")
        return None
    
    async def _connect_local(self, source_config: SourceConfig) -> bool:
        """Connect to local file system source."""
        try:
            local_path = source_config.path or "/"
            path = Path(local_path)
            
            if not path.exists():
                logger.error("Local path does not exist", path=local_path)
                return False
            
            self._connections["local_default"] = {"path": path}
            return True
            
        except Exception as e:
            logger.error("Error connecting to local source", error=str(e))
            return False
    
    async def _list_files_local(self, source_config: SourceConfig, 
                               file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from local file system."""
        files = []
        try:
            local_path = source_config.path or "/"
            path = Path(local_path)
            
            if not path.exists():
                return files
            
            pattern = file_pattern or "*"
            for file_path in path.glob(pattern):
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    file_name = file_path.name
                    file_type = file_path.suffix.lower()
                    
                    checksum = self._calculate_checksum(file_path)
                    
                    file_event = FileEvent(
                        event_id=f"local_{file_name}_{datetime.utcnow().timestamp()}",
                        job_id="",
                        source_type=SourceType.LOCAL,
                        file_path=str(file_path),
                        file_name=file_name,
                        file_size=file_size,
                        file_type=file_type,
                        file_url=None,
                        checksum=checksum
                    )
                    
                    files.append(file_event)
            
        except Exception as e:
            logger.error("Error listing files from local source", error=str(e))
        
        return files
    
    async def _read_file_local(self, source_config: SourceConfig, 
                              file_path: str) -> Optional[bytes]:
        """Read file from local file system."""
        try:
            import aiofiles
            path = Path(file_path)
            if path.exists() and path.is_file():
                async with aiofiles.open(path, 'rb') as f:
                    return await f.read()
            return None
                
        except Exception as e:
            logger.error("Error reading file from local source", 
                        file_path=file_path,
                        error=str(e))
            return None
    
    async def _connect_http(self, source_config: SourceConfig) -> bool:
        """Connect to HTTP source (placeholder)."""
        logger.warning("HTTP connection not implemented yet")
        return False
    
    async def _list_files_http(self, source_config: SourceConfig, 
                              file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from HTTP source (placeholder)."""
        logger.warning("HTTP file listing not implemented yet")
        return []
    
    async def _read_file_http(self, source_config: SourceConfig, 
                             file_path: str) -> Optional[bytes]:
        """Read file from HTTP source (placeholder)."""
        logger.warning("HTTP file reading not implemented yet")
        return None
    
    # ============================================================================
    # MINIO SOURCE CONNECTION (Future Implementation - Commented Out)
    # ============================================================================
    # MinIO connection will use the existing MinIO client from src.clients.minio_client
    # Uncomment and implement when ready to use MinIO as a source
    
    async def _connect_minio(self, source_config: SourceConfig) -> bool:
        """
        Connect to MinIO source.
        Supports multiple MinIO instances (different endpoints, credentials, buckets).
        Each MinIO instance will have a unique connection key based on endpoint + credentials.
        """
        try:
            from src.clients.minio_client import MinIOClient
            
            # Get MinIO configuration from source_config or settings
            minio_config = {
                'minio_endpoint': source_config.endpoint or settings.minio_endpoint,
                'minio_access_key': source_config.access_key or settings.minio_access_key,
                'minio_secret_key': source_config.secret_key or settings.minio_secret_key,
                'minio_secure': source_config.connection_params.get('secure', False) if source_config.connection_params else settings.minio_secure
            }
            
            # Generate unique connection key for this MinIO instance
            # Supports multiple MinIO servers with different endpoints/credentials
            connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
            
            # Check if connection already exists (connection pooling/reuse)
            if connection_key in self._connections:
                existing_connection = self._connections[connection_key]
                # Verify connection is still valid
                if existing_connection.get("client"):
                    try:
                        test_result = await existing_connection["client"].test_connection()
                        if test_result:
                            logger.info("Reusing existing MinIO connection", 
                                      connection_key=connection_key,
                                      endpoint=minio_config['minio_endpoint'])
                            return True
                    except Exception:
                        # Connection is stale, remove it
                        logger.warning("Existing MinIO connection is stale, reconnecting", 
                                    connection_key=connection_key)
                        del self._connections[connection_key]
            
            # Create new MinIO client for this instance
            minio_client = MinIOClient(minio_config)
            
            # Test connection
            connected = await minio_client.test_connection()
            if connected:
                connection_data = {
                    "client": minio_client,
                    "config": source_config,
                    "bucket": source_config.bucket_name,
                    "endpoint": minio_config['minio_endpoint'],
                    "access_key": minio_config['minio_access_key'],
                    "connected_at": datetime.utcnow()
                }
                self._connections[connection_key] = connection_data
                logger.info("Successfully connected to MinIO instance", 
                           connection_key=connection_key,
                           endpoint=minio_config['minio_endpoint'],
                           bucket=source_config.bucket_name,
                           has_client=("client" in connection_data),
                           connection_keys=list(connection_data.keys()),
                           total_connections=len([k for k in self._connections.keys() if k.startswith('minio_')]))
                return True
            else:
                logger.error("Failed to connect to MinIO", 
                           endpoint=minio_config['minio_endpoint'],
                           connection_key=connection_key)
                return False
                
        except Exception as e:
            logger.error("Error connecting to MinIO source", 
                       error=str(e),
                       endpoint=source_config.endpoint)
            return False
        
        # # Future implementation (commented out):
        # try:
        #     from src.clients.minio_client import MinIOClient
        #     
        #     # Get MinIO configuration from source_config or settings
        #     minio_config = {
        #         'minio_endpoint': source_config.endpoint or settings.minio_endpoint,
        #         'minio_access_key': source_config.access_key or settings.minio_access_key,
        #         'minio_secret_key': source_config.secret_key or settings.minio_secret_key,
        #         'minio_secure': source_config.connection_params.get('secure', False) if source_config.connection_params else False
        #     }
        #     
        #     # Generate unique connection key for this MinIO instance
        #     # Supports multiple MinIO servers with different endpoints/credentials
        #     connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
        #     
        #     # Check if connection already exists (connection pooling/reuse)
        #     if connection_key in self._connections:
        #         existing_connection = self._connections[connection_key]
        #         # Verify connection is still valid
        #         if existing_connection.get("client"):
        #             try:
        #                 test_result = await existing_connection["client"].test_connection()
        #                 if test_result:
        #                     logger.info("Reusing existing MinIO connection", 
        #                               connection_key=connection_key,
        #                               endpoint=minio_config['minio_endpoint'])
        #                     return True
        #             except Exception:
        #                 # Connection is stale, remove it
        #                 logger.warning("Existing MinIO connection is stale, reconnecting", 
        #                             connection_key=connection_key)
        #                 del self._connections[connection_key]
        #     
        #     # Create new MinIO client for this instance
        #     minio_client = MinIOClient(minio_config)
        #     
        #     # Test connection
        #     connected = await minio_client.test_connection()
        #     if connected:
        #         self._connections[connection_key] = {
        #             "client": minio_client,
        #             "config": source_config,
        #             "bucket": source_config.bucket_name,
        #             "endpoint": minio_config['minio_endpoint'],
        #             "access_key": minio_config['minio_access_key'],
        #             "connected_at": datetime.utcnow()
        #         }
        #         logger.info("Successfully connected to MinIO instance", 
        #                    connection_key=connection_key,
        #                    endpoint=minio_config['minio_endpoint'],
        #                    bucket=source_config.bucket_name,
        #                    total_connections=len([k for k in self._connections.keys() if k.startswith('minio_')]))
        #         return True
        #     else:
        #         logger.error("Failed to connect to MinIO", 
        #                    endpoint=minio_config['minio_endpoint'],
        #                    connection_key=connection_key)
        #         return False
        #         
        # except Exception as e:
        #     logger.error("Error connecting to MinIO source", 
        #                error=str(e),
        #                endpoint=source_config.endpoint)
        #     return False
    
    async def _list_files_minio(self, source_config: SourceConfig, 
                                file_pattern: Optional[str] = None) -> List[FileEvent]:
        """
        List files from MinIO source.
        Supports multiple MinIO instances - uses unique connection key to find the right instance.
        """
        files = []
        try:
            # Generate connection key to find the correct MinIO instance
            # This supports multiple MinIO servers with different endpoints/credentials
            connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
            connection = self._connections.get(connection_key)
            
            if not connection:
                logger.error("MinIO connection not found", 
                           connection_key=connection_key,
                           endpoint=source_config.endpoint,
                           available_connections=list(self._connections.keys()))
                # Try to reconnect
                connected = await self._connect_minio(source_config)
                if not connected:
                    return files
                # Re-fetch connection after reconnect
                connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
                connection = self._connections.get(connection_key)
            
            if not connection:
                logger.error("Failed to establish MinIO connection", 
                           connection_key=connection_key,
                           available_connections=list(self._connections.keys()))
                return files
            
            # Debug: log connection keys
            logger.info("MinIO connection retrieved", 
                       connection_key=connection_key,
                       connection_keys=list(connection.keys()) if connection else None,
                       has_client=("client" in connection) if connection else False)
            
            minio_client = connection.get("client")
            if not minio_client:
                logger.error("MinIO client not found in connection", 
                           connection_key=connection_key,
                           connection_keys=list(connection.keys()) if connection else None,
                           available_connections=list(self._connections.keys()))
                return files
            bucket_name = source_config.bucket_name or connection.get("bucket")
            if not bucket_name:
                logger.error("Bucket name not specified for MinIO source")
                return files
            
            path_prefix = source_config.path or ""
            
            # List objects from MinIO bucket
            objects = await minio_client.list_objects(bucket_name, prefix=path_prefix, recursive=True)
            
            # Filter by file pattern if provided
            import fnmatch
            from pathlib import Path
            
            pattern = file_pattern or "*"
            
            for obj in objects:
                object_name = obj.get('object_name', '')
                # Skip directories (objects ending with /)
                if object_name.endswith('/'):
                    continue
                
                # Match file pattern
                if fnmatch.fnmatch(object_name, pattern) or fnmatch.fnmatch(Path(object_name).name, pattern):
                    file_size = obj.get('size', 0)
                    file_name = Path(object_name).name
                    file_type = Path(file_name).suffix.lower() if file_name else ""
                    
                    file_event = FileEvent(
                        event_id=f"minio_{file_name}_{datetime.utcnow().timestamp()}",
                        job_id="",  # Will be set by caller
                        source_type=SourceType.MINIO,
                        file_path=object_name,
                        file_name=file_name,
                        file_size=file_size,
                        file_type=file_type,
                        file_url=f"s3://{bucket_name}/{object_name}",
                        checksum=obj.get('etag', '').strip('"') if obj.get('etag') else None,
                        metadata={
                            "bucket": bucket_name,
                            "object_name": object_name,
                            "last_modified": obj.get('last_modified').isoformat() if obj.get('last_modified') else None
                        }
                    )
                    
                    files.append(file_event)
            
            logger.info("Listed files from MinIO source", 
                       bucket=bucket_name,
                       path=path_prefix,
                       file_count=len(files))
            
        except Exception as e:
            logger.error("Error listing files from MinIO source", error=str(e))
        
        return files
        
        # # Future implementation (commented out):
        # files = []
        # try:
        #     # Generate connection key to find the correct MinIO instance
        #     # This supports multiple MinIO servers with different endpoints/credentials
        #     connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
        #     connection = self._connections.get(connection_key)
        #     
        #     if not connection:
        #         logger.error("MinIO connection not found", 
        #                    connection_key=connection_key,
        #                    endpoint=source_config.endpoint,
        #                    available_connections=list(self._connections.keys()))
        #         # Try to reconnect
        #         connected = await self._connect_minio(source_config)
        #         if not connected:
        #             return files
        #         connection = self._connections.get(connection_key)
        #     
        #     minio_client = connection["client"]
        #     bucket_name = source_config.bucket_name or connection.get("bucket")
        #     path_prefix = source_config.path or ""
        #     
        #     # List objects from MinIO bucket
        #     objects = await minio_client.list_objects(bucket_name, prefix=path_prefix)
        #     
        #     # Filter by file pattern if provided
        #     pattern = file_pattern or "*"
        #     import fnmatch
        #     
        #     for obj in objects:
        #         object_name = obj.get('name', '')
        #         if fnmatch.fnmatch(object_name, pattern):
        #             file_size = obj.get('size', 0)
        #             file_name = object_name.split('/')[-1] if '/' in object_name else object_name
        #             file_type = Path(file_name).suffix.lower() if file_name else ""
        #             
        #             file_event = FileEvent(
        #                 event_id=f"minio_{file_name}_{datetime.utcnow().timestamp()}",
        #                 job_id="",  # Will be set by caller
        #                 source_type=SourceType.MINIO,
        #                 file_path=object_name,
        #                 file_name=file_name,
        #                 file_size=file_size,
        #                 file_type=file_type,
        #                 file_url=f"s3://{bucket_name}/{object_name}",
        #                 checksum=obj.get('etag', '').strip('"'),
        #                 metadata={
        #                     "bucket": bucket_name,
        #                     "object_name": object_name,
        #                     "last_modified": obj.get('last_modified', '').isoformat() if obj.get('last_modified') else None
        #                 }
        #             )
        #             
        #             files.append(file_event)
        #     
        #     logger.info("Listed files from MinIO source", 
        #                bucket=bucket_name,
        #                path=path_prefix,
        #                file_count=len(files))
        #     
        # except Exception as e:
        #     logger.error("Error listing files from MinIO source", error=str(e))
        # 
        # return files
    
    async def _read_file_minio(self, source_config: SourceConfig, 
                               file_path: str) -> Optional[bytes]:
        """
        Read file from MinIO source.
        Supports multiple MinIO instances - uses unique connection key to find the right instance.
        """
        try:
            # Generate connection key to find the correct MinIO instance
            connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
            connection = self._connections.get(connection_key)
            
            if not connection:
                logger.error("MinIO connection not found", 
                           connection_key=connection_key,
                           endpoint=source_config.endpoint)
                # Try to reconnect
                connected = await self._connect_minio(source_config)
                if not connected:
                    return None
                connection = self._connections.get(connection_key)
            
            if not connection:
                logger.error("Failed to establish MinIO connection")
                return None
            
            minio_client = connection.get("client")
            if not minio_client:
                logger.error("MinIO client not found in connection", connection_key=connection_key)
                return None
            bucket_name = source_config.bucket_name or connection.get("bucket")
            if not bucket_name:
                logger.error("Bucket name not specified for MinIO source")
                return None
            
            # Download object from MinIO
            file_content = await minio_client.get_object_bytes(bucket_name, file_path)
            
            logger.info("File read from MinIO", 
                       bucket=bucket_name,
                       file_path=file_path,
                       size=len(file_content) if file_content else 0)
            
            return file_content
            
        except Exception as e:
            logger.error("Error reading file from MinIO source",
                       file_path=file_path,
                       error=str(e))
            return None
        
        # # Future implementation (commented out):
        # try:
        #     # Generate connection key to find the correct MinIO instance
        #     connection_key = self._generate_connection_key(SourceType.MINIO, source_config)
        #     connection = self._connections.get(connection_key)
        #     
        #     if not connection:
        #         logger.error("MinIO connection not found", 
        #                    connection_key=connection_key,
        #                    endpoint=source_config.endpoint)
        #         # Try to reconnect
        #         connected = await self._connect_minio(source_config)
        #         if not connected:
        #             return None
        #         connection = self._connections.get(connection_key)
        #     
        #     minio_client = connection["client"]
        #     bucket_name = source_config.bucket_name or connection.get("bucket")
        #     
        #     # Download object from MinIO
        #     file_content = await minio_client.get_object(bucket_name, file_path)
        #     
        #     logger.info("File read from MinIO", 
        #                bucket=bucket_name,
        #                file_path=file_path,
        #                size=len(file_content) if file_content else 0)
        #     
        #     return file_content
        #     
        # except Exception as e:
        #     logger.error("Error reading file from MinIO source", 
        #                 file_path=file_path,
        #                 error=str(e))
        #     return None
    
    # ============================================================================
    # MYSQL DATABASE SOURCE CONNECTION (Future Implementation - Commented Out)
    # ============================================================================
    # MySQL connection will query database tables for file metadata
    # Uncomment and implement when ready to use MySQL as a source
    
    async def _connect_mysql(self, source_config: SourceConfig) -> bool:
        """
        Connect to MySQL database source.
        Supports multiple MySQL instances (different hosts, databases, credentials).
        
        Future implementation: Will connect to MySQL database and query for file records.
        Each MySQL instance will have a unique connection key based on host + port + database.
        """
        logger.warning("MySQL connection not implemented yet - future feature")
        return False
        
        # # Future implementation (commented out):
        # try:
        #     from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        #     from sqlalchemy.orm import sessionmaker
        #     from sqlalchemy import text
        #     
        #     # Build MySQL connection URL
        #     db_host = source_config.database_host or settings.mysql_host
        #     db_port = source_config.database_port or settings.mysql_port
        #     db_name = source_config.database_name or settings.mysql_database
        #     db_user = source_config.database_user or settings.mysql_user
        #     db_password = source_config.database_password or settings.mysql_password
        #     
        #     # Generate unique connection key for this MySQL instance
        #     # Supports multiple MySQL servers with different hosts/databases
        #     connection_key = self._generate_connection_key(SourceType.MYSQL, source_config)
        #     
        #     # Check if connection already exists (connection pooling/reuse)
        #     if connection_key in self._connections:
        #         existing_connection = self._connections[connection_key]
        #         # Verify connection is still valid
        #         if existing_connection.get("session_factory"):
        #             try:
        #                 async with existing_connection["session_factory"]() as session:
        #                     result = await session.execute(text("SELECT 1"))
        #                     result.scalar()
        #                 logger.info("Reusing existing MySQL connection", 
        #                           connection_key=connection_key,
        #                           host=db_host,
        #                           database=db_name)
        #                 return True
        #             except Exception:
        #                 # Connection is stale, remove it
        #                 logger.warning("Existing MySQL connection is stale, reconnecting", 
        #                             connection_key=connection_key)
        #                 if existing_connection.get("engine"):
        #                     await existing_connection["engine"].dispose()
        #                 del self._connections[connection_key]
        #     
        #     database_url = (
        #         f"mysql+aiomysql://{db_user}:{db_password}"
        #         f"@{db_host}:{db_port}/{db_name}"
        #         f"?charset=utf8mb4"
        #     )
        #     
        #     # Create async engine with connection pooling
        #     engine = create_async_engine(
        #         database_url,
        #         pool_size=5,
        #         max_overflow=10,
        #         pool_pre_ping=True,
        #         echo=False
        #     )
        #     
        #     # Create session factory
        #     session_factory = sessionmaker(
        #         engine, class_=AsyncSession, expire_on_commit=False
        #     )
        #     
        #     # Test connection
        #     async with session_factory() as session:
        #         result = await session.execute(text("SELECT 1"))
        #         result.scalar()
        #     
        #     self._connections[connection_key] = {
        #         "engine": engine,
        #         "session_factory": session_factory,
        #         "config": source_config,
        #         "database": db_name,
        #         "host": db_host,
        #         "port": db_port,
        #         "table": source_config.database_table,
        #         "connected_at": datetime.utcnow()
        #     }
        #     
        #     logger.info("Successfully connected to MySQL instance", 
        #                connection_key=connection_key,
        #                host=db_host,
        #                port=db_port,
        #                database=db_name,
        #                table=source_config.database_table,
        #                total_connections=len([k for k in self._connections.keys() if k.startswith('mysql_')]))
        #     return True
        #     
        # except Exception as e:
        #     logger.error("Error connecting to MySQL source", 
        #                error=str(e),
        #                host=source_config.database_host,
        #                database=source_config.database_name)
        #     return False
    
    async def _list_files_mysql(self, source_config: SourceConfig, 
                                file_pattern: Optional[str] = None) -> List[FileEvent]:
        """
        List files from MySQL database source.
        Supports multiple MySQL instances - uses unique connection key to find the right instance.
        
        Future implementation: Will query MySQL table for file records.
        """
        logger.warning("MySQL file listing not implemented yet - future feature")
        return []
        
        # # Future implementation (commented out):
        # files = []
        # try:
        #     # Generate connection key to find the correct MySQL instance
        #     # This supports multiple MySQL servers with different hosts/databases
        #     connection_key = self._generate_connection_key(SourceType.MYSQL, source_config)
        #     connection = self._connections.get(connection_key)
        #     
        #     if not connection:
        #         logger.error("MySQL connection not found", 
        #                    connection_key=connection_key,
        #                    host=source_config.database_host,
        #                    database=source_config.database_name,
        #                    available_connections=list(self._connections.keys()))
        #         # Try to reconnect
        #         connected = await self._connect_mysql(source_config)
        #         if not connected:
        #             return files
        #         connection = self._connections.get(connection_key)
        #     
        #     session_factory = connection["session_factory"]
        #     table_name = source_config.database_table or connection.get("table")
        #     
        #     if not table_name:
        #         logger.error("MySQL table name not specified")
        #         return files
        #     
        #     # Query database for file records
        #     # Expected table columns: file_name, file_path, file_size, file_type, file_url, etc.
        #     from sqlalchemy import text
        #     
        #     async with session_factory() as session:
        #         # Build query based on file_pattern
        #         query = f"SELECT * FROM {table_name} WHERE 1=1"
        #         params = {}
        #         
        #         if file_pattern:
        #             # Convert file pattern to SQL LIKE pattern
        #             sql_pattern = file_pattern.replace('*', '%').replace('?', '_')
        #             query += " AND file_name LIKE :pattern"
        #             params['pattern'] = sql_pattern
        #         
        #         result = await session.execute(text(query), params)
        #         rows = result.fetchall()
        #         
        #         # Convert database rows to FileEvent objects
        #         for row in rows:
        #             row_dict = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
        #             
        #             file_name = row_dict.get('file_name', '')
        #             file_path = row_dict.get('file_path', row_dict.get('file_name', ''))
        #             file_size = int(row_dict.get('file_size', 0))
        #             file_type = row_dict.get('file_type', Path(file_name).suffix.lower() if file_name else "")
        #             file_url = row_dict.get('file_url')
        #             
        #             file_event = FileEvent(
        #                 event_id=f"mysql_{file_name}_{datetime.utcnow().timestamp()}",
        #                 job_id="",  # Will be set by caller
        #                 source_type=SourceType.MYSQL,
        #                 file_path=file_path,
        #                 file_name=file_name,
        #                 file_size=file_size,
        #                 file_type=file_type,
        #                 file_url=file_url,
        #                 checksum=row_dict.get('checksum'),
        #                 metadata={
        #                     "database": connection.get("database"),
        #                     "table": table_name,
        #                     "row_id": row_dict.get('id'),
        #                     "created_at": row_dict.get('created_at').isoformat() if row_dict.get('created_at') else None,
        #                     "updated_at": row_dict.get('updated_at').isoformat() if row_dict.get('updated_at') else None
        #                 }
        #             )
        #             
        #             files.append(file_event)
        #     
        #     logger.info("Listed files from MySQL source", 
        #                database=connection.get("database"),
        #                table=table_name,
        #                file_count=len(files))
        #     
        # except Exception as e:
        #     logger.error("Error listing files from MySQL source", error=str(e))
        # 
        # return files
    
    async def _read_file_mysql(self, source_config: SourceConfig, 
                               file_path: str) -> Optional[bytes]:
        """
        Read file from MySQL database source.
        Supports multiple MySQL instances - uses unique connection key to find the right instance.
        
        Future implementation: Will retrieve file content from MySQL (if stored as BLOB) 
        or fetch from file_url if file is stored externally.
        """
        logger.warning("MySQL file reading not implemented yet - future feature")
        return None
        
        # # Future implementation (commented out):
        # try:
        #     # Generate connection key to find the correct MySQL instance
        #     connection_key = self._generate_connection_key(SourceType.MYSQL, source_config)
        #     connection = self._connections.get(connection_key)
        #     
        #     if not connection:
        #         logger.error("MySQL connection not found", 
        #                    connection_key=connection_key,
        #                    host=source_config.database_host,
        #                    database=source_config.database_name)
        #         # Try to reconnect
        #         connected = await self._connect_mysql(source_config)
        #         if not connected:
        #             return None
        #         connection = self._connections.get(connection_key)
        #     
        #     session_factory = connection["session_factory"]
        #     table_name = source_config.database_table or connection.get("table")
        #     
        #     from sqlalchemy import text
        #     
        #     async with session_factory() as session:
        #         # Query for file content
        #         # Option 1: File content stored as BLOB in database
        #         query = f"SELECT file_content FROM {table_name} WHERE file_path = :file_path"
        #         result = await session.execute(text(query), {"file_path": file_path})
        #         row = result.fetchone()
        #         
        #         if row:
        #             file_content = row[0]  # BLOB content
        #             if file_content:
        #                 logger.info("File read from MySQL BLOB", 
        #                           file_path=file_path,
        #                           size=len(file_content))
        #                 return file_content
        #         
        #         # Option 2: File stored externally, fetch from file_url
        #         query = f"SELECT file_url FROM {table_name} WHERE file_path = :file_path"
        #         result = await session.execute(text(query), {"file_path": file_path})
        #         row = result.fetchone()
        #         
        #         if row and row[0]:
        #             file_url = row[0]
        #             # Download from URL (HTTP/S3/etc.)
        #             import aiohttp
        #             async with aiohttp.ClientSession() as http_session:
        #                 async with http_session.get(file_url) as response:
        #                     if response.status == 200:
        #                         file_content = await response.read()
        #                         logger.info("File read from MySQL file_url", 
        #                                   file_path=file_path,
        #                                   file_url=file_url,
        #                                   size=len(file_content))
        #                         return file_content
        #     
        #     logger.warning("File not found in MySQL source", file_path=file_path)
        #     return None
        #     
        # except Exception as e:
        #     logger.error("Error reading file from MySQL source", 
        #                 file_path=file_path,
        #                 error=str(e))
        #     return None
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error("Error calculating checksum", file_path=str(file_path), error=str(e))
            return ""

    # ============================================================================
    # ONEDRIVE SOURCE CONNECTION
    # ============================================================================
    # OneDrive integration: uses Microsoft Graph directly (token refresh + list + download).
    
    async def _connect_onedrive(self, source_config: SourceConfig) -> bool:
        """
        Connect to OneDrive source (Microsoft Graph).
        No persistent connection required; we cache auth/session metadata per org/user.
        """
        try:
            connection_key = self._generate_connection_key(SourceType.ONEDRIVE, source_config)
            self._connections[connection_key] = {
                "config": source_config,
                "connected_at": datetime.utcnow(),
                "source_type": SourceType.ONEDRIVE,
                "access_token": None,
                "access_token_expires_at": None,
            }
            
            logger.info("OneDrive connector initialized", 
                       connection_key=connection_key,
                       graph_endpoint=(source_config.endpoint or "https://graph.microsoft.com/v1.0"))
            return True
            
        except Exception as e:
            logger.error("Error initializing OneDrive connector", 
                        error=str(e))
            return False

    async def _get_onedrive_access_token(self, source_config: SourceConfig) -> str:
        """Get Microsoft Graph access token using OAuth2 refresh token flow."""
        import httpx
        from datetime import timedelta
        from src.utils.secrets_decrypter import decrypt_maybe

        connection_key = self._generate_connection_key(SourceType.ONEDRIVE, source_config)
        conn = self._connections.get(connection_key) or {}

        # Return cached token if still valid
        token = conn.get("access_token")
        expires_at = conn.get("access_token_expires_at")
        if token and expires_at and isinstance(expires_at, datetime):
            if expires_at - datetime.utcnow() > timedelta(seconds=60):
                return token

        # Get OAuth credentials
        creds = source_config.credentials or {}
        oauth = creds.get("oauth") or {}
        tenant_id = creds.get("tenantId")
        client_id = oauth.get("clientId")
        client_secret_raw = oauth.get("clientSecretEncrypted") or oauth.get("clientSecret")
        refresh_token_raw = oauth.get("refreshTokenEncrypted") or oauth.get("refreshToken")
        
        if not all([tenant_id, client_id, client_secret_raw, refresh_token_raw]):
            raise RuntimeError("OneDrive OAuth config missing required fields")
        
        # Decrypt tokens
        try:
            client_secret = decrypt_maybe(client_secret_raw).strip()
            refresh_token = decrypt_maybe(refresh_token_raw).strip()
            if not client_secret or not refresh_token:
                raise ValueError("Decrypted tokens are empty")
        except Exception as e:
            logger.error("Failed to decrypt OneDrive OAuth tokens", error=str(e))
            raise RuntimeError(f"Failed to decrypt OneDrive OAuth tokens: {str(e)}") from e

        # Request new token
        token_endpoint = oauth.get("tokenEndpoint") or f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        scopes = oauth.get("scopes") or []
        scope_str = " ".join(scopes) if isinstance(scopes, list) else str(scopes)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(token_endpoint, data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": scope_str,
            })
            resp.raise_for_status()
            payload = resp.json()

        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in") or 3600)
        if not access_token:
            raise RuntimeError("Failed to obtain access_token from Microsoft")

        # Cache token
        conn["access_token"] = access_token
        conn["access_token_expires_at"] = datetime.utcnow() + timedelta(seconds=expires_in)
        self._connections[connection_key] = conn
        return access_token
    
    async def _resolve_onedrive_folder_id(self, source_config: SourceConfig, 
                                          drive_id: str, folder_name: str) -> Optional[str]:
        """
        Resolve folderId from folderName by querying Microsoft Graph.
        
        Args:
            source_config: Source configuration
            drive_id: OneDrive drive ID
            folder_name: Name of the folder to find
            
        Returns:
            Folder ID if found, None otherwise
        """
        try:
            import httpx
            
            access_token = await self._get_onedrive_access_token(source_config)
            graph_base = (source_config.endpoint or "https://graph.microsoft.com/v1.0").rstrip("/")
            
            # Search for folder by name in the root of the drive
            # First, get root folder children
            root_url = f"{graph_base}/drives/{drive_id}/root/children"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    root_url,
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                response.raise_for_status()
                data = response.json() or {}
                
                items = data.get("value", []) or []
                
                # Look for folder with matching name
                for item in items:
                    if item.get("folder") and item.get("name") == folder_name:
                        folder_id = item.get("id")
                        logger.info(
                            "Resolved folderId from folderName",
                            folder_name=folder_name,
                            folder_id=folder_id,
                            drive_id=drive_id
                        )
                        return folder_id
                
                logger.warning(
                    "Folder not found by name",
                    folder_name=folder_name,
                    drive_id=drive_id,
                    available_folders=[item.get("name") for item in items if item.get("folder")]
                )
                return None
                
        except Exception as e:
            logger.error(
                "Error resolving folderId from folderName",
                folder_name=folder_name,
                drive_id=drive_id,
                error=str(e)
            )
            return None

    async def _list_files_onedrive_non_recursive(self, source_config: SourceConfig,
                                                 drive_id: str, folder_id: str,
                                                 folder_path: str, file_pattern: Optional[str],
                                                 access_token: str, graph_base: str) -> List[FileEvent]:
        """List files directly from OneDrive folder (non-recursive - only direct files, no subfolders)."""
        import httpx
        from src.models.schemas import FileEvent
        from uuid import uuid4
        from pathlib import Path
        import fnmatch
        
        files: List[FileEvent] = []
        list_url = f"{graph_base}/drives/{drive_id}/items/{folder_id}/children"
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(list_url, headers={"Authorization": f"Bearer {access_token}"})
                response.raise_for_status()
                items = (response.json() or {}).get("value", []) or []
            
            for item in items:
                # Skip folders - only process files
                if not item.get("file"):
                    continue
                
                file_name = item.get("name")
                file_ext = Path(file_name).suffix.lower() if file_name else ""
                
                # Only process .pdf and .docx files
                if file_ext not in ['.pdf', '.docx']:
                    continue
                
                # Apply file pattern filter
                if file_pattern and file_pattern != '*' and file_name:
                    if not fnmatch.fnmatch(file_name, file_pattern):
                        continue
                
                files.append(FileEvent(
                    event_id=str(uuid4()),
                    job_id="",
                    source_type=SourceType.ONEDRIVE,
                    file_path=f"onedrive://{drive_id}/{item.get('id')}",
                    file_name=file_name,
                    file_size=item.get("size", 0) or 0,
                    file_type=file_ext,
                    metadata={
                        "drive_id": drive_id,
                        "file_id": item.get("id"),
                        "folder_path": folder_path,
                    },
                ))
            
            pdf_count = sum(1 for f in files if f.file_type == '.pdf')
            docx_count = sum(1 for f in files if f.file_type == '.docx')
            logger.info(
                "Scanned OneDrive folder",
                folder_path=folder_path,
                total_files=len(files),
                pdf_files=pdf_count,
                docx_files=docx_count,
            )
        except Exception as e:
            logger.error("Error listing OneDrive files", folder_path=folder_path, error=str(e))
        
        return files

    async def _list_files_onedrive(self, source_config: SourceConfig, 
                                  file_pattern: Optional[str] = None) -> List[FileEvent]:
        """List files from OneDrive via Microsoft Graph. Supports multiple folders (non-recursive)."""
        try:
            creds = source_config.credentials or {}
            user_email = creds.get("userEmail")
            drive_id = creds.get("driveId")
            resolved_folders = creds.get("resolvedFolders") or []

            if not user_email or not drive_id:
                logger.error("OneDrive config missing userEmail/driveId", user_email=user_email, drive_id=drive_id)
                return []

            # Filter valid folders (must have folderId and folderPath)
            valid_folders = [
                f for f in resolved_folders 
                if f.get("folderId") and f.get("folderPath", "").strip()
            ]
            
            if not valid_folders:
                logger.warning(
                    "Skipping OneDrive job - no valid folders found",
                    user_email=user_email,
                    drive_id=drive_id,
                    resolved_folders_count=len(resolved_folders),
                )
                return []

            access_token = await self._get_onedrive_access_token(source_config)
            graph_base = (source_config.endpoint or "https://graph.microsoft.com/v1.0").rstrip("/")
            all_file_events: List[FileEvent] = []
            
            # Process each folder (non-recursive - only direct files)
            for folder in valid_folders:
                folder_id = folder.get("folderId")
                folder_path = folder.get("folderPath", "").strip()
                
                logger.info("Scanning OneDrive folder", folder_path=folder_path, folder_id=folder_id)
                
                folder_files = await self._list_files_onedrive_non_recursive(
                    source_config=source_config,
                    drive_id=drive_id,
                    folder_id=folder_id,
                    folder_path=folder_path,
                    file_pattern=file_pattern,
                    access_token=access_token,
                    graph_base=graph_base,
                )
                all_file_events.extend(folder_files)
            
            pdf_count = sum(1 for f in all_file_events if f.file_type.lower() == '.pdf')
            docx_count = sum(1 for f in all_file_events if f.file_type.lower() == '.docx')
            
            logger.info(
                "Listed OneDrive files from all folders",
                total_files=len(all_file_events),
                pdf_files=pdf_count,
                docx_files=docx_count,
                user_email=user_email,
                folders_scanned=len(valid_folders),
            )
            return all_file_events
                
        except Exception as e:
            logger.error("Error listing OneDrive files", error=str(e))
            return []

    async def _read_file_onedrive(self, source_config: SourceConfig,
                                  file_path: str, file_event_metadata: Optional[Dict[str, Any]] = None) -> Optional[bytes]:
        """Read file bytes from OneDrive via Microsoft Graph."""
        try:
            import httpx

            # Extract drive_id and item_id from metadata or file_path
            metadata = file_event_metadata or {}
            drive_id = metadata.get("drive_id")
            item_id = metadata.get("file_id")

            if not drive_id or not item_id:
                # Parse from file_path: "onedrive://{driveId}/{itemId}"
                if file_path.startswith("onedrive://"):
                    parts = file_path.replace("onedrive://", "").split("/", 1)
                    if len(parts) == 2:
                        drive_id, item_id = parts[0], parts[1]

            if not drive_id or not item_id:
                logger.error("OneDrive driveId/itemId missing", file_path=file_path)
                return None

            access_token = await self._get_onedrive_access_token(source_config)
            graph_base = (source_config.endpoint or "https://graph.microsoft.com/v1.0").rstrip("/")

            # Get download URL
            async with httpx.AsyncClient(timeout=30.0) as client:
                meta_resp = await client.get(
                    f"{graph_base}/drives/{drive_id}/items/{item_id}",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                meta_resp.raise_for_status()
                download_url = (meta_resp.json() or {}).get("@microsoft.graph.downloadUrl")

            if not download_url:
                logger.error("OneDrive item missing downloadUrl", drive_id=drive_id, item_id=item_id)
                return None

            # Download file
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                dl_resp = await client.get(download_url)
                dl_resp.raise_for_status()
                return dl_resp.content
            
        except Exception as e:
            logger.error("Error reading OneDrive file", file_path=file_path, error=str(e))
            return None


# Global source connector instance
source_connector = SourceConnector()

