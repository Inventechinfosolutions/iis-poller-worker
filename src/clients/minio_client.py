"""
MinIO/S3 client for connecting to object storage sources.
Handles file listing and reading from S3-compatible storage.
"""

import asyncio
from typing import Optional, List, Dict, Any
from minio import Minio
from minio.error import S3Error
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.settings import settings

logger = structlog.get_logger(__name__)


class MinIOClient:
    """
    MinIO/S3 client for object storage operations.
    Used by source connector for S3 source type.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Use provided config or fall back to settings
        if config:
            endpoint = config.get('minio_endpoint') or config.get('endpoint') or settings.minio_endpoint
            access_key = config.get('minio_access_key') or config.get('access_key') or settings.minio_access_key
            secret_key = config.get('minio_secret_key') or config.get('secret_key') or settings.minio_secret_key
            secure = config.get('minio_secure', settings.minio_secure)
        else:
            endpoint = settings.minio_endpoint
            access_key = settings.minio_access_key
            secret_key = settings.minio_secret_key
            secure = settings.minio_secure
        
        # Clean endpoint URL
        if endpoint and endpoint.startswith(('http://', 'https://')):
            secure = endpoint.startswith('https://')
            endpoint = endpoint.split('://', 1)[1]
        
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        
        if endpoint and access_key and secret_key:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
        else:
            self.client = None
            logger.warning("MinIO client not initialized - missing configuration")
    
    async def test_connection(self) -> bool:
        """Test MinIO/S3 connection and credentials."""
        if not self.client:
            return False
        
        try:
            buckets = await asyncio.to_thread(self.client.list_buckets)
            logger.info("MinIO connection test successful", 
                       endpoint=self.endpoint, 
                       buckets_count=len(buckets))
            return True
        except Exception as e:
            logger.error("MinIO connection test failed", 
                        endpoint=self.endpoint, 
                        error=str(e))
            return False
    
    async def list_objects(self, bucket_name: str, prefix: str = "", 
                          recursive: bool = True) -> List[Dict[str, Any]]:
        """
        List objects in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Object name prefix (for filtering)
            recursive: Whether to list recursively
            
        Returns:
            List of object information dictionaries
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return []
        
        try:
            objects = []
            for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive):
                objects.append({
                    'object_name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'etag': obj.etag
                })
            
            logger.info("Listed objects from MinIO", 
                       bucket_name=bucket_name,
                       prefix=prefix,
                       count=len(objects))
            return objects
            
        except Exception as e:
            logger.error("Failed to list objects", 
                        bucket_name=bucket_name,
                        prefix=prefix,
                        error=str(e))
            return []
    
    async def get_object_bytes(self, bucket_name: str, object_name: str) -> Optional[bytes]:
        """
        Get object bytes from MinIO/S3.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object
            
        Returns:
            Object bytes or None if failed
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return None
        
        try:
            from io import BytesIO
            
            response = self.client.get_object(bucket_name, object_name)
            file_buffer = BytesIO()
            
            try:
                chunk_size = 8192  # 8KB chunks
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    file_buffer.write(chunk)
                
                file_bytes = file_buffer.getvalue()
                
                logger.info("Object retrieved from MinIO", 
                           bucket_name=bucket_name,
                           object_name=object_name,
                           size=len(file_bytes))
                
                return file_bytes
                
            finally:
                response.close()
                response.release_conn()
                file_buffer.close()
                
        except Exception as e:
            logger.error("Failed to get object from MinIO", 
                        bucket_name=bucket_name,
                        object_name=object_name,
                        error=str(e))
            return None
    
    async def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists."""
        if not self.client:
            return False
        
        try:
            return await asyncio.to_thread(self.client.bucket_exists, bucket_name)
        except Exception as e:
            logger.error("Error checking bucket existence", 
                        bucket_name=bucket_name, 
                        error=str(e))
            return False

    async def ensure_bucket(self, bucket_name: str) -> bool:
        """Ensure bucket exists; create if missing."""
        if not self.client:
            logger.error("MinIO client not initialized")
            return False
        try:
            exists = await self.bucket_exists(bucket_name)
            if exists:
                return True
            await asyncio.to_thread(self.client.make_bucket, bucket_name)
            logger.info("Created bucket in MinIO", bucket_name=bucket_name)
            return True
        except Exception as e:
            logger.error("Failed to ensure bucket", bucket_name=bucket_name, error=str(e))
            return False

    async def put_object_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Upload bytes to MinIO/S3 with optional metadata.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object (object key)
            data: File bytes to upload
            content_type: MIME type of the file
            metadata: Optional metadata dictionary to store with the object
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return False

        try:
            from io import BytesIO

            if not await self.ensure_bucket(bucket_name):
                return False

            bio = BytesIO(data)
            
            # Convert metadata dict to MinIO metadata format
            # MinIO's put_object metadata parameter expects keys WITHOUT X-Amz-Meta- prefix
            # It automatically adds X-Amz-Meta- prefix to each key
            # To get double prefix (X-Amz-Meta-X-Amz-Meta-), we pass keys with X-Amz-Meta- prefix
            minio_metadata = {}
            if metadata:
                for key, value in metadata.items():
                    if value is not None:
                        # If key already has X-Amz-Meta- prefix, keep it (MinIO will add another, creating double prefix)
                        # If key doesn't have prefix, add it so MinIO can add another
                        if key.startswith("X-Amz-Meta-"):
                            # Key already has prefix, MinIO will add another one
                            meta_key = key
                        else:
                            # Add prefix so MinIO can add another one for double prefix
                            meta_key = f"X-Amz-Meta-{key}"
                        minio_metadata[meta_key] = str(value)
            
            await asyncio.to_thread(
                self.client.put_object,
                bucket_name,
                object_name,
                bio,
                length=len(data),
                content_type=content_type,
                metadata=minio_metadata if minio_metadata else None,
            )
            logger.info(
                "Object uploaded to MinIO",
                bucket_name=bucket_name,
                object_name=object_name,
                size=len(data),
                has_metadata=bool(metadata),
                metadata_keys=list(metadata.keys()) if metadata else [],
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to upload object to MinIO",
                bucket_name=bucket_name,
                object_name=object_name,
                error=str(e),
            )
            return False


# Global MinIO client instance (will be initialized with config when needed)
minio_client = None

