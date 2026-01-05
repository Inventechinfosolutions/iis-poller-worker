"""
Shared utility for generating connection keys.
Follows DRY principle - single source of truth for connection key generation.
"""

import hashlib
from src.models.schemas import SourceType, SourceConfig


def generate_connection_key(org_id: str, source_config: SourceConfig) -> str:
    """
    Generate a unique connection key for a source configuration.
    Simple, reusable function following KISS principle.
    
    Args:
        org_id: Organization identifier
        source_config: Source configuration
        
    Returns:
        Unique connection key string
    """
    source_type = source_config.source_type
    
    # Build key parts based on source type
    if source_type == SourceType.MINIO:
        endpoint = source_config.endpoint or "default"
        access_key = source_config.access_key or "default"
        bucket = source_config.bucket_name or "default"
        key_parts = f"{org_id}:{source_type.value}:{endpoint}:{access_key}:{bucket}"
    
    elif source_type == SourceType.MYSQL:
        host = source_config.database_host or "default"
        port = source_config.database_port or 0
        database = source_config.database_name or "default"
        key_parts = f"{org_id}:{source_type.value}:{host}:{port}:{database}"
    
    elif source_type == SourceType.S3:
        endpoint = source_config.endpoint or "default"
        access_key = source_config.access_key or "default"
        bucket = source_config.bucket_name or "default"
        key_parts = f"{org_id}:{source_type.value}:{endpoint}:{access_key}:{bucket}"
    
    else:
        # For other sources: use endpoint and path
        endpoint = source_config.endpoint or "default"
        path = source_config.path or "default"
        key_parts = f"{org_id}:{source_type.value}:{endpoint}:{path}"
    
    # Generate hash for unique identification
    return hashlib.md5(key_parts.encode()).hexdigest()

