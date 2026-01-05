"""
Data models and schemas for poller worker service.
"""

from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from enum import Enum


class JobStatus(str, Enum):
    """Job status enumeration."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SourceType(str, Enum):
    """Source type enumeration."""
    DUMMY = "dummy"
    S3 = "s3"
    MINIO = "minio"  # MinIO object storage (future implementation)
    MYSQL = "mysql"  # MySQL database (future implementation)
    FTP = "ftp"
    SFTP = "sftp"
    LOCAL = "local"
    HTTP = "http"


class SourceConfig(BaseModel):
    """Source configuration model."""
    source_type: SourceType = Field(..., description="Type of source")
    endpoint: Optional[str] = Field(None, description="Source endpoint URL")
    access_key: Optional[str] = Field(None, description="Access key for authentication")
    secret_key: Optional[str] = Field(None, description="Secret key for authentication")
    bucket_name: Optional[str] = Field(None, description="Bucket name for S3-like sources")
    path: Optional[str] = Field(None, description="Path to files in source")
    # MySQL database connection fields (future implementation)
    database_host: Optional[str] = Field(None, description="Database host for MySQL")
    database_port: Optional[int] = Field(None, description="Database port for MySQL")
    database_name: Optional[str] = Field(None, description="Database name for MySQL")
    database_user: Optional[str] = Field(None, description="Database user for MySQL")
    database_password: Optional[str] = Field(None, description="Database password for MySQL")
    database_table: Optional[str] = Field(None, description="Database table to query for MySQL")
    credentials: Optional[Dict[str, Any]] = Field(None, description="Additional credentials")
    connection_params: Optional[Dict[str, Any]] = Field(None, description="Additional connection parameters")


class PollingJob(BaseModel):
    """Polling job model."""
    job_id: str = Field(..., description="Unique job identifier")
    org_id: Optional[str] = Field(None, description="Organization identifier")
    connection_list: Optional[List[SourceConfig]] = Field(None, description="List of source connections to poll (MinIO, MySQL, etc.)")
    source_config: Optional[SourceConfig] = Field(None, description="Single source configuration (legacy support)")
    file_pattern: Optional[str] = Field(None, description="File pattern to match")
    priority: str = Field("normal", description="Job priority")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation timestamp")
    scheduled_at: Optional[datetime] = Field(None, description="Scheduled execution time")
    
    @model_validator(mode='after')
    def validate_connections(self):
        """Validate and normalize connection configuration."""
        # If connection_list is not provided, try to use source_config (backward compatibility)
        if not self.connection_list:
            if self.source_config:
                self.connection_list = [self.source_config]
                self.source_config = None  # Clear after conversion
            else:
                raise ValueError("Either 'connection_list' or 'source_config' must be provided")
        
        # If org_id is not provided, generate a default (backward compatibility for old jobs)
        if not self.org_id:
            # For backward compatibility: generate org_id from job_id
            self.org_id = f"org_{self.job_id}"
        
        return self


class FileEvent(BaseModel):
    """File event model for publishing to queue."""
    event_id: str = Field(..., description="Unique event identifier")
    job_id: str = Field(..., description="Associated job identifier")
    source_type: SourceType = Field(..., description="Type of source")
    file_path: str = Field(..., description="Path to the file")
    file_name: str = Field(..., description="Name of the file")
    file_size: int = Field(..., description="Size of the file in bytes")
    file_type: str = Field(..., description="File extension/type")
    file_content: Optional[bytes] = Field(None, description="File content (for small files)")
    file_url: Optional[str] = Field(None, description="URL to access the file")
    checksum: Optional[str] = Field(None, description="File checksum")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional file metadata")
    discovered_at: datetime = Field(default_factory=datetime.utcnow, description="File discovery timestamp")
    processed_at: Optional[datetime] = Field(None, description="File processing timestamp")

