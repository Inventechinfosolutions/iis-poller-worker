"""
Data models and schemas for poller worker service.
"""

import os
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
    ONEDRIVE = "onedrive"  # Microsoft OneDrive integration


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


class MinIOConfiguration(BaseModel):
    """MinIO configuration model (same format as batch-details)."""
    type: str = Field(..., description="Configuration type")
    endpoint: str = Field(..., description="MinIO endpoint")
    accessKey: str = Field(..., description="MinIO access key")
    secretKey: str = Field(..., description="MinIO secret key")
    bucketName: str = Field(..., description="MinIO bucket name")


class MySQLConfiguration(BaseModel):
    """MySQL configuration model (same format as batch-details)."""
    type: str = Field(..., description="Configuration type")
    host: str = Field(..., description="MySQL host")
    port: int = Field(..., description="MySQL port")
    username: str = Field(..., description="MySQL username")
    password: str = Field(..., description="MySQL password")
    database: str = Field(..., description="MySQL database name")
    tableName: str = Field(..., description="MySQL table name")
    columns: Optional[Dict[str, Any]] = Field(None, description="Table column definitions")


class PollingJob(BaseModel):
    """Polling job model."""
    job_id: str = Field(..., description="Unique job identifier")
    org_id: Optional[str] = Field(None, description="Organization identifier")
    connection_list: Optional[List[SourceConfig]] = Field(None, description="List of source connections to poll (MinIO, MySQL, etc.)")
    source_config: Optional[SourceConfig] = Field(None, description="Single source configuration (legacy support)")
    configurationType: Optional[MinIOConfiguration] = Field(None, description="MinIO configuration (same format as batch-details)")
    mysqlConfiguration: Optional[MySQLConfiguration] = Field(None, description="MySQL configuration (same format as batch-details)")
    file_pattern: Optional[str] = Field(None, description="File pattern to match")
    priority: str = Field("normal", description="Job priority")
    batch_size: Optional[int] = Field(None, description="Batch size for processing files (overrides default)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    api_keys: Optional[List[str]] = Field(None, description="List of API keys to use for this job")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation timestamp")
    scheduled_at: Optional[datetime] = Field(None, description="Scheduled execution time")
    
    @model_validator(mode='after')
    def validate_connections(self):
        """Validate and normalize connection configuration."""
        # If connection_list is already provided, use it
        if self.connection_list:
            # If org_id is not provided, generate a default (backward compatibility for old jobs)
            if not self.org_id:
                self.org_id = f"org_{self.job_id}"
            return self
        
        # Try to build connection_list from configurationType, mysqlConfiguration, or metadata.onedrive_config
        if self.configurationType or self.mysqlConfiguration or (self.metadata and self.metadata.get("onedrive_config")):
            self.connection_list = []
            
            # Convert MinIO configuration to SourceConfig
            if self.configurationType:
                minio_config = SourceConfig(
                    source_type=SourceType.MINIO,
                    endpoint=self.configurationType.endpoint,
                    access_key=self.configurationType.accessKey,
                    secret_key=self.configurationType.secretKey,
                    bucket_name=self.configurationType.bucketName,
                    path="",  # Default path, can be overridden if needed
                )
                self.connection_list.append(minio_config)
            
            # Convert MySQL configuration to SourceConfig
            if self.mysqlConfiguration:
                mysql_config = SourceConfig(
                    source_type=SourceType.MYSQL,
                    database_host=self.mysqlConfiguration.host,
                    database_port=self.mysqlConfiguration.port,
                    database_name=self.mysqlConfiguration.database,
                    database_user=self.mysqlConfiguration.username,
                    database_password=self.mysqlConfiguration.password,
                    database_table=self.mysqlConfiguration.tableName,
                )
                self.connection_list.append(mysql_config)
            
            # Convert OneDrive configuration from metadata to SourceConfig
            if self.metadata and self.metadata.get("onedrive_config"):
                config = self.metadata["onedrive_config"]
                self.connection_list.append(SourceConfig(
                    source_type=SourceType.ONEDRIVE,
                    endpoint="https://graph.microsoft.com/v1.0",
                    credentials={
                        "userEmail": config.get("userEmail"),
                        "tenantId": config.get("tenantId"),
                        "driveId": config.get("driveId"),
                        "selectedFolders": config.get("selectedFolders") or [],
                        "resolvedFolders": config.get("resolvedFolders") or [],
                        "oauth": config.get("oauth") or {},
                        "orgId": self.org_id,
                    },
                ))
        
        # If connection_list is still not set, try to use source_config (backward compatibility)
        if not self.connection_list:
            if self.source_config:
                self.connection_list = [self.source_config]
                self.source_config = None  # Clear after conversion
            else:
                raise ValueError("Either 'connection_list', 'source_config', or 'configurationType'/'mysqlConfiguration' must be provided")
        
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

