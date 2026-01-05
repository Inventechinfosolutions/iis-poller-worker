"""
SQLAlchemy ORM model for poller_jobs table.
Tracks polling jobs with status, metrics, and error handling.
"""

import uuid
from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import Column, String, Integer, DateTime, Text, JSON, Enum as SAEnum, Index
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.types import TypeDecorator
from sqlalchemy.sql import func

from src.models.base import Base


class GUID(TypeDecorator):
    """
    Platform-independent GUID type for MySQL.
    Stores UUID as CHAR(36) in MySQL database.
    """
    impl = CHAR
    cache_ok = True
    length = 36

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if isinstance(value, uuid.UUID):
            return str(value)
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if not isinstance(value, uuid.UUID):
            return uuid.UUID(value)
        return value


class JobPriority(PyEnum):
    """Job priority enumeration."""
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


class JobStatus(PyEnum):
    """Job status enumeration."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class PollerJob(Base):
    """
    ORM model for the poller_jobs table.
    Tracks polling jobs with status, metrics, and error handling.
    """
    
    __tablename__ = "poller_jobs"
    
    id = Column(GUID(), primary_key=True, default=uuid.uuid4, nullable=False)
    job_id = Column(String(36), nullable=False)
    org_id = Column(String(36), nullable=False)
    
    connection_list = Column(JSON, nullable=False)
    file_pattern = Column(String(255), nullable=True)
    priority = Column(SAEnum(JobPriority, name="job_priority"), nullable=False, default=JobPriority.NORMAL)
    job_metadata = Column("metadata", JSON, nullable=True)  # Column name in DB is "metadata", but attribute is "job_metadata"
    
    job_status = Column(SAEnum(JobStatus, name="job_status"), nullable=False, default=JobStatus.PENDING)
    
    processed_file_count = Column(Integer, nullable=False, default=0)
    retries_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    
    # Indexes for better query performance
    __table_args__ = (
        Index('idx_job_id', 'job_id'),  # For fast lookups by job_id
        Index('idx_org_id', 'org_id'),
        Index('idx_job_status', 'job_status'),
        Index('idx_created_at', 'created_at'),
        Index('idx_org_status', 'org_id', 'job_status'),
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}
    )

