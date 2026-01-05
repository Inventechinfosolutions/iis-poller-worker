"""
SQLAlchemy ORM model for file_events table.
Tracks file events published to the file event queue.
"""

import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text, JSON, ForeignKey, Index
from sqlalchemy.orm import relationship

from src.models.base import Base
from src.models.poller_jobs import GUID, PollerJob


class FileEvent(Base):
    """
    ORM model for the file_events table.
    Tracks file events published to the file event queue.
    """
    
    __tablename__ = "file_events"
    
    id = Column(GUID(), primary_key=True, default=uuid.uuid4, nullable=False)
    poller_job_id = Column(GUID(), ForeignKey('poller_jobs.id', ondelete='CASCADE'), nullable=False, index=True)
    object_key = Column(String(500), nullable=False)  # File path/object key
    
    # File metadata
    file_name = Column(String(255), nullable=True)
    file_size = Column(Integer, nullable=True)
    file_type = Column(String(50), nullable=True)
    source_type = Column(String(50), nullable=True)
    file_url = Column(Text, nullable=True)
    event_metadata = Column("metadata", JSON, nullable=True)  # Column name in DB is "metadata", but attribute is "event_metadata"
    
    # Timestamps
    processed_at = Column(DateTime, nullable=True)
    
    # Relationship to poller_job
    poller_job = relationship("PollerJob", backref="file_events")
    
    # Indexes for better query performance
    __table_args__ = (
        Index('idx_poller_job_id', 'poller_job_id'),
        Index('idx_object_key', 'object_key'),
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'}
    )

