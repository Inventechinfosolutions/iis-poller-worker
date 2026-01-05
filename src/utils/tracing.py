"""
Simple tracing for request tracking.
Lightweight, production-ready tracing without external dependencies.
"""

import uuid
import time
from typing import Optional, Dict, Any
from datetime import datetime
from contextlib import contextmanager
import structlog

logger = structlog.get_logger(__name__)


class TraceContext:
    """Simple trace context for request tracking."""
    
    def __init__(self, trace_id: Optional[str] = None, span_id: Optional[str] = None):
        self.trace_id = trace_id or str(uuid.uuid4())
        self.span_id = span_id or str(uuid.uuid4())
        self.start_time = time.time()
        self.metadata: Dict[str, Any] = {}
    
    def add_metadata(self, key: str, value: Any):
        """Add metadata to trace."""
        self.metadata[key] = value
    
    def get_duration(self) -> float:
        """Get trace duration in seconds."""
        return time.time() - self.start_time


class SimpleTracer:
    """
    Simple tracer for request tracking.
    Logs trace information for debugging and monitoring.
    """
    
    @staticmethod
    @contextmanager
    def trace(operation: str, **metadata):
        """
        Trace an operation.
        
        Usage:
            with SimpleTracer.trace("process_job", job_id="123"):
                # do work
        """
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())
        start_time = time.time()
        
        logger.info("Trace started",
                   trace_id=trace_id,
                   span_id=span_id,
                   operation=operation,
                   **metadata)
        
        try:
            yield TraceContext(trace_id=trace_id, span_id=span_id)
        except Exception as e:
            duration = time.time() - start_time
            logger.error("Trace error",
                        trace_id=trace_id,
                        span_id=span_id,
                        operation=operation,
                        error=str(e),
                        duration=duration,
                        **metadata)
            raise
        else:
            duration = time.time() - start_time
            logger.info("Trace completed",
                       trace_id=trace_id,
                       span_id=span_id,
                       operation=operation,
                       duration=duration,
                       **metadata)


# Global tracer instance
tracer = SimpleTracer()

