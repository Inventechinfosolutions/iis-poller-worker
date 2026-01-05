"""
API/utility functions to query connection statistics.
Can be used for monitoring, health checks, or reporting.
"""

from typing import Optional, Dict, List, Any
from datetime import datetime
import structlog

from src.models.schemas import SourceConfig
from src.services.file_tracker import file_tracker, ConnectionStatistics

logger = structlog.get_logger(__name__)


async def get_connection_stats_summary(org_id: str) -> Dict[str, Any]:
    """
    Get summary of all connection statistics for an organization.
    
    Args:
        org_id: Organization identifier
        
    Returns:
        Dictionary with summary statistics
    """
    all_stats = await file_tracker.get_all_connection_statistics(org_id)
    
    if not all_stats:
        return {
            "org_id": org_id,
            "total_connections": 0,
            "connections": []
        }
    
    total_success = sum(s.total_success for s in all_stats.values())
    total_failed = sum(s.total_failed for s in all_stats.values())
    total_skipped = sum(s.total_skipped for s in all_stats.values())
    total_processed = sum(s.total_processed for s in all_stats.values())
    
    # Find most recent processing time
    last_processed_times = [s.last_processed_at for s in all_stats.values() if s.last_processed_at]
    most_recent = max(last_processed_times) if last_processed_times else None
    
    connections = []
    for conn_key, stats in all_stats.items():
        connections.append({
            "connection_key": conn_key,
            "source_type": stats.source_type,
            "last_processed_at": stats.last_processed_at.isoformat() if stats.last_processed_at else None,
            "first_processed_at": stats.first_processed_at.isoformat() if stats.first_processed_at else None,
            "total_processed": stats.total_processed,
            "total_success": stats.total_success,
            "total_failed": stats.total_failed,
            "total_skipped": stats.total_skipped,
            "last_job_id": stats.last_job_id
        })
    
    return {
        "org_id": org_id,
        "total_connections": len(all_stats),
        "summary": {
            "total_processed": total_processed,
            "total_success": total_success,
            "total_failed": total_failed,
            "total_skipped": total_skipped,
            "most_recent_processing": most_recent.isoformat() if most_recent else None
        },
        "connections": connections
    }


def get_connection_last_processed(org_id: str, source_config: SourceConfig) -> Optional[datetime]:
    """
    Get the last processed timestamp for a specific connection.
    
    Args:
        org_id: Organization identifier
        source_config: Source configuration
        
    Returns:
        Last processed datetime or None if never processed
    """
    stats = await file_tracker.get_connection_statistics(org_id, source_config)
    return stats.last_processed_at if stats else None


def get_connection_success_rate(org_id: str, source_config: SourceConfig) -> Optional[float]:
    """
    Get success rate for a specific connection.
    
    Args:
        org_id: Organization identifier
        source_config: Source configuration
        
    Returns:
        Success rate (0.0 to 1.0) or None if no processing done
    """
    stats = await file_tracker.get_connection_statistics(org_id, source_config)
    if not stats or stats.total_processed == 0:
        return None
    
    return stats.total_success / stats.total_processed


def list_all_org_connections(org_id: str) -> List[Dict[str, Any]]:
    """
    List all connections for an organization with their statistics.
    
    Args:
        org_id: Organization identifier
        
    Returns:
        List of connection statistics dictionaries
    """
    all_stats = await file_tracker.get_all_connection_statistics(org_id)
    
    result = []
    for conn_key, stats in all_stats.items():
        result.append(stats.to_dict())
    
    return result

