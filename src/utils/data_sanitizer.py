"""
Data sanitization utilities for poller worker service.
Provides comprehensive data cleaning and normalization functions for file metadata and job data.
"""

import re
import string
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)


class DataSanitizer:
    """Comprehensive data sanitization for poller worker data."""
    
    def __init__(self):
        self.string_fields = [
            'job_id', 'file_name', 'file_path', 'source_type', 'file_type',
            'event_id', 'bucket_name', 'object_name', 'endpoint', 'path'
        ]
        
        # Patterns for different data types
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_pattern = re.compile(r'[\+]?[1-9]?[0-9]{7,15}')
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.uuid_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)
    
    def sanitize_job_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize polling job data.
        
        Args:
            data: Raw polling job data
            
        Returns:
            Sanitized job data
        """
        try:
            logger.info("Starting job data sanitization", job_id=data.get('job_id'))
            
            sanitized_data = {}
            
            # Process each field
            for key, value in data.items():
                if key in self.string_fields:
                    sanitized_data[key] = self._sanitize_string_field(key, value)
                elif key == 'source_config':
                    sanitized_data[key] = self._sanitize_source_config(value)
                elif key == 'connection_list':
                    sanitized_data[key] = self._sanitize_connection_list(value)
                elif key == 'metadata':
                    sanitized_data[key] = self._sanitize_metadata(value)
                else:
                    # For unknown fields, apply basic sanitization
                    sanitized_data[key] = self._sanitize_unknown_field(value)
            
            logger.info("Job data sanitization completed", 
                       job_id=sanitized_data.get('job_id'),
                       original_count=len(data),
                       sanitized_count=len(sanitized_data))
            
            return sanitized_data
            
        except Exception as e:
            logger.error("Job data sanitization failed", error=str(e), job_id=data.get('job_id'))
            return data  # Return original data if sanitization fails
    
    def sanitize_file_event_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize file event data.
        
        Args:
            data: Raw file event data
            
        Returns:
            Sanitized file event data
        """
        try:
            logger.info("Starting file event data sanitization", event_id=data.get('event_id'))
            
            sanitized_data = {}
            
            # Process each field
            for key, value in data.items():
                if key in self.string_fields:
                    sanitized_data[key] = self._sanitize_string_field(key, value)
                elif key == 'file_size':
                    sanitized_data[key] = self._sanitize_numeric_field(value)
                elif key == 'metadata':
                    sanitized_data[key] = self._sanitize_metadata(value)
                else:
                    sanitized_data[key] = self._sanitize_unknown_field(value)
            
            logger.info("File event data sanitization completed", 
                       event_id=sanitized_data.get('event_id'),
                       original_count=len(data),
                       sanitized_count=len(sanitized_data))
            
            return sanitized_data
            
        except Exception as e:
            logger.error("File event data sanitization failed", error=str(e), event_id=data.get('event_id'))
            return data  # Return original data if sanitization fails
    
    def _sanitize_string_field(self, field_name: str, value: Any) -> Optional[str]:
        """Sanitize string fields with field-specific rules."""
        if value is None:
            return None
        
        # Convert to string and clean
        str_value = str(value).strip()
        
        if not str_value:
            return None
        
        # Apply field-specific sanitization
        if field_name == 'file_path':
            return self._sanitize_file_path(str_value)
        elif field_name == 'file_name':
            return self._sanitize_file_name(str_value)
        elif field_name in ['object_name', 'document_id']:
            return self._sanitize_object_name(str_value)
        elif field_name == 'endpoint':
            return self._sanitize_endpoint(str_value)
        elif field_name == 'job_id':
            return self._sanitize_job_id(str_value)
        elif field_name == 'event_id':
            return self._sanitize_event_id(str_value)
        else:
            return self._sanitize_general_string(str_value)
    
    def _sanitize_source_config(self, value: Any) -> Dict[str, Any]:
        """Sanitize source configuration."""
        if not value or not isinstance(value, dict):
            return value
        
        sanitized = {}
        for key, val in value.items():
            if isinstance(val, str):
                sanitized[key] = self._sanitize_general_string(val)
            elif isinstance(val, dict):
                sanitized[key] = self._sanitize_metadata(val)
            else:
                sanitized[key] = val
        
        return sanitized
    
    def _sanitize_connection_list(self, value: Any) -> List[Dict[str, Any]]:
        """Sanitize connection list (new format with multiple connections)."""
        if not value or not isinstance(value, list):
            return []
        
        sanitized = []
        for conn in value:
            if isinstance(conn, dict):
                sanitized.append(self._sanitize_source_config(conn))
            else:
                sanitized.append(conn)
        
        return sanitized
    
    def _sanitize_metadata(self, value: Any) -> Dict[str, Any]:
        """Sanitize metadata dictionary."""
        if not value or not isinstance(value, dict):
            return value if value else {}
        
        sanitized = {}
        for key, val in value.items():
            if isinstance(val, str):
                sanitized[key] = self._sanitize_general_string(val)
            elif isinstance(val, (int, float, bool)):
                sanitized[key] = val
            elif isinstance(val, list):
                sanitized[key] = [self._sanitize_unknown_field(item) for item in val]
            elif isinstance(val, dict):
                sanitized[key] = self._sanitize_metadata(val)
            else:
                sanitized[key] = val
        
        return sanitized
    
    def _sanitize_numeric_field(self, value: Any) -> Optional[Union[int, float]]:
        """Sanitize numeric fields."""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return value
            elif isinstance(value, str):
                # Remove non-numeric characters except decimal point
                cleaned = re.sub(r'[^\d.]', '', value)
                if '.' in cleaned:
                    return float(cleaned)
                else:
                    return int(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
        
        return None
    
    def _sanitize_unknown_field(self, value: Any) -> Any:
        """Sanitize unknown field types."""
        if isinstance(value, str):
            return self._sanitize_general_string(value)
        elif isinstance(value, list):
            return [self._sanitize_unknown_field(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._sanitize_unknown_field(v) for k, v in value.items()}
        else:
            return value
    
    def _sanitize_file_path(self, path: str) -> Optional[str]:
        """Sanitize file path."""
        if not path:
            return None
        
        # Clean and normalize
        cleaned = path.strip()
        
        # Remove dangerous path components
        dangerous = ['..', '~', '//']
        for d in dangerous:
            if d in cleaned:
                cleaned = cleaned.replace(d, '')
        
        # Normalize path separators
        cleaned = re.sub(r'[/\\]+', '/', cleaned)
        
        # Remove leading/trailing slashes (except for root paths)
        if cleaned.startswith('/'):
            cleaned = '/' + cleaned.lstrip('/')
        else:
            cleaned = cleaned.strip('/')
        
        return cleaned if cleaned else None
    
    def _sanitize_file_name(self, filename: str) -> Optional[str]:
        """Sanitize file name."""
        if not filename:
            return None
        
        # Clean and normalize
        cleaned = filename.strip()
        
        # Remove path components
        cleaned = cleaned.split('/')[-1].split('\\')[-1]
        
        # Remove dangerous characters
        dangerous_chars = ['..', '<', '>', ':', '"', '|', '?', '*']
        for char in dangerous_chars:
            cleaned = cleaned.replace(char, '')
        
        # Remove control characters
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        return cleaned if cleaned else None
    
    def _sanitize_object_name(self, object_name: str) -> Optional[str]:
        """Sanitize object name (S3/MinIO object key)."""
        if not object_name:
            return None
        
        # Clean and normalize
        cleaned = object_name.strip()
        
        # Preserve UUID format if present
        if self.uuid_pattern.match(cleaned):
            return cleaned
        
        # Remove dangerous characters but preserve path structure
        cleaned = re.sub(r'[<>:"|?*\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        # Normalize path separators
        cleaned = re.sub(r'[/\\]+', '/', cleaned)
        
        return cleaned if cleaned else None
    
    def _sanitize_endpoint(self, endpoint: str) -> Optional[str]:
        """Sanitize endpoint URL."""
        if not endpoint:
            return None
        
        # Clean and normalize
        cleaned = endpoint.strip()
        
        # Remove protocol if present (will be handled by connection logic)
        cleaned = re.sub(r'^https?://', '', cleaned)
        
        # Remove trailing slashes
        cleaned = cleaned.rstrip('/')
        
        # Validate basic format (host:port or host)
        if re.match(r'^[\w\.-]+(:\d+)?$', cleaned):
            return cleaned
        
        return cleaned if cleaned else None
    
    def _sanitize_job_id(self, job_id: str) -> Optional[str]:
        """Sanitize job ID."""
        if not job_id:
            return None
        
        # Clean and normalize
        cleaned = job_id.strip()
        
        # Remove dangerous characters
        cleaned = re.sub(r'[<>:"|?*\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', '_', cleaned)
        
        return cleaned if cleaned else None
    
    def _sanitize_event_id(self, event_id: str) -> Optional[str]:
        """Sanitize event ID."""
        if not event_id:
            return None
        
        # Clean and normalize
        cleaned = event_id.strip()
        
        # Remove dangerous characters
        cleaned = re.sub(r'[<>:"|?*\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', '_', cleaned)
        
        return cleaned if cleaned else None
    
    def _sanitize_general_string(self, text: str) -> Optional[str]:
        """General string sanitization."""
        if not text:
            return None
        
        # Remove extra whitespace (preserve original casing)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # Remove URLs
        text = self.url_pattern.sub('', text)
        
        # Remove extra spaces again
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text if text else None
    
    def validate_sanitized_data(self, data: Dict[str, Any], data_type: str = 'job') -> Dict[str, Any]:
        """Validate sanitized data and return validation results."""
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'field_counts': {}
        }
        
        try:
            if data_type == 'job':
                # Check required fields for job
                required_fields = ['job_id']
                for field in required_fields:
                    if not data.get(field):
                        validation_results['errors'].append(f"Missing required field: {field}")
                        validation_results['is_valid'] = False
                
                # Check for either connection_list (new format) or source_config (legacy)
                connection_list = data.get('connection_list')
                source_config = data.get('source_config')
                
                if connection_list:
                    # Validate connection_list format
                    if not isinstance(connection_list, list) or len(connection_list) == 0:
                        validation_results['errors'].append("Invalid connection_list: must be a non-empty list")
                        validation_results['is_valid'] = False
                    else:
                        # Validate each connection in the list
                        for idx, conn in enumerate(connection_list):
                            if not isinstance(conn, dict):
                                validation_results['errors'].append(f"Invalid connection_list[{idx}]: must be a dict")
                                validation_results['is_valid'] = False
                            elif not conn.get('source_type'):
                                validation_results['errors'].append(f"Invalid connection_list[{idx}]: missing source_type")
                                validation_results['is_valid'] = False
                elif source_config:
                    # Validate legacy source_config format
                    if not isinstance(source_config, dict) or not source_config.get('source_type'):
                        validation_results['errors'].append("Invalid source_config: missing source_type")
                        validation_results['is_valid'] = False
                else:
                    # Neither connection_list nor source_config provided
                    validation_results['errors'].append("Missing required field: connection_list or source_config")
                    validation_results['is_valid'] = False
            
            elif data_type == 'file_event':
                # Check required fields for file event
                required_fields = ['event_id', 'job_id', 'file_name']
                for field in required_fields:
                    if not data.get(field):
                        validation_results['errors'].append(f"Missing required field: {field}")
                        validation_results['is_valid'] = False
            
            # Check field counts
            validation_results['field_counts'] = {
                'total_fields': len(data),
                'string_fields': len([k for k, v in data.items() if k in self.string_fields and v]),
            }
            
            # Check for empty data
            empty_fields = [k for k, v in data.items() if not v]
            if empty_fields:
                validation_results['warnings'].append(f"Empty fields: {empty_fields}")
            
            logger.info("Data validation completed", 
                       data_type=data_type,
                       is_valid=validation_results['is_valid'],
                       error_count=len(validation_results['errors']),
                       warning_count=len(validation_results['warnings']))
            
        except Exception as e:
            logger.error("Data validation failed", error=str(e), data_type=data_type)
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
        
        return validation_results


# Global sanitizer instance
data_sanitizer = DataSanitizer()

