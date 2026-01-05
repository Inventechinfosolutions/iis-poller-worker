"""
Secrets management utility for sanitizing sensitive data.
Prevents credentials from appearing in logs or DLQ messages.
"""

from typing import Any, Dict, List
import re


class SecretsManager:
    """Manages and sanitizes sensitive data."""
    
    # Fields that should be sanitized
    SENSITIVE_FIELDS = {
        'password', 'secret', 'secret_key', 'access_key', 'api_key',
        'token', 'credential', 'credentials', 'auth', 'authorization',
        'database_password', 'minio_secret_key', 'minio_access_key'
    }
    
    # Patterns to detect sensitive data
    SENSITIVE_PATTERNS = [
        re.compile(r'password["\']?\s*[:=]\s*["\']?([^"\',}\s]+)', re.IGNORECASE),
        re.compile(r'secret["\']?\s*[:=]\s*["\']?([^"\',}\s]+)', re.IGNORECASE),
        re.compile(r'token["\']?\s*[:=]\s*["\']?([^"\',}\s]+)', re.IGNORECASE),
    ]
    
    @staticmethod
    def sanitize_dict(data: Dict[str, Any], depth: int = 0, max_depth: int = 10) -> Dict[str, Any]:
        """
        Recursively sanitize dictionary, masking sensitive fields.
        
        Args:
            data: Dictionary to sanitize
            depth: Current recursion depth
            max_depth: Maximum recursion depth
            
        Returns:
            Sanitized dictionary
        """
        if depth > max_depth:
            return data
        
        if not isinstance(data, dict):
            return data
        
        sanitized = {}
        for key, value in data.items():
            key_lower = key.lower()
            
            # Check if key indicates sensitive data
            if any(sensitive in key_lower for sensitive in SecretsManager.SENSITIVE_FIELDS):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, dict):
                sanitized[key] = SecretsManager.sanitize_dict(value, depth + 1, max_depth)
            elif isinstance(value, list):
                sanitized[key] = SecretsManager.sanitize_list(value, depth + 1, max_depth)
            else:
                sanitized[key] = value
        
        return sanitized
    
    @staticmethod
    def sanitize_list(data: List[Any], depth: int = 0, max_depth: int = 10) -> List[Any]:
        """Recursively sanitize list."""
        if depth > max_depth:
            return data
        
        sanitized = []
        for item in data:
            if isinstance(item, dict):
                sanitized.append(SecretsManager.sanitize_dict(item, depth + 1, max_depth))
            elif isinstance(item, list):
                sanitized.append(SecretsManager.sanitize_list(item, depth + 1, max_depth))
            else:
                sanitized.append(item)
        
        return sanitized
    
    @staticmethod
    def sanitize_string(text: str) -> str:
        """Sanitize string by masking sensitive patterns."""
        if not isinstance(text, str):
            return text
        
        sanitized = text
        for pattern in SecretsManager.SENSITIVE_PATTERNS:
            sanitized = pattern.sub(r'\1***REDACTED***', sanitized)
        
        return sanitized
    
    @staticmethod
    def sanitize_for_logging(data: Any) -> Any:
        """Sanitize data for safe logging."""
        if isinstance(data, dict):
            return SecretsManager.sanitize_dict(data)
        elif isinstance(data, list):
            return SecretsManager.sanitize_list(data)
        elif isinstance(data, str):
            return SecretsManager.sanitize_string(data)
        else:
            return data

