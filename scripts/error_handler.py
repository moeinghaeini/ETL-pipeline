#!/usr/bin/env python3
"""
Comprehensive Error Handling and Logging System
Provides centralized error handling, logging, and monitoring capabilities
"""

import os
import sys
import json
import logging
import traceback
import functools
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import queue
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/error_handler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Error categories"""
    SYSTEM = "system"
    DATA_QUALITY = "data_quality"
    NETWORK = "network"
    DATABASE = "database"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONFIGURATION = "configuration"
    BUSINESS_LOGIC = "business_logic"
    EXTERNAL_SERVICE = "external_service"
    PERFORMANCE = "performance"

@dataclass
class ErrorContext:
    """Context information for errors"""
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    input_data: Optional[Dict[str, Any]] = None
    environment: Optional[str] = None
    version: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class ErrorRecord:
    """Comprehensive error record"""
    id: str
    timestamp: datetime
    severity: ErrorSeverity
    category: ErrorCategory
    error_type: str
    message: str
    stack_trace: str
    context: ErrorContext
    resolution: Optional[str] = None
    resolved_at: Optional[datetime] = None
    assigned_to: Optional[str] = None
    tags: List[str] = None
    metadata: Dict[str, Any] = None

class ErrorHandler:
    """Comprehensive error handling and logging system"""
    
    def __init__(self, config_file: str = "config/error_handling.yml"):
        self.config_file = config_file
        self.config = self._load_config()
        self.error_queue = queue.Queue()
        self.error_records: Dict[str, ErrorRecord] = {}
        self.error_patterns: Dict[str, Dict[str, Any]] = {}
        self.alert_thresholds = self.config.get('alert_thresholds', {})
        self.retry_configs = self.config.get('retry_configs', {})
        
        # Initialize error processing thread
        self.processing_thread = threading.Thread(target=self._process_errors, daemon=True)
        self.processing_thread.start()
        
        # Load existing error patterns
        self._load_error_patterns()
        
        logger.info("Error handler initialized successfully")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load error handling configuration"""
        default_config = {
            'log_level': 'INFO',
            'max_error_records': 10000,
            'error_retention_days': 30,
            'alert_thresholds': {
                'critical_errors_per_hour': 5,
                'high_errors_per_hour': 20,
                'medium_errors_per_hour': 50
            },
            'retry_configs': {
                'max_retries': 3,
                'base_delay': 1,
                'max_delay': 60,
                'exponential_backoff': True
            },
            'notification_channels': ['email', 'slack'],
            'error_categories': {
                'system': {'severity': 'high', 'auto_retry': False},
                'data_quality': {'severity': 'medium', 'auto_retry': True},
                'network': {'severity': 'medium', 'auto_retry': True},
                'database': {'severity': 'high', 'auto_retry': True},
                'authentication': {'severity': 'high', 'auto_retry': False},
                'authorization': {'severity': 'high', 'auto_retry': False},
                'configuration': {'severity': 'medium', 'auto_retry': False},
                'business_logic': {'severity': 'medium', 'auto_retry': False},
                'external_service': {'severity': 'medium', 'auto_retry': True},
                'performance': {'severity': 'low', 'auto_retry': False}
            }
        }
        
        try:
            if os.path.exists(self.config_file):
                import yaml
                with open(self.config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    # Merge with defaults
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            else:
                # Create default config file
                os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
                import yaml
                with open(self.config_file, 'w') as f:
                    yaml.dump(default_config, f, default_flow_style=False)
                return default_config
                
        except Exception as e:
            logger.error(f"Failed to load error handling config: {e}")
            return default_config
    
    def _load_error_patterns(self):
        """Load error patterns for analysis"""
        patterns_file = "data/error_patterns.json"
        try:
            if os.path.exists(patterns_file):
                with open(patterns_file, 'r') as f:
                    self.error_patterns = json.load(f)
            else:
                self.error_patterns = {}
        except Exception as e:
            logger.error(f"Failed to load error patterns: {e}")
            self.error_patterns = {}
    
    def _save_error_patterns(self):
        """Save error patterns"""
        patterns_file = "data/error_patterns.json"
        try:
            os.makedirs(os.path.dirname(patterns_file), exist_ok=True)
            with open(patterns_file, 'w') as f:
                json.dump(self.error_patterns, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save error patterns: {e}")
    
    def _process_errors(self):
        """Process errors from the queue"""
        while True:
            try:
                error_record = self.error_queue.get(timeout=1)
                self._handle_error_record(error_record)
                self.error_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing error record: {e}")
    
    def _handle_error_record(self, error_record: ErrorRecord):
        """Handle a single error record"""
        try:
            # Store error record
            self.error_records[error_record.id] = error_record
            
            # Update error patterns
            self._update_error_patterns(error_record)
            
            # Check alert thresholds
            self._check_alert_thresholds(error_record)
            
            # Log error
            self._log_error(error_record)
            
            # Send notifications if needed
            if self._should_notify(error_record):
                self._send_notifications(error_record)
            
            # Clean up old records
            self._cleanup_old_records()
            
        except Exception as e:
            logger.error(f"Failed to handle error record: {e}")
    
    def _update_error_patterns(self, error_record: ErrorRecord):
        """Update error patterns for analysis"""
        pattern_key = f"{error_record.category.value}_{error_record.error_type}"
        
        if pattern_key not in self.error_patterns:
            self.error_patterns[pattern_key] = {
                'count': 0,
                'first_occurrence': error_record.timestamp.isoformat(),
                'last_occurrence': error_record.timestamp.isoformat(),
                'severity': error_record.severity.value,
                'category': error_record.category.value,
                'error_type': error_record.error_type,
                'common_contexts': []
            }
        
        pattern = self.error_patterns[pattern_key]
        pattern['count'] += 1
        pattern['last_occurrence'] = error_record.timestamp.isoformat()
        
        # Track common contexts
        context_key = f"{error_record.context.component}_{error_record.context.operation}"
        context_found = False
        for ctx in pattern['common_contexts']:
            if ctx['context'] == context_key:
                ctx['count'] += 1
                context_found = True
                break
        
        if not context_found:
            pattern['common_contexts'].append({
                'context': context_key,
                'count': 1
            })
        
        # Save patterns periodically
        if pattern['count'] % 10 == 0:
            self._save_error_patterns()
    
    def _check_alert_thresholds(self, error_record: ErrorRecord):
        """Check if error thresholds are exceeded"""
        try:
            # Count errors in the last hour
            cutoff_time = datetime.now() - timedelta(hours=1)
            recent_errors = [
                record for record in self.error_records.values()
                if record.timestamp > cutoff_time
            ]
            
            # Count by severity
            severity_counts = {}
            for record in recent_errors:
                severity = record.severity.value
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Check thresholds
            for severity, threshold_key in [
                ('critical', 'critical_errors_per_hour'),
                ('high', 'high_errors_per_hour'),
                ('medium', 'medium_errors_per_hour')
            ]:
                if severity in severity_counts:
                    threshold = self.alert_thresholds.get(threshold_key, float('inf'))
                    if severity_counts[severity] >= threshold:
                        self._trigger_threshold_alert(severity, severity_counts[severity], threshold)
                        
        except Exception as e:
            logger.error(f"Failed to check alert thresholds: {e}")
    
    def _trigger_threshold_alert(self, severity: str, count: int, threshold: int):
        """Trigger threshold alert"""
        alert_message = f"Error threshold exceeded: {count} {severity} errors in the last hour (threshold: {threshold})"
        logger.warning(alert_message)
        
        # Send alert notification
        self._send_alert_notification(severity, alert_message, count, threshold)
    
    def _should_notify(self, error_record: ErrorRecord) -> bool:
        """Determine if error should trigger notifications"""
        # Notify for high and critical errors
        if error_record.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            return True
        
        # Notify for repeated errors
        pattern_key = f"{error_record.category.value}_{error_record.error_type}"
        if pattern_key in self.error_patterns:
            pattern = self.error_patterns[pattern_key]
            if pattern['count'] > 5:  # More than 5 occurrences
                return True
        
        return False
    
    def _log_error(self, error_record: ErrorRecord):
        """Log error with appropriate level"""
        log_message = f"[{error_record.id}] {error_record.message}"
        
        if error_record.severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message)
        elif error_record.severity == ErrorSeverity.HIGH:
            logger.error(log_message)
        elif error_record.severity == ErrorSeverity.MEDIUM:
            logger.warning(log_message)
        else:
            logger.info(log_message)
    
    def _send_notifications(self, error_record: ErrorRecord):
        """Send error notifications"""
        try:
            # Import notification modules
            from send_alerts import AlertManager, Alert, AlertType, AlertSeverity
            
            alert_manager = AlertManager()
            
            # Map error severity to alert severity
            severity_mapping = {
                ErrorSeverity.CRITICAL: AlertSeverity.CRITICAL,
                ErrorSeverity.HIGH: AlertSeverity.HIGH,
                ErrorSeverity.MEDIUM: AlertSeverity.MEDIUM,
                ErrorSeverity.LOW: AlertSeverity.LOW
            }
            
            alert = Alert(
                type=AlertType.SYSTEM,
                severity=severity_mapping.get(error_record.severity, AlertSeverity.MEDIUM),
                title=f"Error: {error_record.error_type}",
                message=error_record.message,
                timestamp=error_record.timestamp,
                metadata={
                    'error_id': error_record.id,
                    'category': error_record.category.value,
                    'component': error_record.context.component,
                    'operation': error_record.context.operation,
                    'stack_trace': error_record.stack_trace[:500]  # Truncate for notifications
                }
            )
            
            alert_manager.send_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to send error notifications: {e}")
    
    def _send_alert_notification(self, severity: str, message: str, count: int, threshold: int):
        """Send threshold alert notification"""
        try:
            from send_alerts import AlertManager, Alert, AlertType, AlertSeverity
            
            alert_manager = AlertManager()
            
            alert = Alert(
                type=AlertType.SYSTEM,
                severity=AlertSeverity.HIGH,
                title=f"Error Threshold Alert - {severity.upper()}",
                message=message,
                timestamp=datetime.now(),
                metadata={
                    'severity': severity,
                    'count': count,
                    'threshold': threshold,
                    'alert_type': 'threshold_exceeded'
                }
            )
            
            alert_manager.send_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to send threshold alert: {e}")
    
    def _cleanup_old_records(self):
        """Clean up old error records"""
        try:
            retention_days = self.config.get('error_retention_days', 30)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            old_records = [
                record_id for record_id, record in self.error_records.items()
                if record.timestamp < cutoff_date
            ]
            
            for record_id in old_records:
                del self.error_records[record_id]
            
            if old_records:
                logger.info(f"Cleaned up {len(old_records)} old error records")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old records: {e}")
    
    def handle_error(self, error: Exception, context: ErrorContext = None, 
                    severity: ErrorSeverity = None, category: ErrorCategory = None) -> str:
        """Handle an error and return error ID"""
        try:
            # Determine severity and category if not provided
            if severity is None:
                severity = self._determine_severity(error)
            
            if category is None:
                category = self._determine_category(error)
            
            # Create error record
            error_id = f"ERR_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{id(error)}"
            
            error_record = ErrorRecord(
                id=error_id,
                timestamp=datetime.now(),
                severity=severity,
                category=category,
                error_type=type(error).__name__,
                message=str(error),
                stack_trace=traceback.format_exc(),
                context=context or ErrorContext(),
                tags=[],
                metadata={}
            )
            
            # Add to processing queue
            self.error_queue.put(error_record)
            
            return error_id
            
        except Exception as e:
            logger.error(f"Failed to handle error: {e}")
            return f"ERROR_HANDLING_FAILED_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _determine_severity(self, error: Exception) -> ErrorSeverity:
        """Determine error severity based on error type"""
        error_type = type(error).__name__
        
        critical_errors = ['SystemExit', 'KeyboardInterrupt', 'MemoryError']
        high_errors = ['ConnectionError', 'TimeoutError', 'PermissionError', 'FileNotFoundError']
        medium_errors = ['ValueError', 'TypeError', 'KeyError', 'AttributeError']
        
        if error_type in critical_errors:
            return ErrorSeverity.CRITICAL
        elif error_type in high_errors:
            return ErrorSeverity.HIGH
        elif error_type in medium_errors:
            return ErrorSeverity.MEDIUM
        else:
            return ErrorSeverity.LOW
    
    def _determine_category(self, error: Exception) -> ErrorCategory:
        """Determine error category based on error type"""
        error_type = type(error).__name__
        error_message = str(error).lower()
        
        if 'connection' in error_message or 'network' in error_message:
            return ErrorCategory.NETWORK
        elif 'database' in error_message or 'sql' in error_message:
            return ErrorCategory.DATABASE
        elif 'auth' in error_message or 'login' in error_message:
            return ErrorCategory.AUTHENTICATION
        elif 'permission' in error_message or 'access' in error_message:
            return ErrorCategory.AUTHORIZATION
        elif 'config' in error_message or 'setting' in error_message:
            return ErrorCategory.CONFIGURATION
        elif 'data' in error_message or 'quality' in error_message:
            return ErrorCategory.DATA_QUALITY
        elif 'performance' in error_message or 'timeout' in error_message:
            return ErrorCategory.PERFORMANCE
        elif 'external' in error_message or 'api' in error_message:
            return ErrorCategory.EXTERNAL_SERVICE
        else:
            return ErrorCategory.SYSTEM
    
    def retry_on_error(self, max_retries: int = None, delay: float = None, 
                      backoff: bool = None, exceptions: tuple = None):
        """Decorator for retrying functions on error"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                retry_config = self.retry_configs.copy()
                if max_retries is not None:
                    retry_config['max_retries'] = max_retries
                if delay is not None:
                    retry_config['base_delay'] = delay
                if backoff is not None:
                    retry_config['exponential_backoff'] = backoff
                
                retry_exceptions = exceptions or (Exception,)
                last_error = None
                
                for attempt in range(retry_config['max_retries'] + 1):
                    try:
                        return func(*args, **kwargs)
                    except retry_exceptions as e:
                        last_error = e
                        
                        if attempt == retry_config['max_retries']:
                            # Final attempt failed
                            context = ErrorContext(
                                component=func.__module__,
                                operation=func.__name__,
                                metadata={'attempts': attempt + 1, 'max_retries': retry_config['max_retries']}
                            )
                            error_id = self.handle_error(e, context, ErrorSeverity.HIGH, ErrorCategory.SYSTEM)
                            raise e
                        
                        # Calculate delay
                        if retry_config['exponential_backoff']:
                            delay_time = retry_config['base_delay'] * (2 ** attempt)
                        else:
                            delay_time = retry_config['base_delay']
                        
                        delay_time = min(delay_time, retry_config['max_delay'])
                        
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay_time}s...")
                        time.sleep(delay_time)
                
                # This should never be reached
                raise last_error
            
            return wrapper
        return decorator
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error statistics"""
        try:
            now = datetime.now()
            last_hour = now - timedelta(hours=1)
            last_day = now - timedelta(days=1)
            last_week = now - timedelta(weeks=1)
            
            # Filter errors by time period
            recent_errors = [r for r in self.error_records.values() if r.timestamp > last_hour]
            daily_errors = [r for r in self.error_records.values() if r.timestamp > last_day]
            weekly_errors = [r for r in self.error_records.values() if r.timestamp > last_week]
            
            # Calculate statistics
            stats = {
                'timestamp': now.isoformat(),
                'total_errors': len(self.error_records),
                'recent_errors': {
                    'last_hour': len(recent_errors),
                    'last_day': len(daily_errors),
                    'last_week': len(weekly_errors)
                },
                'errors_by_severity': {},
                'errors_by_category': {},
                'top_error_patterns': [],
                'error_trends': {}
            }
            
            # Count by severity
            for severity in ErrorSeverity:
                count = len([r for r in self.error_records.values() if r.severity == severity])
                stats['errors_by_severity'][severity.value] = count
            
            # Count by category
            for category in ErrorCategory:
                count = len([r for r in self.error_records.values() if r.category == category])
                stats['errors_by_category'][category.value] = count
            
            # Top error patterns
            sorted_patterns = sorted(
                self.error_patterns.items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )
            stats['top_error_patterns'] = [
                {
                    'pattern': pattern,
                    'count': data['count'],
                    'severity': data['severity'],
                    'category': data['category']
                }
                for pattern, data in sorted_patterns[:10]
            ]
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get error statistics: {e}")
            return {"error": str(e)}
    
    def resolve_error(self, error_id: str, resolution: str, resolved_by: str) -> bool:
        """Mark an error as resolved"""
        try:
            if error_id in self.error_records:
                error_record = self.error_records[error_id]
                error_record.resolution = resolution
                error_record.resolved_at = datetime.now()
                error_record.assigned_to = resolved_by
                
                logger.info(f"Error {error_id} resolved by {resolved_by}")
                return True
            else:
                logger.warning(f"Error {error_id} not found")
                return False
                
        except Exception as e:
            logger.error(f"Failed to resolve error {error_id}: {e}")
            return False

# Global error handler instance
error_handler = ErrorHandler()

def handle_error(error: Exception, context: ErrorContext = None, 
                severity: ErrorSeverity = None, category: ErrorCategory = None) -> str:
    """Global function to handle errors"""
    return error_handler.handle_error(error, context, severity, category)

def retry_on_error(max_retries: int = None, delay: float = None, 
                  backoff: bool = None, exceptions: tuple = None):
    """Global decorator for retrying functions on error"""
    return error_handler.retry_on_error(max_retries, delay, backoff, exceptions)

def get_error_statistics() -> Dict[str, Any]:
    """Global function to get error statistics"""
    return error_handler.get_error_statistics()

def resolve_error(error_id: str, resolution: str, resolved_by: str) -> bool:
    """Global function to resolve errors"""
    return error_handler.resolve_error(error_id, resolution, resolved_by)

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Error Handler')
    parser.add_argument('action', choices=['stats', 'patterns', 'resolve', 'test'],
                       help='Action to perform')
    parser.add_argument('--error-id', help='Error ID for resolution')
    parser.add_argument('--resolution', help='Resolution description')
    parser.add_argument('--resolved-by', help='Who resolved the error')
    
    args = parser.parse_args()
    
    try:
        if args.action == 'stats':
            stats = get_error_statistics()
            print(json.dumps(stats, indent=2, default=str))
            
        elif args.action == 'patterns':
            patterns = error_handler.error_patterns
            print(json.dumps(patterns, indent=2, default=str))
            
        elif args.action == 'resolve':
            if not all([args.error_id, args.resolution, args.resolved_by]):
                print("Error: --error-id, --resolution, and --resolved-by are required")
                sys.exit(1)
            
            if resolve_error(args.error_id, args.resolution, args.resolved_by):
                print(f"Error {args.error_id} resolved successfully")
            else:
                print(f"Failed to resolve error {args.error_id}")
                sys.exit(1)
                
        elif args.action == 'test':
            # Test error handling
            try:
                raise ValueError("This is a test error")
            except Exception as e:
                context = ErrorContext(
                    component="test_module",
                    operation="test_function",
                    metadata={"test": True}
                )
                error_id = handle_error(e, context, ErrorSeverity.MEDIUM, ErrorCategory.SYSTEM)
                print(f"Test error handled with ID: {error_id}")
        
    except Exception as e:
        logger.error(f"Error handler main failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
