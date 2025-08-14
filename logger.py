#!/usr/bin/env python3
"""
Logging and Monitoring System for BOP Málaga PDF Downloader

This module provides comprehensive logging and monitoring functionality for the
BOP Málaga PDF downloader system. It includes:

- Structured logging with rotation policies
- Error tracking and success metrics
- Email notification system for critical failures
- Performance monitoring and statistics
- Log analysis and reporting capabilities

Features:
- Multi-level logging with configurable output formats
- Automatic log rotation with size and time-based policies
- Email alerts for critical errors and system failures
- Metrics collection and reporting
- Integration with external monitoring systems
- Structured log parsing and analysis tools

Author: Automated Deployment System
Date: 2025
"""

import os
import sys
import json
import smtplib
import logging
import logging.handlers
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import threading
import time
from collections import defaultdict, deque
import traceback


@dataclass
class LogMetrics:
    """Metrics for logging system."""
    total_logs: int = 0
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    debug_count: int = 0
    critical_count: int = 0
    last_error_time: Optional[str] = None
    last_critical_time: Optional[str] = None
    session_start_time: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class DownloadMetrics:
    """Metrics for download operations."""
    total_attempts: int = 0
    successful_downloads: int = 0
    failed_downloads: int = 0
    skipped_downloads: int = 0
    total_size_mb: float = 0.0
    average_download_time: float = 0.0
    last_successful_download: Optional[str] = None
    last_failed_download: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class EmailNotifier:
    """Email notification system for critical alerts."""
    
    def __init__(self, smtp_config: Dict[str, Any]):
        """Initialize email notifier with SMTP configuration."""
        self.enabled = smtp_config.get('enabled', False)
        self.smtp_server = smtp_config.get('smtp_server', '')
        self.smtp_port = smtp_config.get('smtp_port', 587)
        self.username = smtp_config.get('username', '')
        self.password = smtp_config.get('password', '')
        self.from_email = smtp_config.get('from_email', '')
        self.to_emails = smtp_config.get('to_emails', [])
        self.use_tls = smtp_config.get('use_tls', True)
        
        # Rate limiting for email notifications
        self.last_notification_times = defaultdict(float)
        self.min_notification_interval = 300  # 5 minutes between same type of notifications
        
        # Initialize logger for email system
        self.logger = logging.getLogger('EmailNotifier')
    
    def can_send_notification(self, notification_type: str) -> bool:
        """Check if notification can be sent based on rate limiting."""
        if not self.enabled:
            return False
        
        current_time = time.time()
        last_time = self.last_notification_times.get(notification_type, 0)
        
        if current_time - last_time >= self.min_notification_interval:
            self.last_notification_times[notification_type] = current_time
            return True
        
        return False
    
    def send_notification(self, subject: str, message: str, 
                         notification_type: str = 'general',
                         attachments: Optional[List[str]] = None) -> bool:
        """Send email notification."""
        if not self.can_send_notification(notification_type):
            self.logger.debug(f"Notification rate limited: {notification_type}")
            return False
        
        if not self.enabled or not self.to_emails:
            self.logger.debug("Email notifications disabled or no recipients")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"[BOP Málaga Downloader] {subject}"
            
            # Add timestamp to message
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            full_message = f"Timestamp: {timestamp}\n\n{message}"
            
            msg.attach(MIMEText(full_message, 'plain'))
            
            # Add attachments if provided
            if attachments:
                for file_path in attachments:
                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {os.path.basename(file_path)}'
                            )
                            msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                if self.username and self.password:
                    server.login(self.username, self.password)
                
                server.send_message(msg)
            
            self.logger.info(f"Email notification sent: {subject}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
            return False
    
    def send_critical_alert(self, error_message: str, exception: Optional[Exception] = None) -> bool:
        """Send critical error alert."""
        subject = "CRITICAL: BOP Málaga Downloader Failure"
        
        message = f"A critical error has occurred in the BOP Málaga PDF Downloader:\n\n"
        message += f"Error: {error_message}\n\n"
        
        if exception:
            message += f"Exception Details:\n{str(exception)}\n\n"
            message += f"Traceback:\n{traceback.format_exc()}\n\n"
        
        message += "Please check the system logs for more details and take appropriate action."
        
        return self.send_notification(subject, message, 'critical_error')
    
    def send_success_summary(self, metrics: Dict[str, Any]) -> bool:
        """Send daily success summary."""
        subject = "Daily Summary: BOP Málaga Downloader"
        
        message = "Daily summary of BOP Málaga PDF Downloader activity:\n\n"
        message += f"Downloads attempted: {metrics.get('total_attempts', 0)}\n"
        message += f"Successful downloads: {metrics.get('successful_downloads', 0)}\n"
        message += f"Failed downloads: {metrics.get('failed_downloads', 0)}\n"
        message += f"Skipped downloads: {metrics.get('skipped_downloads', 0)}\n"
        message += f"Total size downloaded: {metrics.get('total_size_mb', 0):.2f} MB\n"
        message += f"Average download time: {metrics.get('average_download_time', 0):.2f} seconds\n\n"
        
        if metrics.get('errors'):
            message += f"Errors encountered: {len(metrics['errors'])}\n"
            message += "Recent errors:\n"
            for error in metrics['errors'][-5:]:  # Last 5 errors
                message += f"  - {error}\n"
        
        return self.send_notification(subject, message, 'daily_summary')


class MetricsCollector:
    """Collect and manage system metrics."""
    
    def __init__(self, metrics_file: str = './logs/metrics.json'):
        """Initialize metrics collector."""
        self.metrics_file = metrics_file
        self.log_metrics = LogMetrics()
        self.download_metrics = DownloadMetrics()
        self.error_history = deque(maxlen=100)  # Keep last 100 errors
        self.session_start = datetime.now()
        
        # Initialize session start time
        self.log_metrics.session_start_time = self.session_start.isoformat()
        
        # Load existing metrics if available
        self.load_metrics()
        
        self.logger = logging.getLogger('MetricsCollector')
    
    def load_metrics(self) -> None:
        """Load metrics from file."""
        if os.path.exists(self.metrics_file):
            try:
                with open(self.metrics_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'log_metrics' in data:
                    self.log_metrics = LogMetrics(**data['log_metrics'])
                
                if 'download_metrics' in data:
                    self.download_metrics = DownloadMetrics(**data['download_metrics'])
                
                if 'error_history' in data:
                    self.error_history = deque(data['error_history'], maxlen=100)
                
                self.logger.debug("Metrics loaded from file")
                
            except Exception as e:
                self.logger.warning(f"Could not load metrics: {e}")
    
    def save_metrics(self) -> None:
        """Save metrics to file."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.metrics_file), exist_ok=True)
            
            data = {
                'log_metrics': self.log_metrics.to_dict(),
                'download_metrics': self.download_metrics.to_dict(),
                'error_history': list(self.error_history),
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.logger.debug("Metrics saved to file")
            
        except Exception as e:
            self.logger.error(f"Could not save metrics: {e}")
    
    def record_log_event(self, level: str, message: str) -> None:
        """Record a log event."""
        self.log_metrics.total_logs += 1
        
        level_lower = level.lower()
        if level_lower == 'error':
            self.log_metrics.error_count += 1
            self.log_metrics.last_error_time = datetime.now().isoformat()
            self.error_history.append({
                'timestamp': datetime.now().isoformat(),
                'level': level,
                'message': message
            })
        elif level_lower == 'warning':
            self.log_metrics.warning_count += 1
        elif level_lower == 'info':
            self.log_metrics.info_count += 1
        elif level_lower == 'debug':
            self.log_metrics.debug_count += 1
        elif level_lower == 'critical':
            self.log_metrics.critical_count += 1
            self.log_metrics.last_critical_time = datetime.now().isoformat()
            self.error_history.append({
                'timestamp': datetime.now().isoformat(),
                'level': level,
                'message': message
            })
    
    def record_download_attempt(self) -> None:
        """Record a download attempt."""
        self.download_metrics.total_attempts += 1
    
    def record_download_success(self, file_size_mb: float, download_time: float) -> None:
        """Record a successful download."""
        self.download_metrics.successful_downloads += 1
        self.download_metrics.total_size_mb += file_size_mb
        self.download_metrics.last_successful_download = datetime.now().isoformat()
        
        # Update average download time
        total_successful = self.download_metrics.successful_downloads
        current_avg = self.download_metrics.average_download_time
        self.download_metrics.average_download_time = (
            (current_avg * (total_successful - 1) + download_time) / total_successful
        )
    
    def record_download_failure(self, error_message: str) -> None:
        """Record a failed download."""
        self.download_metrics.failed_downloads += 1
        self.download_metrics.last_failed_download = datetime.now().isoformat()
        
        self.error_history.append({
            'timestamp': datetime.now().isoformat(),
            'level': 'ERROR',
            'message': f"Download failed: {error_message}"
        })
    
    def record_download_skipped(self) -> None:
        """Record a skipped download."""
        self.download_metrics.skipped_downloads += 1
    
    def get_summary_report(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary."""
        session_duration = datetime.now() - self.session_start
        
        return {
            'session_info': {
                'start_time': self.session_start.isoformat(),
                'duration_minutes': session_duration.total_seconds() / 60,
                'current_time': datetime.now().isoformat()
            },
            'log_metrics': self.log_metrics.to_dict(),
            'download_metrics': self.download_metrics.to_dict(),
            'error_summary': {
                'total_errors': len(self.error_history),
                'recent_errors': list(self.error_history)[-10:] if self.error_history else []
            },
            'performance': {
                'success_rate': (
                    self.download_metrics.successful_downloads / 
                    max(self.download_metrics.total_attempts, 1) * 100
                ),
                'error_rate': (
                    self.log_metrics.error_count / 
                    max(self.log_metrics.total_logs, 1) * 100
                )
            }
        }


class BOPMalagaLogger:
    """Main logging system for BOP Málaga PDF Downloader."""
    
    def __init__(self, log_config: Optional[Dict[str, Any]] = None,
                 email_config: Optional[Dict[str, Any]] = None):
        """Initialize the logging system."""
        # Default configuration
        self.config = {
            'logs_dir': './logs',
            'log_file': 'bop_downloader.log',
            'log_level': 'INFO',
            'max_log_size_mb': 10,
            'backup_count': 5,
            'console_logging': True,
            'structured_format': True,
            'metrics_enabled': True,
            'email_notifications': True
        }
        
        # Update with provided configuration
        if log_config:
            self.config.update(log_config)
        
        # Ensure logs directory exists
        self.logs_dir = Path(self.config['logs_dir'])
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.email_notifier = EmailNotifier(email_config or {})
        self.metrics_collector = MetricsCollector(
            str(self.logs_dir / 'metrics.json')
        ) if self.config['metrics_enabled'] else None
        
        # Setup logging
        self.setup_logging()
        
        # Get logger instance
        self.logger = logging.getLogger('BOPMalagaDownloader')
        
        # Start background tasks
        self._start_background_tasks()
        
        self.logger.info("BOP Málaga logging system initialized")
    
    def setup_logging(self) -> None:
        """Setup logging configuration."""
        # Create log file path
        log_file_path = self.logs_dir / self.config['log_file']
        
        # Create formatter
        if self.config['structured_format']:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        # Setup root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config['log_level'].upper()))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=self.config['max_log_size_mb'] * 1024 * 1024,
            backupCount=self.config['backup_count'],
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler
        if self.config['console_logging']:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
        
        # Custom handler for metrics collection
        if self.metrics_collector:
            metrics_handler = MetricsHandler(self.metrics_collector)
            root_logger.addHandler(metrics_handler)
        
        # Custom handler for email notifications
        if self.email_notifier.enabled:
            email_handler = EmailHandler(self.email_notifier)
            email_handler.setLevel(logging.ERROR)  # Only send emails for errors and above
            root_logger.addHandler(email_handler)
    
    def _start_background_tasks(self) -> None:
        """Start background tasks for metrics and notifications."""
        if self.metrics_collector:
            # Start metrics saving thread
            metrics_thread = threading.Thread(
                target=self._metrics_saver_task,
                daemon=True
            )
            metrics_thread.start()
        
        if self.email_notifier.enabled:
            # Start daily summary thread
            summary_thread = threading.Thread(
                target=self._daily_summary_task,
                daemon=True
            )
            summary_thread.start()
    
    def _metrics_saver_task(self) -> None:
        """Background task to periodically save metrics."""
        while True:
            try:
                time.sleep(300)  # Save every 5 minutes
                if self.metrics_collector:
                    self.metrics_collector.save_metrics()
            except Exception as e:
                logging.getLogger('MetricsSaver').error(f"Error saving metrics: {e}")
    
    def _daily_summary_task(self) -> None:
        """Background task to send daily summaries."""
        last_summary_date = datetime.now().date()
        
        while True:
            try:
                time.sleep(3600)  # Check every hour
                current_date = datetime.now().date()
                
                # Send summary if it's a new day
                if current_date > last_summary_date:
                    if self.metrics_collector:
                        summary = self.metrics_collector.get_summary_report()
                        self.email_notifier.send_success_summary(summary)
                    last_summary_date = current_date
                    
            except Exception as e:
                logging.getLogger('DailySummary').error(f"Error sending daily summary: {e}")
    
    def log_download_attempt(self, edicto_id: str) -> None:
        """Log a download attempt."""
        if self.metrics_collector:
            self.metrics_collector.record_download_attempt()
        self.logger.info(f"Download attempt: {edicto_id}")
    
    def log_download_success(self, edicto_id: str, filename: str, 
                           file_size_mb: float, download_time: float) -> None:
        """Log a successful download."""
        if self.metrics_collector:
            self.metrics_collector.record_download_success(file_size_mb, download_time)
        self.logger.info(f"Download successful: {edicto_id} -> {filename} "
                        f"({file_size_mb:.2f} MB in {download_time:.2f}s)")
    
    def log_download_failure(self, edicto_id: str, error_message: str) -> None:
        """Log a failed download."""
        if self.metrics_collector:
            self.metrics_collector.record_download_failure(error_message)
        self.logger.error(f"Download failed: {edicto_id} - {error_message}")
    
    def log_download_skipped(self, edicto_id: str, reason: str = "Already downloaded") -> None:
        """Log a skipped download."""
        if self.metrics_collector:
            self.metrics_collector.record_download_skipped()
        self.logger.debug(f"Download skipped: {edicto_id} - {reason}")
    
    def log_critical_error(self, error_message: str, exception: Optional[Exception] = None) -> None:
        """Log a critical error and send email notification."""
        self.logger.critical(error_message)
        if exception:
            self.logger.critical(f"Exception details: {str(exception)}")
            self.logger.critical(f"Traceback: {traceback.format_exc()}")
        
        # Send email notification
        if self.email_notifier.enabled:
            self.email_notifier.send_critical_alert(error_message, exception)
    
    def get_metrics_summary(self) -> Optional[Dict[str, Any]]:
        """Get current metrics summary."""
        if self.metrics_collector:
            return self.metrics_collector.get_summary_report()
        return None
    
    def shutdown(self) -> None:
        """Shutdown logging system gracefully."""
        self.logger.info("Shutting down logging system")
        
        # Save final metrics
        if self.metrics_collector:
            self.metrics_collector.save_metrics()
        
        # Shutdown logging
        logging.shutdown()


class MetricsHandler(logging.Handler):
    """Custom logging handler for metrics collection."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        """Initialize metrics handler."""
        super().__init__()
        self.metrics_collector = metrics_collector
    
    def emit(self, record: logging.LogRecord) -> None:
        """Handle log record for metrics."""
        try:
            self.metrics_collector.record_log_event(
                record.levelname,
                record.getMessage()
            )
        except Exception:
            # Ignore errors in metrics collection to avoid logging loops
            pass


class EmailHandler(logging.Handler):
    """Custom logging handler for email notifications."""
    
    def __init__(self, email_notifier: EmailNotifier):
        """Initialize email handler."""
        super().__init__()
        self.email_notifier = email_notifier
    
    def emit(self, record: logging.LogRecord) -> None:
        """Handle log record for email notifications."""
        try:
            if record.levelno >= logging.ERROR:
                subject = f"{record.levelname}: {record.funcName}"
                message = f"Function: {record.funcName}\n"
                message += f"Line: {record.lineno}\n"
                message += f"Message: {record.getMessage()}\n"
                
                if record.exc_info:
                    message += f"\nException: {record.exc_text or ''}"
                
                notification_type = 'critical_error' if record.levelno >= logging.CRITICAL else 'error'
                self.email_notifier.send_notification(subject, message, notification_type)
                
        except Exception:
            # Ignore errors in email notifications to avoid logging loops
            pass


def create_logger(log_config: Optional[Dict[str, Any]] = None,
                 email_config: Optional[Dict[str, Any]] = None) -> BOPMalagaLogger:
    """Convenience function to create logger instance."""
    return BOPMalagaLogger(log_config, email_config)


def main():
    """Command-line interface for logger testing and management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BOP Málaga Logger Management')
    parser.add_argument('--test-logging', action='store_true',
                       help='Test logging functionality')
    parser.add_argument('--test-email', action='store_true',
                       help='Test email notifications')
    parser.add_argument('--show-metrics', action='store_true',
                       help='Show current metrics')
    parser.add_argument('--config-file', help='Configuration file path')
    
    args = parser.parse_args()
    
    # Load configuration if provided
    config = {}
    if args.config_file and os.path.exists(args.config_file):
        with open(args.config_file, 'r') as f:
            config = json.load(f)
    
    # Initialize logger
    log_config = config.get('logging', {})
    email_config = config.get('email', {})
    
    logger_system = create_logger(log_config, email_config)
    
    if args.test_logging:
        # Test different log levels
        logger = logging.getLogger('TestLogger')
        logger.debug("This is a debug message")
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
        logger.critical("This is a critical message")
        
        # Test download logging
        logger_system.log_download_attempt("20250814-12345-2025-01")
        logger_system.log_download_success("20250814-12345-2025-01", "test.pdf", 1.5, 2.3)
        logger_system.log_download_failure("20250814-12346-2025-01", "Network timeout")
        logger_system.log_download_skipped("20250814-12347-2025-01")
        
        print("Logging test completed")
    
    if args.test_email:
        if logger_system.email_notifier.enabled:
            success = logger_system.email_notifier.send_notification(
                "Test Notification",
                "This is a test email from the BOP Málaga logging system.",
                "test"
            )
            print(f"Email test {'successful' if success else 'failed'}")
        else:
            print("Email notifications are disabled")
    
    if args.show_metrics:
        metrics = logger_system.get_metrics_summary()
        if metrics:
            print(json.dumps(metrics, indent=2))
        else:
            print("Metrics collection is disabled")
    
    # Shutdown gracefully
    logger_system.shutdown()


if __name__ == '__main__':
    main()
