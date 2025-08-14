#!/usr/bin/env python3
"""
Configuration Module for BOP Málaga PDF Downloader

This module provides centralized configuration management for the BOP Málaga
PDF downloader system. It includes default settings, configuration validation,
and environment-specific overrides.

Features:
- Configurable download directory and storage limits
- Adjustable crawl delays and request timeouts
- Logging configuration with multiple levels
- Email notification settings for alerts
- Storage cleanup policies and thresholds

Author: Automated Deployment System
Date: 2025
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, asdict


@dataclass
class DownloadConfig:
    """Configuration for download behavior."""
    download_dir: str = './downloads'
    max_storage_mb: int = 1000  # 1GB default
    cleanup_days: int = 30
    compress_old_files: bool = False
    compression_days: int = 7
    
    def __post_init__(self):
        """Validate download configuration."""
        if self.max_storage_mb <= 0:
            raise ValueError("max_storage_mb must be positive")
        if self.cleanup_days <= 0:
            raise ValueError("cleanup_days must be positive")
        if self.compression_days < 0:
            raise ValueError("compression_days must be non-negative")


@dataclass
class NetworkConfig:
    """Configuration for network requests."""
    crawl_delay: float = 5.0  # seconds, as per robots.txt
    max_retries: int = 3
    timeout: int = 30
    user_agent: str = 'BOP-Malaga-Downloader/1.0 (Educational/Research Purpose)'
    
    def __post_init__(self):
        """Validate network configuration."""
        if self.crawl_delay < 0:
            raise ValueError("crawl_delay must be non-negative")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")


@dataclass
class LoggingConfig:
    """Configuration for logging system."""
    logs_dir: str = './logs'
    log_level: str = 'INFO'
    log_file: str = 'bop_downloader.log'
    max_log_size_mb: int = 10
    backup_count: int = 5
    console_logging: bool = True
    
    def __post_init__(self):
        """Validate logging configuration."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        if self.max_log_size_mb <= 0:
            raise ValueError("max_log_size_mb must be positive")
        if self.backup_count < 0:
            raise ValueError("backup_count must be non-negative")


@dataclass
class EmailConfig:
    """Configuration for email notifications."""
    enabled: bool = False
    smtp_server: str = ''
    smtp_port: int = 587
    username: str = ''
    password: str = ''
    from_email: str = ''
    to_emails: list = None
    use_tls: bool = True
    
    def __post_init__(self):
        """Validate email configuration."""
        if self.to_emails is None:
            self.to_emails = []
        if self.enabled and not all([self.smtp_server, self.username, self.from_email]):
            raise ValueError("Email enabled but missing required settings")
        if self.smtp_port <= 0 or self.smtp_port > 65535:
            raise ValueError("smtp_port must be between 1 and 65535")


@dataclass
class TrackingConfig:
    """Configuration for tracking system."""
    tracking_file: str = './tracking.json'
    backup_tracking: bool = True
    tracking_backup_days: int = 7
    
    def __post_init__(self):
        """Validate tracking configuration."""
        if self.tracking_backup_days < 0:
            raise ValueError("tracking_backup_days must be non-negative")


class BOPMalagaConfig:
    """Main configuration class for BOP Málaga PDF Downloader."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration with defaults and optional file override."""
        # Initialize with default configurations
        self.download = DownloadConfig()
        self.network = NetworkConfig()
        self.logging = LoggingConfig()
        self.email = EmailConfig()
        self.tracking = TrackingConfig()
        
        # Website-specific settings
        self.base_url = "https://www.bopmalaga.es"
        self.summary_url = f"{self.base_url}/sumario.php"
        self.download_url_template = f"{self.base_url}/descarga.php?archivo={{filename}}"
        
        # Load custom configuration if provided
        if config_file:
            self.load_from_file(config_file)
        
        # Load environment variables
        self.load_from_environment()
        
        # Validate final configuration
        self.validate()
    
    def load_from_file(self, config_file: str) -> None:
        """Load configuration from JSON file."""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Update configurations from file
            if 'download' in config_data:
                self._update_config(self.download, config_data['download'])
            
            if 'network' in config_data:
                self._update_config(self.network, config_data['network'])
            
            if 'logging' in config_data:
                self._update_config(self.logging, config_data['logging'])
            
            if 'email' in config_data:
                self._update_config(self.email, config_data['email'])
            
            if 'tracking' in config_data:
                self._update_config(self.tracking, config_data['tracking'])
            
            logging.info(f"Configuration loaded from {config_file}")
            
        except FileNotFoundError:
            logging.warning(f"Configuration file not found: {config_file}")
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in configuration file {config_file}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading configuration from {config_file}: {e}")
            raise
    
    def load_from_environment(self) -> None:
        """Load configuration from environment variables."""
        env_mappings = {
            # Download settings
            'BOP_DOWNLOAD_DIR': ('download', 'download_dir'),
            'BOP_MAX_STORAGE_MB': ('download', 'max_storage_mb', int),
            'BOP_CLEANUP_DAYS': ('download', 'cleanup_days', int),
            'BOP_COMPRESS_OLD_FILES': ('download', 'compress_old_files', bool),
            
            # Network settings
            'BOP_CRAWL_DELAY': ('network', 'crawl_delay', float),
            'BOP_MAX_RETRIES': ('network', 'max_retries', int),
            'BOP_TIMEOUT': ('network', 'timeout', int),
            'BOP_USER_AGENT': ('network', 'user_agent'),
            
            # Logging settings
            'BOP_LOGS_DIR': ('logging', 'logs_dir'),
            'BOP_LOG_LEVEL': ('logging', 'log_level'),
            'BOP_LOG_FILE': ('logging', 'log_file'),
            'BOP_MAX_LOG_SIZE_MB': ('logging', 'max_log_size_mb', int),
            
            # Email settings
            'BOP_EMAIL_ENABLED': ('email', 'enabled', bool),
            'BOP_SMTP_SERVER': ('email', 'smtp_server'),
            'BOP_SMTP_PORT': ('email', 'smtp_port', int),
            'BOP_EMAIL_USERNAME': ('email', 'username'),
            'BOP_EMAIL_PASSWORD': ('email', 'password'),
            'BOP_FROM_EMAIL': ('email', 'from_email'),
            'BOP_TO_EMAILS': ('email', 'to_emails', 'list'),
            
            # Tracking settings
            'BOP_TRACKING_FILE': ('tracking', 'tracking_file'),
            'BOP_BACKUP_TRACKING': ('tracking', 'backup_tracking', bool),
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    section = config_path[0]
                    attr = config_path[1]
                    converter = config_path[2] if len(config_path) > 2 else str
                    
                    # Convert value based on type
                    if converter == bool:
                        converted_value = value.lower() in ('true', '1', 'yes', 'on')
                    elif converter == 'list':
                        converted_value = [email.strip() for email in value.split(',')]
                    else:
                        converted_value = converter(value)
                    
                    # Set the value
                    config_obj = getattr(self, section)
                    setattr(config_obj, attr, converted_value)
                    
                except (ValueError, AttributeError) as e:
                    logging.warning(f"Invalid environment variable {env_var}={value}: {e}")
    
    def _update_config(self, config_obj: Any, updates: Dict[str, Any]) -> None:
        """Update configuration object with new values."""
        for key, value in updates.items():
            if hasattr(config_obj, key):
                setattr(config_obj, key, value)
            else:
                logging.warning(f"Unknown configuration key: {key}")
    
    def validate(self) -> None:
        """Validate all configuration sections."""
        try:
            # Re-run post_init validation for all configs
            self.download.__post_init__()
            self.network.__post_init__()
            self.logging.__post_init__()
            self.email.__post_init__()
            self.tracking.__post_init__()
            
        except ValueError as e:
            logging.error(f"Configuration validation failed: {e}")
            raise
    
    def create_directories(self) -> None:
        """Create necessary directories based on configuration."""
        directories = [
            self.download.download_dir,
            self.logging.logs_dir,
        ]
        
        for directory in directories:
            try:
                Path(directory).mkdir(parents=True, exist_ok=True)
                logging.debug(f"Created directory: {directory}")
            except Exception as e:
                logging.error(f"Failed to create directory {directory}: {e}")
                raise
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'download': asdict(self.download),
            'network': asdict(self.network),
            'logging': asdict(self.logging),
            'email': asdict(self.email),
            'tracking': asdict(self.tracking),
            'base_url': self.base_url,
            'summary_url': self.summary_url,
            'download_url_template': self.download_url_template,
        }
    
    def save_to_file(self, config_file: str) -> None:
        """Save current configuration to JSON file."""
        try:
            config_dict = self.to_dict()
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)
            logging.info(f"Configuration saved to {config_file}")
        except Exception as e:
            logging.error(f"Failed to save configuration to {config_file}: {e}")
            raise
    
    def get_log_file_path(self) -> str:
        """Get full path to log file."""
        return os.path.join(self.logging.logs_dir, self.logging.log_file)
    
    def get_tracking_file_path(self) -> str:
        """Get full path to tracking file."""
        return self.tracking.tracking_file
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return f"BOPMalagaConfig(download_dir={self.download.download_dir}, " \
               f"max_storage={self.download.max_storage_mb}MB, " \
               f"crawl_delay={self.network.crawl_delay}s, " \
               f"log_level={self.logging.log_level})"


def load_config(config_file: Optional[str] = None) -> BOPMalagaConfig:
    """Convenience function to load configuration."""
    return BOPMalagaConfig(config_file)


def create_sample_config(output_file: str = 'config.json') -> None:
    """Create a sample configuration file."""
    config = BOPMalagaConfig()
    
    # Customize some settings for the sample
    config.download.max_storage_mb = 2000  # 2GB
    config.download.cleanup_days = 45
    config.network.crawl_delay = 5.0
    config.logging.log_level = 'INFO'
    config.email.enabled = False  # Disabled by default
    
    config.save_to_file(output_file)
    print(f"Sample configuration created: {output_file}")


if __name__ == '__main__':
    """Command-line interface for configuration management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BOP Málaga Configuration Manager')
    parser.add_argument('--create-sample', metavar='FILE', 
                       help='Create sample configuration file')
    parser.add_argument('--validate', metavar='FILE',
                       help='Validate configuration file')
    parser.add_argument('--show', metavar='FILE',
                       help='Show configuration from file')
    
    args = parser.parse_args()
    
    if args.create_sample:
        create_sample_config(args.create_sample)
    elif args.validate:
        try:
            config = load_config(args.validate)
            print(f"Configuration is valid: {config}")
        except Exception as e:
            print(f"Configuration validation failed: {e}")
            exit(1)
    elif args.show:
        try:
            config = load_config(args.show)
            print(json.dumps(config.to_dict(), indent=2))
        except Exception as e:
            print(f"Error loading configuration: {e}")
            exit(1)
    else:
        parser.print_help()
