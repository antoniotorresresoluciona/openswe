#!/usr/bin/env python3
"""
Tracking System for BOP Málaga PDF Downloader

This module provides comprehensive tracking and storage management functionality
for the BOP Málaga PDF downloader system. It includes:

- JSON-based tracking of downloaded edicto IDs to prevent duplicates
- Storage cleanup functionality with configurable policies
- File compression for older documents to save space
- Storage threshold monitoring and automatic cleanup
- Backup and recovery mechanisms for tracking data

Features:
- Persistent tracking of downloaded documents
- Automatic cleanup based on age and storage limits
- File compression for space optimization
- Statistics and reporting on storage usage
- Backup and recovery of tracking data

Author: Automated Deployment System
Date: 2025
"""

import os
import json
import gzip
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class DownloadRecord:
    """Record of a downloaded edicto."""
    edicto_id: str
    filename: str
    download_date: str
    file_size: int
    file_path: str
    checksum: Optional[str] = None
    compressed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DownloadRecord':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class StorageStats:
    """Storage statistics."""
    total_files: int = 0
    total_size_mb: float = 0.0
    compressed_files: int = 0
    compressed_size_mb: float = 0.0
    oldest_file_date: Optional[str] = None
    newest_file_date: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class BOPMalagaTracker:
    """Main tracking system for BOP Málaga PDF downloads."""
    
    def __init__(self, tracking_file: str = './tracking.json', 
                 download_dir: str = './downloads'):
        """Initialize the tracking system."""
        self.tracking_file = tracking_file
        self.download_dir = download_dir
        self.backup_dir = os.path.join(os.path.dirname(tracking_file), 'backups')
        
        # Initialize logging
        self.logger = logging.getLogger('BOPMalagaTracker')
        
        # Load tracking data
        self.tracking_data = self.load_tracking_data()
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure necessary directories exist."""
        directories = [
            self.download_dir,
            self.backup_dir,
            os.path.dirname(self.tracking_file)
        ]
        
        for directory in directories:
            if directory:  # Skip empty directory names
                Path(directory).mkdir(parents=True, exist_ok=True)
    
    def load_tracking_data(self) -> Dict[str, Any]:
        """Load tracking data from JSON file."""
        if not os.path.exists(self.tracking_file):
            self.logger.info(f"Creating new tracking file: {self.tracking_file}")
            return self._create_empty_tracking_data()
        
        try:
            with open(self.tracking_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate and migrate data structure if needed
            data = self._validate_and_migrate_data(data)
            
            self.logger.info(f"Loaded tracking data: {len(data.get('downloads', {}))} records")
            return data
            
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.error(f"Error loading tracking data: {e}")
            # Create backup of corrupted file
            if os.path.exists(self.tracking_file):
                backup_name = f"corrupted_tracking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                backup_path = os.path.join(self.backup_dir, backup_name)
                shutil.copy2(self.tracking_file, backup_path)
                self.logger.warning(f"Corrupted tracking file backed up to: {backup_path}")
            
            return self._create_empty_tracking_data()
    
    def _create_empty_tracking_data(self) -> Dict[str, Any]:
        """Create empty tracking data structure."""
        return {
            'version': '1.0',
            'created': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'downloads': {},  # edicto_id -> DownloadRecord
            'statistics': {
                'total_downloads': 0,
                'total_size_mb': 0.0,
                'last_cleanup': None,
                'last_compression': None
            }
        }
    
    def _validate_and_migrate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and migrate tracking data structure."""
        # Ensure required fields exist
        if 'version' not in data:
            data['version'] = '1.0'
        
        if 'downloads' not in data:
            data['downloads'] = {}
        
        if 'statistics' not in data:
            data['statistics'] = {
                'total_downloads': len(data['downloads']),
                'total_size_mb': 0.0,
                'last_cleanup': None,
                'last_compression': None
            }
        
        # Migrate old format if needed
        if isinstance(data.get('downloads'), list):
            # Convert old list format to dict format
            old_downloads = data['downloads']
            data['downloads'] = {}
            for edicto_id in old_downloads:
                data['downloads'][edicto_id] = {
                    'edicto_id': edicto_id,
                    'filename': f"{edicto_id}.pdf",
                    'download_date': datetime.now().isoformat(),
                    'file_size': 0,
                    'file_path': os.path.join(self.download_dir, f"{edicto_id}.pdf"),
                    'compressed': False
                }
        
        data['last_updated'] = datetime.now().isoformat()
        return data
    
    def save_tracking_data(self) -> None:
        """Save tracking data to JSON file."""
        try:
            # Update last_updated timestamp
            self.tracking_data['last_updated'] = datetime.now().isoformat()
            
            # Create backup before saving
            self._create_backup()
            
            # Save to temporary file first, then rename (atomic operation)
            temp_file = f"{self.tracking_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.tracking_data, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            os.rename(temp_file, self.tracking_file)
            
            self.logger.debug(f"Tracking data saved to {self.tracking_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving tracking data: {e}")
            # Clean up temporary file if it exists
            if os.path.exists(f"{self.tracking_file}.tmp"):
                os.remove(f"{self.tracking_file}.tmp")
            raise
    
    def _create_backup(self) -> None:
        """Create backup of tracking file."""
        if not os.path.exists(self.tracking_file):
            return
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"tracking_backup_{timestamp}.json"
            backup_path = os.path.join(self.backup_dir, backup_name)
            
            shutil.copy2(self.tracking_file, backup_path)
            self.logger.debug(f"Tracking backup created: {backup_path}")
            
            # Clean up old backups (keep last 10)
            self._cleanup_old_backups()
            
        except Exception as e:
            self.logger.warning(f"Failed to create tracking backup: {e}")
    
    def _cleanup_old_backups(self, keep_count: int = 10) -> None:
        """Clean up old backup files."""
        try:
            backup_files = []
            for file in os.listdir(self.backup_dir):
                if file.startswith('tracking_backup_') and file.endswith('.json'):
                    file_path = os.path.join(self.backup_dir, file)
                    backup_files.append((file_path, os.path.getmtime(file_path)))
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)
            
            # Remove old backups
            for file_path, _ in backup_files[keep_count:]:
                os.remove(file_path)
                self.logger.debug(f"Removed old backup: {file_path}")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up old backups: {e}")
    
    def add_download(self, edicto_id: str, filename: str, file_path: str) -> bool:
        """Add a new download record."""
        try:
            # Check if already exists
            if edicto_id in self.tracking_data['downloads']:
                self.logger.debug(f"Download already tracked: {edicto_id}")
                return False
            
            # Get file information
            file_size = 0
            checksum = None
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                checksum = self._calculate_checksum(file_path)
            
            # Create download record
            record = DownloadRecord(
                edicto_id=edicto_id,
                filename=filename,
                download_date=datetime.now().isoformat(),
                file_size=file_size,
                file_path=file_path,
                checksum=checksum,
                compressed=False
            )
            
            # Add to tracking data
            self.tracking_data['downloads'][edicto_id] = record.to_dict()
            
            # Update statistics
            self.tracking_data['statistics']['total_downloads'] += 1
            self.tracking_data['statistics']['total_size_mb'] += file_size / (1024 * 1024)
            
            self.logger.info(f"Added download record: {edicto_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding download record for {edicto_id}: {e}")
            return False
    
    def is_downloaded(self, edicto_id: str) -> bool:
        """Check if edicto is already downloaded."""
        return edicto_id in self.tracking_data['downloads']
    
    def get_downloaded_ids(self) -> Set[str]:
        """Get set of all downloaded edicto IDs."""
        return set(self.tracking_data['downloads'].keys())
    
    def get_download_record(self, edicto_id: str) -> Optional[DownloadRecord]:
        """Get download record for specific edicto."""
        record_data = self.tracking_data['downloads'].get(edicto_id)
        if record_data:
            return DownloadRecord.from_dict(record_data)
        return None
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum of file."""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            self.logger.warning(f"Error calculating checksum for {file_path}: {e}")
            return ""
    
    def get_storage_stats(self) -> StorageStats:
        """Get current storage statistics."""
        stats = StorageStats()
        
        try:
            download_dates = []
            
            for record_data in self.tracking_data['downloads'].values():
                record = DownloadRecord.from_dict(record_data)
                
                # Check if file still exists
                if os.path.exists(record.file_path):
                    stats.total_files += 1
                    file_size_mb = record.file_size / (1024 * 1024)
                    stats.total_size_mb += file_size_mb
                    
                    if record.compressed:
                        stats.compressed_files += 1
                        stats.compressed_size_mb += file_size_mb
                    
                    download_dates.append(record.download_date)
            
            # Find oldest and newest dates
            if download_dates:
                download_dates.sort()
                stats.oldest_file_date = download_dates[0]
                stats.newest_file_date = download_dates[-1]
            
        except Exception as e:
            self.logger.error(f"Error calculating storage stats: {e}")
        
        return stats
    
    def cleanup_old_files(self, max_age_days: int = 30) -> Tuple[int, float]:
        """Remove files older than specified days."""
        removed_count = 0
        removed_size_mb = 0.0
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        try:
            to_remove = []
            
            for edicto_id, record_data in self.tracking_data['downloads'].items():
                record = DownloadRecord.from_dict(record_data)
                download_date = datetime.fromisoformat(record.download_date.replace('Z', '+00:00'))
                
                if download_date < cutoff_date:
                    to_remove.append((edicto_id, record))
            
            # Remove old files
            for edicto_id, record in to_remove:
                if os.path.exists(record.file_path):
                    file_size_mb = os.path.getsize(record.file_path) / (1024 * 1024)
                    os.remove(record.file_path)
                    removed_size_mb += file_size_mb
                    self.logger.info(f"Removed old file: {record.filename}")
                
                # Remove from tracking
                del self.tracking_data['downloads'][edicto_id]
                removed_count += 1
            
            # Update statistics
            self.tracking_data['statistics']['last_cleanup'] = datetime.now().isoformat()
            self.tracking_data['statistics']['total_downloads'] -= removed_count
            self.tracking_data['statistics']['total_size_mb'] -= removed_size_mb
            
            if removed_count > 0:
                self.logger.info(f"Cleanup completed: removed {removed_count} files, "
                               f"freed {removed_size_mb:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        
        return removed_count, removed_size_mb
    
    def cleanup_by_storage_limit(self, max_storage_mb: float) -> Tuple[int, float]:
        """Remove oldest files to stay under storage limit."""
        current_stats = self.get_storage_stats()
        
        if current_stats.total_size_mb <= max_storage_mb:
            self.logger.debug(f"Storage under limit: {current_stats.total_size_mb:.2f} MB "
                            f"<= {max_storage_mb} MB")
            return 0, 0.0
        
        removed_count = 0
        removed_size_mb = 0.0
        
        try:
            # Get all records sorted by download date (oldest first)
            records_with_dates = []
            for edicto_id, record_data in self.tracking_data['downloads'].items():
                record = DownloadRecord.from_dict(record_data)
                if os.path.exists(record.file_path):
                    download_date = datetime.fromisoformat(record.download_date.replace('Z', '+00:00'))
                    records_with_dates.append((download_date, edicto_id, record))
            
            records_with_dates.sort(key=lambda x: x[0])  # Sort by date
            
            # Remove oldest files until under limit
            current_size = current_stats.total_size_mb
            for _, edicto_id, record in records_with_dates:
                if current_size <= max_storage_mb:
                    break
                
                if os.path.exists(record.file_path):
                    file_size_mb = os.path.getsize(record.file_path) / (1024 * 1024)
                    os.remove(record.file_path)
                    current_size -= file_size_mb
                    removed_size_mb += file_size_mb
                    self.logger.info(f"Removed for storage limit: {record.filename}")
                
                # Remove from tracking
                del self.tracking_data['downloads'][edicto_id]
                removed_count += 1
            
            # Update statistics
            self.tracking_data['statistics']['total_downloads'] -= removed_count
            self.tracking_data['statistics']['total_size_mb'] -= removed_size_mb
            
            if removed_count > 0:
                self.logger.info(f"Storage cleanup completed: removed {removed_count} files, "
                               f"freed {removed_size_mb:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"Error during storage cleanup: {e}")
        
        return removed_count, removed_size_mb
    
    def compress_old_files(self, compress_age_days: int = 7) -> Tuple[int, float]:
        """Compress files older than specified days."""
        compressed_count = 0
        space_saved_mb = 0.0
        cutoff_date = datetime.now() - timedelta(days=compress_age_days)
        
        try:
            for edicto_id, record_data in self.tracking_data['downloads'].items():
                record = DownloadRecord.from_dict(record_data)
                
                # Skip if already compressed or file doesn't exist
                if record.compressed or not os.path.exists(record.file_path):
                    continue
                
                download_date = datetime.fromisoformat(record.download_date.replace('Z', '+00:00'))
                
                if download_date < cutoff_date:
                    # Compress the file
                    compressed_path = f"{record.file_path}.gz"
                    original_size = os.path.getsize(record.file_path)
                    
                    with open(record.file_path, 'rb') as f_in:
                        with gzip.open(compressed_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Verify compression was successful
                    if os.path.exists(compressed_path):
                        compressed_size = os.path.getsize(compressed_path)
                        space_saved = (original_size - compressed_size) / (1024 * 1024)
                        
                        # Remove original file
                        os.remove(record.file_path)
                        
                        # Update record
                        record.file_path = compressed_path
                        record.compressed = True
                        record.file_size = compressed_size
                        self.tracking_data['downloads'][edicto_id] = record.to_dict()
                        
                        compressed_count += 1
                        space_saved_mb += space_saved
                        
                        self.logger.info(f"Compressed {record.filename}: "
                                       f"saved {space_saved:.2f} MB")
            
            # Update statistics
            if compressed_count > 0:
                self.tracking_data['statistics']['last_compression'] = datetime.now().isoformat()
                self.logger.info(f"Compression completed: {compressed_count} files, "
                               f"saved {space_saved_mb:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"Error during compression: {e}")
        
        return compressed_count, space_saved_mb
    
    def verify_files(self) -> Tuple[List[str], List[str]]:
        """Verify tracked files exist and have correct checksums."""
        missing_files = []
        corrupted_files = []
        
        try:
            for edicto_id, record_data in self.tracking_data['downloads'].items():
                record = DownloadRecord.from_dict(record_data)
                
                if not os.path.exists(record.file_path):
                    missing_files.append(edicto_id)
                    self.logger.warning(f"Missing file: {record.filename}")
                    continue
                
                # Verify checksum if available
                if record.checksum:
                    if record.compressed and record.file_path.endswith('.gz'):
                        # For compressed files, we can't easily verify the original checksum
                        continue
                    
                    current_checksum = self._calculate_checksum(record.file_path)
                    if current_checksum != record.checksum:
                        corrupted_files.append(edicto_id)
                        self.logger.warning(f"Corrupted file detected: {record.filename}")
            
            if missing_files or corrupted_files:
                self.logger.warning(f"File verification: {len(missing_files)} missing, "
                                  f"{len(corrupted_files)} corrupted")
            else:
                self.logger.info("File verification: all files OK")
            
        except Exception as e:
            self.logger.error(f"Error during file verification: {e}")
        
        return missing_files, corrupted_files
    
    def get_summary_report(self) -> Dict[str, Any]:
        """Get comprehensive summary report."""
        stats = self.get_storage_stats()
        missing_files, corrupted_files = self.verify_files()
        
        return {
            'tracking_file': self.tracking_file,
            'download_directory': self.download_dir,
            'total_tracked_downloads': len(self.tracking_data['downloads']),
            'storage_stats': stats.to_dict(),
            'file_integrity': {
                'missing_files': len(missing_files),
                'corrupted_files': len(corrupted_files),
                'missing_file_ids': missing_files[:10],  # First 10 for brevity
                'corrupted_file_ids': corrupted_files[:10]
            },
            'last_updated': self.tracking_data.get('last_updated'),
            'last_cleanup': self.tracking_data['statistics'].get('last_cleanup'),
            'last_compression': self.tracking_data['statistics'].get('last_compression')
        }


def main():
    """Command-line interface for tracker management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BOP Málaga Tracker Management')
    parser.add_argument('--tracking-file', default='./tracking.json',
                       help='Path to tracking file')
    parser.add_argument('--download-dir', default='./downloads',
                       help='Download directory')
    parser.add_argument('--cleanup-days', type=int, default=30,
                       help='Remove files older than N days')
    parser.add_argument('--compress-days', type=int, default=7,
                       help='Compress files older than N days')
    parser.add_argument('--max-storage-mb', type=float, default=1000,
                       help='Maximum storage in MB')
    parser.add_argument('--verify', action='store_true',
                       help='Verify file integrity')
    parser.add_argument('--report', action='store_true',
                       help='Show summary report')
    parser.add_argument('--cleanup', action='store_true',
                       help='Run cleanup operations')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Initialize tracker
    tracker = BOPMalagaTracker(args.tracking_file, args.download_dir)
    
    try:
        if args.verify:
            missing, corrupted = tracker.verify_files()
            print(f"Verification complete: {len(missing)} missing, {len(corrupted)} corrupted")
        
        if args.cleanup:
            # Run cleanup operations
            removed_old, size_old = tracker.cleanup_old_files(args.cleanup_days)
            removed_storage, size_storage = tracker.cleanup_by_storage_limit(args.max_storage_mb)
            compressed, space_saved = tracker.compress_old_files(args.compress_days)
            
            print(f"Cleanup complete:")
            print(f"  Removed {removed_old} old files ({size_old:.2f} MB)")
            print(f"  Removed {removed_storage} files for storage limit ({size_storage:.2f} MB)")
            print(f"  Compressed {compressed} files (saved {space_saved:.2f} MB)")
            
            # Save changes
            tracker.save_tracking_data()
        
        if args.report:
            report = tracker.get_summary_report()
            print(json.dumps(report, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
