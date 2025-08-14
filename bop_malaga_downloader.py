#!/usr/bin/env python3
"""
BOP Málaga PDF Downloader

A comprehensive script to download PDF documents from the Boletín Oficial 
de la Provincia de Málaga (BOP Málaga) website. This script:

- Fetches daily summaries from https://www.bopmalaga.es/sumario.php
- Parses HTML to extract edicto IDs and PDF download links
- Downloads PDFs while respecting robots.txt crawl delay (5 seconds)
- Implements rate limiting and comprehensive error handling
- Validates URLs and prevents duplicate downloads
- Provides structured logging for monitoring and debugging

Author: Automated Deployment System
Date: 2025
"""

import os
import sys
import time
import json
import logging
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Set, Optional, Tuple
import pytz

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)


class BOPMalagaDownloader:
    """Main class for downloading BOP Málaga PDF documents."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the downloader with configuration."""
        self.base_url = "https://www.bopmalaga.es"
        self.summary_url = f"{self.base_url}/sumario.php"
        self.download_url_template = f"{self.base_url}/descarga.php?archivo={{filename}}"
        
        # Default configuration
        self.config = {
            'download_dir': './downloads',
            'logs_dir': './logs',
            'tracking_file': './tracking.json',
            'crawl_delay': 5.0,  # seconds, as per robots.txt
            'max_retries': 3,
            'timeout': 30,
            'user_agent': 'BOP-Malaga-Downloader/1.0 (Educational/Research Purpose)',
            'max_storage_mb': 1000,  # 1GB default
            'cleanup_days': 30,
            'log_level': 'INFO'
        }
        
        # Load custom configuration if provided
        if config_path and os.path.exists(config_path):
            self.load_config(config_path)
        
        # Initialize directories
        self.setup_directories()
        
        # Initialize logging
        self.setup_logging()
        
        # Initialize session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Load tracking data
        self.downloaded_edictos = self.load_tracking_data()
        
        # Statistics
        self.stats = {
            'processed': 0,
            'downloaded': 0,
            'skipped': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        self.logger.info("BOP Málaga Downloader initialized")
    
    def load_config(self, config_path: str) -> None:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                custom_config = json.load(f)
                self.config.update(custom_config)
            self.logger.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            print(f"Warning: Could not load config from {config_path}: {e}")
    
    def setup_directories(self) -> None:
        """Create necessary directories."""
        for dir_path in [self.config['download_dir'], self.config['logs_dir']]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self) -> None:
        """Setup structured logging."""
        log_file = os.path.join(self.config['logs_dir'], 'bop_downloader.log')
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, self.config['log_level'].upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger('BOPMalagaDownloader')
    
    def load_tracking_data(self) -> Set[str]:
        """Load previously downloaded edicto IDs from tracking file."""
        tracking_file = self.config['tracking_file']
        if os.path.exists(tracking_file):
            try:
                with open(tracking_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('downloaded_edictos', []))
            except Exception as e:
                self.logger.warning(f"Could not load tracking data: {e}")
        return set()
    
    def save_tracking_data(self) -> None:
        """Save downloaded edicto IDs to tracking file."""
        tracking_file = self.config['tracking_file']
        try:
            data = {
                'downloaded_edictos': list(self.downloaded_edictos),
                'last_updated': datetime.now().isoformat(),
                'total_downloads': len(self.downloaded_edictos)
            }
            with open(tracking_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"Tracking data saved to {tracking_file}")
        except Exception as e:
            self.logger.error(f"Could not save tracking data: {e}")
    
    def validate_url(self, url: str) -> bool:
        """Validate URL format and domain."""
        try:
            parsed = urlparse(url)
            return (
                parsed.scheme in ['http', 'https'] and
                'bopmalaga.es' in parsed.netloc and
                len(url) < 2000  # Reasonable URL length limit
            )
        except Exception:
            return False
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe filesystem storage."""
        # Remove or replace unsafe characters
        unsafe_chars = '<>:"/\\|?*'
        for char in unsafe_chars:
            filename = filename.replace(char, '_')
        
        # Limit filename length
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:250] + ext
        
        return filename
    
    def make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with error handling and rate limiting."""
        if not self.validate_url(url):
            self.logger.error(f"Invalid URL: {url}")
            return None
        
        for attempt in range(self.config['max_retries']):
            try:
                # Respect crawl delay
                time.sleep(self.config['crawl_delay'])
                
                response = self.session.get(
                    url,
                    timeout=self.config['timeout'],
                    **kwargs
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    self.logger.warning(f"Resource not found (404): {url}")
                    return None
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.config['max_retries'] - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        self.logger.error(f"All request attempts failed for {url}")
        return None
    
    def extract_edicto_links(self, html_content: str) -> List[Dict[str, str]]:
        """Extract edicto information from HTML content."""
        edictos = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for edicto links - based on the pattern observed
            # Links typically have format: edicto.php?edicto=YYYYMMDD-NNNNN-YYYY-NN
            edicto_links = soup.find_all('a', href=lambda x: x and 'edicto.php?edicto=' in x)
            
            for link in edicto_links:
                href = link.get('href')
                if not href:
                    continue
                
                # Extract edicto ID from URL
                if 'edicto=' in href:
                    edicto_id = href.split('edicto=')[1].split('&')[0]
                    
                    # Validate edicto ID format (YYYYMMDD-NNNNN-YYYY-NN)
                    if self.validate_edicto_id(edicto_id):
                        # Generate PDF filename
                        pdf_filename = f"{edicto_id}.pdf"
                        pdf_url = self.download_url_template.format(filename=pdf_filename)
                        
                        # Extract title/description if available
                        title = link.get_text(strip=True) or "Unknown"
                        
                        edictos.append({
                            'id': edicto_id,
                            'title': title,
                            'pdf_url': pdf_url,
                            'pdf_filename': pdf_filename
                        })
            
            # Also look for direct PDF download links
            pdf_links = soup.find_all('a', href=lambda x: x and 'descarga.php?archivo=' in x)
            
            for link in pdf_links:
                href = link.get('href')
                if not href:
                    continue
                
                # Extract filename from URL
                if 'archivo=' in href:
                    filename = href.split('archivo=')[1].split('&')[0]
                    
                    # Extract edicto ID from filename (remove .pdf extension)
                    edicto_id = filename.replace('.pdf', '')
                    
                    if self.validate_edicto_id(edicto_id):
                        full_url = urljoin(self.base_url, href)
                        title = link.get_text(strip=True) or "Unknown"
                        
                        # Avoid duplicates
                        if not any(e['id'] == edicto_id for e in edictos):
                            edictos.append({
                                'id': edicto_id,
                                'title': title,
                                'pdf_url': full_url,
                                'pdf_filename': filename
                            })
            
            self.logger.info(f"Extracted {len(edictos)} edicto links from HTML")
            return edictos
            
        except Exception as e:
            self.logger.error(f"Error extracting edicto links: {e}")
            return []
    
    def validate_edicto_id(self, edicto_id: str) -> bool:
        """Validate edicto ID format: YYYYMMDD-NNNNN-YYYY-NN"""
        try:
            parts = edicto_id.split('-')
            if len(parts) != 4:
                return False
            
            # Check date part (YYYYMMDD)
            date_part = parts[0]
            if len(date_part) != 8 or not date_part.isdigit():
                return False
            
            # Validate date
            year = int(date_part[:4])
            month = int(date_part[4:6])
            day = int(date_part[6:8])
            
            if not (2000 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31):
                return False
            
            # Check other parts are numeric
            if not (parts[1].isdigit() and parts[2].isdigit() and parts[3].isdigit()):
                return False
            
            return True
            
        except (ValueError, IndexError):
            return False
    
    def download_pdf(self, edicto: Dict[str, str]) -> bool:
        """Download a single PDF file."""
        edicto_id = edicto['id']
        pdf_url = edicto['pdf_url']
        filename = edicto['pdf_filename']
        
        # Check if already downloaded
        if edicto_id in self.downloaded_edictos:
            self.logger.debug(f"Skipping already downloaded edicto: {edicto_id}")
            self.stats['skipped'] += 1
            return True
        
        # Sanitize filename
        safe_filename = self.sanitize_filename(filename)
        file_path = os.path.join(self.config['download_dir'], safe_filename)
        
        # Check if file already exists
        if os.path.exists(file_path):
            self.logger.info(f"File already exists: {safe_filename}")
            self.downloaded_edictos.add(edicto_id)
            self.stats['skipped'] += 1
            return True
        
        try:
            self.logger.info(f"Downloading: {edicto_id} -> {safe_filename}")
            
            response = self.make_request(pdf_url, stream=True)
            if not response:
                self.stats['errors'] += 1
                return False
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                self.logger.warning(f"Unexpected content type for {edicto_id}: {content_type}")
            
            # Download file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verify file was downloaded
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                self.logger.info(f"Successfully downloaded: {safe_filename} ({os.path.getsize(file_path)} bytes)")
                self.downloaded_edictos.add(edicto_id)
                self.stats['downloaded'] += 1
                return True
            else:
                self.logger.error(f"Downloaded file is empty or missing: {safe_filename}")
                if os.path.exists(file_path):
                    os.remove(file_path)
                self.stats['errors'] += 1
                return False
                
        except Exception as e:
            self.logger.error(f"Error downloading {edicto_id}: {e}")
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            self.stats['errors'] += 1
            return False
    
    def fetch_daily_summary(self, date: Optional[datetime] = None) -> Optional[str]:
        """Fetch daily summary HTML from BOP Málaga website."""
        if date:
            # Format date for URL parameter
            date_str = date.strftime('%Y-%m-%d')
            url = f"{self.summary_url}?fecha={date_str}"
        else:
            url = self.summary_url
        
        self.logger.info(f"Fetching daily summary from: {url}")
        
        response = self.make_request(url)
        if response:
            return response.text
        else:
            self.logger.error(f"Failed to fetch daily summary from {url}")
            return None
    
    def run(self, date: Optional[datetime] = None) -> Dict[str, int]:
        """Main execution method."""
        self.logger.info("Starting BOP Málaga PDF download process")
        
        try:
            # Fetch daily summary
            html_content = self.fetch_daily_summary(date)
            if not html_content:
                self.logger.error("Could not fetch daily summary")
                return self.stats
            
            # Extract edicto links
            edictos = self.extract_edicto_links(html_content)
            if not edictos:
                self.logger.info("No edictos found in daily summary")
                return self.stats
            
            self.logger.info(f"Found {len(edictos)} edictos to process")
            
            # Download PDFs
            for edicto in edictos:
                self.stats['processed'] += 1
                self.download_pdf(edicto)
            
            # Save tracking data
            self.save_tracking_data()
            
            # Log final statistics
            duration = datetime.now() - self.stats['start_time']
            self.logger.info(f"Download process completed in {duration}")
            self.logger.info(f"Statistics: {self.stats}")
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"Unexpected error in main execution: {e}")
            self.stats['errors'] += 1
            return self.stats
        
        finally:
            # Cleanup
            if hasattr(self, 'session'):
                self.session.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BOP Málaga PDF Downloader')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--date', help='Specific date to download (YYYY-MM-DD format)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Parse date if provided
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD format.")
            sys.exit(1)
    
    # Initialize downloader
    try:
        downloader = BOPMalagaDownloader(config_path=args.config)
        
        if args.verbose:
            downloader.config['log_level'] = 'DEBUG'
            downloader.setup_logging()
        
        # Run download process
        stats = downloader.run(date=target_date)
        
        # Exit with appropriate code
        if stats['errors'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\nDownload process interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
