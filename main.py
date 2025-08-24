#!/usr/bin/env python3
import subprocess
import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def setup_logging():
    """Set up logging to both file and console"""
    log_filename = f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def download_video(url, output_dir="downloads", format_selector="webm"):
    """Download a single video using yt-dlp"""
    thread_id = threading.get_ident()
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(exist_ok=True)
        
        # Build yt-dlp command
        cmd = [
            "yt-dlp",
            "-f", format_selector,
            "-o", f"{output_dir}/%(title)s.%(ext)s",
            "--print", "%(format_id)s %(width)sx%(height)s %(ext)s",
            url
        ]
        
        logger.info(f"[Thread-{thread_id}] Starting download: {url}")
        
        # Run yt-dlp command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Extract quality info from output
        quality_info = result.stdout.strip() if result.stdout.strip() else "Unknown quality"
        logger.info(f"[Thread-{thread_id}] Downloaded quality: {quality_info}")
        logger.info(f"[Thread-{thread_id}] Successfully downloaded: {url}")
        return {'url': url, 'success': True, 'error': None}
        
    except subprocess.CalledProcessError as e:
        logger.error(f"[Thread-{thread_id}] Failed to download {url}: {e.stderr}")
        return {'url': url, 'success': False, 'error': e.stderr}
    except Exception as e:
        logger.error(f"[Thread-{thread_id}] Unexpected error for {url}: {str(e)}")
        return {'url': url, 'success': False, 'error': str(e)}

def read_urls_from_file(file_path):
    """Read URLs from text file, one per line"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        return urls
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return []

def main():
    """Main function to process all URLs"""
    # Configuration
    urls_file = "list.txt"  # Default filename
    output_dir = os.path.expanduser("~/Desktop/videos")
    format_selector = "bestvideo[height>=720]+bestaudio/best[height>=720][vcodec!=none]/best[vcodec!=none]"
    max_workers = 5 # Default number of concurrent downloads
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        urls_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    if len(sys.argv) > 3:
        format_selector = sys.argv[3]
    if len(sys.argv) > 4:
        try:
            max_workers = int(sys.argv[4])
        except ValueError:
            logger.warning(f"Invalid max_workers value: {sys.argv[4]}, using default: {max_workers}")
    
    logger.info(f"Starting batch download from: {urls_file}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Format: {format_selector}")
    logger.info(f"Max concurrent downloads: {max_workers}")
    
    # Read URLs from file
    urls = read_urls_from_file(urls_file)
    
    if not urls:
        logger.error("No URLs found to download")
        return
    
    logger.info(f"Found {len(urls)} URLs to download")
    
    # Download videos in parallel
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_url = {
            executor.submit(download_video, url, output_dir, format_selector): url 
            for url in urls
        }
        
        # Process completed downloads with progress bar
        with tqdm(total=len(urls), desc="Downloading videos") as pbar:
            for future in as_completed(future_to_url):
                result = future.result()
                if result['success']:
                    successful += 1
                else:
                    failed += 1
                pbar.update(1)
    
    # Summary
    logger.info(f"Download completed with {successful} successful ones and {failed} failed ones!")
    logger.info(f"Total: {len(urls)}")

if __name__ == "__main__":
    logger = setup_logging()
    main()