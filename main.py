#!/usr/bin/env python3
import yt_dlp
import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv

def setup_logging():
    """Set up logging to both file and console"""
    from dotenv import load_dotenv
    load_dotenv()
    
    log_dir = os.getenv("LOG_DIR", "./logs")
    
    # Create log directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    log_filename = os.path.join(log_dir, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# FIXED: Initialized logger at the module level for wider scope
logger = setup_logging()

def format_selector(ctx):
    """Select the best 1080p+ video and the best audio."""
    formats = ctx.get('formats')

    # Filter for 1080p+ video-only streams, and sort by bitrate (tbr)
    # This is the most reliable way to get the "highest quality"
    video_formats = sorted([f for f in formats
                            if f['vcodec'] != 'none' and f.get('height', 0) >= 1080
                            and f.get('acodec') == 'none'],
                           key=lambda f: f.get('tbr', 0), reverse=True)

    if not video_formats:
        # Fallback to the best quality available if 1080p+ video-only is not found
        video_formats = sorted([f for f in formats
                                if f['vcodec'] != 'none' and f.get('acodec') == 'none'],
                               key=lambda f: f.get('tbr', 0), reverse=True)

    if not video_formats:
        # If no video-only streams exist, try a single combined format
        # Sort by resolution then bitrate
        combined_formats = sorted([f for f in formats if f['acodec'] != 'none' and f['vcodec'] != 'none'],
                                  key=lambda f: (f.get('height', 0), f.get('tbr', 0)), reverse=True)
        if combined_formats:
            yield combined_formats[0]
            return

    # If we are here, we must have found a video-only format
    if not video_formats:
        # This is a final fallback in case logic fails, shouldn't be reached
        # if a video exists at all.
        logger.warning("Could not find a suitable video format.")
        return

    best_video = video_formats[0]

    # Find the best audio-only format
    audio_formats = sorted([f for f in formats
                            if f['acodec'] != 'none' and f['vcodec'] == 'none'],
                           key=lambda f: f.get('tbr', 0), reverse=True)

    if not audio_formats:
        # We have a video, but no audio. Yield video only.
        logger.warning(f"No separate audio stream found for {best_video.get('title', 'video')}. Downloading video only.")
        yield best_video
        return

    best_audio = audio_formats[0]

    # Combine and yield
    yield {
        'format_id': f'{best_video["format_id"]}+{best_audio["format_id"]}',
        'ext': best_video['ext'],
        'requested_formats': [best_video, best_audio],
        'protocol': f'{best_video["protocol"]}+{best_audio["protocol"]}'
    }

def download_video(url, output_dir, format_selector_func):
    """Download a single video using yt-dlp"""
    thread_name = threading.current_thread().name
    thread_id = threading.get_ident()
    start_time = datetime.now()
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        ydl_opts = {
            'format': format_selector_func,
            'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
            'overwrites': False, # Changed to False to avoid re-downloading
            'no_overwrites': True,
            'concurrent_fragment_downloads': 4,
            # 'progress_hooks': [download_progress_hook],
            'quiet': True, # Suppress yt-dlp's own console output
            'no_warnings': True,
        }
        
        logger.info(f"[{thread_name}-{thread_id}] Starting download: {url}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # The download happens here
            ydl.download([url])
            
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"[{thread_name}-{thread_id}] Successfully downloaded in {duration:.1f}s: {url}")
        return {'url': url, 'success': True, 'error': None}
        
    except yt_dlp.utils.DownloadError as e:
        # Handle specific download errors, like already downloaded files
        if "has already been downloaded" in str(e):
            logger.info(f"[{thread_name}-{thread_id}] Skipping already downloaded file: {url}")
            return {'url': url, 'success': True, 'error': 'Already downloaded'}
        else:
            logger.error(f"[{thread_name}-{thread_id}] Failed to download {url}: {str(e)}")
            return {'url': url, 'success': False, 'error': str(e)}
    except Exception as e:
        logger.error(f"[{thread_name}-{thread_id}] An unexpected error occurred for {url}: {str(e)}")
        return {'url': url, 'success': False, 'error': str(e)}

def read_urls_from_file(file_path):
    """Read URLs from text file, one per line"""
    try:
        urls = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    urls.append(stripped_line)
        return urls
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return []

def main():
    """Main function to process all URLs"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Configuration
    urls_file = "list.txt"  # Default filename
    output_dir = os.getenv("OUTPUT_DIR")
    max_workers = 5 # Default number of concurrent downloads
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        urls_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    if len(sys.argv) > 3:
        try:
            max_workers = int(sys.argv[3])
        except ValueError:
            logger.warning(f"Invalid max_workers value: {sys.argv[3]}, using default: {max_workers}")
    
    logger.info(f"Starting batch download from: {urls_file}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Using custom format selector for best quality 1080p+")
    logger.info(f"Max concurrent downloads: {max_workers}")
    
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
        with tqdm(total=len(urls), desc="Overall Progress", unit="video") as pbar:
            for future in as_completed(future_to_url):
                result = future.result()
                if result['success']:
                    successful += 1
                else:
                    failed += 1
                pbar.update(1)
    
    # Summary
    logger.info(f"Download completed with {successful} successful downloads and {failed} failures.")
    logger.info(f"Total URLs processed: {len(urls)}")

if __name__ == "__main__":
    main()