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
logger = logging.getLogger(__name__)


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


def format_selector(ctx):
    """Select the best 1080p+ video and the best audio."""
    formats = ctx.get('formats')

    # Filter for 1080p+ video-only streams, and sort by bitrate (tbr)
    # This is the most reliable way to get the "highest quality"
    # vcodec != 'none' ensures it's a video stream
    # acodec == 'none' ensures it's a video-only stream
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

    best_video = video_formats[0]

    # Find the best audio-only format
    audio_formats = sorted([f for f in formats
                            if f['acodec'] != 'none' and f['vcodec'] == 'none'],
                           key=lambda f: f.get('tbr', 0), reverse=True)

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
    # thread_name = threading.current_thread().name
    # thread_id = threading.get_ident()
    # start_time = datetime.now()
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        ydl_opts = {
            'format': format_selector_func,
            'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
            'overwrites': False,  # Changed to False to avoid re-downloading
            'no_overwrites': True,
            'concurrent_fragment_downloads': 4,
            # 'progress_hooks': [download_progress_hook],
            'quiet': True,  # Suppress yt-dlp's own console output
            'no_warnings': True,
        }

        # logger.info(f"[{thread_name}-{thread_id}] Starting download: {url}")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # The download happens here
            ydl.download([url])

        # end_time = datetime.now()
        # duration = (end_time - start_time).total_seconds()
        # logger.info(f"[{thread_name}-{thread_id}] Successfully downloaded in {duration:.1f}s: {url}")
        return {'url': url, 'success': True, 'error': None}

    except Exception as e:
        # logger.error(f"[{thread_name}-{thread_id}] An unexpected error occurred for {url}: {str(e)}")
        return {'url': url, 'success': False, 'error': str(e)}


def process_urls_parallel(urls, output_dir, max_workers):
    """Process URLs using parallel threading"""
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

    return successful, failed


def process_urls_sequential(urls, output_dir):
    """Process URLs sequentially (one by one) for easier debugging"""
    successful = 0
    failed = 0

    with tqdm(total=len(urls), desc="Overall Progress", unit="video") as pbar:
        for url in urls:
            result = download_video(url, output_dir, format_selector)
            if result['success']:
                successful += 1
            else:
                failed += 1
            pbar.update(1)

    return successful, failed


def main():
    """Main function to process all URLs"""
    # Load environment variables from .env file
    load_dotenv()

    # Configuration
    urls_file = os.getenv("INPUT_FILE")
    output_dir = os.getenv("OUTPUT_DIR")
    max_workers = int(os.getenv("MAX_WORKERS"))
    use_threading = os.getenv("USE_THREADING", "false").lower() == "true"

    logger.info(f"Using custom format selector for best quality 1080p+")
    logger.info(f"Threading mode: {'Enabled' if use_threading else 'Disabled (Sequential)'}")
    if use_threading:
        logger.info(f"Max concurrent downloads: {max_workers}")

    urls = read_urls_from_file(urls_file)
    if not urls:
        logger.error("No URLs found to download")
        return
    logger.info(f"Found {len(urls)} URLs to download")

    # Download videos based on threading configuration
    if use_threading:
        successful, failed = process_urls_parallel(urls, output_dir, max_workers)
    else:
        successful, failed = process_urls_sequential(urls, output_dir)

    # Summary
    logger.info(f"Download completed with {successful} successful downloads")
    logger.info(f"Download completed with {failed} failures downloads")
    logger.info(f"Total URLs processed: {len(urls)}")


if __name__ == "__main__":
    main()
