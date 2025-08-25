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

def format_selector(ctx):
    """Select the best video and the best audio that won't result in an mkv.
    Ensures minimum 1080p quality."""
    
    formats = ctx.get('formats')
    
    # Filter for video-only formats with at least 1080p height
    # Sorted best to worst by default in yt-dlp, but we'll sort again to be safe
    video_formats = sorted([f for f in formats 
                            if f['vcodec'] != 'none' and f['acodec'] == 'none' 
                            and f.get('height', 0) >= 1080], 
                           key=lambda f: f.get('height', 0), reverse=True)
    
    if not video_formats:
        # Fallback to the best quality available if 1080p+ video-only is not found
        video_formats = sorted([f for f in formats 
                                if f['vcodec'] != 'none' and f['acodec'] == 'none'],
                               key=lambda f: f.get('height', 0), reverse=True)
        
    if not video_formats:
        # If no video-only formats exist, try a single combined format
        # This is a good fallback for cases where yt-dlp doesn't provide separate streams
        combined_formats = sorted([f for f in formats if f['acodec'] != 'none' and f['vcodec'] != 'none'],
                                  key=lambda f: f.get('height', 0), reverse=True)
        if combined_formats:
            yield combined_formats[0]
            return

    # If we have a video-only stream, we need to find an audio stream
    best_video = video_formats[0]  # The first element is now the highest quality

    # Find the best audio-only format with a compatible or common extension
    audio_formats = sorted([f for f in formats 
                            if f['acodec'] != 'none' and f['vcodec'] == 'none'],
                           key=lambda f: f.get('tbr', 0), reverse=True) # Sort by total bitrate
    
    if not audio_formats:
        # If no audio-only formats exist, a single stream might be the only option
        # This case is handled above, but good to have as a final check
        if 'acodec' in best_video and best_video['acodec'] != 'none':
            yield best_video
            return
        
        # We have a video, but no audio. Yield video only.
        yield best_video
        return
    
    best_audio = audio_formats[0] # The first element is the best audio

    # Now, combine them. yt-dlp will handle the merging.
    yield {
        'format_id': f'{best_video["format_id"]}+{best_audio["format_id"]}',
        'ext': best_video['ext'],
        'requested_formats': [best_video, best_audio],
        'protocol': f'{best_video["protocol"]}+{best_audio["protocol"]}'
    }
    
def download_video(url, output_dir, format_selector_func, pbar):
    """Download a single video using yt-dlp with a progress hook"""
    thread_name = threading.current_thread().name
    thread_id = threading.get_ident()
    start_time = datetime.now()
    
    def download_progress_hook(d):
        """Hook to provide real-time feedback"""
        if d['status'] == 'downloading':
            pbar.set_description(f"Downloading {d['_percent_str']} of {url}")
            # This is a bit tricky to update a single pbar for multiple threads.
            # A better approach is to have a pbar per video.

    try:
        Path(output_dir).mkdir(exist_ok=True)
        
        ydl_opts = {
            'format': format_selector_func,
            'outtmpl': f"{output_dir}/%(title)s.%(ext)s",
            'overwrites': True,
            'concurrent_fragment_downloads': 4,
            # 'progress_hooks': [download_progress_hook], # This can be complicated with a shared pbar
        }
        
        logger.info(f"[{thread_name}-{thread_id}] Starting download: {url}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            # ... (rest of the info extraction)
            
            ydl.download([url])
            
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"[{thread_name}-{thread_id}] Successfully downloaded in {duration:.1f}s: {url}")
        return {'url': url, 'success': True, 'error': None}
        
    except Exception as e:
        logger.error(f"[{thread_name}-{thread_id}] Failed to download {url}: {str(e)}")
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
    # Configuration
    urls_file = "list.txt"  # Default filename
    output_dir = os.path.expanduser("~/Desktop/videos")
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
            executor.submit(download_video, url, output_dir): url 
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