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
import json
import lz4.block
import glob
import subprocess

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

def get_firefox_tabs_places():
    """Extract URLs from Firefox places.sqlite database"""
    try:
        import sqlite3
        
        # Find Firefox profile directory
        home = os.path.expanduser("~")
        profile_paths = glob.glob(f"{home}/Library/Application Support/Firefox/Profiles/*/places.sqlite")
        
        if not profile_paths:
            logger.error("No Firefox places.sqlite found")
            return []
            
        places_db = profile_paths[0]  # Use first profile
        logger.info(f"Reading Firefox history from: {places_db}")
        
        # Copy the database to avoid locks
        import tempfile
        import shutil
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            shutil.copy2(places_db, tmp.name)
            temp_db = tmp.name
        
        try:
            conn = sqlite3.connect(temp_db)
            cursor = conn.cursor()
            
            # Get recent URLs (last 50 visited)
            cursor.execute("""
                SELECT DISTINCT url FROM moz_places 
                WHERE url LIKE 'http%' 
                ORDER BY last_visit_date DESC 
                LIMIT 50
            """)
            
            urls = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            logger.info(f"Found {len(urls)} recent URLs from Firefox history")
            return urls[:10]  # Return top 10 most recent
            
        finally:
            os.unlink(temp_db)  # Clean up temp file
            
    except ImportError:
        logger.error("sqlite3 not available")
        return []
    except Exception as e:
        logger.error(f"Error reading Firefox history: {str(e)}")
        return []

def get_firefox_tabs_applescript():
    """Get URLs from open Firefox tabs using AppleScript with keyboard shortcuts"""
    try:
        # Use keyboard shortcuts to get URLs from each tab
        applescript = '''
        tell application "Firefox" to activate
        delay 1
        
        set tabUrls to {}
        set tabCount to 0
        
        tell application "System Events"
            tell process "Firefox"
                -- First, get the number of tabs by using Cmd+1, Cmd+2, etc until we get an error
                repeat with i from 1 to 50  -- max 50 tabs
                    try
                        -- Switch to tab i using Cmd+number
                        key code (i + 17) using command down  -- 18=Cmd+1, 19=Cmd+2, etc
                        delay 0.3
                        
                        -- Select address bar and copy URL
                        key code 37 using command down  -- Cmd+L to select address bar
                        delay 0.2
                        key code 8 using command down   -- Cmd+C to copy
                        delay 0.2
                        
                        -- Get the copied URL from clipboard
                        set clipboardContent to (the clipboard as string)
                        
                        if clipboardContent starts with "http" then
                            set end of tabUrls to clipboardContent
                            set tabCount to tabCount + 1
                        end if
                        
                    on error
                        -- No more tabs, exit loop
                        exit repeat
                    end try
                    
                    -- Safety check - if we haven't found a new URL in the last few tries, stop
                    if i > tabCount + 5 then exit repeat
                end repeat
                
                return tabUrls
            end tell
        end tell
        '''
        
        logger.info("Attempting to get Firefox tabs via AppleScript keyboard automation...")
        logger.info("Note: This will briefly switch between tabs to copy URLs")
        
        result = subprocess.run(['osascript', '-e', applescript], 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0 and result.stdout.strip():
            # Parse the AppleScript result
            urls_text = result.stdout.strip()
            # AppleScript returns comma-separated values
            urls = [url.strip() for url in urls_text.split(',') if url.strip()]
            
            # Filter for HTTP/HTTPS URLs and remove duplicates while preserving order
            seen = set()
            http_urls = []
            for url in urls:
                if url.startswith(('http://', 'https://')) and url not in seen:
                    http_urls.append(url)
                    seen.add(url)
            
            logger.info(f"Found {len(http_urls)} unique open tabs in Firefox")
            return http_urls
        else:
            logger.warning(f"AppleScript failed or returned no data. Error: {result.stderr}")
            return []
            
    except subprocess.TimeoutExpired:
        logger.error("AppleScript timed out after 60 seconds")
        return []
    except Exception as e:
        logger.error(f"Error running AppleScript: {e}")
        return []

def get_firefox_tabs_manual():
    """Prompt user to manually enter URLs since automated methods failed"""
    logger.info("Automated Firefox tab reading failed.")
    logger.info("Please copy and paste your Firefox tab URLs (one per line), then press Enter twice when done:")
    
    urls = []
    while True:
        try:
            url = input().strip()
            if not url:
                break
            if url.startswith(('http://', 'https://')):
                urls.append(url)
                logger.info(f"Added: {url}")
            else:
                logger.warning(f"Skipped non-HTTP URL: {url}")
        except KeyboardInterrupt:
            break
    
    return urls

def get_firefox_tabs():
    """Extract URLs from open Firefox tabs using sessionstore.jsonlz4"""
    try:
        # Find Firefox profile directory
        home = os.path.expanduser("~")
        profile_paths = [
            f"{home}/Library/Application Support/Firefox/Profiles/*/sessionstore.jsonlz4",
            f"{home}/.mozilla/firefox/*/sessionstore.jsonlz4"
        ]
        
        sessionstore_files = []
        for pattern in profile_paths:
            sessionstore_files.extend(glob.glob(pattern))
        
        if not sessionstore_files:
            logger.error("No Firefox sessionstore files found. Trying AppleScript instead...")
            return get_firefox_tabs_applescript()
        
        # Use the most recent sessionstore file
        latest_file = max(sessionstore_files, key=os.path.getmtime)
        logger.info(f"Reading Firefox tabs from: {latest_file}")
        
        # Read and decompress the file
        with open(latest_file, 'rb') as f:
            # Skip the first 8 bytes (magic number)
            f.read(8)
            compressed_data = f.read()
        
        # Decompress using lz4
        decompressed_data = lz4.block.decompress(compressed_data)
        session_data = json.loads(decompressed_data.decode('utf-8'))
        
        # Extract URLs from all windows and tabs
        urls = []
        all_urls = []  # For debugging
        for window in session_data.get('windows', []):
            for tab in window.get('tabs', []):
                entries = tab.get('entries', [])
                if entries:
                    # Get the current URL (last entry)
                    current_entry = entries[-1]
                    url = current_entry.get('url', '')
                    if url:
                        all_urls.append(url)  # Collect all URLs for debugging
                        if url.startswith(('http://', 'https://')):
                            urls.append(url)
        
        logger.info(f"Found {len(all_urls)} total tabs: {all_urls}")
        logger.info(f"Found {len(urls)} HTTP/HTTPS tabs in Firefox")
        
        return urls
        
    except ImportError:
        logger.error("lz4 library not found. Trying Firefox history instead...")
        history_urls = get_firefox_tabs_places()
        if history_urls:
            return history_urls
        else:
            return None
    except Exception as e:
        logger.error(f"Error reading Firefox tabs: {str(e)}. Trying Firefox history...")
        history_urls = get_firefox_tabs_places()
        if history_urls:
            return history_urls
        else:
            return None

def main():
    """Main function to process all URLs"""
    # Configuration
    urls_file = "list.txt"  # Default filename
    output_dir = os.path.expanduser("~/Desktop/videos/firefox")
    max_workers = 5 # Default number of concurrent downloads
    use_firefox = False  # New flag for Firefox tabs
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--firefox":
            use_firefox = True
        else:
            urls_file = sys.argv[1]
    if len(sys.argv) > 2 and not use_firefox:
        output_dir = sys.argv[2]
    elif len(sys.argv) > 2 and use_firefox:
        output_dir = sys.argv[2]
    if len(sys.argv) > 3:
        try:
            max_workers = int(sys.argv[3])
        except ValueError:
            logger.warning(f"Invalid max_workers value: {sys.argv[3]}, using default: {max_workers}")
    
    if use_firefox:
        logger.info("Starting batch download from Firefox tabs")
    else:
        logger.info(f"Starting batch download from: {urls_file}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Using custom format selector for best quality 1080p+")
    logger.info(f"Max concurrent downloads: {max_workers}")
    
    # Read URLs from Firefox tabs or file
    if use_firefox:
        urls = get_firefox_tabs()
    else:
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