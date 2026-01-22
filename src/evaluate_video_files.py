"""
Video File Evaluation for iCloud Re-ingestion

This module evaluates video files to determine if they are ready for re-upload to iCloud.
iCloud requires specific video formats and is picky about date metadata, so this script
analyzes video files for codec compatibility, HDR/SDR status, container format, and date
information, then categorizes them accordingly.

Logic Flow:
-----------
1. Recursively crawls a root folder for video files with specified extensions (mkv, mp4, mov)

2. For each video file found, applies the following decision tree:

   a) SKIP if:
      - File extension doesn't match ONLY_HANDLE_THESE_VIDEO_EXTENSIONS
      - File path contains any keyword from SKIPLIST_PARTIAL_MATCH
      - ffprobe fails to analyze the file
      - No creation date in metadata AND no year (20XX) found in parent folder path
      - No creation date in metadata AND file's modified date year doesn't match folder path year

   b) MOVE (copy without conversion) if:
      - Video has creation date metadata AND is already in compatible format:
        * Container: MOV
        * Video codec: HEVC with hvc1 tag (not HDR)
        * Audio codec: AAC

   c) CONVERT (re-encode to compatible format) if:
      - Video has creation date metadata BUT needs format changes:
        * Container not MOV → remux to MOV
        * Video not hvc1 HEVC → re-encode to libx265 with hvc1 tag
        * Video is HDR → convert to SDR (Rec.709, yuv420p)
        * Audio not AAC → re-encode to AAC

3. Video Stream Analysis:
   - Checks if video codec is HEVC with hvc1 tag (required for iCloud thumbnails)
   - Detects HDR content by checking:
     * Explicit HDR transfer functions (smpte2084, arib-std-b67)
     * Dolby Vision pixel formats
     * 10-bit+ depth with wide color gamut (bt2020)
   - HDR videos are ALWAYS converted to SDR even if already hvc1

4. Audio Stream Analysis:
   - Checks if audio codec is AAC
   - Preserves original channel count (mono or stereo)
   - Re-encodes to AAC only if needed

5. Metadata Handling:
   - Extracts creation date from video metadata tags:
     * 'creation_time' (standard)
     * 'com.apple.quicktime.creationdate' (Apple-specific, case-insensitive)
   - If no metadata date found, uses file mtime BUT only if year matches folder path
   - Preserves Apple QuickTime metadata (make, model, software) if present

6. Results are collected in a list of dictionaries with:
   - file: absolute file path
   - action: 'skip', 'move', or 'convert'
   - reason: detailed explanation (e.g., 'convert: video codec+HDR to SDR+container')
   - creation_time: ISO 8601 timestamp for date metadata
   - apple_metadata: JSON string of Apple QuickTime tags
   - audio_channels: number of audio channels (1=mono, 2=stereo)
   - video_codec_needed: 1 if video needs re-encoding, 0 if can copy
   - audio_codec_needed: 1 if audio needs re-encoding, 0 if can copy

7. Results are exported to:
   - Console output (pandas DataFrame with all columns visible)
   - CSV file with '@' as column separator for use by process_video_files.py

Output Actions:
--------------
- 'move': File is ready for iCloud (already compatible or just needs container change)
- 'convert': File needs video/audio re-encoding (codec, HDR→SDR, or format changes)
- 'skip': File should not be processed (incompatible, no date metadata, or skiplist match)

Conversion Reasons:
------------------
The 'reason' field for 'convert' actions indicates which streams need conversion:
- 'convert: video codec' - Video is not hvc1 HEVC
- 'convert: HDR to SDR' - Video is HDR and needs SDR conversion
- 'convert: audio codec' - Audio is not AAC
- 'convert: container' - Container is not MOV
- Combined: 'convert: video codec+HDR to SDR+audio codec+container'

Configuration:
-------------
- ROOT_VIDEO_FOLDER: Starting directory for file crawl
- ONLY_HANDLE_THESE_VIDEO_EXTENSIONS: File types to process (case-insensitive)
- SKIPLIST_PARTIAL_MATCH: Keywords to exclude files (e.g., specific albums)
- FFMPEG_BINARY_PATH: Path to ffmpeg installation for video analysis
- OUTPUT_CSV_FILE: CSV filename for evaluation report

Example folder structure:
------------------------
data/2018/2018-07-17 Summer Vacation/video_001.mkv
     ^^^^                                          <- Year extracted from path

Scenario 1 - iPhone video with metadata:
- Has com.apple.quicktime.creationdate: 2018-07-17T19:37:54+0200
- Container: MOV, Video: hvc1, Audio: AAC, Color: SDR
→ action: 'move', reason: 'fully compatible already'

Scenario 2 - GoPro video needing conversion:
- Has creation_time: 2018-07-17T14:22:10Z
- Container: MP4, Video: h264, Audio: AAC
→ action: 'convert', reason: 'convert: video codec+container'

Scenario 3 - HDR video from modern camera:
- Has creation_time: 2018-07-17T16:45:33Z
- Container: MOV, Video: hvc1 (HDR/bt2020), Audio: AAC
→ action: 'convert', reason: 'convert: HDR to SDR'

Scenario 4 - WhatsApp video without metadata:
- No creation_time in metadata
- File modified date: 2018-08-03
- Folder path contains: 2018
→ action: 'move', reason: 'date modified year correct'
→ Uses file mtime as creation_time

Scenario 5 - Old video with wrong file date:
- No creation_time in metadata
- File modified date: 2024-01-15 (recently copied)
- Folder path contains: 2018
→ action: 'skip', reason: 'file modified time mismatch'
"""

import os
import subprocess
from datetime import datetime
import pandas as pd
import json
from src.utils import should_skip_by_partial_match, extract_year_from_path


### CONFIGURATION ###
ROOT_VIDEO_FOLDER = "data/2018/videos"
ONLY_HANDLE_THESE_VIDEO_EXTENSIONS = ["mkv", "mp4", "mov"] # case insensitive
SKIPLIST_PARTIAL_MATCH = ["GOEF", "Elia", "BBM", "Trash", "small"]
FFMPEG_BINARY_PATH = r"C:\Program Files\ffmpeg\bin\ffmpeg.exe"
OUTPUT_CSV_FILE = "icloud_video_report.csv"


# Pandas display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def get_video_stream_info(file_path):
    cmd = [FFMPEG_BINARY_PATH.replace('ffmpeg.exe', 'ffprobe.exe'),
           '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', file_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)

def get_creation_time_from_metadata(metadata):
    """Extract creation time from video metadata only (no fallback to file mtime)"""
    for section in ['format', 'streams']:
        if section in metadata:
            items = metadata[section] if isinstance(metadata[section], list) else [metadata[section]]
            for item in items:
                tags = item.get('tags', {})
                for k, v in tags.items():
                    if k.lower() == 'creation_time':
                        return v
                    if k.lower() == 'com.apple.quicktime.creationdate':
                        return v
    return None

def get_file_mtime_as_iso(file_path):
    """Get file modification time as ISO 8601 string"""
    if file_path and os.path.exists(file_path):
        mod_time = os.path.getmtime(file_path)
        return datetime.utcfromtimestamp(mod_time).isoformat() + 'Z'
    return None

def extract_apple_metadata(metadata):
    # Extract Apple QuickTime metadata if present
    apple_keys = [
        'com.apple.quicktime.make',
        'com.apple.quicktime.model',
        'com.apple.quicktime.software',
    ]
    found = {}
    for section in ['format', 'streams']:
        if section in metadata:
            items = metadata[section] if isinstance(metadata[section], list) else [metadata[section]]
            for item in items:
                tags = item.get('tags', {})
                for k, v in tags.items():
                    if k.lower() in apple_keys:
                        found[k] = v
    return found if found else None

def is_hdr_stream(stream):
    # HDR is opt-in and explicitly signalled.
    trc = (stream.get('color_transfer')
           or stream.get('color_trc')
           or '').lower()
    primaries = (stream.get('color_primaries') or '').lower()
    pix_fmt = (stream.get('pix_fmt') or '').lower()

    # Explicit HDR transfer functions
    if trc in ('smpte2084', 'arib-std-b67'):
        return True

    # Dolby Vision sometimes signals this way
    if 'dovi' in pix_fmt:
        return True

    # 10-bit or higher is REQUIRED for HDR
    if any(x in pix_fmt for x in ('10', '12', '16')):
        # Only consider HDR if wide gamut is present
        if primaries in ('bt2020', 'bt2020nc'):
            return True

    return False


def crawl_and_evaluate(root_folder_path, video_extensions, skiplist):
    results = []
    for folder_path, _, file_names in os.walk(root_folder_path):
        for file_name in file_names:
            file_path = os.path.abspath(os.path.join(folder_path, file_name))
            entry = {'file': file_path}
            results.append(entry)

            ext = os.path.splitext(file_name)[1][1:].lower()
            if ext not in video_extensions:
                entry['action'] = 'skip'
                entry['reason'] = 'wrong extension'
                continue

            if should_skip_by_partial_match(file_path, skiplist):
                entry['action'] = 'skip'
                entry['reason'] = 'skiplist match'
                continue

            info = get_video_stream_info(file_path)
            if not info:
                entry['action'] = 'skip'
                entry['reason'] = 'ffprobe failed'
                continue

            # Determine if re-encode is needed
            video_codec_needed = True
            audio_codec_needed = True
            container_needed = ext != 'mov'
            audio_channels = 2
            hdr_to_sdr_needed = False
            video_reason = None
            audio_reason = None
            container_reason = None
            hdr_reason = None
            for stream in info.get('streams', []):
                if stream.get('codec_type') == 'video':
                    # Check codec/tag
                    is_hvc1 = stream.get('codec_name') == 'hevc' and stream.get('codec_tag_string', '').lower() == 'hvc1'
                    # Check HDR/SDR
                    is_hdr = is_hdr_stream(stream)
                    if is_hvc1 and not is_hdr:
                        video_codec_needed = False
                    else:
                        video_codec_needed = True
                        if not is_hvc1:
                            video_reason = 'video codec'
                        if is_hdr:
                            hdr_to_sdr_needed = True
                            hdr_reason = 'HDR to SDR'
                if stream.get('codec_type') == 'audio':
                    if stream.get('codec_name') == 'aac':
                        audio_codec_needed = False
                    else:
                        audio_reason = 'audio codec'
                    audio_channels = int(stream.get('channels', 2))
            if not container_needed:
                container_reason = None
            else:
                container_reason = 'container'

            # Try to get creation time from metadata first
            creation_time = get_creation_time_from_metadata(info)
            apple_metadata = extract_apple_metadata(info)

            # If no creation_time in metadata, check file mtime against year in path
            if not creation_time:
                path_year = extract_year_from_path(file_path)
                if path_year:
                    mod_time = os.path.getmtime(file_path)
                    mod_year = datetime.fromtimestamp(mod_time).year
                    if str(mod_year) == path_year:
                        # Year matches, use file mtime as creation time and continue with codec checks
                        creation_time = get_file_mtime_as_iso(file_path)
                    else:
                        # Year mismatch - skip this file
                        entry['action'] = 'skip'
                        entry['reason'] = 'file modified time mismatch'
                        continue
                else:
                    entry['action'] = 'skip'
                    entry['reason'] = 'no year in path'
                    continue

            # If everything is compatible (video, audio, SDR, MOV), just mark as move
            if not video_codec_needed and not audio_codec_needed and not container_needed and not hdr_to_sdr_needed:
                entry['action'] = 'move'
                entry['reason'] = 'fully compatible already'
                entry['creation_time'] = creation_time
                entry['apple_metadata'] = json.dumps(apple_metadata) if apple_metadata else ''
                entry['audio_channels'] = audio_channels
                entry['video_codec_needed'] = 0
                entry['audio_codec_needed'] = 0
                continue

            # Build reason string
            reasons = []
            if video_codec_needed:
                if video_reason:
                    reasons.append(video_reason)
            if hdr_to_sdr_needed:
                reasons.append(hdr_reason)
            if audio_codec_needed:
                if audio_reason:
                    reasons.append(audio_reason)
            if container_needed:
                if container_reason:
                    reasons.append(container_reason)
            reason_str = 'convert: ' + '+'.join(reasons) if reasons else 'convert'

            entry['action'] = 'convert'
            entry['reason'] = reason_str
            entry['creation_time'] = creation_time
            entry['apple_metadata'] = json.dumps(apple_metadata) if apple_metadata else ''
            entry['audio_channels'] = audio_channels
            entry['video_codec_needed'] = 1 if video_codec_needed else 0
            entry['audio_codec_needed'] = 1 if audio_codec_needed else 0
    return results


if __name__ == "__main__":
    video_extensions = [e.lower() for e in ONLY_HANDLE_THESE_VIDEO_EXTENSIONS]
    results = crawl_and_evaluate(ROOT_VIDEO_FOLDER, video_extensions, SKIPLIST_PARTIAL_MATCH)
    df = pd.DataFrame(results)
    print(df)
    df.to_csv(OUTPUT_CSV_FILE, sep='@', index=False)
