import os
import pandas as pd
import json
import shutil
from datetime import datetime, timezone
import subprocess
import zoneinfo

# Define local timezone (CET/CEST - Europe/Brussels or Europe/Amsterdam)
LOCAL_TIMEZONE = zoneinfo.ZoneInfo('Europe/Brussels')

FFMPEG_BINARY_PATH = r"C:\Program Files\ffmpeg\5.1\bin\ffmpeg.exe"
EXIFTOOL_BINARY_PATH = r"C:\Program Files\exiftool\exiftool.exe"  # You may need to install exiftool
INPUT_CSV_FOLDER_PATH = "report/2021"
INPUT_CSV_FILE_NAME = "icloud_video_report.csv"
PROCESSED_VIDEO_FOLDER_PATH = r"data\processed_videos\2021"c:\Program Files\ffmpeg\5.1\bin\ffmpeg.exe" -y -i "D:\icloud_reingest\data\processed_videos\IMG_3460.MOV" -vf "zscale=transfer=linear:primaries=bt709:matrix=bt709,tonemap=tonemap=hable:desat=0.5,zscale=transfer=bt709" -c:v libx265 -pix_fmt yuv420p -color_primaries bt709 -color_trc bt709 -colorspace bt709 -c:a aac -b:a 128k "D:\icloud_reingest\data\processed_videos\IMG_3460_SDR.MOV""
OUTPUT_CSV_FOLDER_PATH = "report"
OUTPUT_CSV_FILE_NAME = "icloud_video_report_processed.csv"
CSV_SEPARATOR=";"

def convert_video(src, dst, creation_time, video_codec_needed, audio_codec_needed, audio_channels, apple_metadata=None):
    cmd = [FFMPEG_BINARY_PATH, '-y', '-i', src]
    if video_codec_needed:
        cmd += [
            '-c:v', 'libx265',
            '-tag:v', 'hvc1',
            '-profile:v', 'main',
            '-level:v', '4.0',
            '-pix_fmt', 'yuv420p',
            '-r', '30',
            '-x265-params', 'keyint=60:min-keyint=60:scenecut=0:bframes=4:open-gop=0:repeat-headers=1',
            '-color_primaries', 'bt709',
            '-color_trc', 'bt709',
            '-colorspace', 'bt709',
        ]
    else:
        cmd += ['-c:v', 'copy']
    if audio_codec_needed:
        cmd += ['-c:a', 'aac', '-ar', '44100', '-b:a', '100k']
        if audio_channels == 1:
            cmd += ['-ac', '1']
        else:
            cmd += ['-ac', '2']
    else:
        cmd += ['-c:a', 'copy']

    # Set basic creation_time metadata
    if creation_time:
        cmd += ['-metadata', f'creation_time={creation_time}']

    cmd += ['-movflags', '+write_colr+faststart', dst]

    print(f"Converting: {src}")
    subprocess.run(cmd, check=True)

    # After ffmpeg conversion, use exiftool to set Apple-specific QuickTime tags
    # This is more reliable than ffmpeg's -metadata for Apple tags
    if creation_time or apple_metadata:
        exiftool_cmd = [EXIFTOOL_BINARY_PATH, '-overwrite_original']

        if creation_time:
            # Set Apple QuickTime creation date
            exiftool_cmd.append(f'-QuickTime:CreateDate={creation_time}')
            exiftool_cmd.append(f'-QuickTime:ModifyDate={creation_time}')
            exiftool_cmd.append(f'-QuickTime:TrackCreateDate={creation_time}')
            exiftool_cmd.append(f'-QuickTime:TrackModifyDate={creation_time}')
            exiftool_cmd.append(f'-QuickTime:MediaCreateDate={creation_time}')
            exiftool_cmd.append(f'-QuickTime:MediaModifyDate={creation_time}')

        if apple_metadata:
            if 'com.apple.quicktime.make' in apple_metadata or any('make' in k.lower() for k in apple_metadata.keys()):
                make = next((v for k, v in apple_metadata.items() if 'make' in k.lower()), None)
                if make:
                    exiftool_cmd.append(f'-QuickTime:Make={make}')

            if 'com.apple.quicktime.model' in apple_metadata or any('model' in k.lower() for k in apple_metadata.keys()):
                model = next((v for k, v in apple_metadata.items() if 'model' in k.lower()), None)
                if model:
                    exiftool_cmd.append(f'-QuickTime:Model={model}')

            if 'com.apple.quicktime.software' in apple_metadata or any('software' in k.lower() for k in apple_metadata.keys()):
                software = next((v for k, v in apple_metadata.items() if 'software' in k.lower()), None)
                if software:
                    exiftool_cmd.append(f'-QuickTime:Software={software}')

        exiftool_cmd.append(dst)

        # Check if exiftool exists before trying to use it
        if os.path.exists(EXIFTOOL_BINARY_PATH):
            print(f"Setting metadata with exiftool...")
            subprocess.run(exiftool_cmd, check=False)  # Don't fail if exiftool has issues
        else:
            print(f"Warning: exiftool not found at {EXIFTOOL_BINARY_PATH}, skipping Apple metadata")

    # Set file mtime
    if creation_time:
        try:
            dt = None
            try:
                dt = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
            except Exception:
                dt = datetime.strptime(creation_time[:19], '%Y-%m-%dT%H:%M:%S')
            mod_time = dt.timestamp()
            os.utime(dst, (mod_time, mod_time))
        except Exception:
            pass


def process_actions_from_csv(csv_path):
    # ensure output folder exists
    os.makedirs(PROCESSED_VIDEO_FOLDER_PATH, exist_ok=True)

    df = pd.read_csv(csv_path, sep=CSV_SEPARATOR, dtype=str)
    df['derived_file'] = None  # Add new column for output path
    df['processing_status'] = ''  # Add column to track processing errors

    for idx, row in df.iterrows():
        action = row.get('action')
        if action == 'skip':
            continue

        try:
            creation_time = row.get('creation_time')
            if 'apple_metadata' in row and pd.notna(row['apple_metadata']):
                apple_metadata = json.loads(row['apple_metadata'])
            else:
                apple_metadata = None
            val = row.get('audio_channels')
            audio_channels = int(float(val)) if pd.notna(val) else 2
            video_codec_needed = bool(int(float(row.get('video_codec_needed', '1'))))
            audio_codec_needed = bool(int(float(row.get('audio_codec_needed', '1'))))

            # Establish an output file path with a unique prefix from creation_time
            prefix = "unknown_"
            dt_local = None  # Store local datetime for use in mtime
            if creation_time and pd.notna(creation_time):
                try:
                    # Parse UTC datetime format: 2018-09-28T19:02:21.000Z
                    dt_utc = datetime.strptime(creation_time.replace('Z', ''), '%Y-%m-%dT%H:%M:%S.%f')
                    # Make it timezone-aware (UTC)
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                    # Convert to local CET/CEST timezone
                    dt_local = dt_utc.astimezone(LOCAL_TIMEZONE)
                    # Use local time for filename prefix
                    prefix = dt_local.strftime('%Y%m%d_%H%M%S-')
                except Exception as e:
                    print(f"Warning: Could not parse creation_time '{creation_time}': {e}")
                    prefix = "unknown_"
            file_path = row.get('file')
            file_name = os.path.basename(file_path)
            # For convert actions, always use .mov extension for output
            if action == 'convert':
                base_name, _ = os.path.splitext(file_name)
                unique_file_name = f"{prefix}{base_name}.mov"
            else:
                unique_file_name = f"{prefix}{file_name}"
            out_path = os.path.join(PROCESSED_VIDEO_FOLDER_PATH, unique_file_name)
            df.at[idx, 'derived_file'] = out_path  # Store output path in new column

            # Check if output file already exists with size > 0
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                df.at[idx, 'processing_status'] = 'SKIPPED: file already exists'
                print(f"Skipping {file_path}: output file already exists at {out_path}")
                continue

            if action == 'move':
                shutil.copy2(file_path, out_path)
                # Set file mtime to local time (not UTC)
                if dt_local:
                    try:
                        mod_time = dt_local.timestamp()
                        os.utime(out_path, (mod_time, mod_time))
                    except Exception as e:
                        print(f"Warning: Could not set mtime for {out_path}: {e}")
                df.at[idx, 'processing_status'] = 'SUCCESS'
            elif action == 'convert':
                convert_video(file_path, out_path, creation_time, video_codec_needed, audio_codec_needed, audio_channels, apple_metadata)
                # Set file mtime to local time (not UTC) after conversion
                if dt_local:
                    try:
                        mod_time = dt_local.timestamp()
                        os.utime(out_path, (mod_time, mod_time))
                    except Exception as e:
                        print(f"Warning: Could not set mtime for {out_path}: {e}")
                df.at[idx, 'processing_status'] = 'SUCCESS'
            # skip and error do nothing

        except Exception as e:
            # Catch any error for this file and log it
            error_msg = f"ERROR: {type(e).__name__}: {str(e)}"
            df.at[idx, 'processing_status'] = error_msg
            print(f"Error processing {row.get('file')}: {error_msg}")
            # Continue with next file

    return df

if __name__ == '__main__':
    input_csv_path = os.path.join(INPUT_CSV_FOLDER_PATH, INPUT_CSV_FILE_NAME)
    dfp = process_actions_from_csv(input_csv_path)

    # Ensure output folder exists
    os.makedirs(OUTPUT_CSV_FOLDER_PATH, exist_ok=True)
    output_csv_path = os.path.join(OUTPUT_CSV_FOLDER_PATH, OUTPUT_CSV_FILE_NAME)
    dfp.to_csv(output_csv_path, sep=CSV_SEPARATOR, index=False)
    print("Done.")
