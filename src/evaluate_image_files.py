"""
Image File Evaluation for iCloud Re-ingestion

This module evaluates image files to determine if they are ready for re-upload to iCloud.
iCloud is picky about date metadata, so this script categorizes files based on their
date information and compatibility.

Logic Flow:
-----------
1. Recursively crawls a root folder for image files with specified extensions (jpg, jpeg)

2. For each image file found, applies the following decision tree:

   a) SKIP if:
      - File extension doesn't match ONLY_HANDLE_THESE_IMAGE_EXTENSIONS
      - File path contains any keyword from SKIPLIST_PARTIAL_MATCH
      - No year (20XX) is found in the parent folder path
      - Date modified year doesn't match the year found in the folder path

   b) MOVE if:
      - EXIF "DateTimeOriginal" (date taken) metadata is present, OR
      - EXIF date is missing BUT file's modified date year matches the year in folder path

3. Results are collected in a list of dictionaries with:
   - file: absolute file path
   - action: 'skip' or 'move'
   - reason: detailed explanation of why the action was chosen

4. Results are exported to:
   - Console output (pandas DataFrame with all columns visible)
   - CSV file with '@' as column separator for easy review/editing

Output Actions:
--------------
- 'move': File is ready for iCloud re-ingestion (has proper date metadata or acceptable fallback)
- 'skip': File should not be re-ingested (missing/mismatched date information or matches skiplist)

Example folder structure:
------------------------
data/2018/2018-04-15 Birthday Party/IMG_1234.jpg
     ^^^^                                        <- Year extracted from path

If IMG_1234.jpg has EXIF date taken -> action: 'move', reason: 'date taken available'
If no EXIF but file modified in 2018 -> action: 'move', reason: 'date modified year correct'
If no EXIF and file modified in 2019 -> action: 'skip', reason: 'date modified year mismatch'
"""

import os
from datetime import datetime
import pandas as pd
from PIL import Image
from PIL.ExifTags import TAGS
from src.utils import should_skip_by_partial_match, extract_year_from_path


### CONFIGURATION ###
# Starting directory for file crawl
ROOT_PICTURES_FOLDER = "data/2018/pictures"
# File types to process (case-insensitive)
ONLY_HANDLE_THESE_IMAGE_EXTENSIONS = ["jpg", "jpeg"]
# Keywords to exclude files (e.g., specific albums, trash folders)
SKIPLIST_PARTIAL_MATCH = ["GOEF", "Elia", "BBM", "Trash", "small"]
# Folder path for output CSV report
OUTPUT_CSV_FOLDER_PATH = "report"
# CSV file name for output report
OUTPUT_CSV_FILE_NAME = "icloud_image_report.csv"


# Pandas display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def get_exif_date_taken(filepath):
    try:
        image = Image.open(filepath)
        exif_data = image._getexif()
        if exif_data:
            for tag_id, value in exif_data.items():
                tag = TAGS.get(tag_id, tag_id)
                if tag == 'DateTimeOriginal':
                    return value
    except Exception:
        pass
    return None

def crawl_and_evaluate(root_folder_path, image_extensions, skiplist):
    results = []
    for folder_path, _, filenames in os.walk(root_folder_path):
        for file_name in filenames:
            file_path = os.path.abspath(os.path.join(folder_path, file_name))
            entry = {'file': file_path}
            results.append(entry)

            ext = os.path.splitext(file_name)[1][1:].lower()
            if ext not in image_extensions:
                entry['action'] = 'skip'
                entry['reason'] = 'wrong extension'
                continue

            if should_skip_by_partial_match(file_path, skiplist):
                entry['action'] = 'skip'
                entry['reason'] = 'skiplist match'
                continue

            date_taken = get_exif_date_taken(file_path)
            print(f"File: {file_path}, Date Taken: {date_taken}")
            if date_taken:
                entry['action'] = 'move'
                entry['reason'] = 'date taken available'
                continue

            path_year = extract_year_from_path(file_path)
            if path_year:
                mod_time = os.path.getmtime(file_path)
                mod_year = datetime.fromtimestamp(mod_time).year
                if str(mod_year) == path_year:
                    entry['action'] = 'move'
                    entry['reason'] = 'date modified year correct'
                else:
                    entry['action'] = 'skip'
                    entry['reason'] = 'date modified year mismatch'
            else:
                entry['action'] = 'skip'
                entry['reason'] = 'no year in path'
    return results

if __name__ == "__main__":
    image_exts = [e.lower() for e in ONLY_HANDLE_THESE_IMAGE_EXTENSIONS]
    results = crawl_and_evaluate(ROOT_PICTURES_FOLDER, image_exts, SKIPLIST_PARTIAL_MATCH)
    df = pd.DataFrame(results)
    print(df)

    # Ensure output folder exists
    os.makedirs(OUTPUT_CSV_FOLDER_PATH, exist_ok=True)
    OUTPUT_CSV_FILE_PATH = os.path.join(OUTPUT_CSV_FOLDER_PATH, OUTPUT_CSV_FILE_NAME)
    df.to_csv(OUTPUT_CSV_FILE_PATH, sep='@', index=False)
