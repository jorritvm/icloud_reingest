"""
Duplicate Image Evaluation

This module scans a root folder for images, skipping folders that match any keyword in a skiplist.
It evaluates images per folder, splitting them into two groups based on a size threshold (default 800 KB).

For each folder, it calculates a perceptual hash (phash) for every image (resized to PHASH_SIZE x PHASH_SIZE),
then compares phashes between the big and small groups.

If two images are visually similar (phash distance <= 5), they are marked as duplicates,
with the smaller one flagged as the dupe. Results are exported to a CSV file for further review or processing.
"""

import os
import pandas as pd
from PIL import Image
import imagehash
from src.utils import should_skip_by_partial_match

# Configuration
# Root folder to start crawling for images
ROOT_PICTURES_FOLDER = "data/with_dupes"
# List of keywords; folders containing any of these will be skipped
SKIPLIST_PARTIAL_MATCH = ["GOEF", "Elia", "BBM", "Trash", "small", "large"]
# Size threshold in bytes to separate images into big/small groups (800 KB)
SIZE_THRESHOLD_BYTES = 800 * 1024  # 800 KB
# Size for phash calculation (images will be resized to PHASH_SIZE x PHASH_SIZE)
PHASH_SIZE = 64
# Output folder for the CSV report
OUTPUT_CSV_FOLDER_PATH = "report"
# Output CSV file name
OUTPUT_CSV_FILE_NAME = "duplicate_image_report.csv"

# Ensure pandas prints all columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

def get_image_phash(image_path, hash_size=8):
    try:
        with Image.open(image_path) as img:
            img = img.convert("RGB")
            img = img.resize((PHASH_SIZE, PHASH_SIZE))
            return str(imagehash.phash(img, hash_size=hash_size))
    except Exception:
        return None


def evaluate_folder(folder_path, filenames):
    results = []
    big_imgs = []
    small_imgs = []
    for file_name in filenames:
        file_path = os.path.join(folder_path, file_name)
        try:
            size = os.path.getsize(file_path)
        except Exception:
            continue
        entry = {
            'file': file_path,
            'size': size,
            'phash': None,
            'dupe_type': '',
            'dupe_of': ''
        }
        phash = get_image_phash(file_path)
        entry['phash'] = phash
        if size >= SIZE_THRESHOLD_BYTES:
            big_imgs.append(entry)
        else:
            small_imgs.append(entry)
    # Compare phashes between big and small
    for big in big_imgs:
        for small in small_imgs:
            if big['phash'] and small['phash']:
                # Hamming distance threshold for phash similarity
                if imagehash.hex_to_hash(big['phash']) - imagehash.hex_to_hash(small['phash']) <= 5:
                    big['dupe_type'] = 'dupe_big'
                    small['dupe_type'] = 'dupe_small'
                    small['dupe_of'] = big['file']
    results.extend(big_imgs)
    results.extend(small_imgs)
    return results


def crawl_and_evaluate(root_folder, skiplist):
    all_results = []
    for folder_path, dirnames, filenames in os.walk(root_folder):
        if should_skip_by_partial_match(folder_path, skiplist):
            continue
        print(f"Processing folder: {folder_path}")  # Progress print
        # Only process folders with images
        image_files = [f for f in filenames if f.lower().endswith((".jpg", ".jpeg", ".png"))]
        if not image_files:
            continue
        folder_results = evaluate_folder(folder_path, image_files)
        all_results.extend(folder_results)
    return all_results


if __name__ == "__main__":
    results = crawl_and_evaluate(ROOT_PICTURES_FOLDER, SKIPLIST_PARTIAL_MATCH)
    df = pd.DataFrame(results)
    print(df)

    # Ensure output folder exists
    os.makedirs(OUTPUT_CSV_FOLDER_PATH, exist_ok=True)
    output_csv_path = os.path.join(OUTPUT_CSV_FOLDER_PATH, OUTPUT_CSV_FILE_NAME)
    df.to_csv(output_csv_path, sep='@', index=False)
    print(f"Done. Results written to {output_csv_path}")
