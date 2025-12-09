import glob
import os
import re


def load_latest_file_from_dir(INPUT_DIR, file_pattern="*.json"):
    """
    Loads the latest file from a directory based on unix timestamp in filename.
    
    Args:
        INPUT_DIR: Directory to search for files
        file_pattern: Glob pattern to match files (default: "*.json")
                     Examples: "ticket_fields_*.json", "tickets_*.csv", "*.json"
    
    Returns:
        Path to the latest file, or None if no matching files found
    """
    search_pattern = f"{INPUT_DIR}/{file_pattern}"
    matching_files = glob.glob(search_pattern)
    timestamped_files = []

    for filepath in matching_files:
        fname = os.path.basename(filepath)
        # Extract timestamp from filename - look for numbers before file extension
        # Handles formats like: ticket_fields_{timestamp}.json, tickets_{timestamp}.csv
        # Match any file extension (json, csv, etc.)
        match = re.search(r'(\d+)\.(json|csv|txt|parquet)$', fname, re.IGNORECASE)
        if match:
            try:
                timestamp = int(match.group(1))
                timestamped_files.append((timestamp, filepath))
            except ValueError:
                continue

    if not timestamped_files:
        print(f"🚨 Error: No files matching pattern '{file_pattern}' with unix timestamp found in '{INPUT_DIR}'. Please run the extraction first.")
        return None
    else:
        # Select file with largest timestamp
        latest_timestamp, latest_file = max(timestamped_files, key=lambda t_file: t_file[0])
        print(f"✅ Latest file by unix timestamp: {latest_file}")
        return latest_file