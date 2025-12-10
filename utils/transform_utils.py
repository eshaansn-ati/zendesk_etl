import glob
import os
import re
import pandas as pd
from datetime import datetime


def load_latest_file_from_dir(INPUT_DIR, file_pattern="*.json"):
    """
    Loads the latest file from a directory based on unix timestamp in filename.
    
    Args:
        INPUT_DIR: Directory to search for files
        file_pattern: Glob pattern to match files (default: "*.json")
                     Examples: "ticket_fields_*.json", "tickets_*.parquet", "*.json"
    
    Returns:
        Path to the latest file, or None if no matching files found
    """
    search_pattern = f"{INPUT_DIR}/{file_pattern}"
    matching_files = glob.glob(search_pattern)
    timestamped_files = []

    for filepath in matching_files:
        fname = os.path.basename(filepath)
        # Extract timestamp from filename - look for numbers before file extension
        # Handles formats like: ticket_fields_{timestamp}.json, tickets_{timestamp}.parquet
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


def deduplicate_dataframe(df, primary_key_column, updated_at_column=None, entity_name="records"):
    """
    Deduplicates a DataFrame by primary key, keeping the most recent version if updated_at is provided.
    
    The Zendesk Incremental Export API can return duplicate records if they were updated during
    the export window. This function removes duplicates, keeping the most recent version based on
    the updated_at timestamp.
    
    Args:
        df: pandas DataFrame to deduplicate
        primary_key_column: Name of the primary key column (e.g., 'ticket_id', 'user_id', 'organization_id')
        updated_at_column: Optional name of the updated_at timestamp column for keeping most recent version
        entity_name: Name of the entity type for logging (e.g., "tickets", "users", "organizations")
        
    Returns:
        Tuple of (deduplicated DataFrame, number of duplicates removed)
    """
    if primary_key_column not in df.columns:
        print(f"⚠️  Warning: Primary key column '{primary_key_column}' not found. Skipping deduplication.")
        return df, 0
    
    initial_count = len(df)
    
    # If updated_at column is provided, use it to keep the most recent version
    if updated_at_column and updated_at_column in df.columns:
        # Parse updated_at to datetime for proper sorting
        df['_parsed_updated_at'] = pd.to_datetime(df[updated_at_column], errors='coerce')
        # Sort by primary key and updated_at (descending), then drop duplicates keeping first
        df = df.sort_values([primary_key_column, '_parsed_updated_at'], ascending=[True, False])
        df = df.drop_duplicates(subset=[primary_key_column], keep='first')
        df = df.drop(columns=['_parsed_updated_at'])
    else:
        # If no updated_at column, just drop duplicates keeping first occurrence
        df = df.drop_duplicates(subset=[primary_key_column], keep='first')
    
    duplicates_removed = initial_count - len(df)
    
    if duplicates_removed > 0:
        print(f"⚠️  Removed {duplicates_removed} duplicate {entity_name}(s), keeping most recent version(s)")
    else:
        print(f"✅ No duplicates found - all {len(df)} {entity_name} are unique")
    
    return df, duplicates_removed


def extract_timestamp_from_filename(filepath):
    """
    Extracts unix timestamp from a filename.
    
    Handles formats like:
    - organizations_{timestamp}.json
    - organizations_until_{timestamp}.json
    - tickets_{timestamp}.json
    
    Args:
        filepath: Path to the file (can be full path or just filename)
        
    Returns:
        Unix timestamp as integer, or None if not found
    """
    fname = os.path.basename(filepath)
    # Extract timestamp from filename - look for numbers before file extension
    match = re.search(r'(\d+)\.(json|csv|txt|parquet)$', fname, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def export_to_parquet(df, output_dir, entity_name, timestamp=None):
    """
    Exports a pandas DataFrame to Parquet format with timestamped filename.
    
    Args:
        df: pandas DataFrame to export
        output_dir: Directory where the parquet file will be saved
        entity_name: Name of the entity (e.g., "tickets", "users", "organizations")
        timestamp: Optional unix timestamp. If None, current timestamp will be used.
        
    Returns:
        Path to the exported parquet file
    """
    if timestamp is None:
        timestamp = int(datetime.now().timestamp())
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    output_filename = f"{output_dir}/{entity_name}_{timestamp}.parquet"
    
    # Export to parquet
    df.to_parquet(output_filename, index=False)
    
    print(f"\n✅ Success! {entity_name.capitalize()} table saved to: {output_filename}")
    
    return output_filename