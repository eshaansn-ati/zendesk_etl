"""
General Zendesk Data Extraction Functions

This module provides reusable extraction functions for all Zendesk endpoints.
Endpoint-specific extract.py files import and use these functions.
"""

import requests
import json
import time
import os
import sys
from datetime import datetime

from utils.extract_utils import (
    load_last_sync_time,
    save_sync_time,
    get_total_ticket_count
)


def extract_simple_endpoint(subdomain, resource, auth, output_dir):
    """
    Fetches data from a simple Zendesk API endpoint (non-incremental).
    Used for endpoints like ticket_fields.
    
    Args:
        subdomain: Zendesk subdomain
        resource: Resource name (e.g., 'ticket_fields')
        auth: Authentication tuple (email/token, api_token)
        output_dir: Directory to save extracted data
        
    Returns:
        List of records or None if error
    """
    url = f"https://{subdomain}.zendesk.com/api/v2/{resource}.json"
    all_records = []
    
    print(f"\n--- Starting Simple Export for {resource} ---")
    print(f"-> Fetching: {url}")
    
    try:
        response = requests.get(url, auth=auth)
        
        # Handle Rate Limits (429)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            print(f"!!! Rate limit hit. Waiting for {retry_after} seconds...")
            time.sleep(retry_after)
            response = requests.get(url, auth=auth)
        
        response.raise_for_status()
        data = response.json()
        
        # Get the resource key (e.g., 'ticket_fields' from the response)
        records = data.get(resource)
        
        if records:
            all_records.extend(records)
            print(f"-> Successfully fetched {len(all_records)} {resource}.")
            
            # Save the data
            filename = f"{output_dir}/{resource}_{int(datetime.now().timestamp())}.json"
            os.makedirs(output_dir, exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_records, f, ensure_ascii=False, indent=2)
            print(f"-> Data saved to {filename}")
        else:
            print(f"⚠️  No {resource} found in response.")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {resource}: {e}")
        return None
    
    return all_records


def extract_incremental_endpoint(resource, subdomain, start_time, auth, include_param=None):
    """
    Fetches data from Zendesk Incremental Export API.
    Used for endpoints like tickets, users, organizations.
    
    Args:
        resource: Resource name (e.g., 'tickets', 'users')
        subdomain: Zendesk subdomain
        start_time: Unix timestamp to start from
        auth: Authentication tuple (email/token, api_token)
        include_param: Optional include parameter (e.g., 'users' for tickets)
        
    Returns:
        Tuple of (list of records, final_end_time)
    """
    # Build URL with optional include parameter
    url_params = f"start_time={start_time}"
    if include_param:
        url_params += f"&include={include_param}"
    
    url = f"https://{subdomain}.zendesk.com/api/v2/incremental/{resource}.json?{url_params}"
    
    all_records = []
    final_end_time = None
    
    print(f"\n--- Starting Incremental Export for {resource} ---")
    print(f"-> Starting from timestamp: {start_time}")
    
    while url:
        print(f"-> Fetching: {url}")
        response = requests.get(url, auth=auth)
        
        # Handle Rate Limits (429)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            print(f"!!! Rate limit hit. Waiting for {retry_after} seconds...")
            time.sleep(retry_after)
            continue  # Retry the same URL
        
        response.raise_for_status()  # Raise an exception for other bad status codes (4xx, 5xx)
        
        data = response.json()

        
        records = data.get(resource)
        if records:
            all_records.extend(records)
            print(f"   Fetched {len(records)} records. Total records: {len(all_records):,}")
        
        # Check for end of stream
        if data.get('end_of_stream', False):
            print(f"\n -> End of stream reached. No more data available.")
            final_end_time = data.get('end_time')
            print(f"-> Final sync timestamp (API's reported end_time): {final_end_time}")
            print(f"-> Total records fetched in this batch: {len(all_records):,}")
            print("\n -> Extraction Complete: End of stream reached.")
            break
        
        next_url = data.get('next_page')
        
        # Proceed to the next page
        url = next_url
        
        if url:
            time.sleep(1)  # Be kind to the API
    
    return all_records, final_end_time


def run_extraction(cfg, endpoint_config):
    """
    Main extraction orchestration function.
    
    Args:
        cfg: Hydra config object
        endpoint_config: Dictionary with endpoint-specific configuration:
            - use_incremental: bool
            - resource_key: str
            - output_subdir: str
            - timestamp_file: str or None
            - include_param: str or None
            - count_endpoint: str or None
            
    Returns:
        True if successful, False otherwise
    """
    resource = endpoint_config['resource_key']
    
    # Input validation
    if not all([cfg.zendesk.subdomain, cfg.zendesk.email, cfg.zendesk.api_token]):
        print("!!! Error: Zendesk credentials not found in config or environment variables.")
        return False
    
    auth = (f"{cfg.zendesk.email}/token", cfg.zendesk.api_token)
    
    # Set up output directory
    extract_path = cfg.extract[endpoint_config['endpoint']]
    output_dir = f"{cfg.paths.data_dir}/{extract_path}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Handle simple endpoints (non-incremental)
    if not endpoint_config['use_incremental']:
        print(f"\n{'='*60}")
        print(f"Extracting {resource} (Simple API)")
        print(f"{'='*60}")
        
        records = extract_simple_endpoint(cfg.zendesk.subdomain, resource, auth, output_dir)
        
        if records:
            print(f"\n✅ Successfully extracted {len(records)} {resource}")
            return True
        else:
            print(f"\n❌ Failed to extract {resource}")
            return False
    
    # Handle incremental endpoints
    print(f"\n{'='*60}")
    print(f"Extracting {resource} (Incremental API)")
    print(f"{'='*60}")
    
    # Set up timestamp file
    if endpoint_config.get('timestamp_file'):
        timestamp_file = f"{cfg.paths.config_dir}/{endpoint_config['timestamp_file']}"
        os.makedirs(cfg.paths.config_dir, exist_ok=True)
    else:
        timestamp_file = None
    
    # Get total count if available (only for tickets)
    if endpoint_config.get('count_endpoint'):
        count_url = f"https://{cfg.zendesk.subdomain}.zendesk.com/api/v2/{endpoint_config['count_endpoint']}"
        get_total_ticket_count(auth, count_url)
    
    # Load the timestamp from the previous run
    last_sync_time = load_last_sync_time(timestamp_file) if timestamp_file else 0
    
    if last_sync_time == 0:
        print("\n!!! WARNING: Starting from timestamp 0. This will pull ALL historical records!")
        print("!!! This may take a long time and consume a large portion of your daily rate limit.")
        time.sleep(2)
    
    # Extract data
    include_param = endpoint_config.get('include_param')
    all_records, api_end_time = extract_incremental_endpoint(
        resource,
        cfg.zendesk.subdomain,
        last_sync_time,
        auth,
        include_param
    )
    
    # Save fetched records and update the sync timestamp
    if all_records:
        # Save all_records to a JSON file using api_end_time or current timestamp
        if api_end_time:
            filename = f"{output_dir}/{resource}_until_{api_end_time}.json"
        else:
            filename = f"{output_dir}/{resource}_{int(datetime.now().timestamp())}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"\n-> All {len(all_records):,} records saved to {filename}")
    
    # Save the API's end_time
    if timestamp_file and api_end_time:
        save_sync_time(api_end_time, timestamp_file)
        print(f"-> New sync timestamp ({api_end_time}) saved to {timestamp_file}.")
    
    print(f"\n✅ Successfully extracted {len(all_records) if all_records else 0} {resource}")
    return True
