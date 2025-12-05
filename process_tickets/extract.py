from dotenv import load_dotenv
import requests
import time
import os
import sys
import json
import urllib.parse
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.extract_utils import load_last_sync_time, save_sync_time, calculate_start_of_week, get_total_ticket_count

# --- Configuration Variables ---
load_dotenv()

# Ensure these environment variables are set in your .env file
ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN")

DATA_DIR = os.getenv("DATA_DIR")
EXTRACT_TICKETS_PATH = os.getenv("EXTRACT_TICKETS_PATH")
CONFIG_DIR = os.getenv("CONFIG_DIR")

# --- ETL Settings ---
RESOURCE = 'tickets'
TIMESTAMP_FILE = f"{CONFIG_DIR}/last_sync_timestamp.txt"
TICKET_OUTPUT_DIR = f"{DATA_DIR}/{EXTRACT_TICKETS_PATH}"

TOTAL_TICKET_COUNT_URL = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets/count.json"

# --- Core Extraction Function ---
def extract_zendesk_data(resource, subdomain, start_time, auth, target_stop_timestamp):
    """Fetches data from Zendesk Incremental Export API, stopping when the next
       page timestamp exceeds the target_stop_timestamp."""
    
    # URL includes 'include=users' to attach user data to the records
    url = f"https://{subdomain}.zendesk.com/api/v2/incremental/{resource}.json?start_time={start_time}&include=users"
    
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
            continue # Retry the same URL

        response.raise_for_status() # Raise an exception for other bad status codes (4xx, 5xx)

        data = response.json()
        
        records = data.get(resource)
        if records:
            all_records.extend(records)
            print(f"   Fetched {len(records)} records. Total records: {len(all_records):,}")
        
        final_end_time = data.get('end_time')

        next_url = data.get('next_page')

        # --- EARLY EXIT CHECK ---
        if next_url:
            # 1. Parse the next page URL to extract the 'start_time' parameter
            parsed_url = urllib.parse.urlparse(next_url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # The next start_time is what the API would use for the next request
            # [0] is used because parse_qs returns a list of values for each key
            next_start_time = int(query_params.get('start_time', [0])[0])
            
            # 2. Check if the next batch starts AFTER your target stop time
            if next_start_time > target_stop_timestamp:
                print(f"\n🎯 Loop Exited: Next batch start time ({next_start_time}) exceeds target ({target_stop_timestamp}).")
                # Set URL to None to exit the while loop gracefully
                url = None 
                continue
                
        # If the check didn't cause an exit, proceed normally to the next page
        url = next_url
        
        if url:
            time.sleep(1) # Be kind to the API

    print("\n--- Extraction Complete ---")
    print(f"-> Final sync timestamp (API's reported end_time): {final_end_time}")
    print(f"-> Total records fetched in this batch: {len(all_records):,}")

    return all_records, final_end_time

# --- Main Execution ---

def main():
    
    # Input validation
    if not all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN, DATA_DIR, EXTRACT_TICKETS_PATH, CONFIG_DIR]):
        print("!!! Error: ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN, DATA_DIR, EXTRACT_TICKETS_PATH, or CONFIG_DIR not found in environment variables.")
        return

    auth = (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)
    
    # Ensure the output directory exists
    if not os.path.exists(TICKET_OUTPUT_DIR):
        os.makedirs(TICKET_OUTPUT_DIR)

    # Set the target stop time for this run (Start of Week in UTC)
    TARGET_STOP_TIMESTAMP = calculate_start_of_week()
    print(f"🎯 Target Stop Time (Start of Week): {datetime.fromtimestamp(TARGET_STOP_TIMESTAMP).strftime('%Y-%m-%d %H:%M:%S UTC')} ({TARGET_STOP_TIMESTAMP})")


    # 1. Get the total ticket count
    get_total_ticket_count(auth, TOTAL_TICKET_COUNT_URL)
        
    # 2. Load the timestamp from the previous run
    last_sync_time = load_last_sync_time(TIMESTAMP_FILE)
    
    # --- Check for early termination ---
    if last_sync_time >= TARGET_STOP_TIMESTAMP:
        print(f"\n✅ Data already synced up to {datetime.fromtimestamp(last_sync_time).strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        print("   No extraction needed for this period.")
        return

    if last_sync_time == 0:
        print("\n!!! WARNING: Starting from timestamp 0. This will pull ALL historical records!")
        print("!!! This may take a long time and consume a large portion of your daily rate limit.")
        time.sleep(2) 
    
    # 3. Extract data: Pass the target stop time to the extraction function
    all_records, api_end_time = extract_zendesk_data(RESOURCE, ZENDESK_SUBDOMAIN, last_sync_time, auth, TARGET_STOP_TIMESTAMP)
    
    # 4. Save ALL fetched records and update the sync timestamp
    if all_records:
        # Save all_records to a JSON file, using the target time for the filename
        filename = f"{TICKET_OUTPUT_DIR}/{RESOURCE}_until_{TARGET_STOP_TIMESTAMP}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"\n-> All {len(all_records):,} records saved to {filename}")

    # Save the TARGET_STOP_TIMESTAMP, not the API's end_time.
    save_sync_time(TARGET_STOP_TIMESTAMP, TIMESTAMP_FILE)
    print(f"-> New sync timestamp ({TARGET_STOP_TIMESTAMP}) saved.")


if __name__ == "__main__":
    main()