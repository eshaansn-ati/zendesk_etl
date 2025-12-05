from dotenv import load_dotenv
import requests
import time
import os
import json
import urllib.parse
from datetime import datetime, timedelta

# --- Configuration Variables ---
load_dotenv()

# Ensure these environment variables are set in your .env file
ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN")

# --- ETL Settings ---
RESOURCE = 'tickets'
TIMESTAMP_FILE = "./last_sync_timestamp.txt"
OUTPUT_DIR = "out"

# --- Utility Functions ---

def load_last_sync_time():
    """Loads the last successful sync time from the file, defaults to 0."""
    if os.path.exists(TIMESTAMP_FILE):
        with open(TIMESTAMP_FILE, 'r') as f:
            try:
                timestamp = f.read().strip()
                if timestamp:
                    return int(timestamp)
                else:
                    print("Warning: Timestamp file is empty. Starting from 0.")
                    return 0
            except ValueError:
                print("Warning: Could not read timestamp file. Starting from 0.")
                return 0
    return 0

def save_sync_time(timestamp):
    """Saves the current successful sync time to the file."""
    with open(TIMESTAMP_FILE, 'w') as f:
        f.write(str(timestamp))

def calculate_start_of_week():
    """
    Calculates the Unix timestamp for the most recent Monday at 00:00:00 UTC.
    The Zendesk API uses UTC, so we must be precise.
    """
    now_utc = datetime.utcnow()
    
    # Calculate days to subtract to get to Monday (Monday is day 0)
    days_to_monday = now_utc.weekday()
    
    # Calculate the datetime for Monday 00:00:00 UTC
    start_of_week = now_utc - timedelta(days=days_to_monday)
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Convert to Unix timestamp
    return int(start_of_week.timestamp())

def get_total_ticket_count(auth):
    """Fetches the total approximate ticket count for the account."""
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets/count.json"
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get('count', {}).get('value')
        refreshed_at = data.get('count', {}).get('refreshed_at')

        if total_count is not None:
            print(f"✅ Total Approximate Ticket Count: {total_count:,}")
            if refreshed_at:
                refreshed_dt = datetime.fromisoformat(refreshed_at.replace('Z', '+00:00'))
                print(f"   (Count last refreshed at: {refreshed_dt.strftime('%Y-%m-%d %H:%M:%S UTC')})")
            return total_count
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching ticket count: {e}")
        return None
        
    return None

def get_ticket_fields(auth):
    """Fetches all ticket fields (system and custom), saves them to a file, and returns the data."""
    
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/ticket_fields.json"
    all_fields = []
    
    print("\n--- Fetching Ticket Fields ---")
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()

        data = response.json()
        fields = data.get('ticket_fields')
        
        if fields:
            all_fields.extend(fields)
            print(f"-> Successfully fetched {len(all_fields)} ticket fields.")
            
            # --- Saving the Field Data ---
            filename = f"{OUTPUT_DIR}/ticket_fields.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_fields, f, ensure_ascii=False, indent=2)
            print(f"-> Field data saved to {filename}")
            # ---------------------------

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching ticket fields: {e}")
        return None
        
    return all_fields

# --- Core Extraction Function ---

def extract_zendesk_data(resource, start_time, auth, target_stop_timestamp):
    """Fetches data from Zendesk Incremental Export API, stopping when the next
       page timestamp exceeds the target_stop_timestamp."""
    
    # URL includes 'include=users' to attach user data to the records
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/incremental/{resource}.json?start_time={start_time}&include=users"
    
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
        
        # --- CRITICAL EARLY EXIT CHECK ---
        next_url = data.get('next_page')
        
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
    if not all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN]):
        print("!!! Error: ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, or ZENDESK_API_TOKEN not found in environment variables.")
        return

    auth = (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)
    
    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # DYNAMIC CALCULATION: Set the target stop time for this run (Start of Week UTC)
    TARGET_STOP_TIMESTAMP = calculate_start_of_week()
    print(f"🎯 Target Stop Time (Start of Week): {datetime.utcfromtimestamp(TARGET_STOP_TIMESTAMP).strftime('%Y-%m-%d %H:%M:%S UTC')} ({TARGET_STOP_TIMESTAMP})")

    # 1. Query Ticket Fields and Save Data
    get_ticket_fields(auth)
    
    # 2. Get the total ticket count
    get_total_ticket_count(auth)
        
    # 3. Load the timestamp from the previous run
    last_sync_time = load_last_sync_time()
    
    # --- Check for early termination ---
    if last_sync_time >= TARGET_STOP_TIMESTAMP:
        print(f"\n✅ Data already synced up to {datetime.utcfromtimestamp(last_sync_time).strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        print("   No extraction needed for this period.")
        return

    if last_sync_time == 0:
        print("\n!!! WARNING: Starting from timestamp 0. This will pull ALL historical records!")
        print("!!! This may take a long time and consume a large portion of your daily rate limit.")
        time.sleep(2) 
    
    # 4. Extract data: Pass the target stop time to the extraction function
    all_records, api_end_time = extract_zendesk_data(RESOURCE, last_sync_time, auth, TARGET_STOP_TIMESTAMP)
    
    # 5. Save ALL fetched records and update the sync timestamp
    if all_records:
        # Save all_records to a JSON file, using the target time for the filename
        filename = f"{OUTPUT_DIR}/{RESOURCE}_until_{TARGET_STOP_TIMESTAMP}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"\n-> All {len(all_records):,} records saved to {filename}")

    # CRITICAL STEP: Save the TARGET_STOP_TIMESTAMP, not the API's end_time.
    save_sync_time(TARGET_STOP_TIMESTAMP)
    print(f"-> New sync timestamp ({TARGET_STOP_TIMESTAMP}) saved for next run, effectively halting future extraction at start of week.")


if __name__ == "__main__":
    main()