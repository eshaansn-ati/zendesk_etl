import os
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import os

def load_last_sync_time(ts_file):
    """Loads the last successful sync time from the file, defaults to 0."""
    if os.path.exists(ts_file):
        with open(ts_file, 'r') as f:
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

def save_sync_time(timestamp, ts_file):
    """Saves the current successful sync time to the file."""
    with open(ts_file, 'w') as f:
        f.write(str(timestamp))

def calculate_start_of_week():
    """
    Calculates the Unix timestamp for the most recent Monday at 00:00:00 UTC.
    The Zendesk API uses UTC, so we must be precise.
    """
    now_utc = datetime.now()
    
    # Calculate days to subtract to get to Monday (Monday is day 0)
    days_to_monday = now_utc.weekday()
    
    # Calculate the datetime for Monday 00:00:00 UTC
    start_of_week = now_utc - timedelta(days=days_to_monday)
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Convert to Unix timestamp
    return int(start_of_week.timestamp())

def get_total_ticket_count(auth, url):
    """Fetches the total approximate ticket count for the account."""
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