import requests
import json
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN")
DATA_DIR = os.getenv("DATA_DIR")
EXTRACT_FIELDS_PATH = os.getenv("EXTRACT_FIELDS_PATH")

RESOURCE = 'ticket_fields'
FIELD_OUTPUT_DIR = f"{DATA_DIR}/{EXTRACT_FIELDS_PATH}"

def extract_zendesk_data(subdomain, resource, auth, output_dir):
    """Fetches all ticket fields (system and custom), saves them to a file, and returns the data."""
    url = f"https://{subdomain}.zendesk.com/api/v2/{resource}.json"
    all_fields = []
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        data = response.json()
        fields = data.get('ticket_fields')
        
        if fields:
            all_fields.extend(fields)
            print(f"-> Successfully fetched {len(all_fields)} ticket fields.")
            
            # --- Saving the Field Data ---
            filename = f"{output_dir}/{resource}_{int(datetime.now().timestamp())}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_fields, f, ensure_ascii=False, indent=2)
            print(f"-> Field data saved to {filename}")
            # ---------------------------

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching ticket fields: {e}")
        return None
        
    return all_fields

def main():
    if not all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN, DATA_DIR, EXTRACT_FIELDS_PATH]):
        print("!!! Error: ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN, DATA_DIR, or EXTRACT_FIELDS_PATH not found in environment variables.")
        return

    auth = (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    extract_zendesk_data(ZENDESK_SUBDOMAIN, RESOURCE, auth, FIELD_OUTPUT_DIR)

if __name__ == "__main__":
    main()