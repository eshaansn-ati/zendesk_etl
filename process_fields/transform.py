import json
import dotenv
import os
import glob
from datetime import datetime

dotenv.load_dotenv()

DATA_DIR = os.getenv("DATA_DIR")
EXTRACT_FIELDS_PATH = os.getenv("EXTRACT_FIELDS_PATH")
TRANSFORM_FIELDS_PATH = os.getenv("TRANSFORM_FIELDS_PATH")

INPUT_DIR = f"{DATA_DIR}/{EXTRACT_FIELDS_PATH}"

OUTPUT_DIR = f"{DATA_DIR}/{TRANSFORM_FIELDS_PATH}"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def extract_zendesk_fields(data):
    """
    Processes Zendesk ticket_fields data (already loaded as a dictionary) 
    to extract field metadata and custom options.
    """
    field_inventory = []
    for field in data:
        field_inventory.append({
            "id": field["id"],
            "title": field["title"],
            "type": field["type"],
            "description": field["description"],
            "created_at": field["created_at"],
            "updated_at": field["updated_at"],
            "active": field["active"],
            "required": field["required"],
    
        })

        if "custom_field_options" in field:
            field_inventory[-1]["custom_field_options"] = field["custom_field_options"]

    return field_inventory


def main():
    if not all([DATA_DIR, EXTRACT_FIELDS_PATH, TRANSFORM_FIELDS_PATH]):
        print("!!! Error: DATA_DIR, EXTRACT_FIELDS_PATH, or TRANSFORM_FIELDS_PATH not found in environment variables.")
        return

    field_files = glob.glob(f"{INPUT_DIR}/ticket_fields_*.json")
    timestamped_files = []

    for filepath in field_files:
        fname = os.path.basename(filepath)
        parts = fname.split("_")
        # Expect format: ticket_fields_{timestamp}.json
        if len(parts) == 3 and parts[2].endswith(".json"):
            ts_str = parts[2].replace(".json", "")
            try:
                timestamp = int(ts_str)
                timestamped_files.append((timestamp, filepath))
            except ValueError:
                continue

    if not timestamped_files:
        print(f"🚨 Error: No ticket field files with unix timestamp found in '{INPUT_DIR}'. Please run the extraction first.")
        FILE_NAME = None
    else:
        # Select file with largest timestamp
        latest_timestamp, latest_file = max(timestamped_files, key=lambda t_file: t_file[0])
        FILE_NAME = latest_file
        print(f"✅ Latest ticket fields file by unix timestamp: {FILE_NAME}")

    # 1. Check if the file exists in the given directory
    if not os.path.exists(FILE_NAME):
        print(f"🚨 Error: File not found! Please make sure '{FILE_NAME}' is in the given directory.")
    else:
        # 2. Open and load the JSON file
        try:
            with open(FILE_NAME, 'r', encoding='utf-8') as f:
                print(f"Reading data from {FILE_NAME}...")
                json_data = json.load(f)

            # 3. Process the loaded data
            processed_fields = extract_zendesk_fields(json_data)

            # 4. Output the results
            print("\n✅ Processing Complete. Extracted Field Inventory:")
            print("--------------------------------------------------")
            
            # Print the final result in a nicely formatted JSON output
            print(json.dumps(processed_fields, indent=4))
            # Save the processed fields to a JSON file
            output_file = f"{OUTPUT_DIR}/ticket_fields_{int(datetime.now().timestamp())}.json"
            try:
                with open(output_file, 'w', encoding='utf-8') as outf:
                    json.dump(processed_fields, outf, ensure_ascii=False, indent=4)
                print(f"\n✅ Processed field inventory saved to '{output_file}'")
            except Exception as save_err:
                print(f"❌ Error saving processed field inventory to '{output_file}': {save_err}")
        except json.JSONDecodeError:
            print(f"❌ Error: Failed to decode JSON from '{FILE_NAME}'. Check if the file contents are valid JSON.")
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()