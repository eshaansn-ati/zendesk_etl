import json
import dotenv
import os
import glob

dotenv.load_dotenv()

DATA_DIR = os.getenv("DATA_DIR")
EXTRACT_FIELDS_PATH = os.getenv("EXTRACT_FIELDS_PATH")
INPUT_DIR = f"{DATA_DIR}/{EXTRACT_FIELDS_PATH}"

def extract_zendesk_fields(data):
    """
    Processes Zendesk ticket_fields data (already loaded as a dictionary) 
    to extract field metadata and custom options.
    """
    
    # Ensure the main 'ticket_fields' key exists
    if 'ticket_fields' not in data:
        print("Error: JSON data does not contain the 'ticket_fields' key.")
        return []

    field_inventory = []

    for field in data['ticket_fields']:
        
        # 1. Extract Core Metadata
        field_data = {
            'id': field.get('id'),
            'type': field.get('type'),
            'title': field.get('title'),
            'tag': field.get('tag'),
            'active': field.get('active')
        }

        # 2. Extract Options (Conditional Logic for dropdown/multi_select)
        if field_data['type'] in ('dropdown', 'multi_select'):
            
            options_list = []
            
            # Use .get() with an empty list default for safety
            options = field.get('custom_field_options', []) 
            
            for option in options:
                options_list.append({
                    'option_name': option.get('name'),
                    'option_tag': option.get('value'),
                    'option_id': option.get('id')
                })
            
            field_data['custom_options'] = options_list
        else:
            field_data['custom_options'] = 'N/A'
        
        field_inventory.append(field_data)
        
    return field_inventory

# --- Main execution block to handle file loading ---

if __name__ == "__main__":
    # --- Configuration ---

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
            output_file = f"{INPUT_DIR}/processed_field_inventory.json"
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