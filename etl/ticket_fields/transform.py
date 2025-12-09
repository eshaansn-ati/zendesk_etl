"""
Ticket Fields Transformation Script

Transforms ticket_fields data into field inventory and field map.
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from hydra import compose, initialize

# Load environment variables
load_dotenv()

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.transform_utils import load_latest_file_from_dir
from etl.transform import create_id_mapping


def flatten_ticket_fields(json_data):
    """
    Flatten ticket fields data into field inventory.
    
    Args:
        json_data: List of ticket field dictionaries from JSON
        
    Returns:
        List of flattened field inventory records
    """
    field_inventory = []
    for field in json_data:
        field_info = {
            "id": field["id"],
            "title": field["title"],
            "type": field["type"],
            "description": field.get("description"),
            "created_at": field.get("created_at"),
            "updated_at": field.get("updated_at"),
            "active": field.get("active"),
            "required": field.get("required"),
        }
        
        if "custom_field_options" in field:
            field_info["custom_field_options"] = field["custom_field_options"]
        
        field_inventory.append(field_info)
    
    return field_inventory


def main():
    """Main transformation function for ticket_fields."""
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=ticket_fields"] + sys.argv[1:])
        
        data_dir = cfg.paths.data_dir
        extract_path = cfg.extract["ticket_fields"]
        transform_path = cfg.transform["ticket_fields"]
        
        input_dir = f"{data_dir}/{extract_path}"
        output_dir = f"{data_dir}/{transform_path}"
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"Transforming ticket_fields")
        print(f"{'='*60}")
        
        # Try both patterns: ticket_fields_{timestamp}.json (simple) and ticket_fields_until_{timestamp}.json (incremental)
        file_name = load_latest_file_from_dir(input_dir, file_pattern="ticket_fields_*.json")
        if not file_name:
            file_name = load_latest_file_from_dir(input_dir, file_pattern="ticket_fields_until_*.json")
        
        if not file_name or not os.path.exists(file_name):
            print(f"❌ Error: File not found! Please make sure ticket_fields data exists in '{input_dir}'")
            sys.exit(1)
        
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                print(f"Reading data from {file_name}...")
                json_data = json.load(f)
            
            # Extract field inventory
            field_inventory = flatten_ticket_fields(json_data)
            
            # Save processed fields
            timestamp = int(datetime.now().timestamp())
            output_file = f"{output_dir}/ticket_fields_{timestamp}.json"
            with open(output_file, 'w', encoding='utf-8') as outf:
                json.dump(field_inventory, outf, ensure_ascii=False, indent=4)
            print(f"✅ Processed field inventory saved to '{output_file}'")
            
            # Create field map (ID to Title)
            field_map = create_id_mapping(
                data=json_data,
                output_dir=output_dir,
                timestamp=timestamp,
                mapping_name="field_map",
                key_extractor=lambda field: str(field.get("id")),
                value_extractor=lambda field: field.get("title"),
                entity_name="fields"
            )
            
            print(f"\n✅ Successfully transformed ticket_fields")
            
        except json.JSONDecodeError:
            print(f"❌ Error: Failed to decode JSON from '{file_name}'")
            sys.exit(1)
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()

