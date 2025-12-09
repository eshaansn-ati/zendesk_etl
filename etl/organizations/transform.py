"""
Organizations Transformation Script

Transforms organizations data into flattened CSV.
"""

import pandas as pd
import json
import os
import sys
import re
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
from utils.extract_utils import save_sync_time
from etl.transform import create_id_mapping


def flatten_organizations(orgs_data):
    """
    Flatten organization data into a list of dictionaries.
    
    Args:
        orgs_data: List of organization dictionaries from JSON
        
    Returns:
        List of flattened organization records
    """
    flattened_data = []
    for org in orgs_data:
        flat_record = {
            "organization_id": org.get("id"),
            "name": org.get("name"),
            "domain_names": org.get("domain_names"),
            "created_at": org.get("created_at"),
            "updated_at": org.get("updated_at"),
        }
        flattened_data.append(flat_record)
    return flattened_data


def main():
    """Main transformation function for organizations."""
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=organizations"] + sys.argv[1:])
        
        data_dir = cfg.paths.data_dir
        config_dir = cfg.paths.config_dir
        extract_path = cfg.extract["organizations"]
        transform_path = cfg.transform["organizations"]
        
        input_dir = f"{data_dir}/{extract_path}"
        output_dir = f"{data_dir}/{transform_path}"
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(config_dir, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"Transforming organizations")
        print(f"{'='*60}")
        
        # Find latest organizations file
        # Try both patterns: organizations_{timestamp}.json (simple) and organizations_until_{timestamp}.json (incremental)
        file_name = load_latest_file_from_dir(input_dir, file_pattern="organizations_*.json")
        if not file_name:
            file_name = load_latest_file_from_dir(input_dir, file_pattern="organizations_until_*.json")
        
        if not file_name:
            print("❌ Cannot proceed without organizations data file.")
            sys.exit(1)
        
        # Load organizations data
        with open(file_name, 'r', encoding='utf-8') as f:
            orgs_data = json.load(f)
        
        print(f"Loaded {len(orgs_data)} organizations from {file_name}")
        
        
        # Create organization ID to name mapping
        timestamp = int(datetime.now().timestamp())
        org_mapping = create_id_mapping(
            data=orgs_data,
            output_dir=output_dir,
            timestamp=timestamp,
            mapping_name="organization_map",
            key_extractor=lambda org: org.get("id"),
            value_extractor=lambda org: org.get("name"),
            entity_name="organizations"
        )
        
        # Basic transformation - flatten organization data
        flattened_data = flatten_organizations(orgs_data)
        
        # Export to CSV
        output_filename = f"{output_dir}/organizations_{int(datetime.now().timestamp())}.csv"
        df = pd.DataFrame(flattened_data)
        df.to_csv(output_filename, index=False)
        print(f"\n✅ Success! Organizations table saved to: {output_filename}")
        print(f"\n✅ Successfully transformed organizations")


if __name__ == "__main__":
    main()

