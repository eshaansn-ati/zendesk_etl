"""
Users Transformation Script

Transforms users data into flattened Parquet.
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

from utils.transform_utils import load_latest_file_from_dir, deduplicate_dataframe, export_to_parquet
from utils.extract_utils import save_sync_time
from etl.transform import create_id_mapping


def flatten_users(users_data, transformed_at):
    """
    Flatten user data into a list of dictionaries.
    
    Args:
        users_data: List of user dictionaries from JSON
        transformed_at: ISO datetime string indicating when the data was transformed
        
    Returns:
        List of flattened user records
    """
    flattened_data = []
    for user in users_data:
        flat_record = {
            "user_id": user.get("id"),
            "name": user.get("name"),
            "email": user.get("email"),
            "role": user.get("role"),
            "created_at": user.get("created_at"),
            "updated_at": user.get("updated_at"),
            "active": user.get("active"),
            "verified": user.get("verified"),
            "_transformed_at": transformed_at,
        }
        flattened_data.append(flat_record)
    return flattened_data


def main():
    """Main transformation function for users."""
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=users"] + sys.argv[1:])
        
        data_dir = cfg.paths.data_dir
        config_dir = cfg.paths.config_dir
        extract_path = cfg.extract["users"]
        transform_path = cfg.transform["users"]
        
        input_dir = f"{data_dir}/{extract_path}"
        output_dir = f"{data_dir}/{transform_path}"
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(config_dir, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"Transforming users")
        print(f"{'='*60}")
        
        # Find latest users file
        # Try both patterns: users_{timestamp}.json (simple) and users_until_{timestamp}.json (incremental)
        file_name = load_latest_file_from_dir(input_dir, file_pattern="users_*.json")
        if not file_name:
            file_name = load_latest_file_from_dir(input_dir, file_pattern="users_until_*.json")
        
        if not file_name:
            print("❌ Cannot proceed without users data file.")
            sys.exit(1)
        
        # Load users data
        with open(file_name, 'r', encoding='utf-8') as f:
            users_data = json.load(f)
        
        print(f"Loaded {len(users_data)} users from {file_name}")
        
        # Get current timestamp for transformation
        transformed_at = datetime.now().isoformat()
        timestamp = int(datetime.now().timestamp())
        
        # Create user ID to name mapping
        user_mapping = create_id_mapping(
            data=users_data,
            output_dir=output_dir,
            timestamp=timestamp,
            mapping_name="user_map",
            key_extractor=lambda user: user.get("id"),
            value_extractor=lambda user: user.get("name"),
            entity_name="users"
        )
        
        # Basic transformation - flatten user data
        flattened_data = flatten_users(users_data, transformed_at=transformed_at)
        
        # Convert to DataFrame and deduplicate
        df = pd.DataFrame(flattened_data)
        df, _ = deduplicate_dataframe(
            df,
            primary_key_column='user_id',
            updated_at_column='updated_at',
            entity_name='users'
        )
        
        # Export to Parquet
        export_to_parquet(df, output_dir, "users", timestamp=timestamp)
        print(f"\n✅ Successfully transformed users")


if __name__ == "__main__":
    main()

