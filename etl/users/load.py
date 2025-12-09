"""
Users Load Script

Loads transformed users data to PostgreSQL database.
Supports both full and incremental loading modes.
"""

import os
import sys
from dotenv import load_dotenv
from hydra import compose, initialize

# Load environment variables
load_dotenv()

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.db_utils import load_csv_to_postgres
from utils.transform_utils import load_latest_file_from_dir


def main():
    """Main load function for users."""
    # Check for incremental flag
    incremental = "--incremental" in sys.argv or os.getenv("INCREMENTAL_LOAD", "false").lower() == "true"
    
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=users"] + sys.argv[1:])
        
        data_dir = cfg.paths.data_dir
        config_dir = cfg.paths.config_dir
        transform_path = cfg.transform["users"]
        
        transform_dir = f"{data_dir}/{transform_path}"
        
        mode_str = "incremental" if incremental else "full"
        print(f"\n{'='*60}")
        print(f"Loading users to PostgreSQL ({mode_str} mode)")
        print(f"{'='*60}")
        
        # Find latest users CSV file
        csv_file = load_latest_file_from_dir(transform_dir, file_pattern="users_*.csv")
        
        if not csv_file:
            print("❌ Cannot proceed without users CSV file.")
            sys.exit(1)
        
        # Load to PostgreSQL
        load_csv_to_postgres(
            csv_path=csv_file,
            table_name="users",
            primary_key="user_id",
            if_exists="replace",
            incremental=incremental,
            updated_at_column="updated_at",
            config_dir=config_dir
        )
        
        print(f"\n✅ Successfully loaded users to PostgreSQL")


if __name__ == "__main__":
    main()

