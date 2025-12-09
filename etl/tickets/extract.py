"""
Tickets Extraction Script

Extracts tickets data from Zendesk using incremental export API.
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

from etl.extract import run_extraction


def main():
    """Main extraction function for tickets."""
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=tickets"] + sys.argv[1:])
        
        # Build endpoint config from Hydra config
        endpoint_config = {
            'endpoint': cfg.endpoint,
            'use_incremental': cfg.use_incremental,
            'resource_key': cfg.resource_key,
            'output_subdir': cfg.output_subdir,
            'timestamp_file': cfg.timestamp_file,
            'include_param': cfg.include_param,
            'count_endpoint': cfg.count_endpoint if hasattr(cfg, 'count_endpoint') else None
        }
        
        success = run_extraction(cfg, endpoint_config)
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()

