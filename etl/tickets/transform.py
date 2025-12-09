"""
Tickets Transformation Script

Transforms tickets data into flattened CSV.
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


def flatten_tickets(ticket_data):
    """
    Flatten ticket data into a list of dictionaries.
    
    Args:
        ticket_data: List of ticket dictionaries from JSON
        
    Returns:
        List of flattened ticket records
    """
    flattened_data = []
    
    for ticket in ticket_data:
        # Get IDs
        org_id = ticket.get("organization_id")
        requester_id = ticket.get("requester_id")
        assignee_id = ticket.get("assignee_id")
        submitter_id = ticket.get("submitter_id")
        
        # Also check if assignee/requester are objects with id
        assignee_obj = ticket.get("assignee")
        if assignee_obj and isinstance(assignee_obj, dict):
            assignee_id = assignee_obj.get("id") or assignee_id
        
        requester_obj = ticket.get("requester")
        if requester_obj and isinstance(requester_obj, dict):
            requester_id = requester_obj.get("id") or requester_id
        
        submitter_obj = ticket.get("submitter")
        if submitter_obj and isinstance(submitter_obj, dict):
            submitter_id = submitter_obj.get("id") or submitter_id
        
        flat_record = {
            "ticket_id": ticket.get("id"),
            "subject": ticket.get("subject"),
            "description": ticket.get("description"),
            "status": ticket.get("status"),
            "priority": ticket.get("priority"),
            "created_at": ticket.get("created_at"),
            "updated_at": ticket.get("updated_at"),
            "organization_id": org_id,
            "requester_id": requester_id,
            "assignee_id": assignee_id,
            "submitter_id": submitter_id,
        }
        
        flattened_data.append(flat_record)
    
    return flattened_data


def main():
    """Main transformation function for tickets."""
    # Initialize Hydra
    config_path = os.path.relpath(os.path.join(project_root, "config", "hydra"), os.path.dirname(__file__))
    with initialize(config_path=config_path, version_base=None):
        cfg = compose(config_name="config", overrides=["endpoint=tickets"] + sys.argv[1:])
        
        data_dir = cfg.paths.data_dir
        extract_path = cfg.extract["tickets"]
        transform_path = cfg.transform["tickets"]
        
        input_dir = f"{data_dir}/{extract_path}"
        output_dir = f"{data_dir}/{transform_path}"
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"Transforming tickets")
        print(f"{'='*60}")
        
        # Load ticket data
        # Try both patterns: tickets_{timestamp}.json (simple) and tickets_until_{timestamp}.json (incremental)
        file_name = load_latest_file_from_dir(input_dir, file_pattern="tickets_*.json")
        if not file_name:
            file_name = load_latest_file_from_dir(input_dir, file_pattern="tickets_until_*.json")
        
        if not file_name:
            print("❌ Cannot proceed without ticket data file.")
            sys.exit(1)
        
        # Load ticket data
        ticket_data_df = pd.read_json(file_name)
        ticket_data = ticket_data_df.to_dict('records')
        print(f"Loaded {len(ticket_data)} tickets from {file_name}")
        
        # Flatten the data
        print(f"\nStarting transformation of {len(ticket_data)} records...")
        flattened_data = flatten_tickets(ticket_data)
        print("Transformation complete.")
        
        # Export to CSV
        output_filename = f"{output_dir}/tickets_{int(datetime.now().timestamp())}.csv"
        df = pd.DataFrame(flattened_data)
        df.to_csv(output_filename, index=False)
        print(f"\n✅ Success! Table saved to: {output_filename}")
        print(f"\n✅ Successfully transformed tickets")


if __name__ == "__main__":
    main()

