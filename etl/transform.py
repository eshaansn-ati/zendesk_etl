"""
General Zendesk Data Transformation Functions

This module provides reusable transformation functions for all Zendesk endpoints.
Endpoint-specific transform.py files import and use these functions.
"""

import json


def create_id_mapping(data, output_dir, timestamp, mapping_name, key_extractor, value_extractor, entity_name):
    """
    Creates a generic ID to value mapping from data records.
    
    Args:
        data: List of records to create mapping from
        output_dir: Directory to save mapping file
        timestamp: Timestamp for filename
        mapping_name: Name prefix for mapping file (e.g., "organization_map", "user_map", "field_map")
        key_extractor: Function to extract the key (ID) from a record
        value_extractor: Function to extract the value (name/title) from a record
        entity_name: Name of entity type for logging (e.g., "organizations", "users", "fields")
        
    Returns:
        Dictionary mapping key to value
    """
    print(f"\n--- Creating {entity_name.capitalize()} ID Mapping ---")
    
    mapping = {}
    for record in data:
        key = key_extractor(record)
        value = value_extractor(record)
        if key:
            mapping[key] = value
    
    # Save mapping
    if mapping:
        mapping_file = f"{output_dir}/{mapping_name}_{timestamp}.json"
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f"-> {entity_name.capitalize()} ID mapping saved: {mapping_file} ({len(mapping)} {entity_name})")
    
    return mapping

