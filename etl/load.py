"""
Load Functions

Functions for loading data from Parquet and JSON files into PostgreSQL database.
"""

import os
import json
import tempfile
import pandas as pd
import numpy as np
from typing import Optional, List, Tuple
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2 import sql

from utils.load_utils import (
    get_db_connection,
    table_exists,
    create_table_from_df,
    load_last_load_timestamp,
    save_last_load_timestamp,
    parse_iso_timestamp,
    infer_schema,
    add_missing_columns
)

# Default text columns that should be preserved as object dtype
DEFAULT_TEXT_COLUMNS = ['description', 'title', 'type', 'name', 'email', 'subject', 'custom_field_options']


def load_parquet_incremental(
    parquet_path: Optional[str] = None,
    table_name: str = None,
    primary_key: Optional[str | List[str] | Tuple[str, ...]] = None,
    batch_size: int = 10000,
    updated_at_column: str = "updated_at",
    config_dir: str = "./config"
):
    """
    Incremental load function for Parquet files to PostgreSQL.
    
    This function is specifically designed for incremental loads only. It:
    - Loads only records updated since the last load timestamp
    - Requires the updated_at_column to exist in the data
    - Always saves the load timestamp after successful load
    - Never truncates or replaces the table
    
    Args:
        parquet_path: Path to the Parquet file
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name(s). Can be a string for single column,
                     or a list/tuple for composite primary key
        batch_size: Number of rows to insert per batch
        updated_at_column: Column name containing update timestamp (default: "updated_at")
        config_dir: Directory for storing load timestamps
        
    Returns:
        Number of rows loaded
        
    Raises:
        ValueError: If parquet_path or table_name is not provided
        ValueError: If updated_at_column is not found in the data
    """
    if not parquet_path:
        raise ValueError("parquet_path must be provided")
    if not table_name:
        raise ValueError("table_name must be provided")
    
    print(f"Loading Parquet incrementally from {parquet_path} to table {table_name}...")
    
    # Get database connection and cursor
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        table_already_exists = table_exists(conn, table_name)
        
        # Read Parquet file to get schema
        df_full = pd.read_parquet(parquet_path)
        
        # Check if updated_at_column exists
        if updated_at_column not in df_full.columns:
            raise ValueError(
                f"updated_at_column '{updated_at_column}' not found in data. "
                f"Available columns: {list(df_full.columns)}"
            )
        
        # Infer schema: detect boolean columns and preserve text columns
        df_sample, boolean_columns = infer_schema(df_full, sample_size=1000)
        
        # Add _loaded_at column to sample for schema creation
        df_sample['_loaded_at'] = datetime.now()
        
        # Create table if it doesn't exist, otherwise add missing columns
        if not table_already_exists:
            print(f"  Table {table_name} does not exist, creating it...")
            create_table_from_df(conn, df_sample, table_name, primary_key, "append")
        else:
            # Table exists - add any missing columns
            add_missing_columns(conn, df_sample, table_name)
        
        # Get last load timestamp
        last_load_ts = load_last_load_timestamp(table_name, config_dir)
        if last_load_ts > 0:
            print(f"  Last load timestamp: {datetime.fromtimestamp(last_load_ts).isoformat()}")
        else:
            print("  No previous load found, loading all records...")
        
        # Track max updated_at timestamp
        max_updated_at = last_load_ts
        
        # Get current timestamp for _loaded_at column
        current_timestamp = datetime.now()
        
        # Load data in batches
        total_rows = 0
        filtered_rows = 0
        
        # Reuse the already-read Parquet file
        parquet_file = df_full
        total_file_rows = len(parquet_file)
        
        # Process in batches
        for start_idx in range(0, total_file_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_file_rows)
            chunk = parquet_file.iloc[start_idx:end_idx].copy()
            
            # Add _loaded_at column with current timestamp
            chunk['_loaded_at'] = current_timestamp
            
            # Ensure text columns are preserved as strings
            for col in chunk.columns:
                if col in DEFAULT_TEXT_COLUMNS:
                    chunk[col] = chunk[col].astype(str)
                elif col in boolean_columns:
                    # Convert boolean columns properly, handling NaN
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('boolean')
            
            # Parse updated_at timestamps
            chunk['_parsed_updated_at'] = chunk[updated_at_column].apply(parse_iso_timestamp)
            
            # Filter records updated since last load
            if last_load_ts > 0:
                # Keep records where updated_at > last_load_ts or updated_at is null (new records)
                mask = (chunk['_parsed_updated_at'] > last_load_ts) | (chunk['_parsed_updated_at'].isna())
                filtered_rows += (~mask).sum()
                chunk = chunk[mask].copy()
            
            # Track max updated_at
            if not chunk.empty:
                valid_timestamps = chunk['_parsed_updated_at'].dropna()
                if not valid_timestamps.empty:
                    chunk_max = valid_timestamps.max()
                    if pd.notna(chunk_max) and chunk_max > max_updated_at:
                        max_updated_at = int(chunk_max)
            
            # Drop helper column
            chunk = chunk.drop(columns=['_parsed_updated_at'])
            
            if chunk.empty:
                continue
            
            # Convert NaN/NA to None for PostgreSQL
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Convert pandas NAType (from nullable boolean) to None
            for col in chunk.columns:
                if chunk[col].dtype.name == 'boolean':
                    chunk[col] = chunk[col].where(chunk[col].notna(), None)
            
            # Prepare data for insertion
            columns = list(chunk.columns)
            values = []
            for _, row in chunk.iterrows():
                row_values = []
                for val in row:
                    # Handle numpy arrays - convert to list/JSON for PostgreSQL
                    if isinstance(val, np.ndarray):
                        if val.size == 0:
                            row_values.append(None)
                        else:
                            val_list = val.tolist()
                            if isinstance(val_list, list):
                                val_list = [None if (isinstance(v, float) and pd.isna(v)) else v for v in val_list]
                            row_values.append(json.dumps(val_list) if val_list else None)
                    # Handle lists/dicts - convert to JSON string for PostgreSQL
                    elif isinstance(val, (list, dict)):
                        row_values.append(json.dumps(val) if val else None)
                    # Handle scalar NaN values
                    else:
                        try:
                            if pd.isna(val):
                                row_values.append(None)
                            elif str(type(val).__name__) == 'NAType':
                                row_values.append(None)
                            else:
                                row_values.append(val)
                        except (ValueError, TypeError):
                            row_values.append(val)
                values.append(tuple(row_values))
            
            # Build insert query with ON CONFLICT handling
            if primary_key:
                # Handle composite primary key
                if isinstance(primary_key, str):
                    pk_columns_str = primary_key
                    pk_columns_list = [primary_key]
                elif isinstance(primary_key, (list, tuple)):
                    pk_columns_str = ", ".join(primary_key)
                    pk_columns_list = list(primary_key)
                else:
                    pk_columns_str = str(primary_key)
                    pk_columns_list = [primary_key]
                
                update_columns = [col for col in columns if col not in pk_columns_list]
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({pk_columns_str}) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
                """
            else:
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                """
            
            # Execute batch insert
            execute_values(cursor, insert_query, values)
            conn.commit()
            
            total_rows += len(chunk)
            print(f"  Loaded {total_rows} rows...", end='\r')
        
        # Print filtered rows summary
        if filtered_rows > 0:
            print(f"\n  Filtered out {filtered_rows} rows (already loaded)")
        
        # Always save load timestamp for incremental loads
        if total_rows > 0:
            if max_updated_at > last_load_ts:
                save_last_load_timestamp(table_name, max_updated_at, config_dir)
                print(f"  Saved load timestamp: {datetime.fromtimestamp(max_updated_at).isoformat()}")
            else:
                # If no new records but we loaded some (e.g., null timestamps), save current timestamp
                current_ts = int(current_timestamp.timestamp())
                save_last_load_timestamp(table_name, current_ts, config_dir)
                print(f"  Saved load timestamp: {current_timestamp.isoformat()}")
        elif last_load_ts == 0:
            # First load with no data - save current timestamp
            current_ts = int(current_timestamp.timestamp())
            save_last_load_timestamp(table_name, current_ts, config_dir)
            print(f"  Saved initial load timestamp: {current_timestamp.isoformat()}")
        
        print(f"\n✅ Successfully loaded {total_rows} rows into {table_name} (incremental)")
        return total_rows
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def load_json_incremental(
    json_path: str,
    table_name: str,
    primary_key: Optional[str | List[str] | Tuple[str, ...]] = None,
    batch_size: int = 10000,
    updated_at_column: str = "updated_at",
    config_dir: str = "./config"
):
    """
    Incremental load function for JSON files to PostgreSQL.
    
    This function is specifically designed for incremental loads only. It converts
    the JSON file to Parquet format and uses load_parquet_incremental internally.
    
    Args:
        json_path: Path to the JSON file (should be a list of objects)
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name(s). Can be a string for single column,
                     or a list/tuple for composite primary key
        batch_size: Number of rows to insert per batch
        updated_at_column: Column name containing update timestamp (default: "updated_at")
        config_dir: Directory for storing load timestamps
        
    Returns:
        Number of rows loaded
    """
    print(f"Loading JSON incrementally from {json_path} to table {table_name}...")
    
    # Read JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        print("⚠️  Warning: JSON file is empty, no data to load.")
        return 0
    
    # Infer schema: detect boolean columns and preserve text columns
    df, _ = infer_schema(df)
    
    # Add _loaded_at column to DataFrame
    df['_loaded_at'] = datetime.now()
    
    # Use Parquet incremental loading logic with temporary Parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        # Write Parquet - text columns will be preserved as strings
        df.to_parquet(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    try:
        rows_loaded = load_parquet_incremental(
            parquet_path=tmp_path,
            table_name=table_name,
            primary_key=primary_key,
            batch_size=batch_size,
            updated_at_column=updated_at_column,
            config_dir=config_dir
        )
        return rows_loaded
    finally:
        # Clean up temporary file
        os.unlink(tmp_path)


# def load_csv_to_postgres(
#     csv_path: str,
#     table_name: str,
#     primary_key: Optional[str] = None,
#     if_exists: str = "replace",
#     batch_size: int = 10000,
#     incremental: bool = False,
#     updated_at_column: str = "updated_at",
#     config_dir: str = "./config"
# ):
#     """
#     Load a CSV file into a PostgreSQL table.
    
#     Args:
#         csv_path: Path to the CSV file
#         table_name: Name of the PostgreSQL table
#         primary_key: Optional primary key column name
#         if_exists: What to do if table exists ("replace", "append", "fail", "incremental")
#         batch_size: Number of rows to insert per batch
#         incremental: If True, only load records updated since last load
#         updated_at_column: Column name containing update timestamp
#         config_dir: Directory for storing load timestamps
        
#     Returns:
#         Number of rows loaded
#     """
#     mode = "incremental" if incremental else if_exists
#     print(f"Loading CSV from {csv_path} to table {table_name} (mode: {mode})...")
    
#     # Read CSV in chunks for large files
#     conn = get_db_connection()
#     cursor = conn.cursor()
    
#     try:
#         # Check if table exists first
#         table_already_exists = table_exists(conn, table_name)
        
#         # Read first chunk to get schema
#         # Read the full file with low_memory=False to accurately detect column types
#         # This prevents pandas from inferring types per-chunk which can cause inconsistencies
#         # Read full file to get accurate dtypes (pandas will infer correctly with low_memory=False)
#         df_chunk = pd.read_csv(csv_path, low_memory=False)
#         # For schema creation, we only need a sample, but we want accurate dtypes
#         df_chunk = df_chunk.head(1000)  # Use first 1000 rows for schema, but dtypes from full read
        
#         # Infer schema: detect boolean columns and preserve text columns
#         df_chunk, boolean_columns = infer_schema(df_chunk)
        
#         # Create table if it doesn't exist or if replacing
#         if not table_already_exists or if_exists == "replace":
#             create_table_from_df(conn, df_chunk, table_name, primary_key, if_exists)
        
#         # If replacing and table existed, truncate it first
#         if table_already_exists and if_exists == "replace" and not incremental:
#             cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(
#                 sql.Identifier(table_name)
#             ))
#             conn.commit()
        
#         # For incremental loads, get last load timestamp and filter data
#         last_load_ts = 0
#         if incremental:
#             last_load_ts = load_last_load_timestamp(table_name, config_dir)
#             if last_load_ts > 0:
#                 print(f"  Last load timestamp: {datetime.fromtimestamp(last_load_ts).isoformat()}")
#             else:
#                 print("  No previous load found, loading all records...")
        
#         # Track max updated_at timestamp for incremental loads
#         max_updated_at = last_load_ts
        
#         # Load data in batches
#         total_rows = 0
#         filtered_rows = 0
        
        
#         for chunk in pd.read_csv(csv_path, chunksize=batch_size, low_memory=False):
#             # Ensure text columns are preserved as strings
#             for col in chunk.columns:
#                 if col in DEFAULT_TEXT_COLUMNS:
#                     chunk[col] = chunk[col].astype(str)
#                 elif col in boolean_columns:
#                     # Convert boolean columns properly, handling NaN
#                     chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('boolean')
#             # For incremental loads, filter by updated_at
#             if incremental:
#                 if updated_at_column not in chunk.columns:
#                     print(f"⚠️  Warning: {updated_at_column} column not found. Loading all records.")
#                 else:
#                     # Parse updated_at timestamps
#                     chunk['_parsed_updated_at'] = chunk[updated_at_column].apply(parse_iso_timestamp)
                    
#                     # Filter records updated since last load
#                     if last_load_ts > 0:
#                         # Keep records where updated_at > last_load_ts or updated_at is null (new records)
#                         mask = (chunk['_parsed_updated_at'] > last_load_ts) | (chunk['_parsed_updated_at'].isna())
#                         filtered_rows += (~mask).sum()
#                         chunk = chunk[mask].copy()
                    
#                     # Track max updated_at
#                     if not chunk.empty:
#                         valid_timestamps = chunk['_parsed_updated_at'].dropna()
#                         if not valid_timestamps.empty:
#                             chunk_max = valid_timestamps.max()
#                             if pd.notna(chunk_max) and chunk_max > max_updated_at:
#                                 max_updated_at = int(chunk_max)
                    
#                     # Drop helper column
#                     chunk = chunk.drop(columns=['_parsed_updated_at'])
            
#             if chunk.empty:
#                 continue
            
#             # Convert NaN/NA to None for PostgreSQL
#             # Handle pandas nullable boolean types (BooleanDtype) which use NAType
#             chunk = chunk.where(pd.notnull(chunk), None)
            
#             # Convert pandas NAType (from nullable boolean) to None
#             # This handles cases where nullable boolean columns have NAType objects
#             for col in chunk.columns:
#                 if chunk[col].dtype.name == 'boolean':
#                     # Convert NAType to None for psycopg2 compatibility
#                     chunk[col] = chunk[col].where(chunk[col].notna(), None)
            
#             # Prepare data for insertion
#             columns = list(chunk.columns)
#             # Convert to list of tuples, ensuring NAType is converted to None
#             values = []
#             for _, row in chunk.iterrows():
#                 row_values = []
#                 for val in row:
#                     # Convert pandas NAType to None
#                     if pd.isna(val) or str(type(val).__name__) == 'NAType':
#                         row_values.append(None)
#                     else:
#                         row_values.append(val)
#                 values.append(tuple(row_values))
            
#             # Build insert query
#             # Use ON CONFLICT if primary key exists to handle duplicates
#             # This handles both incremental/append mode and duplicate records within the same load
#             if primary_key:
#                 update_columns = [col for col in columns if col != primary_key]
#                 insert_query = f"""
#                     INSERT INTO {table_name} ({', '.join(columns)})
#                     VALUES %s
#                     ON CONFLICT ({primary_key}) DO UPDATE SET
#                         {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
#                 """
#             else:
#                 insert_query = f"""
#                     INSERT INTO {table_name} ({', '.join(columns)})
#                     VALUES %s
#                 """
            
#             # Execute batch insert
#             execute_values(cursor, insert_query, values)
#             conn.commit()
            
#             total_rows += len(chunk)
#             print(f"  Loaded {total_rows} rows...", end='\r')
        
#         if incremental:
#             print(f"\n  Filtered out {filtered_rows} rows (already loaded)")
#             # Save new load timestamp
#             if max_updated_at > last_load_ts:
#                 save_last_load_timestamp(table_name, max_updated_at, config_dir)
#                 print(f"  Saved load timestamp: {datetime.fromtimestamp(max_updated_at).isoformat()}")
        
#         print(f"\n✅ Successfully loaded {total_rows} rows into {table_name}")
#         return total_rows
        
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         cursor.close()
#         conn.close()


def load_parquet_to_postgres(
    parquet_path: Optional[str] = None,
    table_name: str = None,
    primary_key: Optional[str | List[str] | Tuple[str, ...]] = None,
    if_exists: str = "replace",
    batch_size: int = 10000,
    incremental: bool = False,
    updated_at_column: str = "_loaded_at",
    config_dir: str = "./config"
):
    """
    Load a Parquet file into a PostgreSQL table.
    
    Args:
        parquet_path: Path to the Parquet file (preferred)
        csv_path: Path to the Parquet file (for backward compatibility, same as parquet_path)
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name(s). Can be a string for single column,
                     or a list/tuple for composite primary key
        if_exists: What to do if table exists ("replace", "append", "fail", "incremental")
        batch_size: Number of rows to insert per batch
        incremental: If True, only load records updated since last load
        updated_at_column: Column name containing update timestamp
        config_dir: Directory for storing load timestamps
        
    Returns:
        Number of rows loaded
    """
    # Support both parquet_path and csv_path for backward compatibility
    file_path = parquet_path
    if not file_path:
        raise ValueError("Either parquet_path or csv_path must be provided")
    if not table_name:
        raise ValueError("table_name must be provided")
    
    mode = "incremental" if incremental else if_exists
    print(f"Loading Parquet from {file_path} to table {table_name} (mode: {mode})...")
    
    # Get database connection and cursor
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists first
        table_already_exists = table_exists(conn, table_name)
        
        # Read Parquet file to get schema - Parquet preserves types better than CSV
        df_full = pd.read_parquet(file_path)
        
        # Infer schema: detect boolean columns and preserve text columns
        # infer_schema will automatically sample for schema detection
        df_sample, boolean_columns = infer_schema(df_full, sample_size=1000)
        
        # Add _loaded_at column to sample for schema creation
        df_sample['_loaded_at'] = datetime.now()
        
        # Create table if it doesn't exist or if replacing (but not in incremental mode)
        # In incremental mode, we should never drop/replace the table
        if not table_already_exists:
            # Table doesn't exist, create it (use "append" mode to avoid dropping)
            create_table_from_df(conn, df_sample, table_name, primary_key, "append")
        elif if_exists == "replace" and not incremental:
            # Table exists and we want to replace it (non-incremental mode)
            create_table_from_df(conn, df_sample, table_name, primary_key, if_exists)
        elif table_already_exists and incremental:
            # Table exists and we're in incremental mode - add any missing columns
            add_missing_columns(conn, df_sample, table_name)
        
        # If replacing and table existed, truncate it first (only in non-incremental mode)
        if table_already_exists and if_exists == "replace" and not incremental:
            cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(
                sql.Identifier(table_name)
            ))
            conn.commit()
        
        # For incremental loads, get last load timestamp and filter data
        last_load_ts = 0
        if incremental:
            last_load_ts = load_last_load_timestamp(table_name, config_dir)
            if last_load_ts > 0:
                print(f"  Last load timestamp: {datetime.fromtimestamp(last_load_ts).isoformat()}")
            else:
                print("  No previous load found, loading all records...")
        
        # Track max updated_at timestamp for both incremental and full loads
        # This allows future incremental loads to know where to start
        max_updated_at = last_load_ts
        
        # Get current timestamp for _loaded_at column
        current_timestamp = datetime.now()
        
        # Load data in batches
        total_rows = 0
        filtered_rows = 0
        
        # Reuse the already-read Parquet file
        parquet_file = df_full
        total_file_rows = len(parquet_file)
        
        # Process in batches
        for start_idx in range(0, total_file_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_file_rows)
            chunk = parquet_file.iloc[start_idx:end_idx].copy()
            
            # Add _loaded_at column with current timestamp
            chunk['_loaded_at'] = current_timestamp
            
            # Ensure text columns are preserved as strings
            for col in chunk.columns:
                if col in DEFAULT_TEXT_COLUMNS:
                    chunk[col] = chunk[col].astype(str)
                elif col in boolean_columns:
                    # Convert boolean columns properly, handling NaN
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('boolean')
            
            # Track max updated_at timestamp for both incremental and full loads
            # This is needed to save the timestamp for future incremental loads
            if updated_at_column in chunk.columns:
                # Parse updated_at timestamps
                chunk['_parsed_updated_at'] = chunk[updated_at_column].apply(parse_iso_timestamp)
                
                # For incremental loads, filter by updated_at
                if incremental:
                    # Filter records updated since last load
                    if last_load_ts > 0:
                        # Keep records where updated_at > last_load_ts or updated_at is null (new records)
                        mask = (chunk['_parsed_updated_at'] > last_load_ts) | (chunk['_parsed_updated_at'].isna())
                        filtered_rows += (~mask).sum()
                        chunk = chunk[mask].copy()
                
                # Track max updated_at (for both incremental and full loads)
                if not chunk.empty:
                    valid_timestamps = chunk['_parsed_updated_at'].dropna()
                    if not valid_timestamps.empty:
                        chunk_max = valid_timestamps.max()
                        if pd.notna(chunk_max) and chunk_max > max_updated_at:
                            max_updated_at = int(chunk_max)
                
                # Drop helper column
                chunk = chunk.drop(columns=['_parsed_updated_at'])
            elif incremental:
                # Only warn in incremental mode if updated_at_column is missing
                print(f"⚠️  Warning: {updated_at_column} column not found. Loading all records.")
            
            if chunk.empty:
                continue
            
            # Convert NaN/NA to None for PostgreSQL
            # Handle pandas nullable boolean types (BooleanDtype) which use NAType
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Convert pandas NAType (from nullable boolean) to None
            # This handles cases where nullable boolean columns have NAType objects
            for col in chunk.columns:
                if chunk[col].dtype.name == 'boolean':
                    # Convert NAType to None for psycopg2 compatibility
                    chunk[col] = chunk[col].where(chunk[col].notna(), None)
            
            # Prepare data for insertion
            columns = list(chunk.columns)
            # Convert to list of tuples, ensuring NAType is converted to None
            values = []
            for _, row in chunk.iterrows():
                row_values = []
                for val in row:
                    # Handle numpy arrays - convert to list/JSON for PostgreSQL
                    if isinstance(val, np.ndarray):
                        # Convert array to list, handling NaN values
                        if val.size == 0:
                            row_values.append(None)
                        else:
                            # Convert array to list, replacing NaN with None
                            val_list = val.tolist()
                            # Recursively handle NaN in nested structures
                            if isinstance(val_list, list):
                                val_list = [None if (isinstance(v, float) and pd.isna(v)) else v for v in val_list]
                            row_values.append(json.dumps(val_list) if val_list else None)
                    # Handle lists/dicts - convert to JSON string for PostgreSQL
                    elif isinstance(val, (list, dict)):
                        # Convert lists/dicts to JSON string for PostgreSQL
                        row_values.append(json.dumps(val) if val else None)
                    # Handle scalar NaN values
                    else:
                        # Check for NaN/NA - use try/except to handle edge cases
                        try:
                            # pd.isna works for scalars but may raise ValueError for arrays
                            # We already handled arrays above, so this should be safe
                            if pd.isna(val):
                                row_values.append(None)
                            elif str(type(val).__name__) == 'NAType':
                                row_values.append(None)
                            else:
                                row_values.append(val)
                        except (ValueError, TypeError):
                            # If pd.isna raises an error (e.g., ambiguous truth value),
                            # assume it's a valid value (we should have caught arrays above)
                            row_values.append(val)
                values.append(tuple(row_values))
            
            # Build insert query
            # Use ON CONFLICT if primary key exists to handle duplicates
            # This handles both incremental/append mode and duplicate records within the same load
            if primary_key:
                # Handle composite primary key
                if isinstance(primary_key, str):
                    pk_columns_str = primary_key
                    pk_columns_list = [primary_key]
                elif isinstance(primary_key, (list, tuple)):
                    pk_columns_str = ", ".join(primary_key)
                    pk_columns_list = list(primary_key)
                else:
                    pk_columns_str = str(primary_key)
                    pk_columns_list = [primary_key]
                
                update_columns = [col for col in columns if col not in pk_columns_list]
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({pk_columns_str}) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
                """
            else:
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                """
            
            # Execute batch insert
            execute_values(cursor, insert_query, values)
            conn.commit()
            
            total_rows += len(chunk)
            print(f"  Loaded {total_rows} rows...", end='\r')
        
        if incremental:
            print(f"\n  Filtered out {filtered_rows} rows (already loaded)")
        
        # Save load timestamp for both incremental and full loads
        # This allows future incremental loads to know when the last load occurred
        if total_rows > 0:
            if max_updated_at > last_load_ts:
                # Save the maximum updated_at timestamp from the loaded data
                save_last_load_timestamp(table_name, max_updated_at, config_dir)
                print(f"  Saved load timestamp: {datetime.fromtimestamp(max_updated_at).isoformat()}")
            elif not incremental:
                # For full loads, if we loaded data but max_updated_at wasn't updated 
                # (e.g., no updated_at column or all values are null),
                # save current timestamp as fallback
                current_ts = int(current_timestamp.timestamp())
                save_last_load_timestamp(table_name, current_ts, config_dir)
                print(f"  Saved load timestamp: {current_timestamp.isoformat()}")
        
        print(f"\n✅ Successfully loaded {total_rows} rows into {table_name}")
        return total_rows
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()



def load_json_to_postgres(
    json_path: str,
    table_name: str,
    primary_key: Optional[str | List[str] | Tuple[str, ...]] = None,
    if_exists: str = "replace",
    batch_size: int = 10000,
    incremental: bool = False,
    updated_at_column: str = "_loaded_at",
    config_dir: str = "./config"
):
    """
    Load a JSON file into a PostgreSQL table.
    
    Args:
        json_path: Path to the JSON file (should be a list of objects)
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name(s). Can be a string for single column,
                     or a list/tuple for composite primary key
        if_exists: What to do if table exists ("replace", "append", "fail")
        batch_size: Number of rows to insert per batch
        incremental: If True, only load records updated since last load
        updated_at_column: Column name containing update timestamp
        config_dir: Directory for storing load timestamps
        
    Returns:
        Number of rows loaded
    """
    print(f"Loading JSON from {json_path} to table {table_name}...")
    
    # Read JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        print("⚠️  Warning: JSON file is empty, no data to load.")
        return 0
    
    # Infer schema: detect boolean columns and preserve text columns
    df, _ = infer_schema(df)
    
    # Add _loaded_at column to DataFrame
    df['_loaded_at'] = datetime.now()
    
    # Create table schema directly from DataFrame to ensure correct types
    # Use a sample that preserves object dtypes for text columns
    conn = get_db_connection()
    try:
        table_already_exists = table_exists(conn, table_name)
        # In incremental mode, we should never drop/replace the table
        if not table_already_exists:
            # Table doesn't exist, create it (use "append" mode to avoid dropping)
            df_sample = df.head(10).copy() if len(df) > 10 else df.copy()
            # Infer schema on sample to ensure consistency
            df_sample, _ = infer_schema(df_sample, sample_size=10)
            # _loaded_at is already in df_sample from df
            create_table_from_df(conn, df_sample, table_name, primary_key, "append")
        elif if_exists == "replace" and not incremental:
            # Table exists and we want to replace it (non-incremental mode)
            df_sample = df.head(10).copy() if len(df) > 10 else df.copy()
            # Infer schema on sample to ensure consistency
            df_sample, _ = infer_schema(df_sample, sample_size=10)
            # _loaded_at is already in df_sample from df
            create_table_from_df(conn, df_sample, table_name, primary_key, if_exists)
        conn.close()
    except Exception as e:
        conn.close()
        raise e
    
    # Use Parquet loading logic with temporary Parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        # Write Parquet - text columns will be preserved as strings
        df.to_parquet(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    try:
        rows_loaded = load_parquet_to_postgres(
            parquet_path=tmp_path,
            table_name=table_name,
            primary_key=primary_key,
            if_exists=if_exists,
            batch_size=batch_size,
            incremental=incremental,
            updated_at_column=updated_at_column,
            config_dir=config_dir
        )
        return rows_loaded
    finally:
        # Clean up temporary file
        os.unlink(tmp_path)

