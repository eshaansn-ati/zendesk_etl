"""
Database Utilities

Utility functions for PostgreSQL database operations.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_connection():
    """
    Create and return a PostgreSQL database connection.
    
    Reads connection parameters from environment variables:
    - DB_HOST: Database host (default: localhost)
    - DB_PORT: Database port (default: 5432)
    - DB_NAME: Database name
    - DB_USER: Database user
    - DB_PASSWORD: Database password
    
    Returns:
        psycopg2 connection object
    """
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    if not all([dbname, user, password]):
        raise ValueError(
            "Missing required database environment variables. "
            "Please set DB_NAME, DB_USER, and DB_PASSWORD."
        )
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn


def create_table_from_df(
    conn,
    df: pd.DataFrame,
    table_name: str,
    primary_key: Optional[str] = None,
    if_exists: str = "replace"
):
    """
    Create a PostgreSQL table from a pandas DataFrame.
    
    Args:
        conn: PostgreSQL connection object
        df: pandas DataFrame to create table from
        table_name: Name of the table to create
        primary_key: Optional primary key column name
        if_exists: What to do if table exists ("replace", "append", "fail")
    """
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    table_exists = cursor.fetchone()[0]
    
    if table_exists and if_exists == "fail":
        raise ValueError(f"Table {table_name} already exists and if_exists='fail'")
    
    # Generate column definitions
    columns = []
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas dtypes to PostgreSQL types
        if pd.api.types.is_integer_dtype(dtype):
            pg_type = "BIGINT"
        elif pd.api.types.is_bool_dtype(dtype):
            pg_type = "BOOLEAN"
        elif pd.api.types.is_float_dtype(dtype):
            # Only check for boolean if column name suggests it might be boolean
            # (e.g., starts with "is_", "has_", "alert_", ends with "_flag", etc.)
            # This prevents misclassifying numeric IDs as boolean
            col_lower = col.lower()
            is_likely_boolean = (
                col_lower.startswith(('is_', 'has_', 'alert_', 'enable_', 'disable_')) or
                col_lower.endswith(('_flag', '_enabled', '_disabled', '_active', '_required'))
            )
            
            if is_likely_boolean:
                non_null_vals = df[col].dropna().unique()
                if len(non_null_vals) <= 2:
                    # Check if values are boolean-like (0/1, True/False, etc.)
                    str_vals = [str(v).lower() for v in non_null_vals]
                    if all(s in ['true', 'false', '1', '0', '1.0', '0.0'] for s in str_vals):
                        pg_type = "BOOLEAN"
                    else:
                        pg_type = "DOUBLE PRECISION"
                else:
                    pg_type = "DOUBLE PRECISION"
            else:
                pg_type = "DOUBLE PRECISION"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            pg_type = "TIMESTAMP"
        else:
            # Default to TEXT for strings and other types
            pg_type = "TEXT"
        
        col_def = f"{col} {pg_type}"
        columns.append(col_def)
    
    # Create table SQL
    if table_exists and if_exists == "replace":
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(table_name)
        ))
        conn.commit()
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
            {f', PRIMARY KEY ({primary_key})' if primary_key else ''}
        );
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()


def load_last_load_timestamp(table_name: str, config_dir: str = "./config") -> int:
    """
    Load the last load timestamp for a table.
    
    Args:
        table_name: Name of the table
        config_dir: Directory where timestamp files are stored
        
    Returns:
        Unix timestamp of last load, or 0 if not found
    """
    timestamp_file = os.path.join(config_dir, f"last_load_timestamp_{table_name}.txt")
    if os.path.exists(timestamp_file):
        try:
            with open(timestamp_file, 'r') as f:
                timestamp = f.read().strip()
                if timestamp:
                    return int(timestamp)
        except (ValueError, IOError):
            pass
    return 0


def save_last_load_timestamp(table_name: str, timestamp: int, config_dir: str = "./config"):
    """
    Save the last load timestamp for a table.
    
    Args:
        table_name: Name of the table
        timestamp: Unix timestamp to save
        config_dir: Directory where timestamp files are stored
    """
    os.makedirs(config_dir, exist_ok=True)
    timestamp_file = os.path.join(config_dir, f"last_load_timestamp_{table_name}.txt")
    with open(timestamp_file, 'w') as f:
        f.write(str(timestamp))


def parse_iso_timestamp(ts_str: str) -> Optional[int]:
    """
    Parse ISO timestamp string to Unix timestamp.
    Handles formats like: '2025-09-05T09:40:36Z', '2025-08-26 03:52:27+00:00'
    
    Args:
        ts_str: ISO timestamp string
        
    Returns:
        Unix timestamp (seconds since epoch), or None if parsing fails
    """
    if pd.isna(ts_str) or not ts_str:
        return None
    
    try:
        # Try parsing with Z suffix
        if ts_str.endswith('Z'):
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(ts_str.replace(' ', 'T'))
        
        return int(dt.timestamp())
    except (ValueError, AttributeError):
        return None


def load_csv_to_postgres(
    csv_path: str,
    table_name: str,
    primary_key: Optional[str] = None,
    if_exists: str = "replace",
    batch_size: int = 10000,
    incremental: bool = False,
    updated_at_column: str = "updated_at",
    config_dir: str = "./config"
):
    """
    Load a CSV file into a PostgreSQL table.
    
    Args:
        csv_path: Path to the CSV file
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name
        if_exists: What to do if table exists ("replace", "append", "fail", "incremental")
        batch_size: Number of rows to insert per batch
        incremental: If True, only load records updated since last load
        updated_at_column: Column name containing update timestamp
        config_dir: Directory for storing load timestamps
        
    Returns:
        Number of rows loaded
    """
    mode = "incremental" if incremental else if_exists
    print(f"Loading CSV from {csv_path} to table {table_name} (mode: {mode})...")
    
    # Read CSV in chunks for large files
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists first
        table_already_exists = table_exists(conn, table_name)
        
        # Read first chunk to get schema
        # Read the full file with low_memory=False to accurately detect column types
        # This prevents pandas from inferring types per-chunk which can cause inconsistencies
        text_columns = ['description', 'title', 'type', 'name', 'email', 'subject', 'custom_field_options']
        # Read full file to get accurate dtypes (pandas will infer correctly with low_memory=False)
        df_chunk = pd.read_csv(csv_path, low_memory=False)
        # For schema creation, we only need a sample, but we want accurate dtypes
        df_chunk = df_chunk.head(1000)  # Use first 1000 rows for schema, but dtypes from full read
        
        # Detect boolean columns - columns that contain True/False values (even with NaN)
        boolean_columns = []
        for col in df_chunk.columns:
            if col not in text_columns:
                # Check if column contains boolean-like values
                non_null_values = df_chunk[col].dropna()
                if len(non_null_values) > 0:
                    # Check if all non-null values are boolean-like
                    unique_vals = non_null_values.unique()
                    if len(unique_vals) <= 2 and all(str(v).lower() in ['true', 'false', '1', '0', '1.0', '0.0'] for v in unique_vals):
                        boolean_columns.append(col)
        
        # Ensure text columns are preserved as object dtype
        for col in df_chunk.columns:
            if col in text_columns:
                df_chunk[col] = df_chunk[col].astype(str).astype('object')
            elif col in boolean_columns:
                # Convert boolean columns to boolean dtype (handles NaN properly)
                df_chunk[col] = df_chunk[col].astype('boolean')  # pandas nullable boolean
        
        # Create table if it doesn't exist or if replacing
        if not table_already_exists or if_exists == "replace":
            create_table_from_df(conn, df_chunk, table_name, primary_key, if_exists)
        
        # If replacing and table existed, truncate it first
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
        
        # Track max updated_at timestamp for incremental loads
        max_updated_at = last_load_ts
        
        # Load data in batches
        total_rows = 0
        filtered_rows = 0
        
        # Read CSV in chunks
        # For text columns, ensure they're read as strings to match TEXT schema
        text_columns = ['description', 'title', 'type', 'name', 'email', 'subject', 'custom_field_options']
        
        # Detect boolean columns from the schema sample
        # Only detect columns that are explicitly boolean dtype, not numeric columns that happen to have few values
        boolean_columns = []
        for col in df_chunk.columns:
            if col not in text_columns:
                # Only treat as boolean if pandas explicitly says it's boolean dtype
                # Don't infer boolean from float columns with few values (could be IDs)
                if pd.api.types.is_bool_dtype(df_chunk[col]):
                    boolean_columns.append(col)
        
        for chunk in pd.read_csv(csv_path, chunksize=batch_size, low_memory=False):
            # Ensure text columns are preserved as strings
            for col in chunk.columns:
                if col in text_columns:
                    chunk[col] = chunk[col].astype(str)
                elif col in boolean_columns:
                    # Convert boolean columns properly, handling NaN
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('boolean')
            # For incremental loads, filter by updated_at
            if incremental:
                if updated_at_column not in chunk.columns:
                    print(f"⚠️  Warning: {updated_at_column} column not found. Loading all records.")
                else:
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
                    # Convert pandas NAType to None
                    if pd.isna(val) or str(type(val).__name__) == 'NAType':
                        row_values.append(None)
                    else:
                        row_values.append(val)
                values.append(tuple(row_values))
            
            # Build insert query
            # Use ON CONFLICT if primary key exists to handle duplicates
            # This handles both incremental/append mode and duplicate records within the same load
            if primary_key:
                update_columns = [col for col in columns if col != primary_key]
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({primary_key}) DO UPDATE SET
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
            # Save new load timestamp
            if max_updated_at > last_load_ts:
                save_last_load_timestamp(table_name, max_updated_at, config_dir)
                print(f"  Saved load timestamp: {datetime.fromtimestamp(max_updated_at).isoformat()}")
        
        print(f"\n✅ Successfully loaded {total_rows} rows into {table_name}")
        return total_rows
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def table_exists(conn, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the table to check
        
    Returns:
        True if table exists, False otherwise
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists


def load_json_to_postgres(
    json_path: str,
    table_name: str,
    primary_key: Optional[str] = None,
    if_exists: str = "replace",
    batch_size: int = 10000,
    incremental: bool = False,
    updated_at_column: str = "updated_at",
    config_dir: str = "./config"
):
    """
    Load a JSON file into a PostgreSQL table.
    
    Args:
        json_path: Path to the JSON file (should be a list of objects)
        table_name: Name of the PostgreSQL table
        primary_key: Optional primary key column name
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
    import json
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        print("⚠️  Warning: JSON file is empty, no data to load.")
        return 0
    
    # Ensure text columns remain as object dtype to prevent type inference issues
    # Columns that should be preserved as text (not numeric)
    text_columns = ['description', 'title', 'type', 'name', 'email', 'subject', 'custom_field_options']
    for col in df.columns:
        if col in text_columns:
            # Convert to string first, then to object dtype to ensure it's treated as text
            # This prevents pandas from inferring numeric types even if values look numeric
            df[col] = df[col].astype(str).astype('object')
    
    # Create table schema directly from DataFrame to ensure correct types
    # Use a sample that preserves object dtypes for text columns
    conn = get_db_connection()
    try:
        table_already_exists = table_exists(conn, table_name)
        if not table_already_exists or if_exists == "replace":
            # Use first few rows to get a better sample for schema inference
            # but ensure text columns remain as object dtype
            df_sample = df.head(10).copy()
            # Explicitly set text columns to object dtype (convert via string to ensure it's text)
            for col in df_sample.columns:
                if col in text_columns:
                    df_sample[col] = df_sample[col].astype(str).astype('object')
            create_table_from_df(conn, df_sample, table_name, primary_key, if_exists)
        conn.close()
    except Exception as e:
        conn.close()
        raise e
    
    # Use CSV loading logic with temporary CSV
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        # Write CSV - text columns will be preserved as strings
        df.to_csv(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    try:
        rows_loaded = load_csv_to_postgres(
            csv_path=tmp_path,
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

