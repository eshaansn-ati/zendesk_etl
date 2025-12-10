"""
Database Utilities

Utility functions for PostgreSQL database operations.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import pandas as pd
from typing import Optional, Dict, List, Tuple
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


def get_table_columns(conn, table_name: str) -> set:
    """
    Get all column names from an existing table.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the table
        
    Returns:
        Set of column names
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = %s
    """, (table_name,))
    columns = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return columns


def add_missing_columns(conn, df: pd.DataFrame, table_name: str):
    """
    Add missing columns to an existing table based on DataFrame columns.
    
    Args:
        conn: PostgreSQL connection object
        df: pandas DataFrame with the desired schema
        table_name: Name of the table to alter
    """
    cursor = conn.cursor()
    
    # Get existing columns (PostgreSQL stores unquoted identifiers as lowercase)
    existing_columns = get_table_columns(conn, table_name)
    
    # Create a case-insensitive lookup: lowercase -> original case from DB
    existing_columns_lower = {col.lower(): col for col in existing_columns}
    
    # Get DataFrame columns and find truly missing ones (case-insensitive comparison)
    df_columns = list(df.columns)
    missing_columns = []
    
    for col in df_columns:
        # Check if column exists (case-insensitive)
        col_lower = col.lower()
        if col_lower not in existing_columns_lower:
            missing_columns.append(col)
    
    if not missing_columns:
        cursor.close()
        return
    
    print(f"  Adding {len(missing_columns)} missing column(s) to table {table_name}...")
    
    # Add each missing column, committing after each to avoid transaction issues
    for col in missing_columns:
        dtype = df[col].dtype
        
        # Map pandas dtypes to PostgreSQL types (same logic as create_table_from_df)
        if pd.api.types.is_integer_dtype(dtype):
            pg_type = "BIGINT"
        elif pd.api.types.is_bool_dtype(dtype):
            pg_type = "BOOLEAN"
        elif pd.api.types.is_float_dtype(dtype):
            pg_type = "DOUBLE PRECISION"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            pg_type = "TIMESTAMP"
        else:
            # Default to TEXT for strings and other types
            pg_type = "TEXT"
        
        # Use lowercase column name (unquoted) to match PostgreSQL's default behavior
        # PostgreSQL matches unquoted identifiers case-insensitively, so INSERT will work
        # even if DataFrame has mixed case column names
        alter_sql = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
            sql.Identifier(table_name),
            sql.Identifier(col.lower()),
            sql.SQL(pg_type)
        )
        
        try:
            cursor.execute(alter_sql)
            conn.commit()  # Commit after each successful addition
            print(f"    Added column: {col} ({pg_type})")
        except Exception as e:
            # Rollback the failed transaction
            conn.rollback()
            # Check if column already exists (might be a race condition or case-sensitivity issue)
            error_msg = str(e).lower()
            if 'already exists' in error_msg or 'duplicate' in error_msg:
                # Column exists with different case - this is okay, PostgreSQL will match them case-insensitively
                print(f"    ⚠️  Skipping column {col}: column already exists (case-insensitive match)")
            else:
                print(f"    ⚠️  Warning: Could not add column {col}: {e}")
    
    cursor.close()


def create_table_from_df(
    conn,
    df: pd.DataFrame,
    table_name: str,
    primary_key: Optional[str | List[str] | Tuple[str, ...]] = None,
    if_exists: str = "replace"
):
    """
    Create a PostgreSQL table from a pandas DataFrame.
    
    Args:
        conn: PostgreSQL connection object
        df: pandas DataFrame to create table from
        table_name: Name of the table to create
        primary_key: Optional primary key column name(s). Can be a string for single column,
                     or a list/tuple for composite primary key
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
    
    # Handle primary key (single or composite)
    primary_key_clause = ""
    if primary_key:
        if isinstance(primary_key, str):
            primary_key_clause = f", PRIMARY KEY ({primary_key})"
        elif isinstance(primary_key, (list, tuple)):
            pk_columns = ", ".join(primary_key)
            primary_key_clause = f", PRIMARY KEY ({pk_columns})"
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
            {primary_key_clause}
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


def infer_schema(
    df: pd.DataFrame,
    text_columns: Optional[List[str]] = None,
    sample_size: int = 1000
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Infer and fix schema for a DataFrame by detecting boolean columns
    and preserving text columns as object dtype.
    
    This function automatically samples the DataFrame if it's larger than sample_size
    for efficient schema detection, but processes and returns the full DataFrame.
    
    Args:
        df: pandas DataFrame to process
        text_columns: Optional list of column names that should be preserved as text.
                     Defaults to common text columns: ['description', 'title', 'type', 
                     'name', 'email', 'subject', 'custom_field_options']
        sample_size: Number of rows to sample for schema inference if DataFrame is large.
                    Defaults to 1000. Set to None to use the full DataFrame.
    
    Returns:
        Tuple of (processed DataFrame, list of boolean column names)
    """
    if text_columns is None:
        text_columns = ['description', 'title', 'type', 'name', 'email', 'subject', 'custom_field_options']
    
    # Sample DataFrame for schema detection if it's large
    if sample_size is not None and len(df) > sample_size:
        df_sample = df.head(sample_size).copy()
    else:
        df_sample = df.copy()
    
    # Detect boolean columns using the sample
    boolean_columns = []
    for col in df_sample.columns:
        if col not in text_columns:
            # Check if column contains boolean-like values
            non_null_values = df_sample[col].dropna()
            if len(non_null_values) > 0:
                try:
                    # Check if all non-null values are boolean-like
                    # Use try-except to handle unhashable types (e.g., arrays, lists)
                    unique_vals = non_null_values.unique()
                    # Convert to list of strings for comparison
                    unique_vals_str = [str(v).lower() for v in unique_vals]
                    if len(unique_vals) <= 2 and all(s in ['true', 'false', '1', '0', '1.0', '0.0'] for s in unique_vals_str):
                        boolean_columns.append(col)
                except (TypeError, ValueError):
                    # Skip columns with unhashable types (arrays, lists, etc.)
                    # These can't be boolean columns anyway
                    pass
    
    # Process the full DataFrame (or original if no sampling was needed)
    df_processed = df.copy()
    
    # Ensure text columns are preserved as object dtype
    for col in df_processed.columns:
        if col in text_columns:
            df_processed[col] = df_processed[col].astype(str).astype('object')
        elif col in boolean_columns:
            # Convert boolean columns to boolean dtype (handles NaN properly)
            # First convert to numeric, then to boolean for more robust conversion
            try:
                # Convert to numeric first (handles '1', '0', '1.0', '0.0', True, False)
                numeric_col = pd.to_numeric(df_processed[col], errors='coerce')
                # Then convert to boolean (0/1 -> False/True, NaN stays NaN)
                df_processed[col] = numeric_col.astype('boolean')
            except (TypeError, ValueError):
                # If conversion fails, try direct boolean conversion
                # This handles string 'true'/'false' and actual boolean values
                try:
                    df_processed[col] = df_processed[col].replace({
                        'true': True, 'True': True, 'TRUE': True, '1': True, 1: True, 1.0: True,
                        'false': False, 'False': False, 'FALSE': False, '0': False, 0: False, 0.0: False
                    }).astype('boolean')
                except (TypeError, ValueError):
                    # If all else fails, skip boolean conversion for this column
                    # Remove from boolean_columns list
                    boolean_columns.remove(col)
    
    return df_processed, boolean_columns



