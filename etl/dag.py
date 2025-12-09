"""
Zendesk ETL Pipeline DAG

This module defines a Prefect DAG (Directed Acyclic Graph) that orchestrates
the entire Zendesk ETL pipeline with proper dependencies.

Pipeline Structure:
1. Extract ticket_fields → Transform ticket_fields → Load ticket_fields
2. Extract organizations → Transform organizations → Load organizations
3. Extract users → Transform users → Load users
4. Extract tickets → Transform tickets (depends on ticket_fields transform) → Load tickets

The DAG ensures that:
- All extract steps can run in parallel (except tickets depends on ticket_fields)
- Transform steps run after their respective extract steps
- Load steps run after their respective transform steps
- Tickets transform waits for ticket_fields transform to complete
"""

import os
import sys
import subprocess
from pathlib import Path
from prefect import flow, task
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@task(name="extract_ticket_fields", log_prints=True)
def extract_ticket_fields():
    """Extract ticket fields from Zendesk."""
    script_path = project_root / "etl" / "ticket_fields" / "extract.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to extract ticket_fields (exit code: {result.returncode})")
    return True


@task(name="extract_organizations", log_prints=True)
def extract_organizations():
    """Extract organizations from Zendesk."""
    script_path = project_root / "etl" / "organizations" / "extract.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to extract organizations (exit code: {result.returncode})")
    return True


@task(name="extract_users", log_prints=True)
def extract_users():
    """Extract users from Zendesk."""
    script_path = project_root / "etl" / "users" / "extract.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to extract users (exit code: {result.returncode})")
    return True


@task(name="extract_tickets", log_prints=True)
def extract_tickets():
    """Extract tickets from Zendesk."""
    script_path = project_root / "etl" / "tickets" / "extract.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to extract tickets (exit code: {result.returncode})")
    return True


@task(name="transform_ticket_fields", log_prints=True)
def transform_ticket_fields(_extract_result):
    """Transform ticket fields data."""
    script_path = project_root / "etl" / "ticket_fields" / "transform.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to transform ticket_fields (exit code: {result.returncode})")
    return True


@task(name="transform_organizations", log_prints=True)
def transform_organizations(_extract_result):
    """Transform organizations data."""
    script_path = project_root / "etl" / "organizations" / "transform.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to transform organizations (exit code: {result.returncode})")
    return True


@task(name="transform_users", log_prints=True)
def transform_users(_extract_result):
    """Transform users data."""
    script_path = project_root / "etl" / "users" / "transform.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to transform users (exit code: {result.returncode})")
    return True


@task(name="transform_tickets", log_prints=True)
def transform_tickets(_extract_result, _ticket_fields_transform_result):
    """Transform tickets data (depends on ticket_fields transform for field mapping)."""
    script_path = project_root / "etl" / "tickets" / "transform.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to transform tickets (exit code: {result.returncode})")
    return True


@task(name="load_ticket_fields", log_prints=True)
def load_ticket_fields(_transform_result, incremental: bool = False):
    """Load ticket fields to PostgreSQL."""
    script_path = project_root / "etl" / "ticket_fields" / "load.py"
    cmd = [sys.executable, str(script_path)]
    if incremental:
        cmd.append("--incremental")
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to load ticket_fields (exit code: {result.returncode})")
    return True


@task(name="load_organizations", log_prints=True)
def load_organizations(_transform_result, incremental: bool = False):
    """Load organizations to PostgreSQL."""
    script_path = project_root / "etl" / "organizations" / "load.py"
    cmd = [sys.executable, str(script_path)]
    if incremental:
        cmd.append("--incremental")
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to load organizations (exit code: {result.returncode})")
    return True


@task(name="load_users", log_prints=True)
def load_users(_transform_result, incremental: bool = False):
    """Load users to PostgreSQL."""
    script_path = project_root / "etl" / "users" / "load.py"
    cmd = [sys.executable, str(script_path)]
    if incremental:
        cmd.append("--incremental")
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to load users (exit code: {result.returncode})")
    return True


@task(name="load_tickets", log_prints=True)
def load_tickets(_transform_result, incremental: bool = False):
    """Load tickets to PostgreSQL."""
    script_path = project_root / "etl" / "tickets" / "load.py"
    cmd = [sys.executable, str(script_path)]
    if incremental:
        cmd.append("--incremental")
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to load tickets (exit code: {result.returncode})")
    return True


@flow(name="zendesk_etl_pipeline", log_prints=True)
def zendesk_etl_pipeline(incremental: bool = False):
    """
    Main ETL pipeline flow.
    
    Args:
        incremental: If True, use incremental loading mode for database loads.
                    Defaults to False (full load).
    
    Pipeline execution order:
    1. Extract all endpoints (can run in parallel)
    2. Transform ticket_fields first (needed for tickets transform)
    3. Transform organizations and users (can run in parallel)
    4. Transform tickets (waits for ticket_fields transform)
    5. Load all endpoints (can run in parallel after transforms complete)
    """
    print("=" * 80)
    print("Starting Zendesk ETL Pipeline")
    print("=" * 80)
    
    # Step 1: Extract all endpoints (can run in parallel)
    print("\n[Step 1] Extracting data from Zendesk...")
    extract_ticket_fields_task = extract_ticket_fields()
    extract_organizations_task = extract_organizations()
    extract_users_task = extract_users()
    extract_tickets_task = extract_tickets()
    
    # Step 2: Transform ticket_fields first (needed for tickets transform)
    print("\n[Step 2] Transforming ticket_fields...")
    transform_ticket_fields_task = transform_ticket_fields(extract_ticket_fields_task)
    
    # Step 3: Transform organizations and users (can run in parallel)
    print("\n[Step 3] Transforming organizations and users...")
    transform_organizations_task = transform_organizations(extract_organizations_task)
    transform_users_task = transform_users(extract_users_task)
    
    # Step 4: Transform tickets (waits for ticket_fields transform)
    print("\n[Step 4] Transforming tickets...")
    transform_tickets_task = transform_tickets(extract_tickets_task, transform_ticket_fields_task)
    
    # Step 5: Load all endpoints (can run in parallel after transforms complete)
    print("\n[Step 5] Loading data to PostgreSQL...")
    load_ticket_fields_task = load_ticket_fields(transform_ticket_fields_task, incremental=incremental)
    load_organizations_task = load_organizations(transform_organizations_task, incremental=incremental)
    load_users_task = load_users(transform_users_task, incremental=incremental)
    load_tickets_task = load_tickets(transform_tickets_task, incremental=incremental)
    
    print("\n" + "=" * 80)
    print("✅ Zendesk ETL Pipeline completed successfully!")
    print("=" * 80)
    
    return {
        "extract": {
            "ticket_fields": extract_ticket_fields_task,
            "organizations": extract_organizations_task,
            "users": extract_users_task,
            "tickets": extract_tickets_task,
        },
        "transform": {
            "ticket_fields": transform_ticket_fields_task,
            "organizations": transform_organizations_task,
            "users": transform_users_task,
            "tickets": transform_tickets_task,
        },
        "load": {
            "ticket_fields": load_ticket_fields_task,
            "organizations": load_organizations_task,
            "users": load_users_task,
            "tickets": load_tickets_task,
        }
    }


if __name__ == "__main__":
    # Allow running with --incremental flag
    import argparse
    parser = argparse.ArgumentParser(description="Run Zendesk ETL Pipeline")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental loading mode for database loads"
    )
    args = parser.parse_args()
    
    zendesk_etl_pipeline(incremental=args.incremental)

