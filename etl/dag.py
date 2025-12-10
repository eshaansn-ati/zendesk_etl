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


@task(name="skip_placeholder", log_prints=True)
def skip_placeholder():
    """Placeholder task that returns True when a phase is skipped."""
    return True


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
def zendesk_etl_pipeline(
    incremental: bool = False,
    run_extract: bool = True,
    run_transform: bool = True,
    run_load: bool = True
):
    """
    Main ETL pipeline flow.
    
    Args:
        incremental: If True, use incremental loading mode for database loads.
                    Defaults to False (full load).
        run_extract: If True, run extract steps. Defaults to True.
        run_transform: If True, run transform steps. Defaults to True.
        run_load: If True, run load steps. Defaults to True.
    
    Pipeline execution order:
    1. Extract all endpoints (can run in parallel) - if run_extract=True
    2. Transform ticket_fields first (needed for tickets transform) - if run_transform=True
    3. Transform organizations and users (can run in parallel) - if run_transform=True
    4. Transform tickets (waits for ticket_fields transform) - if run_transform=True
    5. Load all endpoints (can run in parallel after transforms complete) - if run_load=True
    """
    print("=" * 80)
    print("Starting Zendesk ETL Pipeline")
    print("=" * 80)
    print(f"Configuration: extract={run_extract}, transform={run_transform}, load={run_load}, incremental={incremental}")
    print("=" * 80)
    
    # Initialize task variables
    extract_ticket_fields_task = None
    extract_organizations_task = None
    extract_users_task = None
    extract_tickets_task = None
    
    transform_ticket_fields_task = None
    transform_organizations_task = None
    transform_users_task = None
    transform_tickets_task = None
    
    load_ticket_fields_task = None
    load_organizations_task = None
    load_users_task = None
    load_tickets_task = None
    
    # Step 1: Extract all endpoints (can run in parallel)
    if run_extract:
        print("\n[Step 1] Extracting data from Zendesk...")
        extract_ticket_fields_task = extract_ticket_fields()
        extract_organizations_task = extract_organizations()
        extract_users_task = extract_users()
        extract_tickets_task = extract_tickets()
    else:
        print("\n[Step 1] Skipping extract (run_extract=False)")
        # Create placeholder tasks to satisfy dependencies
        extract_ticket_fields_task = skip_placeholder()
        extract_organizations_task = skip_placeholder()
        extract_users_task = skip_placeholder()
        extract_tickets_task = skip_placeholder()
    
    # Step 2-4: Transform steps
    if run_transform:
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
    else:
        print("\n[Step 2-4] Skipping transform (run_transform=False)")
        # Create placeholder tasks to satisfy dependencies
        # Note: Transform tasks depend on extract tasks, so we still need extract placeholders
        transform_ticket_fields_task = skip_placeholder()
        transform_organizations_task = skip_placeholder()
        transform_users_task = skip_placeholder()
        transform_tickets_task = skip_placeholder()
    
    # Step 5: Load all endpoints (can run in parallel after transforms complete)
    if run_load:
        print("\n[Step 5] Loading data to PostgreSQL...")
        load_ticket_fields_task = load_ticket_fields(transform_ticket_fields_task, incremental=incremental)
        load_organizations_task = load_organizations(transform_organizations_task, incremental=incremental)
        load_users_task = load_users(transform_users_task, incremental=incremental)
        load_tickets_task = load_tickets(transform_tickets_task, incremental=incremental)
    else:
        print("\n[Step 5] Skipping load (run_load=False)")
    
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
    # Allow running with various flags to control pipeline execution
    import argparse
    parser = argparse.ArgumentParser(description="Run Zendesk ETL Pipeline")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental loading mode for database loads"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Run only extract steps (skip transform and load)"
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Run only transform steps (skip extract and load)"
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Run only load steps (skip extract and transform)"
    )
    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Skip extract steps"
    )
    parser.add_argument(
        "--no-transform",
        action="store_true",
        help="Skip transform steps"
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Skip load steps"
    )
    args = parser.parse_args()
    
    # Determine which phases to run
    # If a "*-only" flag is set, only run that phase
    if args.extract_only:
        run_extract = True
        run_transform = False
        run_load = False
    elif args.transform_only:
        run_extract = False
        run_transform = True
        run_load = False
    elif args.load_only:
        run_extract = False
        run_transform = False
        run_load = True
    else:
        # Otherwise, respect the individual skip flags
        run_extract = not args.no_extract
        run_transform = not args.no_transform
        run_load = not args.no_load
    
    zendesk_etl_pipeline(
        incremental=args.incremental,
        run_extract=run_extract,
        run_transform=run_transform,
        run_load=run_load
    )

