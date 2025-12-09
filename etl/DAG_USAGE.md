# Zendesk ETL Pipeline DAG

This directory contains a Prefect DAG that orchestrates the entire Zendesk ETL pipeline.

## Overview

The DAG manages the execution of:
- **Extract**: Pull data from Zendesk API (tickets, users, organizations, ticket_fields)
- **Transform**: Process and flatten data into CSV format
- **Load**: Load transformed data into PostgreSQL

## Dependencies

The pipeline has the following execution order:

1. **Extract** (all endpoints can run in parallel):
   - Extract ticket_fields
   - Extract organizations
   - Extract users
   - Extract tickets

2. **Transform** (with dependencies):
   - Transform ticket_fields (after extract)
   - Transform organizations (after extract)
   - Transform users (after extract)
   - Transform tickets (after extract AND ticket_fields transform)

3. **Load** (all endpoints can run in parallel after transforms):
   - Load ticket_fields (after transform)
   - Load organizations (after transform)
   - Load users (after transform)
   - Load tickets (after transform)

## Installation

Install Prefect (if not already installed):

```bash
uv sync  # or pip install prefect>=2.14.0
```

## Usage

### Run the Pipeline Locally

Run the entire pipeline:

```bash
python etl/dag.py
```

Run with incremental loading:

```bash
python etl/dag.py --incremental
```

### Using Prefect CLI

You can also use Prefect's CLI to run and monitor the flow:

```bash
# Run the flow
prefect run etl/dag.py

# Run with parameters
prefect run etl/dag.py --param incremental=true

# View flow runs
prefect flow-run ls
```

### Start Prefect Server (Optional)

For a web UI to monitor runs:

```bash
# Start Prefect server
prefect server start

# In another terminal, run your flow
python etl/dag.py
```

Then visit http://localhost:4200 to see the Prefect UI.

### Deploy to Prefect Cloud (Optional)

If you want to deploy to Prefect Cloud for scheduling and monitoring:

```bash
# Login to Prefect Cloud
prefect cloud login

# Deploy the flow
prefect deploy etl/dag.py:zendesk_etl_pipeline --name zendesk-etl-production
```

## Pipeline Visualization

The DAG structure looks like this:

```
                    Extract (Parallel)
                         │
        ┌────────────────┼────────────────┐──────────┐
        │                │                │          |
   ticket_fields    organizations      users      tickets
        │                │                │          │
        ▼                ▼                ▼          │
   Transform        Transform        Transform       │
        │                │                │          │
        └────────────────┼────────────────┘          │
                         │                           │
                    ticket_fields                    │
                    transform done                   │
                         │                           │
                         └──────────┬────────────────┘
                                    │
                                    ▼
                              Transform tickets
                                    │
                                    ▼
                            Load (Parallel)
                                    │
                  ┌─────────────────┼────────────────┐───────────┐
                  │                 │                │           |
            ticket_fields      organizations        users      tickets
```

## Environment Variables

Make sure you have the following environment variables set:

- `ZENDESK_SUBDOMAIN`: Your Zendesk subdomain
- `ZENDESK_EMAIL`: Your Zendesk email
- `ZENDESK_API_TOKEN`: Your Zendesk API token
- `DATA_DIR`: Path to data directory (default: ./data)
- `CONFIG_DIR`: Path to config directory (default: ./config)

Database connection settings should be in your `.env` file (see `utils/db_utils.py` for details).

## Error Handling

If any task fails, Prefect will:
1. Log the error
2. Stop execution of dependent tasks
3. Provide detailed error information in the logs

You can retry failed runs from the Prefect UI or CLI.

## Scheduling (Optional)

To schedule the pipeline to run automatically, you can use Prefect's scheduling features:

```python
from prefect import flow
from prefect.schedules import CronSchedule

@flow(name="zendesk_etl_pipeline_scheduled")
def scheduled_pipeline():
    zendesk_etl_pipeline(incremental=True)

# Schedule to run daily at 2 AM
scheduled_pipeline.schedule = CronSchedule(cron="0 2 * * *")
```

