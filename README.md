# Data Lake Dataload - Prefect Project

A Prefect workflow orchestration project for managing data lake loading operations.

## Project Overview

This project handles ETL operations for loading data from various sources (Campus Solutions Tools, Data Engineering Person API, SnapLogic, SAP) into a PostgreSQL data lake. It consists of three main flows:

1. **Term Raw Flow** - Loads term data from Campus Solutions Tools (runs at 1:00 AM ET daily)
2. **Course Raw Flow** - Loads course data from SnapLogic (runs at 2:00 AM ET daily)
3. **Person Raw Flow** - Loads person data from Data Engineering Person API and multiple sources (runs at 3:00 AM ET daily)

## Project Structure

```
DatalakeDataload/
├── flows/                          # Prefect flow definitions
│   ├── __init__.py
│   ├── person/
│   │   ├── person_flow.py         # Person data loading flow
│   │   └── person_tasks.py        # Person flow tasks
│   ├── course/
│   │   ├── course_flow.py         # Course data loading flow
│   │   └── course_tasks.py        # Course flow tasks
│   ├── term/
│   │   ├── term_flow.py           # Term data loading flow
│   │   └── term_tasks.py          # Term flow tasks
│   └── utils/
│       ├── batch.py               # Batch splitting utilities
│       ├── db.py                  # Database utilities
│       └── logging_helpers.py     # Logging utilities
├── config/                         # Configuration modules
│   ├── __init__.py
│   ├── settings.py                # Environment variables and settings
│   └── resources.py               # Database and API connection resources
├── deployments/                    # Deployment configurations
│   ├── term_raw_deployment.py
│   ├── course_raw_deployment.py
│   └── person_raw_deployment.py
├── prefect.yaml                    # Prefect project configuration
├── pyproject.toml                  # Python project and dependency config
├── .env.example                   # Example environment variables
└── README.md                      # This file
```

## Architecture Overview

- **Prefect** – Workflow orchestration and flow execution
- **httpx** – Asynchronous HTTP-based integration with external APIs
- **asyncio** – Managing concurrent API calls and pipeline stages
- **PostgreSQL** – Data lake storage (target database)
- **asyncpg** – Async PostgreSQL driver for batch inserts

## Data Pipeline Architecture

### Layered Data Architecture

The project uses a three-layer data architecture for all pipelines (Person, Course, and Term):

1. **Raw Layer** (`*_raw` schemas)
   - INSERT-only operations performed by Prefect flows
   - Retains historical log of all changes
   - Examples: `person_raw.person_data`, `course_raw.course_data`, `term_raw.term_data`

2. **Transform Layer** (`*_xform` schemas)
   - UPSERT operations managed by SQL triggers and functions
   - Always contains the current state of data
   - Examples: `person_xform.current_person_data`, `course_xform.current_course_data`, `term_xform.current_term_data`

3. **Curated Layer** (`*_curated` schemas)
   - UPSERT operations managed by SQL triggers and functions
   - Service-based filtering using JSON path definitions
   - Examples: `person_curated.person_data_by_service`, `course_curated.course_data_by_service`, `term_curated.term_data_by_service`

---

## Environment Variables

### PostgreSQL
| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | Hostname for the PostgreSQL server |
| `POSTGRES_PORT` | Port number |
| `POSTGRES_DB`   | Target database name |
| `POSTGRES_USER` | Username |
| `POSTGRES_PASS` | Password |

### SnapLogic Course API
| Variable | Description |
|----------|-------------|
| `SNAPLOGIC_COURSE_URL` | API URL for SnapLogic course data |
| `SNAPLOGIC_COURSE_KEY` | API token for SnapLogic course data |

### Data Engineering Person API
| Variable | Description |
|----------|-------------|
| `DE_PERSON_API_URL` | API URL for Data Engineering Person API |
| `DE_PERSON_API_KEY` | API token for Data Engineering Person API |
| `CS_ENV`            | Campus Solutions environment (`test`, `prod`, etc.) |

### Data Engineering Campus Solutions Tools API
| Variable | Description |
|----------|-------------|
| `DE_CSTOOLS_ENDPOINT` | API URL for Data Engineering Campus Solutions Tools |
| `DE_CSTOOLS_KEY` | API key for Data Engineering Campus Solutions Tools |

### VDS API
| Variable | Description |
|----------|-------------|
| `VDS_URL` | URL endpoint for the VDS API |
| `VDS_USERNAME` | Username for VDS authentication |
| `VDS_PASSWORD` | Password for VDS authentication |

### SAP API
| Variable | Description |
|----------|-------------|
| `SAP_URL` | The base URL of the SAP endpoint for employee data |
| `SAP_KEY` | The API key for authenticating SAP requests |

---

## Prerequisites

- Python 3.9 or higher (requires Python >=3.9, <3.14)
- Prefect 2.14.0 or higher
- PostgreSQL database
- Access to Data Engineering Person API, Campus Solutions Tools, SnapLogic, and SAP APIs

## Setup

### 1. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the example environment file and configure all required variables:

```bash
cp .env.example .env
# Edit .env with your actual configuration
```

Required environment variables:
- **PostgreSQL**: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASS`
- **Campus Solutions**: `CS_ENV`
- **Data Engineering Campus Solutions Tools**: `DE_CSTOOLS_ENDPOINT`, `DE_CSTOOLS_KEY`
- **SnapLogic Course API**: `SNAPLOGIC_COURSE_URL`, `SNAPLOGIC_COURSE_KEY`
- **Data Engineering Person API**: `DE_PERSON_API_URL`, `DE_PERSON_API_KEY`
- **VDS**: `VDS_URL`, `VDS_USERNAME`, `VDS_PASSWORD`
- **SAP**: `SAP_URL`, `SAP_KEY`

### 4. Set Up Database Schema

Run the SQL scripts to set up the required database schemas:

```bash
# Run term schema
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/term.sql

# Run course schema
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/course.sql

# Run person schema
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/person.sql
```

### 5. Start Prefect Server (for local development)

```bash
prefect server start
```

Or connect to Prefect Cloud:

```bash
prefect cloud login
```

## Running Flows

### Run Flow Locally

```bash
# Run term flow
python flows/term/term_flow.py

# Run course flow
python flows/course/course_flow.py

# Run person flow
python flows/person/person_flow.py
```

### Create Deployments

Using the deployment scripts:

```bash
# Create term deployment
python deployments/term_raw_deployment.py

# Create course deployment
python deployments/course_raw_deployment.py

# Create person deployment
python deployments/person_raw_deployment.py
```

Or deploy all at once using the `prefect.yaml` file:

```bash
prefect deploy --all
```

### Start an Agent/Worker

```bash
# For deployments using work pools
prefect worker start --pool "default-agent-pool"

# Or for older agent-based deployments
prefect agent start --pool "default-agent-pool"
```

### Run a Deployment

```bash
# Run term flow deployment
prefect deployment run 'term-raw-flow/term-raw-daily'

# Run course flow deployment
prefect deployment run 'course-raw-flow/course-raw-daily'

# Run person flow deployment
prefect deployment run 'person-raw-flow/person-raw-daily'
```

## Flow Details

### Term Raw Flow
- **Schedule**: Daily at 1:00 AM ET
- **Source**: Campus Solutions Tools `BU_TERM_QRY`
- **Target**: `term_raw.term_data` table
- **Description**: Fetches term data from Campus Solutions Tools, truncates the target table, and inserts new data in JSONB format

### Course Raw Flow
- **Schedule**: Daily at 2:00 AM ET
- **Source**: SnapLogic Course API
- **Target**: `course_raw.course_data` table
- **Description**: Fetches course data for active terms from SnapLogic and inserts into PostgreSQL using batch operations

### Person Raw Flow
- **Schedule**: Daily at 3:00 AM ET
- **Sources**: Campus Solutions Tools, SAP, Data Engineering Person API
- **Target**: `person_raw.person_data` table
- **Description**: Three-stage concurrent pipeline:
  1. **BUIDs fetch** — CS Tools and SAP queried in parallel to build the full BUID population
  2. **CS Tools queries** — Each BUID queried for uidCarTerm data concurrently; results accumulate into rolling batches for students (with terms) and BUIDs-only (without terms)
  3. **Person API + DB inserts** — Batches are streamed to the Data Engineering Person API concurrently; results are inserted into `person_raw.person_data` as they arrive, with sensitive fields (`ssn`, `socialSecurityNumber`, `sexualOrientation`, `finAid`, `finAidReceived`) stripped before insert

---

## Running the Person Flow Ad-Hoc

The person flow accepts three concurrency parameters that control throughput. These can be passed when triggering a run manually.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cstools_semaphore_limit` | `10` | Max concurrent CS Tools queries per BUID |
| `person_api_semaphore_limit` | `5` | Max concurrent Person API batch calls (each batch ~5 min) |
| `insert_semaphore_limit` | `24` | Max concurrent DB insert workers (capped by pool max of 24) |

### Via Prefect CLI

```bash
prefect deployment run 'person-raw-flow/person-raw-daily' \
  --param cstools_semaphore_limit=20 \
  --param person_api_semaphore_limit=10 \
  --param insert_semaphore_limit=24
```

### Via Python (local)

```python
import asyncio
from flows.person.person_flow import person_raw_flow

asyncio.run(person_raw_flow(
    cstools_semaphore_limit=20,
    person_api_semaphore_limit=10,
    insert_semaphore_limit=24,
))
```

### Tuning Notes

- **`cstools_semaphore_limit`** is the first bottleneck — increasing it speeds up the BUID→term query phase. Raise it if CS Tools can handle the load.
- **`person_api_semaphore_limit`** controls how many ~5-minute Person API calls run in parallel. This is the dominant runtime factor. Increasing it proportionally reduces total run time, subject to API capacity.
- **`insert_semaphore_limit`** should not exceed the asyncpg pool's `max_size` (24). Extra workers beyond that just queue for connections.
- The `batch_threshold` (records per Person API batch) is derived automatically as `len(buids) // person_api_semaphore_limit`, so raising `person_api_semaphore_limit` also produces more, smaller batches.

---

## Development

### Testing Flows Locally

All flows support async execution and can be tested locally:

```bash
python flows/person/person_flow.py
python flows/course/course_flow.py
python flows/term/term_flow.py
```

### View Flow Runs in UI

After starting the Prefect server, visit:
- Local: http://localhost:4200
- Cloud: https://app.prefect.cloud

## Common Tasks

### List Deployments

```bash
prefect deployment ls
```

### View Flow Runs

```bash
prefect flow-run ls
```

### Cancel a Flow Run

```bash
prefect flow-run cancel <flow-run-id>
```

### Delete a Deployment

```bash
prefect deployment delete <deployment-name>
```

## Configuration

### Modifying Schedules

Schedules are configured in the deployment files. To change a schedule, edit the relevant deployment file:

```python
# In deployments/term_raw_deployment.py
schedule=CronSchedule(
    cron="0 1 * * *",  # Change this cron expression
    timezone="America/New_York"
)
```

Then redeploy:

```bash
python deployments/term_raw_deployment.py
```

### Adding New Flows

1. Create a new directory under `flows/` for your flow
2. Define your flow using the `@flow` decorator in `<name>_flow.py`
3. Define tasks using the `@task` decorator in `<name>_tasks.py`
4. Import resources from `config.resources`
5. Create a deployment configuration in `deployments/`

## Performance Tuning

### Asyncpg Connection Pool

The asyncpg connection pool is configured in [config/resources.py](config/resources.py):
- Minimum pool size: 12 connections
- Maximum pool size: 24 connections

### Person Flow Concurrency

The person flow defaults are set in [flows/person/person_flow.py](flows/person/person_flow.py):

```python
cstools_semaphore_limit: int = 10     # Concurrent CS Tools queries
person_api_semaphore_limit: int = 5   # Concurrent Person API batch calls
insert_semaphore_limit: int = 24      # Concurrent DB inserts (matches pool max)
```

See [Running the Person Flow Ad-Hoc](#running-the-person-flow-ad-hoc) for tuning guidance.

## Monitoring & Alerts

### Configure Notifications

1. Set up notification blocks in Prefect UI or via code
2. Add notification tasks to your flows
3. Use Prefect automations for automatic alerts on flow failures

### Logging

All flows and tasks use Prefect's logging system:

```python
from prefect.logging import get_run_logger

logger = get_run_logger()
logger.info("Your message here")
```

The person flow emits a heartbeat log every 15 seconds with a live progress table showing CS Tools query progress, Person API batch status, and DB insert counts.

## Best Practices

1. **Use task retries**: Configure retries for tasks that might fail temporarily
2. **Add tags**: Tag your flows and deployments for better organization
3. **Use blocks**: Store credentials and configurations as Prefect blocks
4. **Version control**: Keep deployments versioned
5. **Testing**: Test flows locally before deploying
6. **Monitoring**: Set up alerts for critical flows

## Troubleshooting

### Flow Not Running

- Check that an agent/worker is running for the correct work pool
- Verify the deployment is active: `prefect deployment ls`
- Check flow run logs in the Prefect UI

### Database Connection Issues

- Verify database credentials in `.env`
- Check network connectivity to database
- Ensure asyncpg is installed
- Test connection: `psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB`

### API Authentication Issues

- Verify API credentials and endpoints in `.env`
- Check that API keys are not expired
- Test API connectivity with curl or httpx

### Import Errors

- Verify all dependencies are installed: `pip install -r requirements.txt`
- Check that you're using the correct Python environment
- Ensure Python 3.9+ is being used

## Resources

- [Prefect Documentation](https://docs.prefect.io/)
- [Prefect Community Slack](https://prefect.io/slack)
- [Prefect Discourse](https://discourse.prefect.io/)

## License

[Your License Here]
