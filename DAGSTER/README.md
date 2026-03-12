# Person, Course, and Term Data Load

## Overview
This project contains a data ingestion pipeline built with **Dagster** for loading **Person**, **Course**, and **Term** data from SnapLogic and PeopleSoft into a PostgreSQL data lake. It uses asynchronous and parallel pipelines to efficiently fetch, transform, stream, and store data from external APIs.

---

## Data Pipelines

### Person Pipeline
**Source**: SnapLogic
**Destination Tables**:
1. `person_raw.person_data` – **INSERT only**: retains a historical log of all changes to person data.
2. `person_xform.current_person_data` – **UPSERT**: Managed by SQL triggers and functions to always contain the current person state.
3. `person_curated.person_data_by_service` – **UPSERT**: Managed by SQL triggers and functions to filter records by service category.

Dagster Asset: `person_raw_asset`  

---

### Course Pipeline
**Source**: SnapLogic
**Destination Tables**:
1. `course_raw.course_data` – **INSERT only**: retains a historical log of all changes to course data.
2. `course_xform.current_course_data` – **UPSERT**: Managed by SQL triggers and functions to always contain the current person state.
3. `course_curated.course_data_by_service` – **UPSERT**: Managed by SQL triggers and functions to filter records by service category.

Dagster Asset: `course_raw_asset`

---

### Term Pipeline
**Source**: PeopleSoft BU_TERM_QRY  
**Destination Tables**:
1. `term_raw.term_data` – **INSERT only**: retains a historical log of all changes to term data.
2. `term_xform.current_term_data` – **UPSERT**: Managed by SQL triggers and functions to always contain the current person state.
3. `term_curated.term_data_by_service` – **UPSERT**: Managed by SQL triggers and functions to filter records by service category.

Dagster Asset: `term_raw_asset`

---

## Supporting SQL Files

The project includes three SQL files – `person.sql`, `course.sql`, and `term.sql` – which define the functions and trigger-based logic to propagate data through each layer of the ingestion pipeline:

1. **Raw Layer**  
   Insert operations are performed by Dagster into the raw data schema (e.g., `person_raw.person_data`).

2. **Current Layer**  
   If an inserted record is **new or updated**, a trigger invokes a function that **upserts** the data into a current state table (e.g., `person_xform.current_person_data`).

3. **Curated Layer**  
   A subsequent trigger/function applies **service-based filtering** (using JSON path definitions) and **upserts** the results into a final curated table (e.g., `person_curated.person_data_by_service`).

---

## Architecture Overview

- **Dagster** – Workflow orchestration and asset definition
- **SQLAlchemy (async)** – Database interactions
- **httpx** – Asynchronous HTTP-based integration with external APIs
- **asyncio** – Managing concurrent API calls
- **DeepDiff** – Object comparison for data change detection
- **PostgreSQL** – Datalake storage
- **SQL Server** – Pre-stage source of IDs for person extraction

---

## Environment Variables

### Postgres
| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | Hostname for the PostgreSQL server |
| `POSTGRES_PORT` | Port number |
| `POSTGRES_DB`   | Target database name |
| `POSTGRES_USER` | Username |
| `POSTGRES_PASS` | Password |

### SnapLogic APIs
| Variable | Description |
|----------|-------------|
| `SNAPLOGIC_PERSON_URL` | API URL for SnapLogic person data |
| `SNAPLOGIC_PERSON_KEY` | API token for SnapLogic person data |
| `SNAPLOGIC_COURSE_URL` | API URL for SnapLogic course data |
| `SNAPLOGIC_COURSE_KEY` | API token for SnapLogic course data |
| `CS_ENV`               | Campus Solutions environment for SnapLogic (`ptst`, `prd`, etc.) |

### PeopleSoft APIs
| Variable | Description |
|----------|-------------|
| `BU_TERM_QRY_URL` | URL for BU term query BU_TERM_QRY |
| `BU_TERM_STD_FULL_TERM_URL` | URL for student full term details BU_TERM_STD_FULL_TERM |
| `PEOPLE_SOFT_USER` | Username for PeopleSoft |
| `PEOPLE_SOFT_PASS` | Password for PeopleSoft |

### SQL Server
| Variable | Description |
|----------|-------------|
| `SQLSERVER_HOST` | SQL Server hostname |
| `SQLSERVER_PORT` | SQL Server port |
| `SQLSERVER_DATABASE` | Database name |
| `SQLSERVER_USER` | Username |
| `SQLSERVER_PASS` | Password |

---

## Project Structure
```
PersonDataLoad/
├── defs/
│ ├── assets.py        # Dagster assets: person, course, term logic
│ ├── resources.py     # Dagster resources: Postgres, SnapLogic, PeopleSoft, SQL Server
├── defs.py            # Definitions: wires assets, jobs, schedules, and resources
├── README.md          # This documentation
```

---

## Running the Pipeline

1. Install dependencies:
    ```bash
    uv sync
    ```

2. Export the required environment variables (see above).

3. Start Dagster:
    ```bash
    dagster dev
    ```

4. Trigger assets (e.g., `person_raw_asset`, `course_raw_asset`) via the Dagster UI or CLI.

5. Production job when enabled is scheduled to run daily at **3 AM Eastern Time**.
