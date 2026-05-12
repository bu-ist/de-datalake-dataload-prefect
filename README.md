<div align="center">

# &#128260; de-person-course-term-publish

**Prefect 3 flows for publishing person, course, and term data to the PostgreSQL data lake**

![Python](https://img.shields.io/badge/python-3.13-blue)
![Prefect](https://img.shields.io/badge/prefect-3.x-blue)
![CI](https://github.com/bu-ist/de-person-course-term-publish/actions/workflows/build-and-push.yml/badge.svg)

</div>

## Architecture

```
Sources                          Flows                    PostgreSQL Data Lake
──────────────────────           ──────────────────────   ─────────────────────────────────
Campus Solutions Tools  ──┐
                           ├──▶  term-raw-flow     ──▶   term_raw.*
                           │
SnapLogic Course API    ──────▶  course-raw-flow   ──▶   course_raw.*
                           │
DE Person API          ────┤
Campus Solutions Tools  ──┤
VDS API                ───┤──▶  person-raw-flow   ──▶   person_raw.*
SAP API                ───┘

Data Lake Layer Architecture:
  person_raw / course_raw / term_raw     ← INSERT-only; full history
        │
        ▼  (SQL triggers)
  person_xform / course_xform / term_xform   ← UPSERT; current state
        │
        ▼  (SQL triggers)
  person_curated / course_curated / term_curated  ← service-filtered views
```

## Flows

| Flow | Schedule (ET) | Source | Target |
|------|---------------|--------|--------|
| `term-raw-flow` | Daily 1:00 AM | CS Tools `BU_TERM_QRY` | `term_raw.term_data` |
| `course-raw-flow` | Daily 2:00 AM | SnapLogic Course API | `course_raw.course_data` |
| `person-raw-flow` | Daily 3:00 AM | CS Tools + DE Person API + VDS + SAP | `person_raw.person_data` |

All flows accept `test_run: bool = False`. When `True`, extraction runs normally but database writes are skipped — safe to trigger manually against production APIs.

## Package Structure

```
de-person-course-term-publish/
├── flows/
│   ├── term/           term_flow.py, term_tasks.py
│   ├── course/         course_flow.py, course_tasks.py
│   ├── person/         person_flow.py, person_tasks.py
│   └── utils/          batch.py, db.py, logging_helpers.py
├── config/
│   ├── settings.py     Pydantic Settings (SecretStr for all credentials)
│   └── resources.py    asyncpg pool + API config factories
├── tests/
│   ├── conftest.py     Dummy env vars for import-time settings init
│   └── test_smoke.py   Flow callability + test_run signature checks
├── k8s/
│   └── secrets-template.yaml
├── .github/workflows/
│   └── build-and-push.yml
├── Dockerfile
├── prefect.yaml
└── pyproject.toml
```

## Environment Variables

Injected at runtime via the `de-person-course-term-publish-secrets` Kubernetes Secret (see [k8s/secrets-template.yaml](k8s/secrets-template.yaml)).

### PostgreSQL
| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | Database hostname |
| `POSTGRES_PORT` | Port (default `5432`) |
| `POSTGRES_DB` | Database name |
| `POSTGRES_USER` | Username |
| `POSTGRES_PASS` | Password |

### Campus Solutions Tools
| Variable | Description |
|----------|-------------|
| `CS_ENV` | Environment flag (`PROD`, `TEST`, etc.) |
| `DE_CSTOOLS_ENDPOINT` | API base URL |
| `DE_CSTOOLS_KEY` | API key |

### SnapLogic Course API
| Variable | Description |
|----------|-------------|
| `SNAPLOGIC_COURSE_URL` | API URL |
| `SNAPLOGIC_COURSE_KEY` | API key |

### DE Person API
| Variable | Description |
|----------|-------------|
| `DE_PERSON_API_URL` | API URL |
| `DE_PERSON_API_KEY` | API key |

### VDS API
| Variable | Description |
|----------|-------------|
| `VDS_URL` | API URL |
| `VDS_KEY` | Bearer token |

### SAP API
| Variable | Description |
|----------|-------------|
| `SAP_URL` | API base URL |
| `SAP_KEY` | API key |

## Local Development

```bash
# Install uv (if needed)
curl -Ls https://astral.sh/uv | sh

# Install all dependencies including dev
uv sync

# Copy and populate env file
cp .env.example .env

# Run smoke tests
uv run pytest

# Run a flow locally (requires .env)
uv run python flows/term/term_flow.py

# Test run — skips DB writes
uv run python -c "
import asyncio
from flows.person.person_flow import person_raw_flow
asyncio.run(person_raw_flow(test_run=True))
"
```

## Docker

```bash
# Build
docker build -t de-person-course-term-publish .

# The image is a flow-runner — the Prefect worker overrides CMD at execution time.
# Running directly only prints an informational message.
docker run --rm de-person-course-term-publish
```

## Kubernetes / Deployment

The Prefect worker on the `batch-jobs` work pool pulls and executes flow-run jobs. No CronJobs needed — schedules live in `prefect.yaml`.

```bash
# SSH to the k8s management box, then:

# Terminal 1 — keep running
kubectl port-forward -n prefect-server svc/prefect-server 4200:80

# Terminal 2 — deploy
export PREFECT_API_URL=http://localhost:4200/api
cd /path/to/de-person-course-term-publish
uv run prefect deploy --all
```

### Trigger a test run manually

```bash
prefect deployment run 'term-raw-flow/term-raw-daily' --param test_run=true
prefect deployment run 'course-raw-flow/course-raw-daily' --param test_run=true
prefect deployment run 'person-raw-flow/person-raw-daily' --param test_run=true
```

### Person flow concurrency tuning

| Parameter | Default | Notes |
|-----------|---------|-------|
| `cstools_semaphore_limit` | `10` | Concurrent CS Tools queries |
| `person_api_semaphore_limit` | `5` | Concurrent Person API batches (~5 min each) |
| `insert_semaphore_limit` | `100` | Concurrent DB insert workers |
| `uidcarterm_batch_size` | `600` | BUIDs per uidCarTerm batch |
| `buid_batch_size` | `100` | BUIDs per BUID-only batch |

## CI/CD

| Branch / Tag | Action |
|---|---|
| Pull request | `static-checks` + `lint` + `test` |
| Push to `main` | All checks + `build-and-push` to GHCR |

Images are tagged `latest` and `YYYY.MM.DD-<sha7>` (CalVer + short SHA). The Prefect worker uses `image_pull_policy: Always`, so a new image is picked up automatically on the next scheduled run — no redeployment required unless `prefect.yaml` changes.

```bash
# Force-pull the latest image on next run (already the default behavior)
prefect deployment run 'person-raw-flow/person-raw-daily'

# View recent runs
prefect flow-run ls

# Tail logs for a specific run
prefect flow-run logs <run-id>
```
