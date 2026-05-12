# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-05-07

### Added
- `test_run: bool = False` parameter on all three flows; extraction runs but DB writes are skipped when `True`
- `config/settings.py` — migrated to `SettingsConfigDict`; all credential fields typed as `SecretStr`
- `config/resources.py` — calls `.get_secret_value()` on all `SecretStr` settings at call time
- `tests/conftest.py` + `tests/test_smoke.py` — smoke tests asserting flow callability and `test_run` signature
- `k8s/secrets-template.yaml` — K8s Secret template listing all 16 required env vars
- `Dockerfile` — multi-stage uv builder + slim runtime; non-root `flowuser` (UID 1000); informational CMD
- `.dockerignore` — excludes `.git/`, `.venv/`, `DAGSTER/`, `*.log`, `deployments/`, credentials
- `.github/workflows/build-and-push.yml` — `static-checks → test → build-and-push` pipeline with GHA layer cache, CalVer+SHA tags, provenance+SBOM; replaced `GHCR_PAT` with `GITHUB_TOKEN`
- `prefect.yaml` — `prefect-version: "3.0.0"`, global `pull: set_working_directory`, all deployments moved to `batch-jobs` work pool with `env_from.secretRef` and resource limits

### Changed
- Project renamed from `DatalakeDataload` to `de-person-course-term-publish`
- `pyproject.toml` — migrated from `hatchling` to `setuptools`; pinned `prefect>=3.0,<4`; added `psycopg`, `sqlalchemy`; added `dev` extras with pytest + ruff
- `README.md` — full rewrite with architecture diagram, env var tables, deployment instructions

### Removed
- `requirements.txt` — replaced by `pyproject.toml` + `uv.lock`
- `deployments/term_raw_deployment.py`, `deployments/course_raw_deployment.py`, `deployments/person_raw_deployment.py` — replaced by `prefect.yaml`
- `.github/workflows/docker-publish.yaml` — replaced by `build-and-push.yml`
