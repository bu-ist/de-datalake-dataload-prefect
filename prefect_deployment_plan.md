# Prefect Deployment Plan — `de-person-course-term-publish`

**Audience:** developer maintaining this repo
**Goal:** prepare the project to be the first user-managed Prefect project deployed onto BU Data Engineering's shared `de-eks-nonprod` Prefect platform

---

## Context: what we're deploying onto

The shared Prefect platform on `de-eks-nonprod` is already running. Its key contract values — these are not negotiable from the flow side, they're set by the platform:

| Setting | Value |
|---------|-------|
| Prefect server version | **3.x** (Helm chart `prefect-server v2026.3.x`) |
| API URL | `https://prefect.de-eks-nonprod.bu.edu/api` |
| Work pool exposed | `batch-jobs` (Kubernetes type) |
| Namespace where flow Jobs run | `prefect` |
| Node group used for Jobs | label `workload=batch`, taint `dedicated=batch:NoSchedule` |
| Image registry convention | GHCR (`ghcr.io/bu-ist/<repo>`) |
| Auth | ALB OIDC (Azure Entra ID) — UI auth only; API access from inside the cluster is unauthenticated |

This project has to match those values exactly. Most of the work below is about closing gaps between the project's current shape (Prefect 2.x, `k8s-pool`, no env injection) and that contract.

---

## Phase 1 — Make the project compatible with Prefect 3

### 1.1 Upgrade `prefect` to 3.x

**What:** in `pyproject.toml` and `requirements.txt`, replace `prefect>=2.14.0` with `prefect>=3.0,<4`. Drop `prefect-client` from `[dependency-groups].dev`.

**Why specifically Prefect 3:**

- **The deployed server is Prefect 3.** Helm chart version `2026.3.x` ships Prefect 3.x. Prefect 2 and Prefect 3 are different products with **different REST APIs and database schemas** — they aren't backward-compatible. A Prefect 2 client trying to register a deployment against a Prefect 3 server hits 404s or schema-validation errors on the deployment registration endpoint. There is no compatibility shim.
- **Prefect 3 removed agents in favor of workers.** Our platform runs a `prefect-worker` (the new model). Agents (the 2.x model) don't exist in the cluster, so a 2.x deployment built around `prefect agent start` would never get picked up.
- **The deployment registration mechanism changed.** Prefect 2 used `Deployment.build_from_flow().apply()` and `.deploy()` (you have remnants of this in `deployments/*_raw_deployment.py`). Prefect 3 standardizes on `prefect.yaml` + `prefect deploy`, which is what the platform's deploy helper script (`prefect-deploy.sh`) uses.
- **Some flow APIs subtly changed** (cache key fns, retry parameters, state handling). The flows here look 3-compatible at a glance — `@flow`, `prefect.logging.get_run_logger`, `prefect.exceptions.Abort` — but they need to be tested against an actual 3.x server before we trust them.

**How:**

```bash
# In pyproject.toml
"prefect>=3.0,<4"   # was: "prefect>=2.14.0"

# In requirements.txt
prefect>=3.0,<4    # was: prefect>=2.14.0
```

Then locally:

```bash
uv sync             # or pip install -r requirements.txt
prefect server start         # local Prefect 3 server on :4200
PREFECT_API_URL=http://localhost:4200/api prefect deploy --all
PREFECT_API_URL=http://localhost:4200/api prefect deployment run datalake-dataload/term-raw-daily
```

If the local run succeeds end-to-end against a 3.x server, the upgrade is good.

---

### 1.2 Pick one source of truth for dependencies

**What:** the repo currently has three places that declare dependencies — `pyproject.toml`, `requirements.txt`, and `uv.lock`. The Dockerfile installs from `requirements.txt`. Choose one.

**Why this matters:**

- **Drift is silent.** If someone updates `pyproject.toml` to bump a security-patched library, the Docker image still installs the old version from `requirements.txt`. The "fix" appears merged but never reaches production.
- **`uv.lock` is currently ignored.** It's 624 KB of pinned dependency hashes that nothing in CI or Docker uses. Either it's authoritative or it shouldn't be in the repo.
- **Reproducibility:** without a single locked source, two developers can build different images from the same git SHA.

**How (recommended — switch to `uv`):**

```dockerfile
# Builder stage
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
```

Then delete `requirements.txt`. Or, if staying with pip: regenerate `requirements.txt` from `pyproject.toml` on every CI run and commit the result, and delete `uv.lock`.

---

## Phase 2 — Match the platform's deployment contract

### 2.1 Change work pool from `k8s-pool` to `batch-jobs`

**What:** in all three `prefect.yaml` deployments (lines 18, 30, 42), change `work_pool.name: k8s-pool` to `work_pool.name: batch-jobs`.

**Why this exact name:**

- **The platform's worker subscribes to `batch-jobs` only.** This is configured in [applications/prefect/prefect-values.yaml](https://github.com/bu-ist/de-eks-nonprod/blob/main/applications/prefect/prefect-values.yaml) under `prefect-worker.worker.config.workPool: batch-jobs`. The worker process literally polls the API for work in that pool and ignores all others.
- **`k8s-pool` doesn't exist on the server.** When you run `prefect deploy`, the deployment record is created — but it points at a pool the server doesn't have. The deployment shows up in the UI, schedules trigger flow runs, runs go into `Scheduled` → `Late`, and stay there forever. There's no error, just silence.
- **You can't create your own pool from the flow side.** Pool creation requires admin access to the Prefect API and the platform team controls which pools exist (each pool has its own worker process and node group config).

---

### 2.2 Add `job_variables` to every deployment

**What:** today `prefect.yaml` has only `work_pool.name` — no `job_variables` block. Add image, pull policy, and env injection. Example for one deployment:

```yaml
work_pool:
  name: batch-jobs
  job_variables:
    image: ghcr.io/bu-ist/de-person-course-term-publish:latest
    image_pull_policy: Always
    env_from:
      - secretRef:
          name: de-person-course-term-publish-secrets
```

**Why each piece:**

#### `image:`
The Prefect Kubernetes worker creates a `batch/v1 Job` for every flow run. That Job needs to know which image to pull. The platform's base job template ([applications/prefect/files/base-job-template.json](https://github.com/bu-ist/de-eks-nonprod/blob/main/applications/prefect/files/base-job-template.json)) leaves `image` as a `{{ image }}` template variable. If you don't fill it in via `job_variables.image`, the Job is created with `image: ""` and the pod fails with `ErrImagePull` / `InvalidImageName`. There is no platform default.

#### `image_pull_policy: Always`
Kubernetes' default is `IfNotPresent`. With `IfNotPresent` and a mutable tag like `:latest`:
- Node pulls `ghcr.io/.../de-person-course-term-publish:latest` once
- Image gets cached on that node's container runtime
- Tomorrow, CI pushes a new `:latest` (e.g., bug fix)
- Same node runs tomorrow's flow → reuses the **old** cached image
- The fix silently doesn't take effect, possibly for days, until the node is replaced

This is one of the most common foot-guns on K8s with mutable tags. Setting `Always` forces a registry check on every pod start, which is what you want when the tag is `:latest` / `:main` / `:dev`. (For immutable tags like `:v1.2.3` or `:<sha>`, `IfNotPresent` is correct.)

#### `env_from: [secretRef: ...]`
This is the deal-breaker right now. [config/settings.py](config/settings.py) declares 14 **required** fields in a `pydantic-settings` `BaseSettings` class:

```python
postgres_host, postgres_port, postgres_db, postgres_user, postgres_pass,
cs_env,
de_cstools_endpoint, de_cstools_key,
snaplogic_course_url, snaplogic_course_key,
de_person_api_url, de_person_api_key,
vds_url, vds_key,
sap_url, sap_key
```

Then on line 34: `settings = Settings()` — instantiated at **module import time**.

What happens in the cluster without `env_from`:
1. Worker spawns Job → pod starts
2. Prefect runner imports `flows.term.term_flow`
3. `term_flow.py` line 4: `from config.resources import PostgresResource, ...`
4. Importing `config.resources` triggers `from config.settings import settings`
5. `settings = Settings()` runs → pydantic raises `ValidationError: 14 validation errors for Settings, postgres_host: Field required ...`
6. Pod exits with non-zero before the flow function is ever called
7. Prefect UI shows the run as `Crashed`, K8s pod logs show the pydantic error

Putting all 14 env vars into a Kubernetes Secret named (e.g.) `de-person-course-term-publish-secrets` and referencing it via `env_from.secretRef` injects them as env vars into the pod, which is what `BaseSettings` reads (it's literally `os.environ` lookups under the hood with the `env_file` fallback skipped because `.env` doesn't exist in the image).

**Documentation requirement:** the README should list every required env var so whoever creates the Secret knows what keys to populate. Today only `.env.example` does this and it's not deployment-facing.

---

### 2.3 Set the working directory via `pull:` and align it with the Dockerfile

**What:** `prefect.yaml` currently has `pull: null` (line 8). The Dockerfile's `WORKDIR` is `/opt/prefect/app`. Add a `pull:` step that matches:

```yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /opt/prefect/app
```

**Why this is required:**

- The entrypoints in `prefect.yaml` are **relative paths**: `flows/term/term_flow.py:term_raw_flow`. Prefect resolves these against the current working directory at flow-run time.
- The Prefect worker doesn't automatically `cd` into the image's `WORKDIR`. It runs `pull:` steps first (which is where `set_working_directory` belongs), then imports the entrypoint.
- Without a `pull:` step, the working directory is whatever the worker's runtime defaults to — typically `/`. The relative entrypoint resolves to `/flows/term/term_flow.py`, which doesn't exist → `ModuleNotFoundError: No module named 'flows'`.
- Setting `pull` to `/opt/prefect/app` makes Python find `flows/` and `config/` (which are at `/opt/prefect/app/flows/` and `/opt/prefect/app/config/` because the Dockerfile does `COPY . .` while WORKDIR is `/opt/prefect/app`).

The `PYTHONPATH=/opt/prefect/app` line in the Dockerfile (line 40) helps imports work *if* the working directory is wrong, but it doesn't help with the relative-path entrypoint resolution. Both belt and suspenders are fine; `pull:` is mandatory.

---

## Phase 3 — Image hygiene

These don't block first deploy in *every* environment, but at least one of them (non-root) almost certainly does on `de-eks-nonprod` due to cluster security policies.

### 3.1 Run as a non-root user

**What:** in the Dockerfile runtime stage, create a UID-1000 user, `chown` the app dir, and `USER` to it before `CMD`.

```dockerfile
FROM python:3.13-slim AS runtime
WORKDIR /opt/prefect/app

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 1000 -m -s /bin/bash prefect

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder --chown=prefect:prefect /opt/prefect/app /opt/prefect/app

USER prefect
```

**Why:**

- **Pod Security Admission:** EKS clusters at BU enforce a PodSecurity policy. Containers running as `runAsUser: 0` get rejected at admission time with `pods "..." is forbidden: violates PodSecurity ... runAsNonRoot != true`. The pod never even starts; the flow run goes straight to `Crashed` with a K8s admission error.
- **Defense in depth:** even where root is allowed, container root is the *kernel's* root (containers share the host kernel). Any container-escape vulnerability becomes a host root issue. Non-root containment is the standard mitigation.
- **UID 1000 specifically:** matches the convention used elsewhere in the cluster (Dagster code servers, etc.). Consistent UIDs make any future shared-volume scenarios less painful.

### 3.2 Add a `.dockerignore`

**What:** create `.dockerignore` at the repo root.

```
.git/
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.mypy_cache/
.env
.env.*
DAGSTER/
docs/
*.md
!README.md
tests/
node_modules/
*.log
```

**Why:**

- **`.dockerignore` is a different file from `.prefectignore`.** The repo has `.prefectignore` (good — controls what gets bundled into Prefect deployment payloads when not using a baked image). But that file is invisible to Docker. Without `.dockerignore`, every `docker build` copies the entire repo into the build context, then into the image.
- **Image size and content:**
  - `.git/` for an active project can be 100s of MB; it ships into builder layer and pollutes the runtime layer because of `COPY . .` on Dockerfile line 20.
  - `DAGSTER/` is "Legacy reference code" per `.prefectignore` line 56 — it has no place in a runtime image.
  - `.venv/` (if a developer ran `uv sync` locally) embeds host-specific binaries into the image.
- **Secret leakage:** if a developer has a real `.env` locally with production credentials, `COPY . .` puts it inside the image. Anyone who can pull the image can read it. `.dockerignore` is the only line of defense against this.
- **Build cache invalidation:** Docker layer cache invalidates whenever any file in the COPY context changes. Including `.git/` means every commit busts the cache layer, even when no Python source changed → CI builds become 5+ minute every time instead of 30 seconds.

### 3.3 Replace the misleading `CMD`

**What:** change Dockerfile line 46 from:

```dockerfile
CMD ["prefect", "worker", "start", "--pool", "default-agent-pool"]
```

to:

```dockerfile
CMD ["python", "-c", "print('flow image — invoked by Prefect worker, not run directly')"]
```

**Why:**

- **The current CMD is harmless but wrong.** This image is a **flow-runner image**, not a worker image. The platform already has a `prefect-worker` deployment running in the `prefect` namespace; we don't want a second worker.
- **The Prefect worker overrides `command` per Job.** When a flow run is scheduled, the worker creates a Job whose container's `args` are set to `prefect flow-run execute ...` (or similar), bypassing the image's `CMD` entirely. So the existing CMD never runs in production.
- **But it runs if anyone ever does `docker run ghcr.io/bu-ist/de-person-course-term-publish:latest`** — for instance, during local debugging. They'd see a worker start trying to connect to `default-agent-pool` (which doesn't exist) and get confused. The replacement CMD makes intent explicit.
- **And `--pool default-agent-pool` is wrong** even as worker-image semantics — the pool is `batch-jobs`. Leaving this CMD in place would cause misleading errors if anyone ever did try to run the image as a worker.

---

## Phase 4 — CI/CD polish

The existing `.github/workflows/docker-publish.yaml` builds and pushes correctly. These are improvements to bring it in line with the rest of the org's data-engineering pipelines.

### 4.1 Add a `test` job, gate the build on it

**What:**

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - name: Smoke import all flow modules
        run: |
          uv run python -c "from flows.term.term_flow import term_raw_flow"
          uv run python -c "from flows.course.course_flow import course_raw_flow"
          uv run python -c "from flows.person.person_flow import person_raw_flow"

  build-and-push:
    needs: test
    # ... existing build job
```

**Why:**

- **Right now nothing stops a broken commit from publishing `:latest`.** A typo, a broken import, a missing dependency — all get pushed to GHCR and become the next scheduled flow's image. Production breaks at 1 AM ET tomorrow.
- **A smoke import is the cheapest valuable test.** It catches: missing dependencies (`ModuleNotFoundError`), syntax errors, circular imports, broken `from X import Y` after a refactor, removed library APIs. It runs in seconds. It's not a substitute for real tests, but it's vastly better than nothing.
- **`needs: test` ensures atomicity.** Without it, the build job can run in parallel with the test job and push a broken image even when the test job fails.

### 4.2 Add `concurrency` block

**What:** at the workflow root:

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

**Why:**

- **Prevents racing builds on the same tag.** If two pushes to `main` land within seconds (e.g., a follow-up "fix" commit), both trigger CI. Both build, both push to `:latest`. The slower one wins — but if the first build's image was already pulled by a node mid-run, that node uses the *first* (now-overwritten-but-cached) image and the second image's changes don't take effect for that run.
- **Saves CI minutes.** No reason to build a stale commit when a newer one is already in flight.

### 4.3 Add GitHub Actions build cache

**What:**

```yaml
- name: Build and push container
  uses: docker/build-push-action@v6
  with:
    context: .
    file: Dockerfile
    push: true
    tags: |
      ...
    platforms: linux/amd64
    cache-from: type=gha
    cache-to: type=gha,mode=max
```

**Why:**

- **Current build does cold installs every time.** The `apt-get install build-essential libpq-dev` + `pip install -r requirements.txt` chain takes minutes. With `type=gha` cache, those layers cache hit when only Python source changed (which is the common case) → CI drops from ~5 minutes to ~30 seconds.
- **`mode=max` caches every intermediate layer**, not just the final one. Worth it for multi-stage builds like this one where the builder stage's apt+pip layer is the expensive part.

### 4.4 Document the branch → tag mapping

**What:** add a section to the README explaining that pushes to `main` produce `:latest` and `:<sha>`, and tags `v*.*.*` produce `:vX.Y.Z`. If `dev` / `test` branches should also publish, extend the `on.push.branches` list and the tag mapping.

**Why:**

- **Right now the workflow only triggers on `main` and version tags.** That's a perfectly valid choice, but other DE projects also publish `:test` from the `test` branch and `:dev` from `dev` for staging environments. Without documentation, the next engineer can't tell whether the absence is intentional or an oversight.

---

## Phase 5 — Cleanup (don't ship duplicates)

### 5.1 Delete `deployments/*_raw_deployment.py`

**What:** remove `deployments/term_raw_deployment.py`, `deployments/person_raw_deployment.py`, `deployments/course_raw_deployment.py`.

**Why:**

- **These are Prefect 2.x-style deployment scripts** that use `flow.deploy(...)` programmatically. After migrating to Prefect 3 and using `prefect.yaml` exclusively, they become a parallel registration path.
- **Running both creates duplicate deployments.** If someone runs `python deployments/term_raw_deployment.py` *and* the team runs `prefect deploy`, you get two separate deployment records pointing at the same flow, possibly with different work pools and different schedules. Both fire. You get duplicate runs.
- **Single source of truth:** `prefect.yaml` is the file the platform's deploy script (`applications/prefect/scripts/prefect-deploy.sh` in `de-eks-nonprod`) reads. That's the only path that should exist.

### 5.2 Note `setup.sh`'s local-only API URL

**What:** add a comment in `setup.sh` line that exports `PREFECT_API_URL=http://localhost:4200/api` clarifying it's for local dev only.

**Why:**

- It's harmless in production (the script doesn't run in the image), but a future engineer reading `setup.sh` could think the project assumes a localhost server in all contexts. A one-line comment removes that ambiguity.

### 5.3 Add `DAGSTER/` to `.dockerignore` (already in `.prefectignore`)

**What:** covered by Phase 3.2's `.dockerignore` template.

**Why:** `.prefectignore` excludes `DAGSTER/` from Prefect deploy bundles, but if the image is the deployment vehicle (which it is, via `job_variables.image`), `.prefectignore` doesn't help — only `.dockerignore` does.

---

## Recommended order of operations

1. **Branch off `main`:** `git checkout -b prefect-3-eks-readiness`
2. **Phase 1** — Prefect 3 upgrade + dep source consolidation. Test all three flows end-to-end against a local Prefect 3 server before continuing.
3. **Phase 2** — `prefect.yaml` updates (work pool, `job_variables`, `pull:` working directory). Re-test locally.
4. **Phase 3** — Dockerfile non-root user, `.dockerignore`, CMD replacement. Build the image locally (`docker build .`) and confirm it runs.
5. **Open a PR.** Let CI build and publish the image to GHCR. Manually pull the image and check size, contents, user, working dir.
6. **Coordinate with the platform owner** to:
   - Create the Kubernetes Secret `de-person-course-term-publish-secrets` in the `prefect` namespace with all 14 env vars
   - Confirm the GHCR pull secret exists in the `prefect` namespace
   - Run `applications/prefect/scripts/prefect-deploy.sh ~/Dev/de-person-course-term-publish` to register the three deployments
   - Trigger a manual run of `term-raw-daily` from the Prefect UI as a smoke test (smallest of the three flows)
7. **Phase 4 + 5** in a follow-up PR — they don't block first deploy.

**Estimated scope:** half a day for someone familiar with the project. The longest pole is local testing of the Prefect 3 upgrade.

---

## Acceptance criteria for "ready to deploy"

- [ ] `prefect>=3.0,<4` installed in the image (verified via `docker run --entrypoint pip <image> show prefect`)
- [ ] `prefect.yaml` has `work_pool.name: batch-jobs`, valid `job_variables.image`, `image_pull_policy: Always`, `env_from.secretRef`, and a `pull:` step matching the Dockerfile WORKDIR
- [ ] Image runs as UID 1000 (verified via `docker run --entrypoint id <image>`)
- [ ] `.dockerignore` exists and excludes at least `.git/`, `.env*`, `.venv/`, `DAGSTER/`
- [ ] CI has `test` job, `needs: test` on build, `concurrency` block, and GHA cache
- [ ] README documents required env vars and branch → tag mapping
- [ ] `deployments/*.py` removed (single source of truth: `prefect.yaml`)
- [ ] Image published to `ghcr.io/bu-ist/de-person-course-term-publish:latest` and pullable
