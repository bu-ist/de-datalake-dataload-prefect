# Prefect Deployment Plan — `de-person-course-term-publish`

**Audience:** developer maintaining this repo
**Goal:** prepare the project to run on BU Data Engineering's shared Prefect platform on `de-eks-nonprod`. The platform is already live with two other flow projects (`bu-web-vectorizer`, `course-catalog-vectorizer`); this plan brings this repo in line with the patterns those flows already use.

The companion document on the platform side is [docs/prefect-first-project-deployment.md](https://github.com/bu-ist/de-eks-nonprod/blob/main/docs/prefect-first-project-deployment.md). The platform owner needs that one — you don't.

---

## Context: what we're deploying onto

The shared Prefect platform on `de-eks-nonprod` is already running. Its key contract values — these are not negotiable from the flow side, they're set by the platform:

| Setting | Value |
|---------|-------|
| Prefect server version | **3.x** (Helm chart `prefect-server v2026.3.x`) |
| API URL | `https://prefect.de-eks-nonprod.bu.edu/api` (browser/OIDC); CLI uses port-forward `localhost:4200/api` |
| Work pool exposed | `batch-jobs` (Kubernetes type) |
| Namespace where flow Jobs run | `prefect` |
| Node group used for Jobs | label `workload=batch`, taint `dedicated=batch:NoSchedule` |
| **Image registry** | **AWS ECR `889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/<project>`** (mirrors Dagster's `dag/*` convention). **Not GHCR.** |
| Image pull auth | Node IAM (no `imagePullSecrets`) |
| Code delivery | Pre-baked into image at `WORKDIR /app`; no git pulls at run time |
| Secret store | AWS Secrets Manager via External Secrets Operator |
| Auth | ALB OIDC (Azure Entra ID) — UI auth only; in-cluster API access is unauthenticated |

Most of the work below closes gaps between the project's current shape (Prefect 2.x, `k8s-pool`, no env injection, GHCR-only CI) and that contract.

---

## Phase 1 — Make the project compatible with Prefect 3

### 1.1 Upgrade `prefect` to 3.x

**What:** in `pyproject.toml` and `requirements.txt`, replace `prefect>=2.14.0` with `prefect>=3.0,<4`. Drop `prefect-client` from `[dependency-groups].dev`.

**Why specifically Prefect 3:**

- **The deployed server is Prefect 3.** Helm chart `2026.3.x` ships Prefect 3.x. Prefect 2 and Prefect 3 have **different REST APIs and database schemas** — they aren't backward-compatible. A 2.x client trying to register a deployment against a 3.x server hits 404s or schema-validation errors. There is no compatibility shim.
- **Prefect 3 removed agents in favor of workers.** The platform runs `prefect-worker` (the new model). Agents (the 2.x model) don't exist in the cluster.
- **The deployment registration mechanism changed.** Prefect 2 used `Deployment.build_from_flow().apply()` and `flow.deploy()` (you have remnants in `deployments/*_raw_deployment.py`). Prefect 3 standardizes on `prefect.yaml` + `prefect deploy`, which is what the platform's deploy helper uses.
- **Some flow APIs subtly changed** (cache key fns, retry parameters, state handling). The flows here look 3-compatible at a glance — `@flow`, `prefect.logging.get_run_logger`, `prefect.exceptions.Abort` — but they need a real run against a 3.x server before we trust them.

**How:**

```bash
# pyproject.toml
"prefect>=3.0,<4"   # was: "prefect>=2.14.0"

# requirements.txt
prefect>=3.0,<4    # was: prefect>=2.14.0
```

Then locally:

```bash
uv sync
prefect server start                                    # local Prefect 3 server on :4200
PREFECT_API_URL=http://localhost:4200/api prefect deploy --all
PREFECT_API_URL=http://localhost:4200/api prefect deployment run datalake-dataload/term-raw-daily
```

If the local run succeeds end-to-end against a 3.x server, the upgrade is good.

### 1.2 Pick one source of truth for dependencies

**What:** the repo currently has three places that declare dependencies — `pyproject.toml`, `requirements.txt`, and `uv.lock`. The Dockerfile installs from `requirements.txt`. Choose one.

**Why this matters:**

- **Drift is silent.** If someone bumps a security-patched library in `pyproject.toml`, the Docker image still installs the old version from `requirements.txt`. The "fix" appears merged but never reaches the runtime.
- **`uv.lock` is currently ignored.** 624 KB of pinned hashes that nothing in CI or Docker uses. Either it's authoritative or it shouldn't be in the repo.
- **Reproducibility:** without a single locked source, two developers can build different images from the same git SHA.

**Recommended — switch to `uv`:**

```dockerfile
FROM python:3.13-slim AS builder
RUN pip install uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
```

Then delete `requirements.txt`. (Or, if staying with pip: regenerate `requirements.txt` from `pyproject.toml` on every CI run, commit the result, and delete `uv.lock`.)

### 1.3 Fix the `.env.example` ↔ `config/settings.py` mismatch

**What:** `.env.example` declares `VDS_USERNAME` and `VDS_PASSWORD`. `config/settings.py` declares `vds_url` and `vds_key` only. Either pydantic-Settings is missing fields or `.env.example` lies about what's required.

**Why this matters:** the platform owner will populate AWS Secrets Manager from `config/settings.py` (the authoritative source — pydantic raises `ValidationError` on missing fields). If `.env.example` lists keys that aren't in Settings, those values are stored but never read. Worse: if Settings actually needs `VDS_USERNAME`/`VDS_PASSWORD` and they got dropped during a refactor, `.env.example` is right and Settings is wrong → the flow can't connect to VDS.

**Action:** decide which is right, fix the other, and confirm with the operations team that what's in `config/settings.py` matches the credentials that will actually be in AWS Secrets Manager.

---

## Phase 2 — Match the platform's deployment contract

### 2.1 Change work pool from `k8s-pool` to `batch-jobs`

**What:** in all three `prefect.yaml` deployments, change `work_pool.name: k8s-pool` to `work_pool.name: batch-jobs`.

**Why this exact name:**

- **The platform's worker subscribes to `batch-jobs` only.** Configured in [applications/prefect/prefect-values.yaml](https://github.com/bu-ist/de-eks-nonprod/blob/main/applications/prefect/prefect-values.yaml). The worker process polls the API for work in that pool and ignores all others.
- **`k8s-pool` doesn't exist on the server.** When you run `prefect deploy`, the deployment record is created — but it points at a pool the server doesn't have. The deployment shows up in the UI, schedules trigger flow runs, runs go into `Scheduled` → `Late`, and stay there forever. There's no error, just silence.
- **You can't create your own pool from the flow side.** Pool creation requires admin access; the platform team controls which pools exist (each pool has its own worker process and node group config).

### 2.2 Add `job_variables` — image, pull policy, env injection

**What:** today `prefect.yaml` has only `work_pool.name`. Add this exact block to every deployment (mirrors `bu-web-vectorizer` and `course-catalog-vectorizer`):

```yaml
work_pool:
  name: batch-jobs
  job_variables:
    image: 889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/de-person-course-term-publish:test
    image_pull_policy: Always
    namespace: prefect
    env_from:
      - secretRef:
          name: de-person-course-term-publish-secrets
```

**Why each piece:**

#### `image:` — ECR, not GHCR
The platform runs in account `889914499666` and uses **ECR `flow/<project>`** as the image registry. EKS nodes have IAM permissions to pull from this ECR; no `imagePullSecrets` needed. Pushing only to GHCR means `ImagePullBackOff` because GHCR images are private by default and there's no GHCR pull secret in the cluster.

The two existing flow projects (`web-crawler`, `course-catalog-vectorizer`) both use `889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/<name>:test`. Match exactly.

The tag `:test` is the deployed convention; matches Dagster code servers in the same cluster. Branch → tag mapping (Phase 4): `dev` → `:dev`, `test` → `:test`, `main` → `:latest`. The `prefect.yaml` deployment that runs in nonprod points at `:test`.

#### `image_pull_policy: Always`
Kubernetes' default is `IfNotPresent`. With `IfNotPresent` and a mutable tag like `:test`:
- Node pulls the image once, caches it
- CI pushes a new `:test` (e.g., bug fix)
- Same node runs tomorrow's flow → reuses the **old** cached image
- The fix silently doesn't take effect, possibly for days, until the node is replaced

`Always` forces a registry check on every pod start, which is what you want when the tag is mutable. (For immutable tags like `:v1.2.3` or `:<sha>`, `IfNotPresent` is correct.)

#### `env_from: [secretRef: ...]` — the deal-breaker
[config/settings.py](config/settings.py) declares **15 required + 1 defaulted** pydantic fields:

```
postgres_host, postgres_port (default 5432), postgres_db, postgres_user, postgres_pass,
cs_env,
de_cstools_endpoint, de_cstools_key,
snaplogic_course_url, snaplogic_course_key,
de_person_api_url, de_person_api_key,
vds_url, vds_key,
sap_url, sap_key
```

`settings = Settings()` runs at **module import time**. So in the cluster without `env_from`:
1. Worker spawns Job → pod starts
2. Prefect runner imports `flows.term.term_flow`
3. That triggers `from config.resources import ...` → which triggers `from config.settings import settings`
4. `Settings()` → pydantic raises `ValidationError: 15 validation errors for Settings, postgres_host: Field required ...`
5. Pod exits before the flow function is ever called
6. Prefect UI shows the run as `Crashed`

The platform owner will create:
- A single AWS Secrets Manager entry at `eks/prefect/config/de-person-course-term-publish` containing every required env var as a JSON blob
- An `ExternalSecret` in the `prefect` namespace named `de-person-course-term-publish-secrets` that syncs that JSON into a Kubernetes Secret

`env_from.secretRef` is what links your pod to those credentials. The name `de-person-course-term-publish-secrets` is the convention — match it exactly so the platform owner doesn't have to reverse-engineer naming.

### 2.3 Set the working directory via `pull:` and align with the Dockerfile

**What:** change `prefect.yaml`'s `pull: null` (line 8) to:

```yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /app
```

And change the Dockerfile's `WORKDIR` from `/opt/prefect/app` to `/app` (or vice versa — they must match).

**Why `/app` specifically:** both deployed flow projects use `WORKDIR /app` + `set_working_directory: /app`. This is the platform convention. Aligning with it means anyone reading `prefect.yaml` for any flow gets the same answer.

**Why this `pull:` step is required:**

- The entrypoints in `prefect.yaml` are **relative paths**: `flows/term/term_flow.py:term_raw_flow`. Prefect resolves them against the current working directory at flow-run time.
- The Prefect worker doesn't automatically `cd` into the image's `WORKDIR`. It runs `pull:` steps first (which is where `set_working_directory` belongs), then imports the entrypoint.
- Without a `pull:` step, the working directory is whatever the worker's runtime defaults to — typically `/`. The relative entrypoint resolves to `/flows/term/term_flow.py`, which doesn't exist → `ModuleNotFoundError: No module named 'flows'`.

The `PYTHONPATH=/app` (or current `/opt/prefect/app`) line in the Dockerfile helps imports if the cwd is wrong, but doesn't help with relative-path entrypoint resolution. Both belt and suspenders are fine; `pull:` is mandatory.

---

## Phase 3 — Image hygiene

### 3.1 Run as a non-root user

**What:** in the Dockerfile runtime stage, create a UID-1000 user, `chown` the app dir, and `USER` to it before `CMD`.

```dockerfile
FROM python:3.13-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 1000 -m -s /bin/bash prefect

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder --chown=prefect:prefect /app /app

USER prefect
```

**Why:**

- **Pod Security Admission:** EKS at BU enforces a PodSecurity policy. Containers running as `runAsUser: 0` get rejected at admission time with `pods "..." is forbidden: violates PodSecurity ... runAsNonRoot != true`. The pod never starts; the flow run goes straight to `Crashed`.
- **Defense in depth:** even where root is allowed, container root is the *kernel's* root. Any container-escape vulnerability becomes a host root issue.
- **UID 1000 specifically:** matches Dagster code servers and other flows in the same cluster. Consistent UIDs make any future shared-volume scenarios less painful.

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

- **`.dockerignore` is a different file from `.prefectignore`.** The repo has `.prefectignore` (good — but it's invisible to Docker). Without `.dockerignore`, every `docker build` copies the entire repo into the build context, then into the image.
- **Image size and content:** `.git/` for an active project can be 100s of MB. `DAGSTER/` is "Legacy reference code" per `.prefectignore` — no place in a runtime image. `.venv/` (if a developer ran `uv sync` locally) embeds host-specific binaries.
- **Secret leakage:** if a developer has a real `.env` locally with production credentials, `COPY . .` puts it inside the image. Anyone who can pull the image can read it. `.dockerignore` is the only line of defense.
- **Build cache invalidation:** Docker layer cache invalidates whenever any file in the COPY context changes. Including `.git/` means every commit busts the cache, even when no Python source changed → CI builds become 5+ minutes every time.

### 3.3 Replace the misleading `CMD`

**What:** change the Dockerfile's
```dockerfile
CMD ["prefect", "worker", "start", "--pool", "default-agent-pool"]
```
to
```dockerfile
CMD ["python", "-c", "print('flow image — invoked by Prefect worker, not run directly')"]
```

**Why:**

- **The current CMD is harmless but wrong.** This is a **flow-runner image**, not a worker image. The platform already runs `prefect-worker` in the cluster.
- **The worker overrides `command` per Job.** When a flow run starts, the worker creates a Job whose container `args` are set to `prefect flow-run execute ...`, bypassing the image's `CMD` entirely. So the existing CMD never runs in production.
- **But it runs if anyone does `docker run <image>` locally** — they'd see a worker start trying to connect to `default-agent-pool` (which doesn't exist) and get confused. The replacement makes intent explicit.
- **`--pool default-agent-pool` is wrong** in any case — the actual pool is `batch-jobs`.

---

## Phase 4 — CI/CD: push to ECR (the big change)

The existing `.github/workflows/docker-publish.yaml` builds and pushes to GHCR. **The platform pulls from ECR, not GHCR.** So this workflow needs to be replaced with one that pushes to both: ECR (consumed by the cluster) and GHCR (organization mirror).

The reference pattern is [de-dag-api-ingestion/.github/workflows/build-and-push.yml](https://github.com/bu-ist/de-dag-api-ingestion/blob/main/.github/workflows/build-and-push.yml). Copy that file and substitute the project-specific values.

### 4.1 Replace the workflow

```yaml
name: Build and Push Container

on:
  push:
    branches: [dev, test, main]
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

permissions:
  contents: read
  packages: write
  id-token: write          # Required for OIDC auth to AWS

env:
  GHCR_REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}
  AWS_REGION: us-east-1
  ECR_REPOSITORY: flow/de-person-course-term-publish     # <-- the only project-specific value
  ECR_NONPROD_REGISTRY: 889914499666.dkr.ecr.us-east-1.amazonaws.com
  ECR_NONPROD_ROLE: arn:aws:iam::889914499666:role/github-actions-ecr-role
  ECR_PROD_REGISTRY: 388301380638.dkr.ecr.us-east-1.amazonaws.com
  ECR_PROD_ROLE: arn:aws:iam::388301380638:role/github-actions-ecr-role

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-python@v6
        with: { python-version: '3.13' }
      - run: pip install uv
      - run: uv sync --extra dev
      - name: Smoke import all flow modules
        run: |
          uv run python -c "from flows.term.term_flow import term_raw_flow"
          uv run python -c "from flows.course.course_flow import course_raw_flow"
          uv run python -c "from flows.person.person_flow import person_raw_flow"

  build-and-push:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: docker/setup-buildx-action@v3

      - name: Set ECR target
        id: ecr-target
        run: |
          if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
            echo "registry=${{ env.ECR_PROD_REGISTRY }}" >> "$GITHUB_OUTPUT"
            echo "role=${{ env.ECR_PROD_ROLE }}" >> "$GITHUB_OUTPUT"
          else
            echo "registry=${{ env.ECR_NONPROD_REGISTRY }}" >> "$GITHUB_OUTPUT"
            echo "role=${{ env.ECR_NONPROD_ROLE }}" >> "$GITHUB_OUTPUT"
          fi

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ${{ env.GHCR_REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ steps.ecr-target.outputs.role }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Log in to Amazon ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: |
            ${{ env.GHCR_REGISTRY }}/${{ env.IMAGE_NAME }}
            ${{ steps.ecr-target.outputs.registry }}/${{ env.ECR_REPOSITORY }}
          tags: |
            type=raw,value=dev,enable=${{ github.ref == 'refs/heads/dev' }}
            type=raw,value=test,enable=${{ github.ref == 'refs/heads/test' }}
            type=raw,value=latest,enable=${{ github.ref == 'refs/heads/main' }}
            type=raw,value={{date 'YYYYMMDD'}}-{{sha}},enable=${{ github.ref == 'refs/heads/main' }}

      - name: Build and push
        uses: docker/build-push-action@v6
        with:
          context: .
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          platforms: linux/amd64
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

### 4.2 Why each piece

#### `id-token: write` + `aws-actions/configure-aws-credentials` (OIDC)
The role `arn:aws:iam::889914499666:role/github-actions-ecr-role` already exists; its trust policy allows GitHub Actions OIDC tokens from `bu-ist/*` repos. No long-lived AWS keys live in GitHub Secrets — the role is assumed at runtime via federated identity.

If the workflow runs and gets `AccessDenied` on the assume-role, ask the platform owner to extend the role's trust policy to include this specific repo.

#### Push to **both** GHCR and ECR
- **ECR is consumed by the cluster** — the platform's flow Pods pull from there.
- **GHCR is the org mirror** — convenient for browsing image versions in GitHub UI alongside the source.
- `docker/metadata-action` generates tag pairs against both registries; `docker/build-push-action` pushes once and uploads to both.

#### Branch → tag mapping
- `dev` branch → `:dev` (future staging deployments)
- `test` branch → `:test` (current `prefect.yaml` deployments target this)
- `main` branch → `:latest` and dated SHA (future prod deployments)

The `prefect.yaml` `job_variables.image` field is what binds a Prefect deployment to a tag. Changing the deployment's tag is a `prefect.yaml` edit + `prefect deploy` re-registration.

#### `needs: test`
Right now nothing stops a broken commit from publishing `:test` (used by the live deployments). A typo, a broken import, a removed library API — all get pushed and become the next scheduled flow's image. Production breaks at 1 AM ET tomorrow.

The smoke-import test catches: missing dependencies, syntax errors, circular imports, broken `from X import Y`. Runs in seconds. Not a substitute for real tests, but vastly better than nothing.

#### `concurrency` block
Prevents racing builds on the same tag. If two pushes to `test` land within seconds, both trigger CI; both push to `:test`. The slower one wins — but if the first build's image was already pulled by a node mid-run, that node uses the *first* (now-overwritten-but-cached) image. `cancel-in-progress: true` cancels the older run.

#### `cache-from: type=gha` / `cache-to: type=gha,mode=max`
Cold builds with `apt-get install build-essential libpq-dev` + `pip install -r requirements.txt` take minutes. With GHA cache, those layers cache-hit when only Python source changed → CI drops from ~5 minutes to ~30 seconds. `mode=max` caches every intermediate layer (worth it for multi-stage builds).

### 4.3 Coordinate ECR repo creation with the platform owner

**Before merging the new workflow:** the platform owner must create the ECR repo:

```bash
aws ecr create-repository --repository-name flow/de-person-course-term-publish
```

The OIDC role can push to existing repos but doesn't have `ecr:CreateRepository` permission. Without this, the first CI build fails with `RepositoryNotFoundException`.

---

## Phase 5 — Cleanup

### 5.1 Delete `deployments/*_raw_deployment.py`

**What:** remove `deployments/term_raw_deployment.py`, `deployments/person_raw_deployment.py`, `deployments/course_raw_deployment.py`.

**Why:**

- **These are Prefect 2.x-style deployment scripts** that use `flow.deploy(...)` programmatically. After migrating to Prefect 3 + `prefect.yaml`, they become a parallel registration path.
- **Running both creates duplicate deployments.** If anyone runs `python deployments/term_raw_deployment.py` AND the platform team runs `prefect deploy`, you get two deployment records, possibly with different work pools and schedules. Both fire. You get duplicate runs.
- **Single source of truth:** `prefect.yaml` is the file the platform's `prefect-deploy.sh` reads. That's the only path that should exist.

### 5.2 Note `setup.sh`'s local-only API URL

Add a comment on the line that exports `PREFECT_API_URL=http://localhost:4200/api` clarifying it's for local dev only. Harmless in production (the script doesn't run in the image), but a future engineer reading `setup.sh` could think the project assumes localhost server in all contexts.

---

## Recommended order of operations

1. **Branch off `main`:** `git checkout -b prefect-3-eks-readiness`
2. **Phase 1** — Prefect 3 upgrade, dep source consolidation, settings/.env.example reconciliation. Test all three flows end-to-end against a local Prefect 3 server before continuing.
3. **Phase 2** — `prefect.yaml` updates (work pool, `job_variables` with **ECR image**, `pull:` working directory). Re-test locally with image substituted to a local dev tag.
4. **Phase 3** — Dockerfile non-root user, `.dockerignore`, CMD replacement. Build the image locally (`docker build .`) and confirm it runs.
5. **Phase 4** — Replace the GitHub Actions workflow. **Coordinate with the platform owner first** to ensure the ECR repo `flow/de-person-course-term-publish` is created.
6. **Open a PR.** Let CI build and push to both ECR and GHCR. Verify in AWS Console that `889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/de-person-course-term-publish:test` exists and is pullable.
7. **Coordinate with the platform owner** to:
   - Populate AWS Secrets Manager at `eks/prefect/config/de-person-course-term-publish` (16 keys)
   - Add the matching `ExternalSecret` block in `de-eks-nonprod`
   - Run `applications/prefect/scripts/prefect-deploy.sh ~/de-person-course-term-publish` to register the three deployments
   - Trigger a manual run of `term-raw-daily` from the Prefect UI as a smoke test
8. **Phase 5** in a follow-up PR — doesn't block first deploy.

**Estimated scope:** half a day for someone familiar with the project. Longest pole is local testing of the Prefect 3 upgrade.

---

## Acceptance criteria for "ready to deploy"

- [ ] `prefect>=3.0,<4` installed in the image (verified via `docker run --entrypoint pip <image> show prefect`)
- [ ] `config/settings.py` and `.env.example` agree on field names (no `VDS_USERNAME`/`VDS_PASSWORD` mismatch)
- [ ] `prefect.yaml` has `work_pool.name: batch-jobs`, `job_variables.image: 889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/de-person-course-term-publish:test`, `image_pull_policy: Always`, `env_from.secretRef.name: de-person-course-term-publish-secrets`, and a `pull:` step matching the Dockerfile WORKDIR
- [ ] Dockerfile WORKDIR aligned to `/app` (or whatever the `pull:` step uses)
- [ ] Image runs as UID 1000 (verified via `docker run --entrypoint id <image>`)
- [ ] `.dockerignore` exists and excludes at least `.git/`, `.env*`, `.venv/`, `DAGSTER/`
- [ ] CI has `test` job, `needs: test` on build, `concurrency` block, GHA cache, OIDC auth to ECR, and pushes to **both ECR and GHCR**
- [ ] `deployments/*.py` removed (single source of truth: `prefect.yaml`)
- [ ] Image published to `889914499666.dkr.ecr.us-east-1.amazonaws.com/flow/de-person-course-term-publish:test` and pullable from a workstation with the nonprod AWS profile
