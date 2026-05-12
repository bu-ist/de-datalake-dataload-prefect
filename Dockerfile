# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Dependencies first — cached layer when only flow code changes.
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Flow and config code.
COPY flows ./flows
COPY config ./config
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM python:3.13-slim-bookworm AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 flowuser && \
    useradd --uid 1000 --gid flowuser --no-create-home --shell /usr/sbin/nologin flowuser

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/flows ./flows
COPY --from=builder /app/config ./config

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

USER flowuser

# Flow-runner image — the Prefect worker overrides CMD with `prefect flow-run execute ...`.
# Running this image directly will only print this message.
CMD ["python", "-c", "print('de-person-course-term-publish — invoked by Prefect worker, not run directly')"]
