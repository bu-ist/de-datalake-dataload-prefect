# --- Builder Stage ---
FROM python:3.13-slim AS builder

WORKDIR /opt/prefect/app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file for caching layers
COPY requirements.txt .

# Install Python build tools and dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# --- Runtime Stage ---
FROM python:3.13-slim AS runtime

WORKDIR /opt/prefect/app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages and CLI scripts from builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /opt/prefect/app /opt/prefect/app

# Set Python environment flags
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/prefect/app

# Expose port for Prefect agent
EXPOSE 8080

# Default command: run Prefect worker
CMD ["prefect", "worker", "start", "--pool", "default-agent-pool"]
