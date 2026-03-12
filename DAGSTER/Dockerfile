# --- Builder Stage ---
FROM python:3.13-slim AS builder

WORKDIR /opt/dagster/app

# Install build dependencies and ODBC prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    gnupg \
    ca-certificates \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft package repo using keyring (apt-key is deprecated/removed)
RUN install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && rm -rf /var/lib/apt/lists/*

# Copy project metadata for caching layers
COPY pyproject.toml .
COPY README.md .

# Install Python build tools and dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -e .

# Copy the rest of the source code
COPY . .

# --- Runtime Stage ---
FROM python:3.13-slim AS runtime

WORKDIR /opt/dagster/app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    unixodbc \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Reuse Microsoft repo keyring and sources from builder, then install ODBC driver
COPY --from=builder /etc/apt/keyrings/microsoft.gpg /etc/apt/keyrings/microsoft.gpg
COPY --from=builder /etc/apt/sources.list.d/mssql-release.list /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages and CLI scripts from builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /opt/dagster/app /opt/dagster/app

# Set Python environment flags
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/dagster/app/src

# Expose port for Dagster gRPC server
EXPOSE 3030

# Default command: run Dagster code server for hot-reloading
CMD ["python", "-m", "dagster", "code-server", "start", \
     "--host", "0.0.0.0", "--port", "3030", \
     "--working-directory", "/opt/dagster/app/src/PersonDataLoad", \
     "--module-name", "definitions"]