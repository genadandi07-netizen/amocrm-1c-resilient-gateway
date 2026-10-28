# Multi-stage build; no external # syntax frontend is pinned on purpose so a
# build never depends on fetching an extra image from Docker Hub.
FROM python:3.10-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Build metadata first: pyproject.toml references `readme = "README.md"`,
# so both files must be present before setuptools builds the sdist/wheel.
COPY pyproject.toml README.md ./

# Then the package sources. Installing before the sources exist (`pip install
# -e .` on an empty WORKDIR) is what used to break this stage.
COPY gateway/ ./gateway/
COPY scripts/ ./scripts/
COPY config/ ./config/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    # Shared by both services through ./data:/app/data (overridable via .env).
    GATEWAY_DB_PATH=/app/data/gateway.db

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY . .

# Build-time guard: the `uvicorn` console script (declared in pyproject.toml
# dependencies) must be on PATH for the CMD below to start.
RUN command -v uvicorn >/dev/null && python -c "import uvicorn, gateway.app"

RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Starts as root only to fix ownership of the bind-mounted SQLite directory
# (./data:/app/data) and immediately drops to the unprivileged `appuser`.
ENTRYPOINT ["python", "/app/scripts/docker_entrypoint.py"]
CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
