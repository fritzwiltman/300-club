# Multi-stage build to keep the final image lean.
#
# Stage 1 — builder: install Python deps in a virtualenv
# Stage 2 — runtime: copy only what is needed to run

# -----------------------------------------------------------------------
# Stage 1: builder
# -----------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies (needed for psycopg2-binary wheel)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Create and activate a virtualenv so we can copy it cleanly later
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# Security: run as non-root user
RUN groupadd --gid 1001 appgroup \
 && useradd --uid 1001 --gid 1001 --no-create-home appuser

WORKDIR /app

# Runtime OS deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
 && rm -rf /var/lib/apt/lists/*

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY --chown=appuser:appgroup . .

USER appuser

EXPOSE 8000

# Gunicorn: 2 workers is sufficient for a low-traffic app on a single Lightsail instance.
# Increase workers if you upgrade the instance.
CMD ["gunicorn", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "2", \
     "--timeout", "60", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "backend.wsgi:application"]
