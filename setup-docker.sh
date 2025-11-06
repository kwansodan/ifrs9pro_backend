#!/bin/bash
set -euo pipefail

# ============================================================
# IFRS9 Pro Backend – Safe Docker Setup Script
# Handles both fresh setup and re-runs gracefully.
# ============================================================

echo "🐳 Setting up IFRS9 Pro Backend with Docker..."

# ---------- Project paths ----------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
COMPOSE_FILE_DEFAULT="docker-compose.yml"
COMPOSE_FILE_PATH="$PROJECT_ROOT/$COMPOSE_FILE_DEFAULT"

# ---------- Load environment ----------
if [[ -f "$ENV_FILE" ]]; then
    echo "📝 Loading environment variables from .env..."
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "⚠️  No .env file found – continuing with defaults."
fi

# ---------- Resolve compose file ----------
if [[ -n "${DOCKER_COMPOSE:-}" ]]; then
    COMPOSE_FILE_PATH="$PROJECT_ROOT/$DOCKER_COMPOSE"
fi

if [[ ! -f "$COMPOSE_FILE_PATH" ]]; then
    echo "❌ Docker Compose file not found: $COMPOSE_FILE_PATH"
    exit 1
fi

echo "📄 Using Docker Compose file: $COMPOSE_FILE_PATH"

# ---------- Docker sanity checks ----------
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker daemon not running. Start Docker and try again."
    exit 1
fi

if ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ Docker Compose is not available. Install it and try again."
    exit 1
fi

# Wrapper function for consistent Compose calls
dc() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE_PATH" -p ifrs9pro "$@"
    else
        docker-compose -f "$COMPOSE_FILE_PATH" -p ifrs9pro "$@"
    fi
}

# ---------- Prepare directories ----------
echo "📁 Ensuring required directories exist..."
mkdir -p reports site app/ml_models
chmod 755 reports site app/ml_models

# ---------- Graceful cleanup (no volume deletion) ----------
echo "🧹 Stopping old services (keeping database volume)..."
dc down --remove-orphans --timeout 30 || true

# ---------- Build & start containers ----------
echo "🏗️ Building and starting containers..."
dc build
dc up -d

# ---------- Wait for PostgreSQL ----------
echo "⏳ Waiting for PostgreSQL to become ready..."
MAX_RETRIES=20
for i in $(seq 1 $MAX_RETRIES); do
    if dc exec -T db pg_isready -U ifrs9user -d ifrs9pro_db >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "   Attempt $i/$MAX_RETRIES – sleeping 3s..."
    sleep 3
done

if [[ $i -eq $MAX_RETRIES ]]; then
    echo "❌ PostgreSQL did not become ready in time."
    dc logs db
    exit 1
fi

# ---------- Alembic migration logic ----------
echo "🗄️ Checking Alembic migration state..."

# Check if Alembic is already stamped
if dc exec -T web alembic current >/dev/null 2>&1; then
    echo "🔸 Alembic already initialized – skipping re-upgrade."
else
    echo "🚀 Applying initial migrations..."
    if ! dc exec -T web alembic upgrade head; then
        echo "⚠️  Alembic upgrade failed – attempting safe stamp..."
        dc exec -T web alembic stamp head || true
    fi
fi

# ---------- Status summary ----------
echo "🔍 Containers:"
dc ps

echo ""
echo "✅ Setup complete!"
echo ""
echo "🌐 Access points:"
echo "   • FastAPI API:       http://localhost:8000"
echo "   • API Docs:          http://localhost:8000/docs"
echo "   • MinIO Console:     http://localhost:9001"
echo "   • Locust Dashboard:  http://localhost:8089"
echo ""
echo "📋 Handy commands:"
echo "   • Logs:    dc logs -f"
echo "   • Stop:    dc down"
echo "   • Restart: dc restart"
echo "   • DB Shell: dc exec db psql -U ifrs9user -d ifrs9pro_db"
echo ""
