#!/bin/bash
set -euo pipefail

# ============================================================
# IFRS9 Pro – Production Deployment Script
# Uses Dockerfile.prod and docker-compose.prod.yml
# Safe to re-run. Does not delete volumes. Runs migrations.
# ============================================================

echo "🚀 Deploying IFRS9 Pro – PRODUCTION MODE"

# ----------------- Paths -----------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.prod.yml"

# ----------------- Load environment -----------------
if [[ -f "$ENV_FILE" ]]; then
    echo "📝 Loading environment variables from .env..."
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "❌ Missing .env file! Production deployment requires it."
    exit 1
fi

# ----------------- Sanity Checks -----------------
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker daemon is not running."
    exit 1
fi

if ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ Docker Compose not installed."
    exit 1
fi

# Wrapper
dc() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    else
        docker-compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    fi
}

# ----------------- Prepare directories -----------------
echo "📁 Creating persistent directories..."
mkdir -p reports logs app/ml_models
chmod 755 reports logs app/ml_models

# ----------------- Stop current containers (safe) -----------------
echo "🛑 Stopping running services WITHOUT deleting data..."
dc down --remove-orphans --timeout 30 || true

# ----------------- Rebuild & Start -----------------
echo "🏗️ Building production images..."
dc build --no-cache

echo "📦 Starting production containers..."
dc up -d

# ----------------- Wait for PostgreSQL -----------------
echo "⏳ Waiting for PostgreSQL..."
MAX_RETRIES=30

for i in $(seq 1 $MAX_RETRIES); do
    if dc exec -T db pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready."
        break
    fi
    echo "   Attempt $i/$MAX_RETRIES – retrying in 2s..."
    sleep 2
done

if [[ "$i" -eq "$MAX_RETRIES" ]]; then
    echo "❌ PostgreSQL failed to start in time. Logs:"
    dc logs db
    exit 1
fi

# ----------------- Run Alembic migrations -----------------
echo "🗄️ Running Alembic migrations..."

if dc exec -T web alembic current >/dev/null 2>&1; then
    echo "🔹 Alembic already initialized, upgrading..."
else
    echo "🔹 Fresh DB detected, stamping + upgrading..."
    dc exec -T web alembic stamp head || true
fi

if dc exec -T web alembic upgrade head; then
    echo "✅ Migrations applied successfully."
else
    echo "❌ Migration failed. Showing logs:"
    dc logs web
    exit 1
fi

# ----------------- Summary -----------------
echo ""
echo "🎉 IFRS9 PRO – PRODUCTION DEPLOYMENT COMPLETE!"
echo ""
echo "🔍 Running services:"
dc ps

echo ""
echo "🌐 Access endpoints:"
echo "   • API:                https://YOUR_DOMAIN"
echo "   • API Docs:           https://YOUR_DOMAIN/docs"
echo "   • MinIO Console:      https://MINIO_DOMAIN"
echo ""
echo "📋 Useful commands:"
echo "   • Logs:               dc logs -f"
echo "   • Restart:            dc restart"
echo "   • Stop:               dc down"
echo "   • DB Shell:           dc exec db psql -U $POSTGRES_USER -d $POSTGRES_DB"
echo ""
