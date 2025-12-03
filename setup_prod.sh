#!/bin/bash
set -euo pipefail

# ============================================================
# IFRS9 Pro – Production Deployment Script with Rollback
# Uses Dockerfile.prod and docker-compose.prod.yml
# ============================================================

echo "🚀 Deploying IFRS9 Pro – PRODUCTION MODE (with rollback)"

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

# ----------------- Docker Compose wrapper -----------------
dc() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    else
        docker-compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    fi
}

# ----------------- Save current commit hash -----------------
cd "$PROJECT_ROOT"
PREV_COMMIT=$(git rev-parse HEAD)
echo "🔹 Current commit: $PREV_COMMIT (for rollback)"

# ----------------- Prepare directories -----------------
mkdir -p reports logs app/ml_models
chmod 755 reports logs app/ml_models

# ----------------- Deployment function -----------------
deploy() {
    echo "🛑 Stopping running services (safe)..."
    dc down --remove-orphans --timeout 30 || true

    echo "🏗️ Building production images..."
    dc build --no-cache

    echo "📦 Starting production containers..."
    dc up -d

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
    if [[ "$i" -eq $MAX_RETRIES ]]; then
        echo "❌ PostgreSQL failed to start in time."
        dc logs db
        return 1
    fi

    echo "🗄️ Running Alembic migrations..."
    if dc exec -T web alembic current >/dev/null 2>&1; then
        echo "🔹 Alembic already initialized, upgrading..."
    else
        echo "🔹 Fresh DB detected, stamping + upgrading..."
        dc exec -T web alembic stamp head || true
    fi

    if ! dc exec -T web alembic upgrade head; then
        echo "❌ Migration failed"
        dc logs web
        return 1
    fi

    return 0
}

# ----------------- Deploy with rollback -----------------
if deploy; then
    echo "🎉 Deployment successful!"
else
    echo "⚠️ Deployment failed! Rolling back to previous commit $PREV_COMMIT..."
    git reset --hard "$PREV_COMMIT"
    dc down --remove-orphans --timeout 30 || true
    dc up -d
    echo "🔹 Rollback complete."
    exit 1
fi

# ----------------- Summary -----------------
echo ""
echo "🎉 IFRS9 PRO – PRODUCTION DEPLOYMENT COMPLETE!"
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
