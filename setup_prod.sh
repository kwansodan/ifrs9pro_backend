#!/bin/bash
set -euo pipefail

# ============================================================
# IFRS9 Pro – Auto Deployment Script (Production)
# Rebuilds only when images changed.
# Safe to run on EVERY PUSH (via CI/CD or webhook).
# ============================================================

echo "🚀 Starting IFRS9 Pro Deployment (AUTO MODE)"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.prod.yml"

# ----------------- Load Environment -----------------
if [[ -f "$ENV_FILE" ]]; then
    echo "📝 Loading .env..."
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "❌ Missing .env file!"
    exit 1
fi

# ----------------- Docker Compose Wrapper -----------------
dc() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    else
        docker-compose -f "$COMPOSE_FILE" -p ifrs9pro "$@"
    fi
}

# ----------------- Build (ONLY IF CHANGES EXIST) -----------------
echo "🏗️ Building updated images (cached build)..."
dc build

# ----------------- Services Up (Zero Downtime Recreate) -----------------
echo "📦 Starting / Recreating containers..."
dc up -d --remove-orphans

# ----------------- Wait for DB -----------------
echo "⏳ Waiting for PostgreSQL..."
for x in {1..30}; do
    if dc exec -T db pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready."
        break
    fi
    sleep 2
done

# ----------------- Run Migrations -----------------
echo "🗄️ Running Alembic migrations..."
dc exec -T web alembic upgrade head || {
    echo "❌ Migration failed!"
    dc logs web
    exit 1
}

echo "🎉 Deployment Complete!"
dc ps
