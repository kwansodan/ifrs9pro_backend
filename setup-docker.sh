#!/bin/bash
set -euo pipefail

# ============================================================
# IFRS9 Pro Backend – Safe Docker Setup Script
# - Volume-safe
# - Idempotent
# - Always runs Alembic correctly
# ============================================================

echo "🐳 Setting up IFRS9 Pro Backend with Docker..."

# ---------- Project paths ----------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
COMPOSE_FILE_PATH="$PROJECT_ROOT/docker-compose.yml"

# ---------- Load environment ----------
if [[ -f "$ENV_FILE" ]]; then
    echo "📝 Loading environment variables..."
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "⚠️  .env not found – using defaults."
fi

# ---------- Docker sanity checks ----------
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker daemon is not running."
    exit 1
fi

# ---------- Compose wrapper ----------
dc() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE_PATH" -p ifrs9pro "$@"
    else
        docker-compose -f "$COMPOSE_FILE_PATH" -p ifrs9pro "$@"
    fi
}

# ---------- Prepare directories ----------
echo "📁 Preparing directories..."
mkdir -p reports site app/ml_models
chmod -R u+rwX,g+rwX reports site app/ml_models

# ---------- Stop existing containers (keep volumes) ----------
echo "🧹 Stopping existing services (preserving DB)..."
dc down --remove-orphans --timeout 30 || true

# ---------- Build & start ----------
echo "🏗️ Building and starting services..."
dc build
dc up -d

# ---------- Wait for PostgreSQL ----------
echo "⏳ Waiting for PostgreSQL..."
MAX_RETRIES=20
for i in $(seq 1 "$MAX_RETRIES"); do
    if dc exec -T db pg_isready -U ifrs9user -d ifrs9pro_db >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready."
        break
    fi
    echo "   Attempt $i/$MAX_RETRIES — sleeping 3s..."
    sleep 3
done

if [[ "$i" -eq "$MAX_RETRIES" ]]; then
    echo "❌ PostgreSQL failed to start."
    dc logs db
    exit 1
fi

# ---------- Alembic migrations (CORRECT) ----------
echo "🗄️ Running Alembic migrations..."
dc exec -T web alembic upgrade head


# ---------- Final status ----------
echo "🔍 Container status:"
dc ps

echo ""
echo "✅ Setup complete."
echo ""
echo "🌐 Services:"
echo "   • API:        http://localhost:8000"
echo "   • Docs:       http://localhost:8000/docs"
echo "   • MinIO:      http://localhost:9001"
echo "   • Locust:     http://localhost:8089"
echo ""
echo "📋 Commands:"
echo "   • Logs:     dc logs -f"
echo "   • Stop:     dc down"
echo "   • Restart:  dc restart"
echo "   • DB Shell: dc exec db psql -U ifrs9user -d ifrs9pro_db"
echo ""
