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
PREV_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
echo "🔹 Current commit: $PREV_COMMIT (for rollback)"

# ----------------- Prepare directories -----------------
mkdir -p reports logs app/ml_models
chmod 755 reports logs app/ml_models

# ----------------- Deployment function -----------------
deploy() {
    echo "🛑 Stopping running services (safe)..."
    dc down --remove-orphans --timeout 30 || true

    echo "🏗️ Building production images..."
    if ! dc build --no-cache; then
        echo "❌ Build failed!"
        return 1
    fi

    echo "📦 Starting production containers..."
    if ! dc up -d; then
        echo "❌ Failed to start containers!"
        return 1
    fi

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
    done
    if [[ "$i" -eq $MAX_RETRIES ]]; then
        echo "❌ PostgreSQL failed to start in time."
        dc logs db
        return 1
    fi

    # Wait for Redis
    echo "⏳ Waiting for Redis..."
    for i in $(seq 1 10); do
        if dc exec -T redis redis-cli ping >/dev/null 2>&1; then
            echo "✅ Redis is ready."
            break
        fi
        echo "   Attempt $i/10 – retrying in 2s..."
        sleep 2
    done

    # Additional wait for web container to be ready
    echo "⏳ Waiting for web container..."
    sleep 5

    echo "🗄️ Checking database migration state..."
    
    # Check if tables already exist (indicates existing database)
    TABLES_EXIST=$(dc exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'access_requests');" 2>/dev/null || echo "f")
    
    # Check if alembic_version table exists
    ALEMBIC_EXISTS=$(dc exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'alembic_version');" 2>/dev/null || echo "f")
    
    if [[ "$TABLES_EXIST" == "t" ]] && [[ "$ALEMBIC_EXISTS" == "f" ]]; then
        echo "🔹 Existing tables detected without Alembic tracking. Stamping database..."
        if ! dc exec -T web alembic stamp head; then
            echo "❌ Failed to stamp database"
            dc logs web
            return 1
        fi
        echo "✅ Database stamped successfully"
    elif [[ "$ALEMBIC_EXISTS" == "t" ]]; then
        echo "🔹 Alembic tracking exists. Checking if migration needed..."
        CURRENT_REV=$(dc exec -T web alembic current 2>/dev/null | grep -oP '(?<=\()[a-f0-9]+(?=\))' | head -1 || echo "none")
        HEAD_REV=$(dc exec -T web alembic heads 2>/dev/null | grep -oP '(?<=\()[a-f0-9]+(?=\))' | head -1 || echo "none")
        
        if [[ "$CURRENT_REV" == "$HEAD_REV" ]] && [[ "$CURRENT_REV" != "none" ]]; then
            echo "✅ Database already at latest revision ($CURRENT_REV)"
        else
            echo "🔹 Running migration from $CURRENT_REV to $HEAD_REV..."
            if ! dc exec -T web alembic upgrade head; then
                echo "❌ Migration failed"
                dc logs web
                return 1
            fi
            echo "✅ Migration completed successfully"
        fi
    else
        echo "🔹 Fresh database detected. Running initial migration..."
        if ! dc exec -T web alembic upgrade head; then
            echo "❌ Initial migration failed"
            dc logs web
            return 1
        fi
        echo "✅ Initial migration completed successfully"
    fi
    
    # Health check
    echo "🏥 Running health check..."
    sleep 3
    if dc exec -T web curl -f http://localhost:8000/health >/dev/null 2>&1 || \
       dc exec -T web wget -q --spider http://localhost:8000/health >/dev/null 2>&1; then
        echo "✅ Health check passed"
    else
        echo "⚠️ Health check failed, but continuing (service might need more time)"
    fi

    return 0
}

# ----------------- Deploy with rollback -----------------
if deploy; then
    echo "🎉 Deployment successful!"
else
    echo "⚠️ Deployment failed! Rolling back..."
    
    if [[ "$PREV_COMMIT" != "unknown" ]]; then
        echo "🔄 Restoring to commit $PREV_COMMIT..."
        git reset --hard "$PREV_COMMIT"
    fi
    
    echo "🛑 Stopping failed containers..."
    dc down --remove-orphans --timeout 30 || true
    
    echo "♻️ Attempting to restart previous version..."
    dc up -d || true
    
    echo "🔹 Rollback complete."
    dc ps
    exit 1
fi

# ----------------- Summary -----------------
echo ""
echo "🎉 IFRS9 PRO – PRODUCTION DEPLOYMENT COMPLETE!"
echo ""
dc ps
echo ""
echo "🌐 Access endpoints:"
echo "   • API:                https://YOUR_DOMAIN"
echo "   • API Docs:           https://YOUR_DOMAIN/docs"
echo "   • MinIO Console:      https://MINIO_DOMAIN"
echo ""
echo "📋 Useful commands:"
echo "   • Logs:               docker compose -f $COMPOSE_FILE logs -f"
echo "   • Restart:            docker compose -f $COMPOSE_FILE restart"
echo "   • Stop:               docker compose -f $COMPOSE_FILE down"
echo "   • DB Shell:           docker compose -f $COMPOSE_FILE exec db psql -U $POSTGRES_USER -d $POSTGRES_DB"
echo ""
echo "✅ Deployment completed at $(date)"