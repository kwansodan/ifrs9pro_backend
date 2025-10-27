#!/bin/bash

# IFRS9 Pro Backend - Docker Setup Script
echo "🐳 Setting up IFRS9 Pro Backend with Docker..."

# Define project paths
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Check if docker-compose.yml exists
if [ ! -f "$DOCKER_DIR/docker-compose.yml" ]; then
    echo "❌ Could not find docker-compose.yml in $DOCKER_DIR"
    echo "   Make sure your docker-compose.yml file is inside the 'docker' folder."
    exit 1
fi

# Check if .env.docker exists
if [ ! -f "$DOCKER_DIR/.env.docker" ]; then
    echo "❌ Could not find .env.docker in $DOCKER_DIR"
    echo "   Please create .env.docker file with required environment variables."
    exit 1
fi

# Load environment variables from .env.docker
echo "📝 Loading environment variables..."
export $(grep -v '^#' "$DOCKER_DIR/.env.docker" | grep POSTGRES_PASSWORD | xargs)

# Move to project root
cd "$PROJECT_ROOT"

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p reports site app/ml_models

# Set proper permissions
echo "🔐 Setting permissions..."
chmod 755 reports site app/ml_models

# Check if Docker is running
echo "🔍 Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Build and start services (using the docker folder)
echo "🏗️ Building and starting services..."
docker-compose -f "$DOCKER_DIR/docker-compose.yml" down --remove-orphans
docker-compose -f "$DOCKER_DIR/docker-compose.yml" build --no-cache
docker-compose -f "$DOCKER_DIR/docker-compose.yml" up -d

# Wait for database to be ready
echo "⏳ Waiting for database to be ready..."
sleep 10

# Run database migrations
echo "🗄️ Running database migrations..."
docker-compose -f "$DOCKER_DIR/docker-compose.yml" exec web alembic upgrade head

# Check if services are running
echo "🔍 Checking service status..."
docker-compose -f "$DOCKER_DIR/docker-compose.yml" ps

echo "✅ Setup complete!"
echo ""
echo "🌐 Your application should be available at:"
echo "   - FastAPI: http://localhost:8000"
echo "   - API Docs: http://localhost:8000/docs"
echo "   - Database: localhost:5432"
echo ""
echo "📋 Useful commands:"
echo "   - View logs: docker-compose -f docker/docker-compose.yml logs -f"
echo "   - Stop services: docker-compose -f docker/docker-compose.yml down"
echo "   - Restart services: docker-compose -f docker/docker-compose.yml restart"
echo "   - Access database: docker-compose -f docker/docker-compose.yml exec db psql -U ifrs9user -d ifrs9pro_db"
