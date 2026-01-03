#!/usr/bin/env bash
set -euo pipefail

echo ">>> Stopping all running containers"
docker stop $(docker ps -aq) 2>/dev/null || true

echo ">>> Removing all containers"
docker rm -f $(docker ps -aq) 2>/dev/null || true

echo ">>> Removing all images"
docker rmi -f $(docker images -aq) 2>/dev/null || true

echo ">>> Removing all volumes (DATA LOSS)"
docker volume rm $(docker volume ls -q) 2>/dev/null || true

echo ">>> Removing all networks"
docker network rm $(docker network ls -q) 2>/dev/null || true

echo ">>> Final system prune"
docker system prune -a --volumes -f

echo ">>> Verification"
docker ps -a
docker images
docker volume ls
docker network ls

echo ">>> Docker wipe complete"
