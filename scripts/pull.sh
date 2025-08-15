#!/bin/bash
set -e
cd "$(dirname "$0")" || exit 1
cd ..

git pull
docker compose -f ./config/docker-compose.dev.yml pull
docker compose -f ./config/docker-compose.prod.yml pull
docker image prune -f
