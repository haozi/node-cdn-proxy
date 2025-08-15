#!/bin/bash

# 生产环境部署入口
set -e
cd "$(dirname "$0")" || exit 1
cd ..

docker compose -f ./config/docker-compose.prod.yml pull
docker compose -f ./config/docker-compose.prod.yml up --remove-orphans -d
docker image prune -f
