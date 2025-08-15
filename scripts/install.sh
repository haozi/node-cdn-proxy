#!/bin/bash
set -e
cd "$(dirname "$0")" || exit 1
cd ..
root=$(pwd)
cd ./Node.js/ && pnpm install && cd -

cd $root
rm -rf ./pnpm-lock.yaml
# 判断是否安装了 docker
if ! which docker &> /dev/null; then
  echo "Docker is not installed. Skipping Docker-related operations."
else
  docker compose -f ./config/docker-compose.dev.yml down
  docker compose -f ./config/docker-compose.dev.yml pull

  docker compose -f ./config/docker-compose.prod.yml down
  docker compose -f ./config/docker-compose.prod.yml pull
  docker image prune -f
fi

cd $root
