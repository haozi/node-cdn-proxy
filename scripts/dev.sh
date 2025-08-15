#!/bin/bash
set -e
cd "$(dirname "$0")" || exit 1
cd ..

./node_modules/.bin/concurrently \
  "docker compose -f ./config/docker-compose.dev.yml up" \
  "pnpm run start" \
  "sleep 5 && open http://localhost:8787/"
