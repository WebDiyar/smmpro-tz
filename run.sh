#!/usr/bin/env bash
# Поднять сервис и прогнать e2e-тесты против настоящего Postgres.
set -euo pipefail
cd "$(dirname "$0")"

# Если 5432 на хосте занят, БД публикуется на другой порт через .env (файл в .gitignore).
[ -f .env ] || echo "DB_PORT=55432" > .env

docker compose up -d --build db migrate api alpha beta
docker compose run --rm tests pytest tests/e2e -v
# docker compose down -v   # раскомментировать, чтобы снести контейнеры и данные
