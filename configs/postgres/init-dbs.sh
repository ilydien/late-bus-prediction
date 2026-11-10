#!/bin/bash
# Runs on first init of an empty data directory. Idempotent: POSTGRES_DB already
# exists, so an unguarded CREATE DATABASE would abort the entrypoint.
set -e

create_db() {
  local name="$1"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -tAc "SELECT 1 FROM pg_database WHERE datname = '${name}'" | grep -q 1 \
    || psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
      -c "CREATE DATABASE \"${name}\""
}

create_db wmata
create_db traffic
