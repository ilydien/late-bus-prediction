#!/usr/bin/env bash
# Enable Spock (and Snowflake sequences) in both domain databases.
set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is not set}"
: "${POSTGRES_DB:?POSTGRES_DB is not set}"
: "${POSTGRES_DB_TRAFFIC:?POSTGRES_DB_TRAFFIC is not set}"

for db in "${POSTGRES_DB}" "${POSTGRES_DB_TRAFFIC}"; do
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" \
    -c "CREATE EXTENSION IF NOT EXISTS spock;"

  # Snowflake is optional: sequences are only needed if a model uses serial
  # columns. Do not fail the bootstrap if the image ships without it.
  psql --username "${POSTGRES_USER}" --dbname "${db}" \
    -c "CREATE EXTENSION IF NOT EXISTS snowflake;" \
    || echo "snowflake extension unavailable in ${db}; using plain sequences"
done
