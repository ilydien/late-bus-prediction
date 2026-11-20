#!/usr/bin/env bash
# Enable Spock (and Snowflake sequences) in both domain databases.
set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is not set}"
: "${POSTGRES_DB:?POSTGRES_DB is not set}"
: "${POSTGRES_DB_TRAFFIC:?POSTGRES_DB_TRAFFIC is not set}"
: "${PGEDGE_USER:?PGEDGE_USER is not set}"

for db in "${POSTGRES_DB}" "${POSTGRES_DB_TRAFFIC}"; do
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" \
    -c "CREATE EXTENSION IF NOT EXISTS spock;"

  # The apply worker connects to its provider as PGEDGE_USER and calls Spock
  # control functions there (pause/resume_apply_workers). Extension functions
  # are owned by POSTGRES_USER and are not granted to PUBLIC by default.
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" \
    -v repl_user="${PGEDGE_USER}" <<'SQL'
GRANT USAGE ON SCHEMA spock TO :"repl_user";
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA spock TO :"repl_user";
SQL

  # Snowflake is optional: sequences are only needed if a model uses serial
  # columns. Do not fail the bootstrap if the image ships without it.
  psql --username "${POSTGRES_USER}" --dbname "${db}" \
    -c "CREATE EXTENSION IF NOT EXISTS snowflake;" \
    || echo "snowflake extension unavailable in ${db}; using plain sequences"
done
