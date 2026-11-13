#!/usr/bin/env bash
# Create the two domain databases and the replication role used in Spock DSNs.
set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is not set}"
: "${POSTGRES_DB:?POSTGRES_DB is not set}"
: "${POSTGRES_DB_TRAFFIC:?POSTGRES_DB_TRAFFIC is not set}"
: "${PGEDGE_USER:?PGEDGE_USER is not set}"
: "${PGEDGE_PASSWORD:?PGEDGE_PASSWORD is not set}"

create_db() {
  local name="$1"

  if ! psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres \
    -tAc "SELECT 1 FROM pg_database WHERE datname = '${name}'" | grep -q 1; then
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres \
      -c "CREATE DATABASE \"${name}\""
  fi
}

create_db "${POSTGRES_DB}"
create_db "${POSTGRES_DB_TRAFFIC}"

# Spock subscriptions connect with this role. It needs REPLICATION; the
# administrative functions are executed by POSTGRES_USER (superuser).
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres \
  -v repl_user="${PGEDGE_USER}" \
  -v repl_password="${PGEDGE_PASSWORD}" <<'SQL'
SELECT format(
         'CREATE ROLE %I LOGIN REPLICATION PASSWORD %L',
         :'repl_user', :'repl_password'
       )
WHERE NOT EXISTS (
  SELECT 1 FROM pg_roles WHERE rolname = :'repl_user'
)
\gexec

-- The apply worker reads every replicated table through this role. PG 14+
-- grants read access to all tables without making the role a superuser.
SELECT format('GRANT pg_read_all_data TO %I', :'repl_user')
\gexec
SQL
