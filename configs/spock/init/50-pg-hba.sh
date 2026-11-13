#!/usr/bin/env bash
# Allow the two Spock nodes (and the one-shot wiring job) to connect from the
# compose network. This is a development stack; do not copy this to production.
set -Eeuo pipefail

: "${PGDATA:?PGDATA is not set}"
: "${POSTGRES_USER:?POSTGRES_USER is not set}"

HBA="${PGDATA}/pg_hba.conf"

append_rule() {
  local rule="$1"
  grep -qF "$rule" "$HBA" || printf '%s\n' "$rule" >>"$HBA"
}

append_rule "host all all 0.0.0.0/0 scram-sha-256"
append_rule "host replication all 0.0.0.0/0 scram-sha-256"

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres \
  -c "SELECT pg_reload_conf();"
