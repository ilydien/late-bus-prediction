#!/usr/bin/env bash
# One-shot Spock wiring job.
#
# Creates the Spock node definitions on every node, adds existing public tables
# to the `default` replication set, and creates bidirectional subscriptions for
# each database. Safe to run again: every step is guarded by a catalog lookup.
#
# NODES   comma-separated "spock_name:host" entries (2 nodes)
# DATABASES space-separated database names
set -Eeuo pipefail

: "${NODES:?NODES is not set (expected name:host,name:host)}"
: "${DATABASES:?DATABASES is not set}"
: "${ADMIN_USER:?ADMIN_USER is not set}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is not set}"
: "${REPL_USER:?REPL_USER is not set}"
: "${REPL_PASSWORD:?REPL_PASSWORD is not set}"

PSQL_OPTS=(-X -q -v ON_ERROR_STOP=1)

psql_admin() {
  local host="$1"
  local db="$2"
  local sql="$3"
  PGPASSWORD="${ADMIN_PASSWORD}" psql "${PSQL_OPTS[@]}" \
    "host=${host} port=5432 dbname=${db} user=${ADMIN_USER}" -c "${sql}"
}

query_admin() {
  local host="$1"
  local db="$2"
  local sql="$3"
  PGPASSWORD="${ADMIN_PASSWORD}" psql "${PSQL_OPTS[@]}" -tA \
    "host=${host} port=5432 dbname=${db} user=${ADMIN_USER}" -c "${sql}"
}

wait_for_node() {
  local host="$1"
  local attempt

  for attempt in $(seq 1 60); do
    if pg_isready -h "${host}" -p 5432 -U "${ADMIN_USER}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: ${host}:5432 did not become ready" >&2
  return 1
}

# Strip characters that would break a single-quoted SQL string. Passwords in
# this dev stack are simple, but keep the script honest.
escape_sql() {
  printf '%s' "$1" | sed "s/'/''/g"
}

IFS=',' read -r -a NODE_ENTRIES <<<"${NODES}"
read -r -a DB_ENTRIES <<<"${DATABASES}"

for entry in "${NODE_ENTRIES[@]}"; do
  wait_for_node "${entry#*:}"
done

for db in "${DB_ENTRIES[@]}"; do
  echo "=== database: ${db} ==="

  # 1. Node definitions (one per node, per database).
  for entry in "${NODE_ENTRIES[@]}"; do
    node_name="${entry%%:*}"
    host="${entry#*:}"
    dsn="host=${host} port=5432 dbname=${db} user=${REPL_USER} password=${REPL_PASSWORD}"
    dsn="$(escape_sql "${dsn}")"

    exists="$(query_admin "${host}" "${db}" \
      "SELECT count(*) FROM spock.node WHERE node_name = '${node_name}'")"

    if [ "${exists}" = "0" ]; then
      echo "creating node ${node_name} on ${host}/${db}"
      psql_admin "${host}" "${db}" \
        "SELECT spock.node_create(node_name := '${node_name}', dsn := '${dsn}');"
    else
      echo "node ${node_name} already exists on ${host}/${db}"
    fi
  done

  # 2. Existing tables in public. Tables created later are picked up by
  #    spock.include_ddl_repset=on. Tables without an index-based replica
  #    identity are skipped by Spock (see README Known Issues).
  for entry in "${NODE_ENTRIES[@]}"; do
    node_name="${entry%%:*}"
    host="${entry#*:}"

    if ! psql_admin "${host}" "${db}" \
      "SELECT spock.repset_add_all_tables('default', ARRAY['public']);"; then
      echo "WARNING: could not add existing tables to the default set on ${node_name}/${db}" >&2
    fi
  done

  # 3. Bidirectional subscriptions. forward_origins=[] keeps changes local to
  #    the provider and prevents a replication loop.
  for entry in "${NODE_ENTRIES[@]}"; do
    local_name="${entry%%:*}"
    local_host="${entry#*:}"

    for peer in "${NODE_ENTRIES[@]}"; do
      peer_name="${peer%%:*}"
      peer_host="${peer#*:}"

      if [ "${local_name}" = "${peer_name}" ]; then
        continue
      fi

      sub_name="sub_${local_name}_${peer_name}_${db}"
      status_count="$(query_admin "${local_host}" "${db}" \
        "SELECT count(*) FROM spock.sub_show_status('${sub_name}')")"

      if [ "${status_count}" = "0" ]; then
        dsn="host=${peer_host} port=5432 dbname=${db} user=${REPL_USER} password=${REPL_PASSWORD}"
        dsn="$(escape_sql "${dsn}")"
        echo "creating subscription ${sub_name}"
        psql_admin "${local_host}" "${db}" \
          "SELECT spock.sub_create(
             subscription_name := '${sub_name}',
             provider_dsn := '${dsn}',
             replication_sets := ARRAY['default', 'default_insert_only', 'ddl_sql'],
             forward_origins := ARRAY[]::text[]
           );"
      else
        echo "subscription ${sub_name} already exists"
      fi

      # Give the apply worker a moment to reach `replicating`; do not fail the
      # bootstrap if the initial catch-up takes longer.
      for attempt in $(seq 1 30); do
        status="$(query_admin "${local_host}" "${db}" \
          "SELECT status FROM spock.sub_show_status('${sub_name}')" || true)"
        if [ "${status}" = "replicating" ]; then
          break
        fi
        sleep 2
      done
      echo "subscription ${sub_name} status: ${status:-unknown}"
    done
  done
done

echo "Spock wiring complete."
