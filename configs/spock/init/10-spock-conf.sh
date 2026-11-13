#!/usr/bin/env bash
# Runs from /docker-entrypoint-initdb.d on every Spock node before the
# PostgreSQL server is handed over to the compose healthcheck.
#
# shared_preload_libraries requires a restart, so the next script
# (20-restart.sh) restarts the temporary server started by the image
# entrypoint. This mirrors the official pgEdge compose example.
set -Eeuo pipefail

: "${PGDATA:?PGDATA is not set}"
: "${NODE_ORDINAL:?NODE_ORDINAL is not set}"

PGCONF="${PGDATA}/postgresql.conf"

set_conf() {
  local key="$1"
  local value="$2"
  local pattern
  pattern="^[[:space:]]*$(printf '%s' "$key" | sed 's/\./\\./g')[[:space:]]*="

  if grep -qE "$pattern" "$PGCONF"; then
    sed -i -E "s|${pattern}.*|${key} = ${value}|" "$PGCONF"
  else
    printf '%s = %s\n' "$key" "$value" >>"$PGCONF"
  fi
}

set_conf listen_addresses "'*'"
set_conf wal_level logical
set_conf max_worker_processes 20
set_conf max_replication_slots 32
set_conf max_wal_senders 32
set_conf max_active_replication_origins 32
set_conf track_commit_timestamp on
set_conf shared_preload_libraries "'spock'"

# Spock / pgEdge settings. Custom GUCs with a prefix are accepted even before
# the extension is created in a database.
set_conf spock.enable_ddl_replication on
set_conf spock.include_ddl_repset on
set_conf spock.allow_ddl_from_functions on
set_conf spock.conflict_resolution "'last_update_wins'"
set_conf spock.save_resolutions on

# Snowflake sequences give cluster-wide unique IDs. The extension is created
# per database by 40-extensions.sh.
set_conf snowflake.node "${NODE_ORDINAL}"

# CVE-2026-6471 added output_plugin_libraries; only set it when the server
# actually knows the parameter, otherwise PostgreSQL refuses to start.
if postgres --describe-config 2>/dev/null | grep -q '^output_plugin_libraries'; then
  set_conf output_plugin_libraries "'pgoutput, test_decoding, spock_output'"
fi
