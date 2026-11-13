#!/usr/bin/env bash
# Reload the configuration written by 10-spock-conf.sh. The image entrypoint
# started a temporary server for the init scripts; restarting it is enough for
# shared_preload_libraries to take effect.
set -Eeuo pipefail

: "${PGDATA:?PGDATA is not set}"

pg_ctl -D "${PGDATA}" -m fast -w restart
