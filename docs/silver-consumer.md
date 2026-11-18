# NATS -> Silver consumer

The `consumer` service turns bronze object events into silver rows
automatically. It is a long-running process, not a one-shot job.

## Flow

```
SeaweedFS object -> seaweedfs-bridge -> NATS JetStream
        -> consumer (durable pull subscriptions)
        -> Spark bronze_to_silver.py (writes *_staging)
        -> merge_silver.py (upsert into real silver tables)
        -> ack NATS message
```

Messages are acked only after Spark **and** the merge succeed. On failure the
messages are nacked and retried.

## Start

The service is part of the `dev` profile:

```bash
docker compose -f compose.dev.yaml --profile dev up -d consumer
```

It also starts with the rest of the stack:

```bash
docker compose -f compose.dev.yaml --profile dev up -d
```

All services in this repository are under the `dev` profile, so a plain
`docker compose up` starts nothing. Use `--profile dev`.

Check health and logs:

```bash
docker compose -f compose.dev.yaml --profile dev ps consumer
docker compose -f compose.dev.yaml --profile dev logs -f consumer
```

## Staging and idempotency

DDL:

- `silver/ddl/002_staging_wmata.sql`
- `silver/ddl/002_staging_traffic.sql`

Spark writes only to:

- `bus_trip_snapshot_staging`
- `route_stats_staging`
- `ddot_aadt_2020_staging`
- `ddot_am_speed_2019_staging`
- `ddot_taz_staging`

`consumer/merge_silver.py` then:

1. reads the target primary key from `pg_index`;
2. upserts staging in chunks with `ON CONFLICT`;
3. deletes the merged staging rows and commits per chunk;
4. truncates staging at the end.

This means:

- the same event can be retried without creating duplicates;
- a new batch does not delete older silver rows;
- a Spark failure leaves messages unacked and retryable;
- a consumer restart keeps its dedup state in the `consumer-state` volume.

The dedup key is `bucket|key|etag`. The state file is
`/work/consumer_state.json`; the heartbeat is `/work/consumer_heartbeat`.

## Configuration

Useful environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `CONSUMER_POLL_INTERVAL` | `5` | idle sleep between pulls |
| `CONSUMER_FETCH_TIMEOUT` | `2` | NATS fetch timeout |
| `CONSUMER_ERROR_BACKOFF` | `30` | retry delay after a failed batch |
| `CONSUMER_BATCH_SIZE` | `200` | max events per Spark batch |
| `WMATA_BRONZE_PREFIXES` | `bronze_` | comma-separated WMATA prefixes |
| `TRAFFIC_BRONZE_PREFIX` | `ddot/` | traffic prefix |
| `SILVER_MERGE_CHUNK` | `20000` | rows per upsert chunk |

The default `bronze_` prefix covers both `bronze_2020/...` and
`bronze_2026/...`.

## HAProxy timeout

Long upserts can take minutes. `silver_rw` and `gold_rw` use a 10-minute
client/server timeout in `configs/haproxy/haproxy.cfg`; the defaults remain
30 seconds for other frontends.

## Known limitations

- Staging tables are not CDC-enabled; only the real silver tables are.
- `route_stats` is recomputed from the static GTFS zip on each WMATA batch.
- The consumer is a batch micro-batcher: Spark starts per batch, so a very
  large backlog should be drained in smaller `CONSUMER_BATCH_SIZE` batches.
- The 2026 source poller is not part of this service; it must place objects in
  the bronze prefixes above.
