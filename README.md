# WMATA Real-Time Lateness Platform

A medallion-architecture data platform for **WMATA** (Washington Metropolitan Area Transit Authority) transit and traffic data. Bronze objects live in **SeaweedFS** (S3), silver and gold tables live in dedicated **PostgreSQL** clusters, and changes are streamed into **NATS JetStream** through **Debezium** and SeaweedFS filer webhooks.

> **Status:** infrastructure scaffold. Containers, databases, buckets, streams and CDC wiring are in place. dbt models, Dagster assets, dlt pipelines, ML inference and the dashboard are still placeholders — see [Status & Roadmap](#status--roadmap).

---

## Table of Contents

- [Architecture](#architecture)
- [Layer Model](#layer-model)
- [Repository Layout](#repository-layout)
- [Data Flow](#data-flow)
- [NATS JetStream](#nats-jetstream)
- [Debezium CDC](#debezium-cdc)
- [SeaweedFS and the Event Bridge](#seaweedfs-and-the-event-bridge)
- [NATS NUI](#nats-nui)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Setup & Run](#setup--run)
- [Service Ports](#service-ports)
- [Configuration Files](#configuration-files)
- [Smoke Tests](#smoke-tests)
- [Status & Roadmap](#status--roadmap)
- [Known Issues](#known-issues)
- [Troubleshooting](#troubleshooting)

---

## Layer Model

| Layer | Store | Written by | Read by | Databases / buckets | Schema |
|-------|-------|------------|---------|---------------------|--------|
| **Bronze** | SeaweedFS S3 | dlt (planned) | nobody in dbt/Dagster | `wmata`, `traffic` buckets | n/a |
| **Silver** | `postgres-silver` | dbt | Debezium, Dagster | `wmata`, `traffic` | `public` |
| **Gold** | `postgres-gold` | Dagster assets | dashboard / consumers (planned) | `wmata`, `traffic` | `public` |

The infrastructure database (`postgres-infra`) is separate: it holds `dagster_db` and `mlflow_db`.

---

## Repository Layout

```text
.
├── compose.dev.yaml            # All services (dev profile)
├── .env.example                # Environment template
├── dagster/
│   ├── definitions.py          # Definitions + four IO managers (no assets yet)
│   ├── resources/
│   │   └── postgres_io_manager.py
│   ├── etl/Containerfile       # User-code image (dbt, dlt, GX, ML libs)
│   └── daemon/Containerfile    # Webserver + daemon image
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Four targets: silver/gold x wmata/traffic
│   └── models/
│       ├── sources.yml         # Single `silver` source, no fixed database
│       ├── silver/traffic/
│       └── gold/{wmata,traffic}/
├── dlt/
│   └── Containerfile           # Ingestion image only (no pipelines yet)
├── seaweedfs/
│   └── bridge/                 # Filer webhook -> NATS JetStream
│       ├── app.py
│       └── Containerfile
├── configs/
│   ├── postgres/               # init-dbs.sh, init-dbs-infra.sh
│   ├── seaweedfs/              # s3.json, notification.toml
│   ├── debezium/               # wmata.properties, traffic.properties
│   ├── nui/contexts/           # NATS NUI connection contexts
│   ├── dagster/                # dagster.yaml, workspace.yaml
│   ├── vector/                 # vector.yaml
│   ├── mlflow/                 # Containerfile
│   └── dashboard.placeholder/  # Caddyfile (not wired to a service yet)
├── monitor/
│   ├── health_monitor.py
│   └── Containerfile
├── shared/
│   ├── alerts.py
│   ├── logging.py
│   └── retry.py
└── storage/                    # gitignored; created at runtime
```

---

## Data Flow

1. **Ingestion (planned).** dlt pipelines write raw objects into the SeaweedFS buckets.
2. **Bronze events.** The SeaweedFS filer emits a webhook for every create/update/delete under the two bronze buckets. `seaweedfs-bridge` maps the object path to a subject and publishes the raw envelope to JetStream.
3. **Silver.** dbt models build the silver tables in `postgres-silver` (one database per domain).
4. **Silver CDC.** Two Debezium Server instances tail the silver WAL with `pgoutput` and publish change events to the silver streams.
5. **Gold.** Dagster assets read silver and write gold tables in `postgres-gold` through the layer-specific IO managers.
6. **Consumers (planned).** Inference and dashboard services consume the NATS streams.

### Event payloads

- **Bronze:** the raw SeaweedFS envelope

  ```json
  {"key": "/buckets/wmata/...", "event_type": "create", "message_data": {...}}
  ```

- **Silver:** Debezium change events on `<domain>.realtime.silver_data.public.<table>`.
- **Inference:** reserved for the lateness inference service (`wmata.realtime.inference`).

---

## NATS JetStream

`nats-init` creates the streams once and owns their retention; Debezium and the bridge only publish.

| Stream | Subjects | Retention | Max bytes |
|--------|----------|-----------|-----------|
| `wmata-bronze` | `wmata.realtime.bronze_data`, `wmata.realtime.bronze_data.>` | 3 days | 2 GB |
| `traffic-bronze` | `traffic.realtime.bronze_data`, `traffic.realtime.bronze_data.>` | 3 days | 2 GB |
| `wmata-silver` | `wmata.realtime.silver_data`, `wmata.realtime.silver_data.>` | 7 days | 5 GB |
| `traffic-silver` | `traffic.realtime.silver_data`, `traffic.realtime.silver_data.>` | 7 days | 5 GB |
| `wmata-inference` | `wmata.realtime.inference`, `wmata.realtime.inference.>` | 7 days | 1 GB |

All streams use file storage, `limits` retention, `discard old`, and a **2 minute duplicate window**. Subject convention:

```text
<domain>.realtime.<layer>_data[.<schema>.<table>]
```

with `domain ∈ {wmata, traffic}` and `layer ∈ {bronze, silver}`.

---

## Debezium CDC

Debezium Server runs **one connector per instance**, so there are two containers.

| Setting | `debezium-wmata` | `debezium-traffic` |
|---------|------------------|--------------------|
| Source | `postgres-silver:5432` | `postgres-silver:5432` |
| Database | `wmata` | `traffic` |
| Slot | `wmata_silver` | `traffic_silver` |
| Publication | `wmata_silver_pub` | `traffic_silver_pub` |
| Topic prefix | `wmata.realtime.silver_data` | `traffic.realtime.silver_data` |
| Sink | NATS JetStream | NATS JetStream |

Both use `plugin.name=pgoutput`, `schema.include.list=public`, and `create-stream=false`. The sink subject list must contain **both** the bare prefix and `<prefix>.>` because schema-change events are emitted at the bare prefix.

`table.include.list` is still a placeholder, so every table in the `public` schema is currently captured. Debezium tails the Postgres WAL only; it never reads SeaweedFS.

---

## SeaweedFS and the Event Bridge

### Buckets

| Bucket | Purpose | Identity |
|--------|---------|----------|
| `wmata` | Bronze objects for the WMATA domain | `wmata-key-id` |
| `traffic` | Bronze objects for the traffic domain | `traffic-key-id` |
| `mlflow` | MLflow artifacts | `mlflow-key-id` |

Buckets are created by `seaweedfs-init` after the filer is up. Credentials live in `configs/seaweedfs/s3.json` and the S3 gateway listens on `:8333`.

### Notifications

`configs/seaweedfs/notification.toml` configures the filer webhook:

- endpoint `http://seaweedfs-bridge:8080/events`
- event types `create`, `update`, `delete`
- `path_prefixes` limited to `/buckets/wmata/` and `/buckets/traffic/`

SeaweedFS has no native NATS notification target, and the S3 `PutBucketNotificationConfiguration` API is not used. The webhook is the supported path.

### Bridge

`seaweedfs/bridge/app.py` is a small stdlib HTTP server:

- `POST /events` — receives the SeaweedFS envelope, routes by path prefix, publishes to JetStream.
- `GET /health` — liveness probe.

Routing:

```text
/buckets/wmata/...   -> wmata.realtime.bronze_data
/buckets/traffic/... -> traffic.realtime.bronze_data
```

Each publish sets `Nats-Msg-Id` to the SHA-256 of the payload so retries are deduplicated inside the stream's 2 minute window. If NATS is unavailable the bridge returns `503`, which makes SeaweedFS retry with backoff.

---

## NATS NUI

`configs/nui/contexts/` is mounted read-only at `/nui-contexts` and imported at startup via `--nats-cli-contexts`. Imports are persisted in the `/db` volume and are idempotent (matched by import path), so restarting the container does not create duplicates.

| Context file | Connection name | Subject family |
|--------------|-----------------|----------------|
| `wmata-bronze.json` | `wmata-bronze` | `wmata.realtime.bronze_data` |
| `wmata-silver.json` | `wmata-silver` | `wmata.realtime.silver_data` |
| `wmata-inference.json` | `wmata-inference` | `wmata.realtime.inference` |
| `traffic-bronze.json` | `traffic-bronze` | `traffic.realtime.bronze_data` |
| `traffic-silver.json` | `traffic-silver` | `traffic.realtime.silver_data` |

Open <http://localhost:31311> and the connections are already listed.

---

## Tech Stack

| Area | Technology |
|------|------------|
| Orchestration | Dagster (webserver, daemon, gRPC user code) |
| Transformations | dbt-postgres `1.9.0` |
| Ingestion | dlt (`dlt[postgres]`) |
| Databases | PostgreSQL `18.4-alpine3.24` (infra, silver, gold) |
| Object storage | SeaweedFS `4.53` (S3 gateway + filer) |
| Messaging | NATS `2.15.2-alpine3.22` with JetStream |
| CDC | Debezium Server `3.0.0.Final` |
| ML tracking | MLflow `3.14.0` (Postgres backend, SeaweedFS artifacts) |
| Metrics | VictoriaMetrics |
| Logs | VictoriaLogs + Vector |
| Runtime | Python `3.12`, `nats-py==2.16.0` |
| Alerting | Telegram Bot API (optional) |

The Dagster user-code image also installs `great_expectations`, `xgboost`, `scikit-learn`, `pandas`, `numpy`, and `boto3` for the planned modeling work.

---

## Prerequisites

- Docker or Podman with Compose v2 (`docker compose`, not `docker-compose`)
- (Optional) AWS CLI or any S3 client for the smoke tests
- (Optional) Telegram bot token + chat ID for alerts

---

## Setup & Run

### 1. Configure the environment

```bash
cp .env.example .env
```

Environment groups:

| Group | Variables |
|-------|-----------|
| Host binding | `HOST_IP` |
| Databases | `POSTGRES_DB_{INFRA,WMATA,TRAFFIC}`, `POSTGRES_{INFRA,SILVER,GOLD}_{HOST,USER,PASSWORD,PORT,HOST_PORT}` |
| Retention | `VM_RETENTION`, `VL_RETENTION` |
| SeaweedFS | `SEAWEEDFS_S3_ENDPOINT`, `SEAWEEDFS_ACCESS_KEY`, `SEAWEEDFS_SECRET_KEY`, bucket names, per-domain keys |
| Alerts | `TELEGRAM_BOT_API_KEY`, `TELEGRAM_CHAT_ID` |

Inside the Compose network every PostgreSQL cluster listens on **5432**; the host-only ports are `5433` (silver) and `5434` (gold). Do not put the host ports in `POSTGRES_*_PORT`.

### 2. Start services

```bash
docker compose -f compose.dev.yaml --profile dev up -d
```

Startup order is enforced with health and completion checks:

1. `postgres-infra`, `postgres-silver`, `postgres-gold` become healthy.
2. `seaweedfs` becomes healthy, then `seaweedfs-init` creates the buckets.
3. `nats` becomes healthy, then `nats-init` creates the streams.
4. `seaweedfs-bridge` starts after the streams exist.
5. `debezium-wmata` / `debezium-traffic` start after `postgres-silver` is healthy and `nats-init` completes.
6. `mlflow` starts after `postgres-infra` and `seaweedfs-init`.

### 3. Verify

```bash
docker compose -f compose.dev.yaml --profile dev ps
```

Wait until infrastructure services report `healthy` and one-shot services report `exited (0)`.

### 4. dbt

```bash
# Parse for a specific target
docker compose -f compose.dev.yaml --profile dev run --rm dbt parse --target silver_wmata

# Build silver for one domain
docker compose -f compose.dev.yaml --profile dev run --rm dbt run --target silver_wmata
```

Available targets: `silver_wmata` (default), `gold_wmata`, `silver_traffic`, `gold_traffic`.

### 5. Dagster

Open <http://localhost:3000>. `dagster/definitions.py` currently registers four IO managers and **no assets or schedules**.

---

## Service Ports

| Service | Host port | Container port | Notes |
|---------|-----------|----------------|-------|
| `postgres-infra` | `5432` | `5432` | `dagster_db`, `mlflow_db` |
| `postgres-silver` | `5433` | `5432` | `wmata`, `traffic` |
| `postgres-gold` | `5434` | `5432` | `wmata`, `traffic` |
| `seaweedfs` (S3) | `8333` | `8333` | S3 gateway |
| `seaweedfs` (filer) | `8888` | `8888` | Filer HTTP |
| `seaweedfs` (master) | — | `9333` | Internal only |
| `seaweedfs-bridge` | `8081` | `8080` | Webhook receiver |
| `nats` | `4222` | `4222` | Client connections |
| `nats` | `8222` | `8222` | Monitoring HTTP |
| `nats-nui` | `31311` | `31311` | Web GUI |
| `dagster-webserver` | `3000` | `3000` | Dagster UI |
| `mlflow` | `5000` | `5000` | Tracking UI |
| `victoriametrics` | `8428` | `8428` | Metrics HTTP |
| `victorialogs` | `9428` | `9428` | Logs HTTP |
| `debezium-*` | — | `8080` | Health endpoint, internal only |
| `vector` | — | — | No exposed port |

All host ports bind to `${HOST_IP:-127.0.0.1}`.

---

## Configuration Files

| Path | Purpose |
|------|---------|
| `configs/postgres/init-dbs.sh` | Creates `wmata` + `traffic` in each layer cluster |
| `configs/postgres/init-dbs-infra.sh` | Creates `dagster_db` + `mlflow_db` |
| `configs/seaweedfs/s3.json` | S3 identities and credentials |
| `configs/seaweedfs/notification.toml` | Filer webhook target, filters, retry policy |
| `configs/debezium/wmata.properties` | Silver CDC for the `wmata` database |
| `configs/debezium/traffic.properties` | Silver CDC for the `traffic` database |
| `configs/nui/contexts/*.json` | NUI connections imported at startup |
| `configs/dagster/dagster.yaml` | Dagster storage + scheduler |
| `configs/dagster/workspace.yaml` | gRPC user-code location |
| `configs/vector/vector.yaml` | Container log collection |
| `configs/mlflow/Containerfile` | MLflow image with Postgres + S3 drivers |
| `configs/dashboard.placeholder/Caddyfile` | Reverse-proxy plan (no service yet) |

---

## Smoke Tests

```bash
# 1. Streams exist
docker compose -f compose.dev.yaml --profile dev \
  run --rm --entrypoint nats nats-init -s nats:4222 stream ls

# 2. NATS NUI imported the contexts
docker compose -f compose.dev.yaml --profile dev logs nats-nui | grep -i 'imported connection'

# 3. Upload an object and watch the bronze stream
echo '{"test":1}' > /tmp/sample.json
AWS_ACCESS_KEY_ID=wmata-key-id AWS_SECRET_ACCESS_KEY=wmata-secret-key \
  aws --endpoint-url http://localhost:8333 s3 cp /tmp/sample.json s3://wmata/sample.json
docker compose -f compose.dev.yaml --profile dev \
  run --rm --entrypoint nats nats-init -s nats:4222 sub 'wmata.realtime.bronze_data.>'

# 4. Debezium readiness
docker compose -f compose.dev.yaml --profile dev \
  exec debezium-wmata curl -sf http://localhost:8080/q/health/ready

# 5. dbt project parses
docker compose -f compose.dev.yaml --profile dev run --rm dbt parse --target silver_wmata
```

Step 3 requires the AWS CLI (or any S3 client). Deleting the object should also appear on the stream as a `delete` event.

---

## Status & Roadmap

| Area | State |
|------|-------|
| Compose infrastructure (Postgres x3, SeaweedFS, NATS, Debezium x2, MLflow, VictoriaMetrics, VictoriaLogs) | Implemented |
| Bucket bootstrap (`seaweedfs-init`) | Implemented |
| Stream bootstrap (`nats-init`) | Implemented |
| Bronze webhook bridge | Implemented |
| NUI context import | Implemented |
| Silver CDC configuration | Configured; needs real silver tables |
| dbt models | Placeholder (`.gitkeep` only, `sources.yml` has `PLACEHOLDER_TABLE`) |
| Dagster assets, jobs, schedules | Placeholder (`assets=[]`, no schedules) |
| dlt pipelines | Placeholder (image only) |
| ML training / MLflow experiments | Not started |
| Inference service | Commented out in `compose.dev.yaml` |
| Dashboard backend / frontend | Commented out; source not present |
| Great Expectations suites | Library installed, no suites |
| Health monitor | Code present, service commented out |
| Caddy reverse proxy | Config placeholder, no service |

---

## Known Issues

- `dbt/models/silver/wmata/` does not exist yet (only `silver/traffic/`).
- `dbt_project.yml` declares `macro-paths: ["macros"]`, but `dbt/macros/` is missing.
- `dbt/models/sources.yml` still references `PLACEHOLDER_TABLE`.
- Debezium has no `table.include.list`, so all `public` tables are captured; add one before production use.
- A stale local `.env` may still use `wmata-bronze` / `traffic-bronze` as bucket names. `.env.example` (and the compose bootstrap) use `wmata` / `traffic`; treat `.env.example` as authoritative.
- `seaweedfs-init` ends each `weed shell` call with `|| true`, so a failed bucket creation can pass silently. Check the logs and the bucket list.
- The SeaweedFS master UI on `9333` is not published to the host.
- There is no authentication or TLS on NATS, SeaweedFS, or the databases. This stack is for local development only.
- `monitor`, `inference`, `dashboard-*`, `dlt-*`, and `dbt` run only when invoked explicitly; the `dbt` service is a one-shot task, not a long-running service.

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `nats` container restarts or is unhealthy | Use the `-alpine3.22` tag; the scratch image has no shell or `wget` for the healthcheck. |
| SeaweedFS healthcheck fails | `-ip.bind=0.0.0.0` must be set so `127.0.0.1:9333` responds inside the container. |
| `seaweedfs-bridge` returns `503` | NATS is down or the stream does not exist yet; check `nats-init` logs. |
| Debezium cannot write offsets | The dev service runs as `user: "0"` so the `storage/debezium/*` bind mounts are writable. |
| Need to reset CDC state | Stop the stack, remove `storage/debezium/*`, and drop the replication slots/publications in `postgres-silver`. |
| Need a clean slate | `docker compose --profile dev down` removes containers but not bind mounts; delete `storage/` to reset all state. |
| dbt connects to the wrong port | `POSTGRES_*_PORT` must be `5432` inside the network. Only `POSTGRES_*_HOST_PORT` is `5433`/`5434`. |
