# Batch & ML Platform — Implementation and Handoff

Dokumen ini merangkum perubahan batch + ML yang sudah dibuat, status otomatisasi,
cara menjalankan, serta arahan untuk tim **stream processing** dan **dashboard**.

Repository branch:

```text
feat/batch-ml
```

Commit terakhir:

```text
85c488c docs(batch): add runbook, test report, and generated-file ignores
```

---

## 1. Ringkasan arsitektur

```text
WMATA public archive 2020
        │ dlt
        ▼
SeaweedFS bronze (JSONL.GZ)
        │ filer webhook
        ▼
NATS JetStream
        │ consumer
        ▼
Spark micro-batch
        ▼
PostgreSQL silver
        │ Spark incremental
        ▼
PostgreSQL gold
        ├── quality gate (SQL + Great Expectations)
        ├── Parquet snapshot (SeaweedFS)
        ├── MLflow training
        └── FastAPI serving
```

Layer:

| Layer | Teknologi | Lokasi |
|---|---|---|
| Bronze | SeaweedFS S3 | `s3://wmata/bronze_2020/` |
| Silver | PostgreSQL + Spock | `wmata`, `traffic` |
| Gold | PostgreSQL + Spock | `wmata` |
| Analytical copy | Parquet + Snappy | `s3://wmata/gold_parquet/` |
| ML | LightGBM + MLflow | MLflow + `s3://mlflow/` |
| Serving | FastAPI | `:18000` |
| Orchestration | Dagster | `:3000` |

---

## 2. Yang sudah dibuat

### 2.1 Ingest bronze

File:

```text
dlt/ingest_wmata_2020.py
dlt/Containerfile
dagster/assets/ingest.py
```

Fitur:

- download arsip WMATA 2020 harian
- ekstrak dan normalisasi timestamp
- tulis ke `s3://wmata/bronze_2020/bus_positions/`
- state persisten di volume `dlt-state`
- skip tanggal yang sudah ada di bronze
- otomatis jalan sekali saat `docker compose --profile dev up -d`
- Dagster asset `bronze_ingest` untuk manual/scheduled

Sumber:

```text
http://busdata-00-us-west-2.s3.amazonaws.com/<date>.tar.gz
```

Batas:

- hanya 2020
- arsip 2025 tidak di-ingest
- live 2026 belum ada poller

### 2.2 Streaming bronze → NATS → silver

File:

```text
consumer/bronze_consumer.py
consumer/merge_silver.py
consumer/Containerfile
spark/jobs/bronze_to_silver.py
silver/ddl/001_silver_wmata.sql
silver/ddl/001_silver_traffic.sql
silver/ddl/002_staging_wmata.sql
silver/ddl/002_staging_traffic.sql
configs/haproxy/haproxy.cfg
```

Fitur:

- `seaweedfs-bridge` publish event ke NATS
- consumer long-running
- durable pull subscription
- dedup `bucket|key|etag`
- state di volume `consumer-state`
- ack hanya setelah Spark + merge sukses
- Spark menulis ke `*_staging`
- `merge_silver.py` upsert `ON CONFLICT` per chunk
- HAProxy RW timeout dinaikkan ke 10 menit

Tabel silver:

```text
wmata.bus_trip_snapshot
wmata.route_stats
traffic.ddot_aadt_2020
traffic.ddot_am_speed_2019
traffic.ddot_taz
```

Batas:

- delete/tombstone belum dipropagasikan
- DLQ belum ada
- allowed lateness baru didokumentasikan
- Spark bukan Structured Streaming; ini micro-batch per event batch

### 2.3 Gold

File:

```text
spark/jobs/build_gold.py
spark/jobs/export_gold_parquet.py
spark/jobs/verify_parquet.py
dagster/assets/parquet_export.py
```

Tabel:

```text
wmata.trip_delay_features
wmata.route_hourly_delay
wmata.route_daily_reliability
wmata.route_ddot_features
wmata.model_predictions
wmata.model_performance_daily
wmata.training_watermark
wmata.trip_delay_features_staging
```

Fitur:

- incremental watermark
- overlap 20 menit
- staging + upsert
- backfill range `--start-ts` / `--end-ts`
- Parquet snapshot:

```text
s3://wmata/gold_parquet/<table>/version=<run_id>/
```

- manifest:

```text
s3://wmata/gold_parquet/_manifest/<run_id>.json
s3://wmata/gold_parquet/_latest.json
```

- retention 7 snapshot
- Parquet partition:
  - `service_date`
  - `model_version`

Batas:

- marts masih full rebuild; incremental hanya `trip_delay_features`
- Parquet immutable, bukan Iceberg/Delta
- Trino belum punya katalog Parquet/S3

### 2.4 Data quality

File:

```text
quality/checks.py
quality/gx_gate.py
gold/ddl/004_quality.sql
dagster/assets/quality_audit.py
docs/data_quality.md
docs/great_expectations.md
```

Custom checks:

- silver row count
- null keys
- duplicate PK
- deviation/speed/progress range
- freshness
- route coverage
- DDOT coverage
- gold target null/range
- feature null ratio
- model version
- inference error ratio
- marts row count

Great Expectations:

```text
great_expectations 1.25.0
```

GX staging gate berjalan otomatis di `gold_trip_delay_features`:

```text
Spark staging
→ GX staging
→ custom staging
→ finalize gold
→ custom gold
```

Daily audit:

```text
data_quality_audit
data_quality_audit_job
data_quality_audit_schedule
05:00 America/New_York
RUNNING
```

Output:

```text
public.data_quality_runs
quality/reports/latest.json
quality/reports/gx_latest.json
gx_quality/gx/uncommitted/data_docs/local_site/index.html
public.pipeline_runs
```

Hasil terakhir:

```text
checks: 32
pass: 31
warn: 1
fail: 0
gx_success: true
```

Warning freshness karena data masih 2020.

Batas:

- quarantine row-level belum ada
- alert DQ belum terhubung ke monitor
- GX gold baru berjalan di audit, bukan di setiap gold build

### 2.5 Governance / lineage

File:

```text
governance/run_log.py
gold/ddl/005_pipeline_runs.sql
docs/batch_contract.md
docs/data_catalog.md
docs/data_dictionary.md
docs/lineage.json
docs/streaming_handoff.md
```

Tabel:

```text
public.pipeline_runs
```

Kolom:

```text
run_id, parent_run_id, job_name, layer, status,
started_at, finished_at, input_datasets, output_datasets,
source_manifest, row_counts, git_commit, error
```

Terintegrasi ke:

- consumer
- gold asset
- Parquet export
- data quality audit
- training
- batch inference
- manual backfill

### 2.6 ML

File:

```text
ml/train_trip_delay.py
ml/promote_model.py
ml/batch_inference.py
ml/Containerfile
dagster/assets/inference.py
gold/ddl/006_inference.sql
docs/f11_report.md
```

Model:

```text
trip_delay_lgbm
```

Versi:

| Versi | MAE | RMSE | R² | F1 |
|---|---:|---:|---:|---:|
| v1/M2 | 2.18757 | 3.37939 | 0.802143 | 0.75049 |
| v2/M3 | 2.18609 | 3.37950 | 0.802130 | 0.75154 |
| v3 | 2.18349 | 3.37065 | 0.800909 | 0.75202 |

Champion tetap v1; v3 eksperimen.

Fitur training:

- target delay trip 14–16 menit ke depan
- 22 fitur
- rolling 12 minggu, minimum 4 minggu
- test 7 hari, valid 7 hari
- MLflow tags:
  - `git_commit`
  - `dataset_hash`
  - `feature_hash`
  - `dataset_version`
- artifact:
  - `feature_contract.json`
  - `route_categories.json`

Batch inference:

```text
model alias champion
→ gold.trip_delay_features
→ model_predictions
```

Kolom status:

```text
status success/error
error
inference_run_id
```

Batas:

- production inference nanti stream processing
- batch inference hanya offline/backfill
- training belum dijadwalkan karena menunggu live 2026

### 2.7 Serving API

File:

```text
serving/api/Containerfile
serving/api/main.py
gold/ddl/007_serving.sql
docs/serving_api.md
docs/serving_queries.md
```

Service:

```text
serving-api
http://127.0.0.1:18000
http://127.0.0.1:18000/docs
```

Endpoint:

```text
GET /health
GET /v1/freshness
GET /v1/routes/delay
GET /v1/reliability/daily
GET /v1/routes/hourly
GET /v1/predictions/latest
GET /v1/model/performance
```

Status code:

```text
200 success
400 start_date > end_date
422 invalid date/limit
503 DB unavailable
```

API membaca gold read-only:

```text
haproxy:5436
```

### 2.8 Tests

```text
30 tests
coverage 42%
```

Test files:

```text
tests/test_quality_checks.py
tests/test_quality_integration.py
tests/test_run_log.py
tests/test_run_log_db.py
tests/test_batch_inference.py
tests/test_train_helpers.py
tests/test_promote_gate.py
tests/test_consumer_helpers.py
tests/test_serving_api.py
```

API tests: 6 pass.

GX tests:

- success staging
- success gold
- failure staging

---

## 3. Data saat ini

Angka snapshot saat pengujian terakhir:

| Tabel | Rows |
|---|---:|
| `wmata.bus_trip_snapshot` | 18.206.575 |
| `wmata.route_stats` | 250 |
| `traffic.ddot_aadt_2020` | 7.734 |
| `traffic.ddot_am_speed_2019` | 7.292 |
| `traffic.ddot_taz` | 393 |
| `wmata.trip_delay_features` | 2.565.962 |
| `wmata.route_hourly_delay` | 107.279 |
| `wmata.route_daily_reliability` | 7.566 |
| `wmata.route_ddot_features` | 146 |
| `wmata.model_predictions` | 1.589.232 |

Parquet snapshot terakhir:

```text
version=7ae9cdca-adbb-4d0c-8a21-464064f031a0
```

`_latest.json` menunjuk snapshot terbaru.

---

## 4. Schedules

| Schedule | Cron | Status | Fungsi |
|---|---|---|---|
| `parquet_export_job_schedule` | `30 3 * * *` | **RUNNING** | gold + Parquet |
| `data_quality_audit_schedule` | `0 5 * * *` | **RUNNING** | quality audit |
| `batch_ingest_job_schedule` | `0 1 * * *` | STOPPED | ingest dlt |
| `batch_inference_job_schedule` | `0 4 * * *` | STOPPED | batch inference |
| `weekly_retrain_job_schedule` | `0 2 * * 1` | STOPPED | gold + training + promotion |

`batch_refresh_job_schedule` sudah dihapus karena redundan.

---

## 5. Cara menjalankan

### 5.1 Prasyarat

- Docker Engine + Docker Compose v2
- minimal RAM 16 GB untuk seluruh stack
- repo berada di filesystem POSIX
- `.env` sudah diisi
- port host tidak bentrok

Cek versi:

```bash
docker --version
docker compose version
```

### 5.2 Siapkan `.env`

```bash
cp .env.example .env
```

Sesuaikan minimal:

```text
HOST_IP
POSTGRES_*_PASSWORD
SEAWEEDFS_ACCESS_KEY
SEAWEEDFS_SECRET_KEY
WMATA_KEY_ID
WMATA_SECRET_KEY
MLFLOW_KEY_ID
MLFLOW_SECRET_KEY
```

Jangan commit `.env`.

Validasi compose:

```bash
docker compose -f compose.dev.yaml --profile dev config >/dev/null
```

### 5.3 Build image

Build service compose:

```bash
docker compose -f compose.dev.yaml --profile dev build
```

Build image batch/ML khusus:

```bash
docker build -t wmata-spark-gold -f spark/gold/Containerfile spark
docker build -t wmata-ml -f ml/Containerfile ml
docker build -t wmata-lateness-dlt -f dlt/Containerfile dlt
```

### 5.4 Start stack

```bash
docker compose -f compose.dev.yaml --profile dev up -d
```

Penting: gunakan `--profile dev`. Tanpa profile itu, semua service punya
`profiles: ["dev"]`, sehingga tidak ada yang start.

Saat start:

```text
infra
→ dlt ingest 2020 sekali
→ bridge
→ consumer
→ NATS → silver
```

`dlt` adalah one-shot. Container akan exit 0 setelah selesai:

```bash
docker compose -f compose.dev.yaml --profile dev ps -a dlt
```

Log:

```bash
docker compose -f compose.dev.yaml --profile dev logs -f dlt
docker compose -f compose.dev.yaml --profile dev logs -f consumer
```

Pada environment baru, dlt akan mengunduh sekitar 971 MB data 2020.
Pada environment yang sudah punya bronze, dlt akan:

```text
SKIP existing
```

### 5.5 Apply DDL

Compose **tidak** otomatis apply DDL. Pada clone bersih, apply setelah database
healthy.

Silver `wmata`:

```bash
set -a; . ./.env; set +a
for f in silver/ddl/001_silver_wmata.sql silver/ddl/002_staging_wmata.sql; do
  docker run --rm --network wmata-lateness_wmata-lateness-dev \
    -e PGPASSWORD="$POSTGRES_SILVER_PASSWORD" \
    -v "$PWD/silver/ddl:/ddl:ro" postgres:18.4-alpine3.24 \
    psql -h haproxy -p 5433 -U postgres -d wmata -v ON_ERROR_STOP=1 \
    -f "/ddl/$(basename "$f")"
done
```

Silver `traffic`:

```bash
for f in silver/ddl/001_silver_traffic.sql silver/ddl/002_staging_traffic.sql; do
  docker run --rm --network wmata-lateness_wmata-lateness-dev \
    -e PGPASSWORD="$POSTGRES_SILVER_PASSWORD" \
    -v "$PWD/silver/ddl:/ddl:ro" postgres:18.4-alpine3.24 \
    psql -h haproxy -p 5433 -U postgres -d traffic -v ON_ERROR_STOP=1 \
    -f "/ddl/$(basename "$f")"
done
```

Gold `wmata`:

```bash
for f in gold/ddl/*.sql; do
  docker run --rm --network wmata-lateness_wmata-lateness-dev \
    -e PGPASSWORD="$POSTGRES_GOLD_PASSWORD" \
    -v "$PWD/gold/ddl:/ddl:ro" postgres:18.4-alpine3.24 \
    psql -h haproxy -p 5434 -U postgres -d wmata -v ON_ERROR_STOP=1 \
    -f "/ddl/$(basename "$f")"
done
```

Kalau consumer sudah jalan sebelum DDL selesai, consumer akan retry karena
pesan tidak di-ack sampai Spark + merge sukses. Setelah DDL tersedia, retry
akan berhasil.

### 5.6 Verifikasi

Service:

```bash
docker compose -f compose.dev.yaml --profile dev ps
```

Health:

```bash
curl http://127.0.0.1:18000/health
curl http://127.0.0.1:18000/v1/freshness
```

Dagster:

```text
http://127.0.0.1:3000
```

MLflow:

```text
http://127.0.0.1:15000
```

Serving API docs:

```text
http://127.0.0.1:18000/docs
```

NATS monitoring:

```text
http://127.0.0.1:8222/jsz?consumers=true
```

### 5.7 Schedule

Default setelah start:

```text
parquet_export_job_schedule   03:30 ET   RUNNING
data_quality_audit_schedule   05:00 ET   RUNNING
batch_ingest_job_schedule     01:00 ET   STOPPED
batch_inference_job_schedule  04:00 ET   STOPPED
weekly_retrain_job_schedule   Senin 02:00 ET STOPPED
```

Aktifkan/stop lewat Dagster UI:

```text
http://127.0.0.1:3000
```

atau GraphQL/CLI.

### 5.8 Logs dan stop

Log service:

```bash
docker compose -f compose.dev.yaml --profile dev logs -f consumer
docker compose -f compose.dev.yaml --profile dev logs -f serving-api
docker compose -f compose.dev.yaml --profile dev logs -f dagster-daemon
```

Stop tanpa menghapus data:

```bash
docker compose -f compose.dev.yaml --profile dev down
```

Jangan gunakan `down -v` kecuali ingin menghapus volume database:

```bash
# DESTRUKTIF untuk volume database
docker compose -f compose.dev.yaml --profile dev down -v
```

`storage/` adalah bind mount terpisah. `down -v` tidak otomatis menghapus
`storage/`, tetapi menghapus named volume seperti data Spock.

### 5.9 Port penting

| Service | Port |
|---|---:|
| Dagster | `3000` |
| MLflow | `15000` |
| Serving API | `18000` |
| SeaweedFS S3 | `8333` |
| SeaweedFS Filer | `8888` |
| NATS | `4222` |
| NATS monitoring | `8222` |
| Bridge | `8081` |
| Spark Master UI | `8082` |
| Spark Worker UI | `8083` |
| HAProxy stats | `18404` |
| Silver RW | `15433` |
| Gold RW | `15434` |
| Silver RO | `15435` |
| Gold RO | `15436` |

---

## 6. Arahan untuk tim stream processing

### 6.1 Mulai dari dokumen ini

```text
docs/streaming_handoff.md
docs/batch_contract.md
docs/data_dictionary.md
```

### 6.2 Yang sudah tersedia

- `consumer` long-running
- NATS stream `wmata-bronze`, `traffic-bronze`
- durable consumer `silver-wmata`, `silver-traffic`
- dedup dan ack
- staging + upsert silver
- `pipeline_runs`
- sample manifest

### 6.3 Yang perlu dikerjakan

1. **Live 2026 poller/producer**
   - ambil data live WMATA
   - tulis ke bronze
   - publish event dengan `event_id`, `event_time`, `ingest_time`, `payload_version`

2. **DLQ**
   - event gagal berulang masuk DLQ
   - replay tool

3. **Allowed lateness / watermark**
   - definisikan policy
   - update `batch_contract.md`

4. **Replay**
   - replay range/event
   - jangan mengubah schema tanpa koordinasi

5. **Stream inference**
   - konsumsi event
   - load `models:/trip_delay_lgbm@champion`
   - tulis `model_predictions`
   - gunakan `inference_run_id`, `status`, `error`

6. **Metrics dan alert**
   - lag/offset
   - throughput
   - consumer restart
   - freshness

### 6.4 Batasan penting

- Jangan ubah schema silver tanpa update `batch_contract.md`.
- Jangan ack sebelum Spark + merge sukses.
- Jangan menghapus dedup state.
- `batch_inference` tetap offline; inference produksi lewat stream.
- Stream sekarang idle karena tidak ada data 2026.

### 6.5 Handoff file

```text
consumer/
silver/ddl/
spark/jobs/bronze_to_silver.py
tests/fixtures/bronze_manifest_sample.json
compose.dev.yaml (consumer, nats, seaweedfs-bridge)
```

---

## 7. Arahan untuk tim dashboard

### 7.1 Gunakan API

```text
http://127.0.0.1:18000
http://127.0.0.1:18000/docs
```

Jangan query PostgreSQL langsung untuk dashboard; gunakan API read-only.

### 7.2 Endpoint

```text
/v1/routes/delay
/v1/reliability/daily
/v1/routes/hourly
/v1/predictions/latest
/v1/model/performance
/v1/freshness
```

### 7.3 Minimal dashboard

Empat metrik:

1. on-time percentage
2. average delay per route
3. reliability harian
4. latest prediction / insight ML

Filter:

1. route
2. date range

Tambahan:

- freshness
- model version
- status inference

### 7.4 Dokumentasi

```text
docs/serving_api.md
docs/serving_queries.md
```

### 7.5 Yang belum ada

- dashboard frontend
- auth/RBAC
- DQ alert dashboard
- API rate limit
- API audit log

---

## 8. Status F-01–F-20

| Req | Status |
|---|---|
| F-01 Deployment | 🟡 |
| F-02 Batch & schedule | 🟡 |
| F-03 Incremental/retry/backfill | ✅ |
| F-04 Stream processing | ✅ |
| F-05 Event-time/DLQ | 🟡 |
| F-06 Medallion | ✅ |
| F-07 Storage/Parquet | ✅ |
| F-08 Quality gate | ✅ |
| F-09 Metadata/lineage | ✅ |
| F-10 Serving API | ✅ |
| F-11 ML versioning | ✅ |
| F-12 Inference | ✅ |
| F-13 Dashboard | ❌ |
| F-14 Metrics/logs | 🟡 |
| F-15 Alerts | ❌ |
| F-16 Security | ❌ |
| F-17 Backup/restore | ❌ |
| F-18 Tests | ✅ |
| F-19 Benchmark | ❌ |
| F-20 Demo/report | 🟡 |

Ringkasan:

```text
✅ 10
🟡 5
❌ 5
```

---

## 9. Next steps

### Batch

1. **F-19 benchmark**
   - Spark silver
   - merge
   - gold
   - Parquet
   - inference
2. **F-17 backup/restore**
3. **F-15 alert**
4. **F-01 resource budget/ADR**
5. **F-14 job metrics**

### Streaming

1. live 2026 poller
2. DLQ
3. late policy
4. stream inference
5. lag/alert

### Dashboard

1. API client
2. 4 KPI
3. filter route/date
4. freshness
5. ML insight

---

## 10. Known limitations

- Data aktif masih 2020.
- Live 2026 belum ada.
- Arsip 2025 tidak di-ingest.
- Batch inference bukan production inference.
- Weekly retrain belum aktif.
- Belum ada dashboard.
- Belum ada alert.
- Belum ada backup/restore.
- Belum ada auth/RBAC.
- Belum ada benchmark.
- GX context tidak boleh di-commit karena berisi connection string.
- `quality/reports` hanya menyimpan `latest.json` dan `gx_latest.json`.
- `gx_quality/gx/` di-ignore.
- `data/`, `storage/`, `.env`, `.coverage` di-ignore.
