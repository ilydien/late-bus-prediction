# Standar Observability: Log, Metrik & Healthcheck

Dokumen ini adalah kontrak bersama seluruh tim (`late-bus-prediction`) agar log, metrik, dan healthcheck dari setiap komponen (ingestion, batch/dbt, MLflow/inference, API/serving, infra) seragam dan dapat ditelusuri di **VictoriaMetrics**, **VictoriaLogs**, dan **React Monitoring Dashboard**.

---

## 1. Standar Log JSON (`shared.logging`)

Seluruh service dan pipeline Python **wajib** memakai `shared.logging.setup_json_logging(service)` dan menyertakan `run_id` di setiap eksekusi job, consumer batch, atau request.

### Skema Field JSON

| Field | Wajib? | Tipe | Keterangan |
|---|---|---|---|
| `timestamp` | Wajib | ISO-8601 UTC | Waktu log dibuat (contoh: `2026-11-15T07:30:00.123456+00:00`) |
| `level` | Wajib | String | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `service` | Wajib | String | Nama layanan/komponen (mis. `dlt-wmata`, `dagster-etl`, `inference`, `api`, `health-monitor`) |
| `run_id` | Wajib | String | ID unik per eksekusi job/request (otomatis dari `new_run_id()` atau `run_context()`) |
| `message` | Wajib | String | Pesan singkat dan jelas |
| `domain` | Opsional | String | `wmata`, `traffic`, atau `infra` |
| `layer` | Opsional | String | `bronze`, `silver`, `gold`, `serving`, atau `ops` |
| `job` | Opsional | String | Nama job/asset/endpoint |
| `duration_ms` | Opsional | Number | Durasi eksekusi dalam milidetik |
| `row_count` | Opsional | Integer | Jumlah baris/event yang diproses |
| `error_type` | Opsional | String | Nama kelas exception saat gagal |
| `exception` | Opsional | String | Stack trace jika terjadi exception |

### Contoh Pemakaian di Python

```python
from shared.logging import setup_json_logging, run_context
import logging

setup_json_logging("dlt-wmata")
log = logging.getLogger(__name__)

with run_context("ingest"):
    log.info(
        "Ingested WMATA bus positions",
        extra={"domain": "wmata", "layer": "bronze", "job": "wmata_bus", "row_count": 412},
    )
```

> **Aturan Review:** PR yang menulis log plain-text `print()` atau tanpa `run_id` pada pipeline/API baru harus diperbaiki sebelum merge.

---

## 2. Standar Metrik (`shared.metrics`)

### Konvensi Penamaan & Label

- **Format nama:** `wmata_<component>_<metric>_<unit>` (`snake_case`, satuan eksplisit: `_seconds`, `_bytes`, `_total`, `_timestamp`).
- **Label wajib pada metrik aplikasi:** `service`, `domain`, `layer`.
- **Hindari high-cardinality labels:** Jangan masukkan `run_id`, `vehicle_id`, atau timestamp ke dalam label metrik Prometheus (taruh `run_id` di **log**, bukan di label metrik).

### Metrik Standar Pipeline & Stream

| Nama Metrik | Tipe | Label Wajib | Pemilik |
|---|---|---|---|
| `wmata_pipeline_job_runs_total` | Counter | `service, job, domain, layer, status` | Pemilik job (Orang 2/3) |
| `wmata_pipeline_job_duration_seconds` | Gauge | `service, job, domain, layer` | Pemilik job (Orang 2/3) |
| `wmata_pipeline_job_last_success_timestamp` | Gauge | `service, job, domain, layer` | Pemilik job (Orang 2/3) |
| `wmata_pipeline_rows_processed_total` | Counter | `service, job, domain, layer` | Pemilik job (Orang 2/3) |
| `wmata_stream_last_event_timestamp` | Gauge | `service, domain, stream` | Orang 2 (Streaming) |
| `wmata_api_requests_total` | Counter | `service, endpoint, status_code` | Orang 2 (Serving) |
| `wmata_service_up` | Gauge (`1`/`0`) | `service` | Orang 4 (`health-monitor`) |

### Contoh Pemakaian `track_job`

```python
from shared.metrics import track_job

with track_job(service="dagster-etl", job="silver_wmata_bus", domain="wmata", layer="silver"):
    # Otomatis mengikat run_id, menulis log start/finish/error, dan push metrik ke VictoriaMetrics
    run_etl()
```

---

## 3. Kontrak `/health` dan `healthcheck` Compose

Setiap layanan HTTP baru (`api`, `inference`, `dashboard-backend`, `bridge`, `monitor`) **wajib** menyediakan endpoint:

- `GET /health`
- HTTP `200` saat sehat, `503` saat dependensi kritis gagal.
- Body JSON minimal:

```json
{
  "status": "ok",
  "service": "health-monitor",
  "timestamp": "2026-11-15T07:30:00+00:00",
  "checks": {
    "postgres_infra": "ok",
    "victoriametrics": "ok"
  }
}
```

Setiap layanan long-running di `compose.dev.yaml` **wajib** memiliki blok `healthcheck` (interval `10s–30s`, timeout `3s–10s`, retries `3–15`). Layanan lain yang bergantung padanya wajib memakai `condition: service_healthy`.
