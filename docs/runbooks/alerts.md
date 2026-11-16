# Runbook Alerting Platform (`late-bus-prediction`)

Panduan singkat penanganan 3 kategori alert utama yang dievaluasi oleh **vmalert** dan dikirim ke Telegram melalui **monitor** (`http://monitor:8090/api/v2/alerts`).

---

## Alert A: `PipelineJobFailed`

- **Severity:** `CRITICAL`
- **Owner:** Pemilik job terkait (Orang 3 untuk Batch/ML, Orang 2 untuk Stream)
- **Gejala:** Metrik `wmata_pipeline_job_runs_total{status="failed"}` meningkat dalam 5 menit terakhir.

### Langkah Cek
1. Buka **VictoriaMetrics VMUI** (`http://localhost:8428/vmui`), **React Monitoring Dashboard** (`http://localhost:5173`), atau **Dagster UI** (`http://localhost:3000`).
2. Buka **VictoriaLogs VMUI** (`http://localhost:9428/select/vmui`) dan query:
   ```logsql
   level:ERROR job:<nama_job>
   ```
3. Ambil nilai `run_id` dari log error tersebut, lalu telusuri seluruh log terkait:
   ```logsql
   run_id:<run_id>
   ```

### Perbaikan
- Periksa konektivitas database target (`haproxy:5433` silver / `haproxy:5434` gold) atau SeaweedFS (`seaweedfs:8333`).
- Jika kegagalan disebabkan data input korup atau transient API error, periksa payload/DLQ lalu jalankan ulang job/backfill yang bersifat idempoten.

---

## Alert B: `StreamLagOrStale`

- **Severity:** `WARNING`
- **Owner:** Orang 2 (Streaming)
- **Gejala:** `jetstream_consumer_num_pending > 1000` selama 3 menit atau umur event terakhir `wmata_stream_last_event_timestamp` melewati 5 menit.

### Langkah Cek
1. Cek metrik `jetstream_consumer_num_pending` & `jetstream_stream_total_messages` di **VictoriaMetrics VMUI** (`http://localhost:8428/vmui`) / **React Monitoring Dashboard**, atau buka **NATS NUI** (`http://localhost:31311`).
2. Periksa status consumer dan stream di terminal:
   ```bash
   docker compose -f compose.dev.yaml --profile dev run --rm --entrypoint nats nats-init -s nats:4222 stream ls
   ```
3. Cek log consumer / collector / `seaweedfs-bridge` di VictoriaLogs (`service:seaweedfs-bridge OR service:debezium-wmata`).

### Perbaikan
- Pastikan container consumer atau Debezium masih berjalan (`docker compose -f compose.dev.yaml --profile dev ps`).
- Jika consumer crash, restart container consumer dan verifikasi pending message turun kembali hingga alert berstatus `RESOLVED`.

---

## Alert C: `ServiceDown` / `ResourceHigh`

- **Severity:** `CRITICAL`
- **Owner:** Orang 4 (Platform & Ops)
- **Gejala:** `wmata_service_up == 0` selama 1 menit, RAM/disk host > 85%, atau `vm_free_disk_space_bytes < 1 GB`.

### Langkah Cek
1. Cek status container dan healthcheck:
   ```bash
   docker compose -f compose.dev.yaml --profile dev ps
   curl -s http://localhost:8090/health | python3 -m json.tool
   ```
2. Periksa log container bermasalah:
   ```bash
   docker compose -f compose.dev.yaml --profile dev logs --tail=100 <service_name>
   ```
3. Cek pemakaian CPU/RAM/Disk container di **VictoriaMetrics VMUI** (`container_cpu_usage_seconds_total`, `container_memory_working_set_bytes`) atau **React Monitoring Dashboard**.

### Perbaikan
- Untuk service down: perbaiki penyebab di log lalu jalankan `docker compose -f compose.dev.yaml --profile dev up -d <service_name>`.
- Untuk disk/RAM penuh: bersihkan container/image tidak terpakai (`docker system prune`) atau naikkan retensi/limit volume. Pastikan alert mengirim notifikasi `RESOLVED` setelah pulih.
