# Runbook Alerting Platform (`late-bus-prediction`)

Panduan singkat penanganan 5 kategori alert utama yang dievaluasi oleh **vmalert** dan dikirim ke Telegram melalui **monitor** (`http://monitor:8090/api/v2/alerts`).

### Uji Cepat Relay Telegram
Setelah mengisi `TELEGRAM_BOT_API_KEY` dan `TELEGRAM_CHAT_ID` di `.env` dan me-restart `monitor`, kirim uji alert langsung:
```bash
curl -s -X POST http://localhost:8090/api/monitoring/test-alert \
  -H 'Content-Type: application/json' \
  -d '{"status":"FIRING","level":"INFO","message":"Smoke test Telegram alert"}'
```

---

## Alert A: `PipelineJobFailed`

- **Severity:** `CRITICAL`
- **Owner:** Pemilik job terkait (Batch/ML atau Streaming)
- **Gejala:** Metrik `wmata_pipeline_job_runs_total{status="failed"}` meningkat dalam 5 menit terakhir.

### Langkah Cek
1. Buka **MetroPulse DC → Pipeline Health** (`http://localhost:18000/pipeline`), **VictoriaMetrics VMUI** (`http://localhost:8428/vmui`), atau **Dagster UI** (`http://localhost:3000`).
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
- **Owner:** Streaming
- **Gejala:** `jetstream_consumer_num_pending > 1000` selama 3 menit atau umur event terakhir `wmata_stream_last_event_timestamp` melewati 5 menit.

### Langkah Cek
1. Cek metrik `jetstream_consumer_num_pending` & `jetstream_stream_total_messages` di **MetroPulse DC (`/pipeline`)**, **VictoriaMetrics VMUI** (`http://localhost:8428/vmui`), atau buka **NATS NUI** (`http://localhost:31311`).
2. Periksa status consumer dan stream di terminal:
   ```bash
   docker compose -f compose.dev.yaml --profile dev run --rm --entrypoint nats nats-init -s nats:4222 stream ls
   ```
3. Cek log consumer / collector / `seaweedfs-bridge` di VictoriaLogs (`service:seaweedfs-bridge OR service:bronze-consumer OR service:debezium-wmata`).

### Perbaikan
- Pastikan container `consumer`, `wmata-poller`, `tomtom-poller`, atau `debezium-*` masih berjalan (`docker compose -f compose.dev.yaml --profile dev ps`).
- Jika consumer crash, restart container `consumer` dan verifikasi pending message turun kembali hingga alert berstatus `RESOLVED`.

---

## Alert C: `ServiceDown` / `ResourceHigh`

- **Severity:** `CRITICAL`
- **Owner:** Platform & Ops
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
3. Cek pemakaian CPU/RAM/Disk container di **VictoriaMetrics VMUI** (`container_cpu_usage_seconds_total`, `container_memory_working_set_bytes`) atau **MetroPulse DC (`/pipeline`)**.

### Perbaikan
- Untuk service down: perbaiki penyebab di log lalu jalankan `docker compose -f compose.dev.yaml --profile dev up -d <service_name>`.
- Untuk disk/RAM penuh: bersihkan container/image tidak terpakai (`docker system prune`) atau naikkan retensi/limit volume. Pastikan alert mengirim notifikasi `RESOLVED` setelah pulih.

---

## Alert D: `SpockReplicationDown` / `HAProxyBackendDown`

- **Severity:** `CRITICAL`
- **Owner:** Platform & Ops
- **Gejala:** `spock_subscription_is_replicating == 0` selama 1 menit atau `haproxy_backend_active_servers == 0`.

### Langkah Cek
1. Cek HAProxy Stats di `http://localhost:8404/` untuk melihat node Silver/Gold mana yang `DOWN`.
2. Cek status subscription Spock langsung pada node terkait:
   ```bash
   docker compose -f compose.dev.yaml --profile dev exec spock-silver-1 \
     psql -U postgres -d wmata -c 'SELECT subscription_name, status FROM spock.sub_show_status();'
   ```
3. Cek tabel konflik jika ada divergensi: `SELECT * FROM spock.resolutions ORDER BY commit_time DESC LIMIT 20;`.

### Perbaikan
- Jika salah satu node mati, nyalakan ulang container `spock-*` terkait dan pastikan `spock-wire-silver` / `spock-wire-gold` selesai `exited (0)`.
- Jika tabel tidak tereplikasi, pastikan tabel tersebut memiliki Primary Key lalu panggil `spock.repset_add_all_tables`.

---

## Alert E: `DlqMessagesPresent`

- **Severity:** `WARNING`
- **Owner:** Streaming
- **Gejala:** Ada pesan baru masuk ke stream `wmata-dlq` dalam 10 menit terakhir.

### Langkah Cek
1. Inspeksi isi pesan DLQ via `nats-nui` (`http://localhost:31311`) atau CLI:
   ```bash
   docker compose -f compose.dev.yaml --profile dev run --rm --entrypoint nats nats-init -s nats:4222 stream view wmata-dlq --last 10
   ```
2. Cari error terkait di VictoriaLogs: `service:bronze-consumer level:WARNING OR level:ERROR`.

### Perbaikan
- Jika disebabkan gangguan sementara S3/Spark, jalankan replay dengan `tools/replay_dlq.py` setelah akar masalah teratasi.
