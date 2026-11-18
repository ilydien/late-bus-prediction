# Serving API

Read-only FastAPI over the WMATA gold marts.

## Base URL

```text
http://127.0.0.1:18000
```

OpenAPI:

```text
http://127.0.0.1:18000/docs
http://127.0.0.1:18000/openapi.json
```

## Endpoints

| Method | Path | Parameters | Description |
|---|---|---|---|
| GET | `/health` | — | DB healthcheck |
| GET | `/v1/freshness` | — | latest snapshot/service/prediction time |
| GET | `/v1/routes/delay` | `start_date`, `end_date`, `route_id?` | delay per route/day |
| GET | `/v1/reliability/daily` | `start_date`, `end_date`, `route_id?` | on-time/late reliability |
| GET | `/v1/routes/hourly` | `start_date`, `end_date`, `route_id?` | hourly delay |
| GET | `/v1/predictions/latest` | `route_id?`, `limit` | latest successful predictions |
| GET | `/v1/model/performance` | `start_date?`, `end_date?` | model metrics per day |

## Status codes

| Code | Meaning |
|---|---|
| 200 | success |
| 400 | `start_date > end_date` |
| 422 | invalid date format / invalid limit |
| 503 | database unavailable |

## Example

```bash
curl 'http://127.0.0.1:18000/v1/routes/delay?start_date=2020-02-01&end_date=2020-02-08&route_id=F8'
```

```json
{
  "data": [
    {
      "route_id": "F8",
      "service_date": "2020-02-08",
      "trips": 123,
      "avg_deviation": 2.4,
      "on_time_pct": 0.81
    }
  ],
  "freshness": {
    "max_snapshot_ts": "2020-02-08 23:59:00+00:00",
    "max_service_date": "2020-02-08",
    "max_predicted_at": "2026-11-18 04:14:00+00:00",
    "generated_at": "2026-11-18 04:30:00+00:00"
  }
}
```

## Invalid date

```bash
curl -i 'http://127.0.0.1:18000/v1/routes/delay?start_date=bad&end_date=2020-02-08'
# HTTP/1.1 422 Unprocessable Entity
```

```bash
curl -i 'http://127.0.0.1:18000/v1/routes/delay?start_date=2020-02-09&end_date=2020-02-08'
# HTTP/1.1 400 Bad Request
```

## Database

The API reads through the gold read-only HAProxy frontend:

```text
haproxy:5436
database: wmata
```

## Compose

```bash
docker compose -f compose.dev.yaml --profile dev up -d serving-api
docker compose -f compose.dev.yaml --profile dev logs -f serving-api
```
