# Serving SQL queries

All queries read from gold PostgreSQL (`wmata`). The API uses the views in
`gold/ddl/007_serving.sql`.

## 1. Delay per route per day

```sql
SELECT route_id, service_date, trips, avg_deviation, median_deviation,
       p90_deviation, on_time_pct, late_pct, avg_speed_kmh
FROM public.serving_route_daily
WHERE service_date BETWEEN :start_date AND :end_date
ORDER BY service_date, route_id;
```

## 2. Delay per route per hour

```sql
SELECT route_id, service_date, hour_local, trips, avg_deviation,
       on_time_pct, late_pct, avg_speed_kmh
FROM public.serving_route_hourly
WHERE service_date BETWEEN :start_date AND :end_date
ORDER BY service_date, hour_local, route_id;
```

## 3. Reliability harian

```sql
SELECT route_id, service_date, trips, on_time_pct, late_pct,
       avg_deviation, p90_deviation
FROM public.serving_route_daily
WHERE service_date BETWEEN :start_date AND :end_date
ORDER BY service_date, route_id;
```

## 4. Top route terlambat

```sql
SELECT route_id,
       sum(trips) AS trips,
       avg(avg_deviation) AS avg_delay,
       avg(late_pct) AS late_pct
FROM public.serving_route_daily
WHERE service_date BETWEEN :start_date AND :end_date
GROUP BY route_id
ORDER BY avg_delay DESC
LIMIT 10;
```

## 5. Prediksi terbaru

```sql
SELECT input_record_id, model_version, predicted_at, trip_key, snapshot_ts,
       route_id, predicted_delay_min, actual_delay_min, abs_error,
       predicted_late, actual_late, inference_run_id
FROM public.serving_latest_predictions
ORDER BY predicted_at DESC
LIMIT 50;
```

## 6. Model performance

```sql
SELECT service_date, model_version, n_predictions, mae, rmse,
       accuracy_late_5, avg_error
FROM public.serving_model_performance
WHERE service_date BETWEEN :start_date AND :end_date
ORDER BY service_date DESC, model_version;
```

## Freshness

```sql
SELECT max_snapshot_ts, max_service_date, max_predicted_at, generated_at
FROM public.serving_freshness;
```
