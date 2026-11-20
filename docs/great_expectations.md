# Great Expectations quality gate

Great Expectations 1.25 is used as a formal quality layer on top of the
custom SQL checks.

## Runtime context

Generated at runtime under:

```text
gx_quality/gx/
```

The generated context contains the PostgreSQL connection string, so it is
ignored by Git:

```text
gx_quality/gx/
```

Only `gx_quality/.gitkeep` is committed. The suite/checkpoint definition lives
in code (`quality/gx_gate.py`), so it is reproducible.

## Datasets

| Dataset | Table |
|---|---|
| `staging` | `public.trip_delay_features_staging` |
| `gold` | `public.trip_delay_features` |

## Expectations

- table row count >= 1
- not-null:
  - `trip_key`
  - `snapshot_ts`
  - `route_id`
  - `target_deviation_15m`
- unique compound key:
  - `(trip_key, snapshot_ts)`
- target range:
  - `[-60, 60]`

## Dagster integration

Inside `gold_trip_delay_features`:

```text
Spark writes staging
→ GX staging gate
→ custom staging checks
→ finalize gold
→ custom gold checks
```

If GX fails:

```text
run fails
gold finalize is not executed
```

## Daily audit

`data_quality_audit_schedule` runs daily at 05:00 America/New_York. It runs
custom checks for silver/gold plus the GX gold checkpoint. See
`docs/data_quality.md`.

## Manual run

Staging:

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.gx_gate --dataset staging --fail-on-error
```

Gold:

```bash
docker exec -e PYTHONPATH=/opt/dagster/app \
  wmata-lateness-dagster-user-code-1 \
  python -m quality.gx_gate --dataset gold --fail-on-error
```

## Reports

JSON summary:

```text
quality/reports/gx_latest.json
quality/reports/gx_<run_id>.json
```

Data Docs:

```text
gx_quality/gx/uncommitted/data_docs/local_site/index.html
```

## Custom checks

`quality/checks.py` remains for cross-dataset and business checks:

- route coverage
- DDOT coverage
- freshness
- inference error ratio
- model version
- marts row counts

GX handles schema/row/range/uniqueness expectations; custom checks handle
cross-dataset rules that GX does not express cleanly.
