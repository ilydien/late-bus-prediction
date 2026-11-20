# Data quality automation

## Layers

| Layer | Gate |
|---|---|
| Silver | custom SQL checks |
| Gold staging | Great Expectations + custom SQL |
| Gold | custom SQL checks |
| Gold audit | GX gold + all custom checks |

## Gold asset

`gold_trip_delay_features` runs:

```text
Spark staging
→ GX staging gate
→ custom staging checks
→ finalize gold
→ custom gold checks
```

If GX or custom staging fails, gold finalize is not executed.

## Daily audit

Asset:

```text
data_quality_audit
```

Job:

```text
data_quality_audit_job
```

Schedule:

```text
data_quality_audit_schedule
cron: 0 5 * * *
timezone: America/New_York
status: RUNNING
```

The audit runs even when gold is skipped, and checks the current silver/gold
state.

## Output

```text
public.data_quality_runs
quality/reports/latest.json
quality/reports/gx_latest.json
gx_quality/gx/uncommitted/data_docs/local_site/index.html
public.pipeline_runs
```

## Manual run

```bash
docker exec -e DAGSTER_HOME=/tmp/dagster_home \
  wmata-lateness-dagster-user-code-1 \
  sh -c 'mkdir -p /tmp/dagster_home && dagster asset materialize \
    -f /opt/dagster/app/definitions.py --select data_quality_audit'
```

## Current result

```json
{
  "checks": 32,
  "pass": 31,
  "warn": 1,
  "fail": 0,
  "gx_success": true
}
```

The warning is freshness because the current gold data is from 2020.

## Failure behavior

- critical custom check failure → asset fails
- GX failure → asset fails
- `pipeline_runs.status = failed`
- report JSON contains the failing check
- no gold mutation in the audit itself

## Known gaps

- row-level quarantine is not implemented
- data-quality alerts are not wired to the monitor
- freshness warning is expected for the 2020 historical dataset
