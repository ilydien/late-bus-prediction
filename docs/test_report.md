# Batch/ML test report

## Run

```bash
docker run --rm --network wmata-lateness_wmata-lateness-dev \
  --env-file .env \
  -v "$PWD/ml:/app" -v "$PWD/consumer:/app/consumer:ro" \
  -v "$PWD/tests:/app/tests:ro" -v "$PWD/quality:/app/quality:ro" \
  -v "$PWD/governance:/app/governance:ro" \
  -e PYTHONPATH=/app wmata-ml \
  sh -c 'cd /app && coverage run --source=train_trip_delay,batch_inference,promote_model,consumer,quality,governance -m unittest discover -s tests && coverage report -m'
```

## Result

```text
Ran 30 tests in 0.045s

OK

TOTAL  943 statements  545 missed  42% coverage
```

Coverage by module:

| Module | Coverage |
|---|---:|
| `batch_inference.py` | 41% |
| `consumer/bronze_consumer.py` | 31% |
| `consumer/merge_silver.py` | 0% |
| `governance/run_log.py` | 73% |
| `promote_model.py` | 44% |
| `quality/checks.py` | 70% |
| `train_trip_delay.py` | 34% |

The total exceeds the F-18 minimum of 40%. The lowest modules are the
DB/Spark orchestration paths (`merge_silver`, consumer main loop, training
main); their pure helpers are covered separately.

## Test files

- `tests/test_quality_checks.py`
- `tests/test_quality_integration.py`
- `tests/test_run_log.py`
- `tests/test_run_log_db.py`
- `tests/test_batch_inference.py`
- `tests/test_train_helpers.py`
- `tests/test_promote_gate.py`
- `tests/test_consumer_helpers.py`


## F-10 serving API tests

```text
Ran 6 tests ... OK
```

Test file: `tests/test_serving_api.py`.

Covers:

- `/health`
- invalid date format -> 422
- `start_date > end_date` -> 400
- routes delay response
- freshness response
- limit validation

## Great Expectations staging gate

```text
dataset: staging
table: trip_delay_features_staging
success: true
checkpoint: staging_trip_delay_checkpoint
```

Report:

```text
quality/reports/gx_latest.json
```

Data Docs:

```text
gx_quality/gx/uncommitted/data_docs/local_site/index.html
```

The generated GX context is ignored by Git because it contains the database
connection string. Suite/checkpoint definitions live in `quality/gx_gate.py`.


## Data quality audit

```text
checks: 32
pass: 31
warn: 1
fail: 0
gx_success: true
```

Schedule: `data_quality_audit_schedule`, daily 05:00 America/New_York, RUNNING.
