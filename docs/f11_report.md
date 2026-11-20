# F-11 experiment and versioning report

## Model versions

| Version | Run ID | MAE | RMSE | R² | F1 late >5 |
|---|---|---:|---:|---:|---:|
| v1 (M2) | `2c018fb28a6241f2893deef8f775bcf7` | 2.18757 | 3.37939 | 0.802143 | 0.75049 |
| v2 (M3) | `ebd9be7dda4146b9b7d1824df78e506d` | 2.18609 | 3.37950 | 0.802130 | 0.75154 |
| v3 | `32e19c0a90414c0883ff4d1d41f144d9` | 2.18349 | 3.37065 | 0.800909 | 0.75202 |

v3 is the first version with the new versioning metadata.

## v3 metadata

```text
git_commit      : a71af23ae129e4da3b83833d0ebc8b4ba77a178c
dataset_version : gold-2020-02-08
dataset_hash    : 2a465b4f93510be0
feature_hash    : 46fe13e56672f9e2
features        : 22
```

Artifacts:

```text
feature_contract.json
route_categories.json   (251 route categories)
model/
```

## Split

Same rolling policy as the training code:

```text
train : 2020-01-06 .. 2020-01-25
valid : 2020-01-26 .. 2020-02-01
test  : 2020-02-02 .. 2020-02-08
```

## Batch inference with v3

Manual run:

```text
run_id        : manual-v3-inference
model_version : v3
rows_read     : 500
success       : 500
errors        : 0
```

Predictions are stored in `gold.model_predictions` with:

```text
model_version    = v3
inference_run_id = manual-v3-inference
status           = success
```

## Promotion state

- `champion` remains **v1**.
- v3 is an experiment and is not auto-promoted.
- Promotion gate still requires MAE improvement >= 2.5%, RMSE <= +2%, F1 drop <= 0.01.

## Evidence queries

```sql
SELECT run_id, job_name, status, row_counts
FROM public.pipeline_runs
WHERE job_name IN ('trip_delay_model', 'batch_inference_v3')
ORDER BY started_at DESC;
```

```sql
SELECT model_version, status, count(*)
FROM public.model_predictions
WHERE inference_run_id = 'manual-v3-inference'
GROUP BY model_version, status;
```
