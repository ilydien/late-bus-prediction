"""Lightweight lineage/run log for batch and ML jobs.

The table is ``public.pipeline_runs`` in the gold database. The helper is
intentionally small: it records run identity, input/output datasets, source
manifest, row counts, status, and error.

It is safe to call from the consumer, Dagster assets, and training scripts.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
GOLD_DB = os.environ.get("POSTGRES_DB_WMATA", "wmata")
USER = os.environ.get("POSTGRES_GOLD_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_GOLD_PASSWORD", "postgres")


def connect():
    return psycopg2.connect(
        host=GOLD_HOST,
        port=GOLD_PORT,
        dbname=GOLD_DB,
        user=USER,
        password=PASSWORD,
        connect_timeout=10,
    )


def new_run_id() -> str:
    return str(uuid.uuid4())


def git_commit() -> str:
    value = os.environ.get("GIT_COMMIT") or os.environ.get("GIT_SHA")
    if value:
        return value
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        pass
    for base in (Path.cwd(), Path(__file__).resolve().parents[1], Path("/opt/dagster/app"), Path("/app")):
        git_dir = base / ".git"
        try:
            head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
            if head.startswith("ref: "):
                return (git_dir / head[5:]).read_text(encoding="utf-8").strip()
            return head
        except OSError:
            continue
    return "unknown"


def start_run(
    run_id: str,
    job_name: str,
    layer: str,
    input_datasets: list[str] | None = None,
    output_datasets: list[str] | None = None,
    source_manifest: dict | None = None,
    parent_run_id: str | None = None,
) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.pipeline_runs
                  (run_id, parent_run_id, job_name, layer, status, started_at,
                   input_datasets, output_datasets, source_manifest, git_commit)
                VALUES (%s, %s, %s, %s, 'running', now(), %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                  parent_run_id = EXCLUDED.parent_run_id,
                  job_name = EXCLUDED.job_name,
                  layer = EXCLUDED.layer,
                  status = 'running',
                  started_at = COALESCE(public.pipeline_runs.started_at, now()),
                  input_datasets = EXCLUDED.input_datasets,
                  output_datasets = EXCLUDED.output_datasets,
                  source_manifest = EXCLUDED.source_manifest,
                  git_commit = EXCLUDED.git_commit,
                  error = NULL
                """,
                (
                    run_id,
                    parent_run_id,
                    job_name,
                    layer,
                    Json(input_datasets or []),
                    Json(output_datasets or []),
                    Json(source_manifest or {}),
                    git_commit(),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def finish_run(
    run_id: str,
    status: str,
    row_counts: dict | None = None,
    error: str | None = None,
    output_datasets: list[str] | None = None,
) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.pipeline_runs
                SET status = %s,
                    finished_at = now(),
                    row_counts = COALESCE(%s, row_counts),
                    output_datasets = COALESCE(%s, output_datasets),
                    error = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    Json(row_counts) if row_counts is not None else None,
                    Json(output_datasets) if output_datasets is not None else None,
                    error,
                    run_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def record_row_counts(run_id: str, row_counts: dict) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.pipeline_runs SET row_counts = %s WHERE run_id = %s",
                (Json(row_counts), run_id),
            )
        conn.commit()
    finally:
        conn.close()


def get_run(run_id: str) -> dict | None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, parent_run_id, job_name, layer, status,
                       started_at, finished_at, input_datasets, output_datasets,
                       source_manifest, row_counts, git_commit, error
                FROM public.pipeline_runs WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def safe_start_run(*args, **kwargs) -> bool:
    try:
        start_run(*args, **kwargs)
        return True
    except Exception as exc:  # noqa: BLE001 - lineage must not stop data flow
        print(f"WARN pipeline_runs start failed: {exc}", flush=True)
        return False


def safe_finish_run(*args, **kwargs) -> bool:
    try:
        finish_run(*args, **kwargs)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"WARN pipeline_runs finish failed: {exc}", flush=True)
        return False


def safe_record_row_counts(run_id: str, row_counts: dict) -> bool:
    try:
        record_row_counts(run_id, row_counts)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"WARN pipeline_runs row_counts failed: {exc}", flush=True)
        return False


if __name__ == "__main__":
    print(json.dumps({"run_id": new_run_id(), "git_commit": git_commit()}, indent=2))
