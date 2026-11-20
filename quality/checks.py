"""SQL-based batch quality gate.

The checks are deliberately small and dependency-light: psycopg2 plus SQL.
Results are written to ``public.data_quality_runs`` in the gold database and
to a JSON report under ``quality/reports``.

Usage:
  python quality/checks.py --layer all --fail-on-critical
  python quality/checks.py --layer gold_staging
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

SILVER_HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
SILVER_PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
GOLD_HOST = os.environ.get("POSTGRES_GOLD_HOST", "haproxy")
GOLD_PORT = int(os.environ.get("POSTGRES_GOLD_PORT", "5434"))
USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")
DB_WMATA = os.environ.get("POSTGRES_DB_WMATA", "wmata")
DB_TRAFFIC = os.environ.get("POSTGRES_DB_TRAFFIC", "traffic")

FRESHNESS_WARN_HOURS = float(os.environ.get("QUALITY_FRESHNESS_WARN_HOURS", "48"))
ROUTE_COVERAGE_MAX = float(os.environ.get("QUALITY_ROUTE_COVERAGE_MAX", "0.05"))
DDOT_COVERAGE_MIN = float(os.environ.get("QUALITY_DDOT_COVERAGE_MIN", "0.50"))
NULL_RATE_MAX = float(os.environ.get("QUALITY_NULL_RATE_MAX", "0.02"))
REPORT_DIR = Path(
    os.environ.get("QUALITY_REPORT_DIR", Path(__file__).resolve().parent / "reports")
)


class QualityGateError(RuntimeError):
    """Raised when a critical quality check fails."""


def connect(database: str, target: str = "gold"):
    host = GOLD_HOST if target == "gold" else SILVER_HOST
    port = GOLD_PORT if target == "gold" else SILVER_PORT
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=USER,
        password=PASSWORD,
        connect_timeout=10,
    )


def scalar(conn, sql: str, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return None if row is None else row[0]


def max_check(observed, threshold: float, severity: str = "critical") -> str:
    if observed is None:
        return "fail" if severity == "critical" else "warn"
    ok = float(observed) <= float(threshold)
    if ok:
        return "pass"
    return "fail" if severity == "critical" else "warn"


def min_check(observed, threshold: float, severity: str = "critical") -> str:
    if observed is None:
        return "fail" if severity == "critical" else "warn"
    ok = float(observed) >= float(threshold)
    if ok:
        return "pass"
    return "fail" if severity == "critical" else "warn"


def add(
    results: list[dict],
    layer: str,
    dataset: str,
    check_name: str,
    status: str,
    severity: str,
    observed,
    threshold,
    details: dict | None = None,
) -> None:
    results.append(
        {
            "layer": layer,
            "dataset": dataset,
            "check_name": check_name,
            "status": status,
            "severity": severity,
            "observed": None if observed is None else float(observed),
            "threshold": None if threshold is None else float(threshold),
            "details": details or {},
        }
    )


def silver_checks(results: list[dict]) -> None:
    conn = connect(DB_WMATA, "silver")
    try:
        total = scalar(conn, "SELECT count(*) FROM public.bus_trip_snapshot")
        add(results, "silver", "bus_trip_snapshot", "row_count", min_check(total, 1), "critical", total, 1)

        nulls = scalar(
            conn,
            """
            SELECT count(*) FROM public.bus_trip_snapshot
            WHERE trip_key IS NULL OR snapshot_ts IS NULL
               OR route_id IS NULL OR event_time IS NULL
            """,
        )
        add(results, "silver", "bus_trip_snapshot", "null_keys", max_check(nulls, 0), "critical", nulls, 0)

        dup = scalar(
            conn,
            """
            SELECT count(*) FROM (
              SELECT trip_key, snapshot_ts, count(*) AS c
              FROM public.bus_trip_snapshot
              GROUP BY trip_key, snapshot_ts
              HAVING count(*) > 1
            ) d
            """,
        )
        add(results, "silver", "bus_trip_snapshot", "duplicate_pk", max_check(dup, 0), "critical", dup, 0)

        dev = scalar(
            conn,
            "SELECT count(*) FROM public.bus_trip_snapshot "
            "WHERE deviation_min < -60 OR deviation_min > 60",
        )
        add(results, "silver", "bus_trip_snapshot", "deviation_range", max_check(dev, 0), "critical", dev, 0)

        speed = scalar(
            conn,
            "SELECT count(*) FROM public.bus_trip_snapshot "
            "WHERE speed_kmh < 0 OR speed_kmh > 120",
        )
        add(results, "silver", "bus_trip_snapshot", "speed_range", max_check(speed, 0), "critical", speed, 0)

        progress = scalar(
            conn,
            "SELECT count(*) FROM public.bus_trip_snapshot "
            "WHERE progress_pct < 0 OR progress_pct > 1",
        )
        add(results, "silver", "bus_trip_snapshot", "progress_range", max_check(progress, 0), "critical", progress, 0)

        max_ts = scalar(conn, "SELECT max(snapshot_ts) FROM public.bus_trip_snapshot")
        if max_ts is None:
            add(results, "silver", "bus_trip_snapshot", "freshness_hours", "fail", "critical", None, FRESHNESS_WARN_HOURS)
        else:
            age = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600.0
            add(
                results,
                "silver",
                "bus_trip_snapshot",
                "freshness_hours",
                max_check(age, FRESHNESS_WARN_HOURS, "warning"),
                "warning",
                age,
                FRESHNESS_WARN_HOURS,
                {"max_snapshot_ts": max_ts.isoformat()},
            )

        routes = scalar(conn, "SELECT count(*) FROM public.route_stats")
        add(results, "silver", "route_stats", "row_count", min_check(routes, 1), "critical", routes, 1)
        route_dup = scalar(
            conn,
            "SELECT count(*) FROM (SELECT route_id FROM public.route_stats GROUP BY route_id HAVING count(*) > 1) d",
        )
        add(results, "silver", "route_stats", "duplicate_pk", max_check(route_dup, 0), "critical", route_dup, 0)

        total_routes = scalar(conn, "SELECT count(DISTINCT route_id) FROM public.bus_trip_snapshot")
        missing_routes = scalar(
            conn,
            """
            SELECT count(DISTINCT b.route_id)
            FROM public.bus_trip_snapshot b
            LEFT JOIN public.route_stats r ON r.route_id = b.route_id
            WHERE b.route_id IS NOT NULL AND r.route_id IS NULL
            """,
        )
        ratio = 0.0 if not total_routes else float(missing_routes or 0) / float(total_routes)
        add(
            results,
            "silver",
            "bus_trip_snapshot",
            "route_coverage_missing_ratio",
            max_check(ratio, ROUTE_COVERAGE_MAX, "warning"),
            "warning",
            ratio,
            ROUTE_COVERAGE_MAX,
            {"missing_routes": missing_routes, "distinct_routes": total_routes},
        )
    finally:
        conn.close()

    tconn = connect(DB_TRAFFIC, "silver")
    try:
        for table, pk in (
            ("ddot_aadt_2020", "objectid"),
            ("ddot_am_speed_2019", "objectid"),
            ("ddot_taz", "taz"),
        ):
            total = scalar(tconn, f"SELECT count(*) FROM public.{table}")
            add(results, "silver", table, "row_count", min_check(total, 1), "critical", total, 1)
            dup = scalar(
                tconn,
                f"SELECT count(*) FROM (SELECT {pk} FROM public.{table} GROUP BY {pk} HAVING count(*) > 1) d",
            )
            add(results, "silver", table, "duplicate_pk", max_check(dup, 0), "critical", dup, 0)
    finally:
        tconn.close()


def gold_checks(results: list[dict]) -> None:
    conn = connect(DB_WMATA, "gold")
    try:
        total = scalar(conn, "SELECT count(*) FROM public.trip_delay_features")
        add(results, "gold", "trip_delay_features", "row_count", min_check(total, 1), "critical", total, 1)

        dup = scalar(
            conn,
            """
            SELECT count(*) FROM (
              SELECT trip_key, snapshot_ts, count(*) AS c
              FROM public.trip_delay_features
              GROUP BY trip_key, snapshot_ts
              HAVING count(*) > 1
            ) d
            """,
        )
        add(results, "gold", "trip_delay_features", "duplicate_pk", max_check(dup, 0), "critical", dup, 0)

        null_target = scalar(
            conn,
            "SELECT count(*) FROM public.trip_delay_features WHERE target_deviation_15m IS NULL",
        )
        add(
            results,
            "gold",
            "trip_delay_features",
            "target_null",
            max_check(null_target, 0),
            "critical",
            null_target,
            0,
        )

        target_range = scalar(
            conn,
            "SELECT count(*) FROM public.trip_delay_features "
            "WHERE target_deviation_15m < -60 OR target_deviation_15m > 60",
        )
        add(
            results,
            "gold",
            "trip_delay_features",
            "target_range",
            max_check(target_range, 0),
            "critical",
            target_range,
            0,
        )

        for column in ("route_id", "deviation_now", "speed_kmh", "progress_pct"):
            nulls = scalar(
                conn,
                f"SELECT count(*) FROM public.trip_delay_features WHERE {column} IS NULL",
            )
            ratio = 0.0 if not total else float(nulls or 0) / float(total)
            add(
                results,
                "gold",
                "trip_delay_features",
                f"null_{column}_ratio",
                max_check(ratio, NULL_RATE_MAX, "warning"),
                "warning",
                ratio,
                NULL_RATE_MAX,
                {"null_rows": nulls, "total_rows": total},
            )

        for table, pk in (
            ("route_hourly_delay", "route_id, service_date, hour_local"),
            ("route_daily_reliability", "route_id, service_date"),
        ):
            total = scalar(conn, f"SELECT count(*) FROM public.{table}")
            add(results, "gold", table, "row_count", min_check(total, 1), "critical", total, 1)
            dup = scalar(
                conn,
                f"""
                SELECT count(*) FROM (
                  SELECT {pk}, count(*) AS c FROM public.{table}
                  GROUP BY {pk} HAVING count(*) > 1
                ) d
                """,
            )
            add(results, "gold", table, "duplicate_pk", max_check(dup, 0), "critical", dup, 0)

        predictions = scalar(conn, "SELECT count(*) FROM public.model_predictions")
        add(results, "gold", "model_predictions", "row_count", min_check(predictions, 1), "warning", predictions, 1)
        null_model = scalar(
            conn,
            "SELECT count(*) FROM public.model_predictions WHERE model_version IS NULL",
        )
        add(results, "gold", "model_predictions", "null_model_version", max_check(null_model, 0), "critical", null_model, 0)

        inference_errors = scalar(
            conn,
            "SELECT count(*) FROM public.model_predictions WHERE status = 'error'",
        )
        error_ratio = 0.0 if not predictions else float(inference_errors or 0) / float(predictions)
        add(
            results,
            "gold",
            "model_predictions",
            "inference_error_ratio",
            max_check(error_ratio, 0.01, "warning"),
            "warning",
            error_ratio,
            0.01,
            {"error_rows": inference_errors, "total_rows": predictions},
        )

        ddot = scalar(conn, "SELECT count(*) FROM public.route_ddot_features")
        with connect(DB_WMATA, "silver") as sconn:
            routes = scalar(sconn, "SELECT count(*) FROM public.route_stats")
        ratio = 0.0 if not routes else float(ddot or 0) / float(routes)
        add(
            results,
            "gold",
            "route_ddot_features",
            "route_coverage_ratio",
            min_check(ratio, DDOT_COVERAGE_MIN, "warning"),
            "warning",
            ratio,
            DDOT_COVERAGE_MIN,
            {"ddot_routes": ddot, "route_stats": routes},
        )
    finally:
        conn.close()


def staging_checks(results: list[dict]) -> None:
    conn = connect(DB_WMATA, "gold")
    try:
        total = scalar(conn, "SELECT count(*) FROM public.trip_delay_features_staging")
        add(results, "gold_staging", "trip_delay_features_staging", "row_count", min_check(total, 1), "critical", total, 1)

        dup = scalar(
            conn,
            """
            SELECT count(*) FROM (
              SELECT trip_key, snapshot_ts, count(*) AS c
              FROM public.trip_delay_features_staging
              GROUP BY trip_key, snapshot_ts
              HAVING count(*) > 1
            ) d
            """,
        )
        add(results, "gold_staging", "trip_delay_features_staging", "duplicate_pk", max_check(dup, 0), "critical", dup, 0)

        null_target = scalar(
            conn,
            "SELECT count(*) FROM public.trip_delay_features_staging "
            "WHERE target_deviation_15m IS NULL",
        )
        add(
            results,
            "gold_staging",
            "trip_delay_features_staging",
            "target_null",
            max_check(null_target, 0),
            "critical",
            null_target,
            0,
        )
    finally:
        conn.close()


def persist(run_id: str, results: list[dict]) -> None:
    conn = connect(DB_WMATA, "gold")
    try:
        rows = [
            (
                run_id,
                r["layer"],
                r["dataset"],
                r["check_name"],
                r["status"],
                r["severity"],
                r["observed"],
                r["threshold"],
                Json(r["details"]),
            )
            for r in results
        ]
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO public.data_quality_runs
                  (run_id, layer, dataset, check_name, status, severity,
                   observed, threshold, details)
                VALUES %s
                ON CONFLICT (run_id, layer, dataset, check_name) DO UPDATE SET
                  status = EXCLUDED.status,
                  severity = EXCLUDED.severity,
                  observed = EXCLUDED.observed,
                  threshold = EXCLUDED.threshold,
                  details = EXCLUDED.details,
                  checked_at = now()
                """,
                rows,
                page_size=100,
            )
        conn.commit()
    finally:
        conn.close()


def summarize(results: list[dict]) -> dict:
    critical_fail = any(
        r["status"] == "fail" and r["severity"] == "critical" for r in results
    )
    warnings = any(r["status"] != "pass" and r["severity"] != "critical" for r in results)
    overall = "fail" if critical_fail else ("warn" if warnings else "pass")
    return {
        "overall": overall,
        "total": len(results),
        "pass": sum(1 for r in results if r["status"] == "pass"),
        "fail": sum(1 for r in results if r["status"] == "fail"),
        "warn": sum(1 for r in results if r["status"] == "warn"),
    }


def write_report(report: dict, report_dir: Path | None = None) -> Path:
    directory = report_dir or REPORT_DIR
    directory.mkdir(parents=True, exist_ok=True)
    run_id = report["run_id"]
    path = directory / f"{run_id}.json"
    latest = directory / "latest.json"
    text = json.dumps(report, indent=2, default=str)
    path.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    return path


def run_checks(
    run_id: str | None = None,
    layer: str = "all",
    fail_on_critical: bool = False,
    persist_results: bool = True,
    report_dir: Path | None = None,
) -> dict:
    run_id = run_id or str(uuid.uuid4())
    results: list[dict] = []
    if layer in ("all", "silver"):
        silver_checks(results)
    if layer == "gold_staging":
        staging_checks(results)
    if layer in ("all", "gold"):
        gold_checks(results)

    summary = summarize(results)
    report = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layer": layer,
        "summary": summary,
        "results": results,
    }
    if persist_results:
        persist(run_id, results)
    write_report(report, report_dir)
    if fail_on_critical and summary["overall"] == "fail":
        raise QualityGateError(json.dumps(report["summary"]))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--layer", choices=["all", "silver", "gold", "gold_staging"], default="all")
    ap.add_argument("--fail-on-critical", action="store_true")
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--report-dir", type=Path)
    args = ap.parse_args()
    try:
        report = run_checks(
            run_id=args.run_id,
            layer=args.layer,
            fail_on_critical=args.fail_on_critical,
            persist_results=not args.no_persist,
            report_dir=args.report_dir,
        )
    except QualityGateError as exc:
        print(f"QUALITY FAILED: {exc}", flush=True)
        raise SystemExit(1)
    print(json.dumps(report["summary"], indent=2), flush=True)


if __name__ == "__main__":
    main()
