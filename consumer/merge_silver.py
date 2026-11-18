"""Merge Spark staging tables into the silver tables.

Spark writes only to ``*_staging`` tables. This script performs an idempotent
upsert into the real silver tables and then truncates the staging tables.

Usage:
  python merge_silver.py --truncate-only   # before Spark
  python merge_silver.py                   # after Spark
"""

from __future__ import annotations

import argparse
import os

import psycopg2
from psycopg2.extras import execute_values

from governance import run_log

HOST = os.environ.get("POSTGRES_SILVER_HOST", "haproxy")
PORT = int(os.environ.get("POSTGRES_SILVER_PORT", "5433"))
USER = os.environ.get("POSTGRES_SILVER_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_SILVER_PASSWORD", "postgres")
CHUNK = int(os.environ.get("SILVER_MERGE_CHUNK", "20000"))

DATABASES = {
    "wmata": os.environ.get("POSTGRES_DB_WMATA", "wmata"),
    "traffic": os.environ.get("POSTGRES_DB_TRAFFIC", "traffic"),
}

TABLES = {
    "wmata": ["bus_trip_snapshot", "route_stats"],
    "traffic": ["ddot_aadt_2020", "ddot_am_speed_2019", "ddot_taz"],
}


def connect(database: str):
    return psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=database,
        user=USER,
        password=PASSWORD,
        connect_timeout=10,
    )


def columns(cur, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def primary_key(cur, table: str) -> list[str]:
    cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a
          ON a.attrelid = i.indrelid
         AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey::int2[], a.attnum)
        """,
        (f"public.{table}",),
    )
    return [row[0] for row in cur.fetchall()]


def truncate_staging(cur, database: str) -> int:
    count = 0
    for table in TABLES[database]:
        cur.execute(f"TRUNCATE public.{table}_staging")
        count += 1
    return count


def merge_table(conn, cur, table: str) -> tuple[int, int]:
    staging = f"{table}_staging"
    cur.execute(f"SELECT count(*) FROM public.{staging}")
    staged = int(cur.fetchone()[0])
    if staged == 0:
        return 0, 0

    cols = columns(cur, table)
    if not cols:
        raise RuntimeError(f"target table public.{table} not found")
    pk = primary_key(cur, table)
    if not pk:
        raise RuntimeError(f"target table public.{table} has no primary key")

    col_sql = ", ".join(cols)
    pk_sql = ", ".join(pk)
    updates = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in cols if col not in pk
    )
    conflict = f"ON CONFLICT ({pk_sql}) DO UPDATE SET {updates}" if updates else (
        f"ON CONFLICT ({pk_sql}) DO NOTHING"
    )
    insert_sql = (
        f"INSERT INTO public.{table} ({col_sql}) VALUES %s {conflict}"
    )

    merged = 0
    processed = 0
    while True:
        # ctid lets us delete exactly the chunk we just merged, without
        # relying on a staging primary key (staging tables intentionally have
        # none). Commit per chunk keeps HAProxy's idle timeout from cutting a
        # long-running 500k-row merge.
        cur.execute(
            f"SELECT ctid::text, {col_sql} FROM public.{staging} LIMIT {CHUNK}"
        )
        rows = cur.fetchall()
        if not rows:
            break
        ctids = [row[0] for row in rows]
        values = [row[1:] for row in rows]
        execute_values(cur, insert_sql, values, page_size=min(CHUNK, 5000))
        merged += len(values)
        cur.execute(
            f"DELETE FROM public.{staging} WHERE ctid = ANY(%s::tid[])",
            (ctids,),
        )
        conn.commit()
        processed += len(rows)
        print(
            f"{table}: merged chunk rows={len(rows)} total={processed}/{staged}",
            flush=True,
        )

    cur.execute(f"TRUNCATE public.{staging}")
    conn.commit()
    return staged, merged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truncate-only", action="store_true")
    ap.add_argument("--run-id")
    args = ap.parse_args()

    counts: dict[str, dict] = {}
    for database, dbname in DATABASES.items():
        conn = connect(dbname)
        try:
            with conn.cursor() as cur:
                if args.truncate_only:
                    n = truncate_staging(cur, database)
                    conn.commit()
                    print(f"{database}: truncated {n} staging tables", flush=True)
                    continue

                for table in TABLES[database]:
                    staged, merged = merge_table(conn, cur, table)
                    counts[f"{database}.{table}"] = {
                        "staged": staged,
                        "merged": merged,
                    }
                    if staged:
                        print(
                            f"{database}.{table}: staged={staged} merged={merged}",
                            flush=True,
                        )
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    if args.run_id and counts:
        run_log.safe_record_row_counts(args.run_id, counts)


if __name__ == "__main__":
    main()
