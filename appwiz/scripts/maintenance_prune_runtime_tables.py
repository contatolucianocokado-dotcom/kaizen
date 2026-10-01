#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

APP_TZ = ZoneInfo("America/Sao_Paulo")


def cutoff_date(days: int) -> str:
    return (datetime.now(APP_TZ) - timedelta(days=int(days))).strftime("%Y-%m-%d")


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def prune_table(conn: sqlite3.Connection, table: str, date_col: str, keep_days: int, batch_size: int) -> int:
    if not table_exists(conn, table):
        return 0
    cutoff = cutoff_date(keep_days)
    total = 0
    while True:
        rows = conn.execute(
            f"SELECT rowid FROM {table} WHERE substr({date_col}, 1, 10) < ? LIMIT ?",
            (cutoff, int(batch_size)),
        ).fetchall()
        if not rows:
            break
        conn.executemany(f"DELETE FROM {table} WHERE rowid = ?", rows)
        conn.commit()
        total += len(rows)
        time.sleep(0.03)
    return total


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    if not table_exists(conn, table):
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="roas_cache.sqlite3")
    parser.add_argument("--metrics-days", type=int, default=7)
    parser.add_argument("--snapshots-days", type=int, default=14)
    parser.add_argument("--runs-days", type=int, default=14)
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()

    db_path = Path(args.db)
    log_path = db_path.with_name("logs").joinpath("runtime_maintenance.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path, timeout=60) as conn:
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA journal_mode=WAL")
        before = {
            "roi_control_entity_metrics": count_rows(conn, "roi_control_entity_metrics"),
            "decision_context_snapshots": count_rows(conn, "decision_context_snapshots"),
            "roi_control_runs": count_rows(conn, "roi_control_runs"),
        }
        deleted = {
            "roi_control_entity_metrics": prune_table(conn, "roi_control_entity_metrics", "generated_at", args.metrics_days, args.batch_size),
            "decision_context_snapshots": prune_table(conn, "decision_context_snapshots", "captured_at", args.snapshots_days, args.batch_size),
            "roi_control_runs": prune_table(conn, "roi_control_runs", "generated_at", args.runs_days, args.batch_size),
        }
        conn.execute("PRAGMA optimize")
        after = {
            "roi_control_entity_metrics": count_rows(conn, "roi_control_entity_metrics"),
            "decision_context_snapshots": count_rows(conn, "decision_context_snapshots"),
            "roi_control_runs": count_rows(conn, "roi_control_runs"),
        }

    payload = {
        "created_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "keep_days": {"metrics": args.metrics_days, "snapshots": args.snapshots_days, "runs": args.runs_days},
        "before": before,
        "deleted": deleted,
        "after": after,
    }
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
