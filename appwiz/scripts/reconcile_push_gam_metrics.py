from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


APP_TZ = timezone(timedelta(hours=-3))


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def local_day(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return utc_now().astimezone(APP_TZ).date().isoformat()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text[:10]
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(APP_TZ).date().isoformat()


def parse_day(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    return date.fromisoformat(value)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS push_message_daily_metrics (
          day TEXT NOT NULL,
          message_id TEXT NOT NULL,
          project_id TEXT NOT NULL,
          site TEXT NOT NULL,
          utm_campaign TEXT NOT NULL,
          revenue REAL NOT NULL DEFAULT 0,
          impressions REAL NOT NULL DEFAULT 0,
          clicks REAL NOT NULL DEFAULT 0,
          pmr REAL NOT NULL DEFAULT 0,
          ecpm REAL NOT NULL DEFAULT 0,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY (day, message_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS push_daily_optimization_plans (
          plan_id TEXT PRIMARY KEY,
          project_id TEXT NOT NULL,
          plan_day TEXT NOT NULL,
          source_day TEXT NOT NULL,
          required_sends INTEGER NOT NULL DEFAULT 8,
          eligible_messages INTEGER NOT NULL DEFAULT 0,
          keep_count INTEGER NOT NULL DEFAULT 0,
          pause_count INTEGER NOT NULL DEFAULT 0,
          new_content_count INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'proposed',
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )


def load_queue_items(conn: sqlite3.Connection, start_day: str, end_day: str) -> list[sqlite3.Row]:
    if not table_exists(conn, "push_send_queue"):
        return []
    rows = conn.execute(
        """
        SELECT *
        FROM push_send_queue
        ORDER BY project_id, scheduled_at
        """,
    ).fetchall()
    return [
        row
        for row in rows
        if start_day <= local_day(str(row["sent_at"] or row["scheduled_at"] or row["created_at"] or "")) <= end_day
    ]


def gam_metrics_for_utm(conn: sqlite3.Connection, day: str, utm_campaign: str) -> dict[str, Any]:
    if not table_exists(conn, "gam_attr_daily"):
        return {"rows": 0, "revenue": 0.0, "impressions": 0.0, "clicks": 0.0, "pmr": 0.0}
    rows = conn.execute(
        """
        SELECT revenue, impressions, clicks, total_requests, match_rate, site, attr_raw
        FROM gam_attr_daily
        WHERE day = ?
          AND (
            attr_id = ?
            OR attr_raw = ?
            OR attr_raw LIKE ?
          )
        """,
        (day, utm_campaign, f"utm_campaign={utm_campaign}", f"%utm_campaign={utm_campaign}%"),
    ).fetchall()
    revenue = sum(float(row["revenue"] or 0.0) for row in rows)
    impressions = sum(float(row["impressions"] or 0.0) for row in rows)
    clicks = sum(float(row["clicks"] or 0.0) for row in rows)
    requests = sum(float(row["total_requests"] or 0.0) for row in rows)
    weighted_match = sum(float(row["match_rate"] or 0.0) * float(row["impressions"] or 0.0) for row in rows)
    pmr = 0.0
    if requests > 0:
        pmr = impressions / requests
    elif impressions > 0 and weighted_match > 0:
        pmr = weighted_match / impressions
    ecpm = (revenue / impressions * 1000.0) if impressions > 0 else 0.0
    sites = sorted({str(row["site"] or "").strip() for row in rows if str(row["site"] or "").strip()})
    return {
        "rows": len(rows),
        "revenue": round(revenue, 6),
        "impressions": round(impressions, 6),
        "clicks": round(clicks, 6),
        "pmr": round(pmr, 6),
        "ecpm": round(ecpm, 6),
        "sites": sites,
    }


def upsert_metric(
    conn: sqlite3.Connection,
    day: str,
    row: sqlite3.Row,
    metrics: dict[str, Any],
) -> None:
    payload = json.loads(row["payload_json"] or "{}")
    message_id = str(row["message_id"] or payload.get("message_id") or row["utm_campaign"] or row["send_id"])
    site = str(row["project_id"] or "")
    if metrics.get("sites"):
        site = str(metrics["sites"][0])
    metadata = {
        "source": "push_send_queue+gam_attr_daily",
        "send_id": row["send_id"],
        "queue_status": row["status"],
        "scheduled_at": row["scheduled_at"],
        "sent_at": row["sent_at"],
        "gam_rows": metrics["rows"],
        "payload": payload,
    }
    conn.execute(
        """
        INSERT INTO push_message_daily_metrics (
          day, message_id, project_id, site, utm_campaign,
          revenue, impressions, clicks, pmr, ecpm, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(day, message_id) DO UPDATE SET
          project_id = excluded.project_id,
          site = excluded.site,
          utm_campaign = excluded.utm_campaign,
          revenue = excluded.revenue,
          impressions = excluded.impressions,
          clicks = excluded.clicks,
          pmr = excluded.pmr,
          ecpm = excluded.ecpm,
          metadata_json = excluded.metadata_json
        """,
        (
            day,
            message_id,
            row["project_id"],
            site,
            row["utm_campaign"],
            metrics["revenue"],
            metrics["impressions"],
            metrics["clicks"],
            metrics["pmr"],
            metrics["ecpm"],
            json.dumps(metadata, ensure_ascii=False),
        ),
    )


def build_plan(conn: sqlite3.Connection, project_id: str, plan_day: str, source_day: str, minimum_impressions: int) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT *
        FROM push_message_daily_metrics
        WHERE project_id = ?
          AND day = ?
          AND impressions >= ?
        ORDER BY ecpm DESC, revenue DESC
        """,
        (project_id, source_day, minimum_impressions),
    ).fetchall()
    ranked = [dict(row) for row in rows]
    winners = ranked[:5]
    losers = sorted(ranked, key=lambda item: (float(item["ecpm"] or 0.0), float(item["revenue"] or 0.0)))[:3]
    keep_count = min(5, len(winners))
    pause_count = min(3, len(losers))
    new_content_count = pause_count
    payload = {
        "rule_version": "push_8_sends_gam_reconciliation_v1",
        "required_sends": 8,
        "source_day": source_day,
        "minimum_impressions": minimum_impressions,
        "eligible_messages": len(ranked),
        "winners": winners,
        "losers": losers,
        "actions": {
            "keep_best": keep_count,
            "pause_worst": pause_count,
            "create_variations_from_winners": new_content_count,
        },
    }
    safe_project = project_id.replace(".", "_").replace("-", "_")
    plan_id = f"push_opt_{safe_project}_{plan_day}"
    conn.execute(
        """
        INSERT INTO push_daily_optimization_plans (
          plan_id, project_id, plan_day, source_day, required_sends,
          eligible_messages, keep_count, pause_count, new_content_count,
          status, payload_json
        )
        VALUES (?, ?, ?, ?, 8, ?, ?, ?, ?, 'proposed', ?)
        ON CONFLICT(plan_id) DO UPDATE SET
          source_day = excluded.source_day,
          eligible_messages = excluded.eligible_messages,
          keep_count = excluded.keep_count,
          pause_count = excluded.pause_count,
          new_content_count = excluded.new_content_count,
          status = excluded.status,
          payload_json = excluded.payload_json
        """,
        (
            plan_id,
            project_id,
            plan_day,
            source_day,
            len(ranked),
            keep_count,
            pause_count,
            new_content_count,
            json.dumps(payload, ensure_ascii=False),
        ),
    )
    return {"plan_id": plan_id, "project_id": project_id, **payload["actions"], "eligible_messages": len(ranked)}


def reconcile(args: argparse.Namespace) -> dict[str, Any]:
    today = utc_now().astimezone(APP_TZ).date()
    start = parse_day(args.start_day, today - timedelta(days=1)).isoformat()
    end = parse_day(args.end_day, today).isoformat()
    plan_day = parse_day(args.plan_day, today).isoformat()
    source_day = parse_day(args.source_day, today - timedelta(days=1)).isoformat()
    result: dict[str, Any] = {
        "start_day": start,
        "end_day": end,
        "metrics_updated": 0,
        "matched_gam_rows": 0,
        "by_project": defaultdict(lambda: {"queue_items": 0, "matched": 0, "revenue": 0.0, "impressions": 0.0}),
        "plans": [],
    }
    with sqlite3.connect(args.db_path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        items = load_queue_items(conn, start, end)
        projects: set[str] = set()
        for item in items:
            project_id = str(item["project_id"] or "")
            projects.add(project_id)
            day = local_day(str(item["sent_at"] or item["scheduled_at"] or item["created_at"] or ""))
            utm_campaign = str(item["utm_campaign"] or "").strip()
            result["by_project"][project_id]["queue_items"] += 1
            if not utm_campaign:
                continue
            metrics = gam_metrics_for_utm(conn, day, utm_campaign)
            upsert_metric(conn, day, item, metrics)
            result["metrics_updated"] += 1
            result["matched_gam_rows"] += int(metrics["rows"])
            if metrics["rows"]:
                result["by_project"][project_id]["matched"] += 1
            result["by_project"][project_id]["revenue"] += float(metrics["revenue"] or 0.0)
            result["by_project"][project_id]["impressions"] += float(metrics["impressions"] or 0.0)
        for project_id in sorted(projects):
            result["plans"].append(build_plan(conn, project_id, plan_day, source_day, args.minimum_impressions))
        conn.commit()
    result["by_project"] = {
        project: {
            "queue_items": values["queue_items"],
            "matched": values["matched"],
            "revenue": round(values["revenue"], 6),
            "impressions": round(values["impressions"], 6),
        }
        for project, values in result["by_project"].items()
    }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile AiWebPush sends with GAM UTM revenue.")
    parser.add_argument("--db-path", type=Path, default=Path("/var/www/app.wizseoservicesaustralia.com/public_html/roas_cache.sqlite3"))
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--source-day")
    parser.add_argument("--plan-day")
    parser.add_argument("--minimum-impressions", type=int, default=100)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    payload = reconcile(args)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
