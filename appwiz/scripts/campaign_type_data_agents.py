#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "roas_cache.sqlite3"


def expected(channel_type: str, campaign_id: str, ad_group_id: str) -> tuple[str, str, str]:
    channel = str(channel_type or "").strip().upper()
    if channel == "DEMAND_GEN" and ad_group_id:
        return "agente-demand-gen", "ad_group", str(ad_group_id)
    if channel == "DISPLAY":
        return "agente-display", "campaign", str(campaign_id)
    if channel == "PERFORMANCE_MAX":
        return "agente-pmax", "campaign", str(campaign_id)
    if channel == "SEARCH":
        return "agente-search", "campaign", str(campaign_id)
    return "agente-campanha-generica", "campaign", str(campaign_id or ad_group_id)


def latest_ad_group_id(conn: sqlite3.Connection, site: str, campaign_id: str) -> str:
    row = conn.execute(
        """
        select ad_group_id
        from google_ads_campaign_settings_daily
        where campaign_id=?
          and coalesce(ad_group_id,'') != ''
          and (coalesce(site,'')=? or ?='')
        order by day desc
        limit 1
        """,
        (campaign_id, site, site),
    ).fetchone()
    return str(row["ad_group_id"] or "") if row else ""


def scan(conn: sqlite3.Connection) -> list[dict[str, object]]:
    conn.row_factory = sqlite3.Row
    issues: list[dict[str, object]] = []
    checks = [
        ("ads_daily", "rowid, day, site, campaign_id, campaign, ad_group_id, channel_type, attr_level, attr_id"),
        ("ads_hourly", "rowid, day, site, campaign_id, campaign, ad_group_id, channel_type, attr_level, attr_id"),
        ("google_ads_campaign_settings_daily", "rowid, day, site, campaign_id, campaign, ad_group_id, channel_type, '' as attr_level, attr_id"),
        ("ads_placement_daily", "rowid, day, site, campaign_id, campaign, ad_group_id, channel_type, '' as attr_level, attr_id"),
    ]
    for table, cols in checks:
        if conn.execute("select 1 from sqlite_master where type='table' and name=?", (table,)).fetchone() is None:
            continue
        rows = conn.execute(
            f"select {cols} from {table} where day >= date('now','localtime','-35 day') and coalesce(campaign_id,'') != ''"
        ).fetchall()
        for row in rows:
            ad_group_id = str(row["ad_group_id"] or "")
            if str(row["channel_type"] or "").strip().upper() == "DEMAND_GEN" and not ad_group_id:
                ad_group_id = latest_ad_group_id(conn, str(row["site"] or ""), str(row["campaign_id"] or ""))
            agent, attr_level, attr_id = expected(row["channel_type"], row["campaign_id"], ad_group_id)
            current_attr = str(row["attr_id"] or "")
            current_level = str(row["attr_level"] or "")
            current_ad_group = str(row["ad_group_id"] or "")
            bad_attr = bool(attr_id and current_attr != attr_id)
            bad_level = table in {"ads_daily", "ads_hourly"} and current_level and current_level != attr_level
            bad_ad_group = table in {"ads_daily", "ads_hourly", "ads_placement_daily"} and bool(ad_group_id and current_ad_group != ad_group_id)
            if bad_attr or bad_level or bad_ad_group:
                issues.append({
                    "table": table,
                    "rowid": row["rowid"],
                    "day": row["day"],
                    "site": row["site"],
                    "campaign_id": row["campaign_id"],
                    "ad_group_id": row["ad_group_id"],
                    "expected_ad_group_id": ad_group_id,
                    "channel_type": row["channel_type"],
                    "agent": agent,
                    "current_attr_id": current_attr,
                    "expected_attr_id": attr_id,
                    "current_attr_level": current_level,
                    "expected_attr_level": attr_level,
                    "campaign": row["campaign"],
                })
    return issues


def fix(conn: sqlite3.Connection, issues: list[dict[str, object]]) -> int:
    count = 0
    for issue in issues:
        table = str(issue["table"])
        expected_ad_group_id = str(issue.get("expected_ad_group_id") or "")
        if table in {"ads_daily", "ads_hourly"}:
            conn.execute(
                f"update {table} set attr_id=?, attr_level=?, ad_group_id=case when ? != '' then ? else ad_group_id end where rowid=?",
                (issue["expected_attr_id"], issue["expected_attr_level"], expected_ad_group_id, expected_ad_group_id, issue["rowid"]),
            )
        elif table == "ads_placement_daily":
            conn.execute(
                f"update {table} set attr_id=?, ad_group_id=case when ? != '' then ? else ad_group_id end where rowid=?",
                (issue["expected_attr_id"], expected_ad_group_id, expected_ad_group_id, issue["rowid"]),
            )
        else:
            conn.execute(
                f"update {table} set attr_id=? where rowid=?",
                (issue["expected_attr_id"], issue["rowid"]),
            )
        count += 1
    conn.commit()
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DB))
    parser.add_argument("--fix", action="store_true")
    args = parser.parse_args()
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma busy_timeout=30000")
    issues = scan(conn)
    fixed = fix(conn, issues) if args.fix and issues else 0
    print({"issues": len(issues), "fixed": fixed})
    for issue in issues[:30]:
        print(issue)


if __name__ == "__main__":
    main()
