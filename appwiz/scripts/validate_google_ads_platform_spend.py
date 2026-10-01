#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from google.ads.googleads.client import GoogleAdsClient

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
for candidate in (ROOT, SCRIPTS):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from run_system_guard_agents import event
from roas_store import upsert_ads_rows

APP_TZ = ZoneInfo("America/Sao_Paulo")
DEFAULT_ABS_TOLERANCE = 5.0
DEFAULT_REL_TOLERANCE = 0.05


def clean_id(value: object) -> str:
    return re.sub(r"\D+", "", str(value or ""))

def valid_site(value: object) -> bool:
    return bool(re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", str(value or "").strip().lower()))

def domain_from_campaign(value: object) -> str:
    domains = re.findall(r"([a-z0-9.-]+\.[a-z]{2,})", str(value or "").lower())
    return domains[0] if domains else ""

def resolve_project_site(config_site: object, campaign_name: object = "") -> str:
    campaign_domain = domain_from_campaign(campaign_name)
    config_site = str(config_site or "").strip().lower()
    if campaign_domain:
        return campaign_domain
    return config_site


def now_sp() -> datetime:
    return datetime.now(APP_TZ)


def load_config(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def google_ads_client(ads_cfg: dict) -> GoogleAdsClient:
    data = {
        "developer_token": ads_cfg["developer_token"],
        "client_id": ads_cfg["client_id"],
        "client_secret": ads_cfg["client_secret"],
        "refresh_token": ads_cfg["refresh_token"],
        "use_proto_plus": True,
    }
    login_customer_id = clean_id(ads_cfg.get("login_customer_id") or ads_cfg.get("customer_id"))
    if len(login_customer_id) == 10:
        data["login_customer_id"] = login_customer_id
    return GoogleAdsClient.load_from_dict(data, version=ads_cfg.get("api_version", "v23"))


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS google_ads_platform_spend_validation_daily (day TEXT NOT NULL, customer_id TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', site TEXT NOT NULL DEFAULT '', google_ads_cost REAL NOT NULL DEFAULT 0, platform_cost REAL NOT NULL DEFAULT 0, diff REAL NOT NULL DEFAULT 0, diff_pct REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT '', checked_at TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (day, customer_id))")


def platform_cost(conn: sqlite3.Connection, customer_id: str, day: str) -> float:
    row = conn.execute("SELECT COALESCE(SUM(cost),0) cost FROM ads_daily WHERE customer_id=? AND day=?", (customer_id, day)).fetchone()
    return float(row["cost"] or 0)


def google_ads_cost(service, customer_id: str, day: str) -> tuple[float, int]:
    total = 0.0
    rows = 0
    query = f"SELECT campaign.id, metrics.cost_micros FROM campaign WHERE segments.date = '{day}' AND campaign.status != REMOVED"
    for batch in service.search_stream(customer_id=customer_id, query=query):
        for result in batch.results:
            rows += 1
            total += float(result.metrics.cost_micros or 0) / 1_000_000
    return total, rows


def validate_account_day(conn: sqlite3.Connection, service, *, account: dict, day: str, checked_at: str, abs_tolerance: float, rel_tolerance: float) -> int:
    customer_id = clean_id(account.get("customer_id"))
    site = str(account.get("site") or account.get("name") or customer_id)
    name = str(account.get("name") or site)
    if not customer_id:
        return 0
    try:
        ads_cost, api_rows = google_ads_cost(service, customer_id, day)
        api_error = ""
    except Exception as exc:
        ads_cost = 0.0
        api_rows = 0
        api_error = str(exc)
    stored_cost = platform_cost(conn, customer_id, day)
    diff = round(stored_cost - ads_cost, 2)
    base = max(abs(ads_cost), 0.01)
    diff_pct = round(diff / base, 4)
    divergent = abs(diff) > abs_tolerance and abs(diff_pct) > rel_tolerance
    status = "api_error" if api_error else ("divergent" if divergent else "ok")
    metadata = {"day": day, "customer_id": customer_id, "site": site, "account": name, "google_ads_cost": round(ads_cost, 2), "platform_cost": round(stored_cost, 2), "diff": diff, "diff_pct": diff_pct, "api_rows": api_rows, "api_error": api_error, "abs_tolerance": abs_tolerance, "rel_tolerance": rel_tolerance}
    conn.execute("INSERT OR REPLACE INTO google_ads_platform_spend_validation_daily (day, customer_id, account, site, google_ads_cost, platform_cost, diff, diff_pct, status, checked_at, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (day, customer_id, name, site, ads_cost, stored_cost, diff, diff_pct, status, checked_at, json.dumps(metadata, ensure_ascii=False, sort_keys=True)))
    if api_error:
        event(conn, now=checked_at, agent="consistencia-dados", priority="HIGH", title="Falha ao validar investimento Google Ads", message=f"Nao foi possivel consultar Google Ads para {site} em {day}: {api_error[:220]}", entity_type="customer", entity_id=customer_id, project_id=site, metadata=metadata, impact_score=85, next_action="Corrigir acesso/API Google Ads ou usar Apps Script de fechamento antes de confiar no investimento da plataforma.", auto_fix="google_ads_validation_api_error")
        return 1
    if divergent:
        direction = "menor" if stored_cost < ads_cost else "maior"
        event(conn, now=checked_at, agent="consistencia-dados", priority="HIGH", title="Investimento Ads divergente", message=f"{site} em {day}: Google Ads R${ads_cost:.2f}, plataforma R${stored_cost:.2f}. Plataforma esta {direction} em R${abs(diff):.2f} ({abs(diff_pct)*100:.2f}%).", entity_type="customer", entity_id=customer_id, project_id=site, metadata=metadata, impact_score=90, next_action="Bloquear decisao por ROI/investimento ate reconciliar Apps Script, ads_daily e conta Google Ads.", auto_fix="ads_spend_validation_divergent")
        return 1
    return 0


def fetch_ads_rows_for_day(service, *, account: dict, day: str) -> list[dict]:
    customer_id = clean_id(account.get("customer_id"))
    config_site = str(account.get("site") or account.get("name") or customer_id)
    account_name = str(account.get("name") or config_site)
    campaign_rows = {}
    campaign_query = f"SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, metrics.cost_micros, metrics.clicks, metrics.impressions, metrics.conversions FROM campaign WHERE segments.date = '{day}' AND campaign.status != REMOVED"
    for batch in service.search_stream(customer_id=customer_id, query=campaign_query):
        for result in batch.results:
            c = result.campaign; m = result.metrics; campaign_id = str(c.id)
            channel_type = str(c.advertising_channel_type.name if hasattr(c.advertising_channel_type, 'name') else c.advertising_channel_type)
            campaign_rows[campaign_id] = {'day': day, 'account': account_name, 'customer_id': customer_id, 'campaign_id': campaign_id, 'campaign': c.name or '', 'ad_group_id': '', 'ad_group_name': '', 'channel_type': channel_type, 'attr_level': 'campaign', 'attr_id': campaign_id, 'site': resolve_project_site(config_site, c.name), 'cost': float(m.cost_micros or 0) / 1000000, 'clicks': int(m.clicks or 0), 'impressions': int(m.impressions or 0), 'conversions': float(m.conversions or 0), 'daily_budget': 0.0, 'campaign_status': str(c.status.name if hasattr(c.status, 'name') else c.status), 'ad_group_status': ''}
    adgroup_rows = []
    adgroup_totals = {}
    adgroup_query = f"SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, ad_group.id, ad_group.name, ad_group.status, metrics.cost_micros, metrics.clicks, metrics.impressions, metrics.conversions FROM ad_group WHERE segments.date = '{day}' AND campaign.status != REMOVED AND ad_group.status != REMOVED"
    for batch in service.search_stream(customer_id=customer_id, query=adgroup_query):
        for result in batch.results:
            c = result.campaign; g = result.ad_group; m = result.metrics
            cost = float(m.cost_micros or 0) / 1000000
            clicks = int(m.clicks or 0)
            impressions = int(m.impressions or 0)
            conversions = float(m.conversions or 0)
            if cost <= 0 and impressions <= 0 and clicks <= 0:
                continue
            campaign_id = str(c.id); ad_group_id = str(g.id)
            channel_type = str(c.advertising_channel_type.name if hasattr(c.advertising_channel_type, 'name') else c.advertising_channel_type)
            adgroup_totals.setdefault(campaign_id, {'cost': 0.0, 'clicks': 0, 'impressions': 0, 'conversions': 0.0})
            adgroup_totals[campaign_id]['cost'] += cost
            adgroup_totals[campaign_id]['clicks'] += clicks
            adgroup_totals[campaign_id]['impressions'] += impressions
            adgroup_totals[campaign_id]['conversions'] += conversions
            attr_is_adgroup = channel_type.upper() == 'DEMAND_GEN'
            adgroup_rows.append({'day': day, 'account': account_name, 'customer_id': customer_id, 'campaign_id': campaign_id, 'campaign': c.name or '', 'ad_group_id': ad_group_id, 'ad_group_name': g.name or '', 'channel_type': channel_type, 'attr_level': 'ad_group' if attr_is_adgroup else 'campaign', 'attr_id': ad_group_id if attr_is_adgroup else campaign_id, 'site': resolve_project_site(config_site, c.name), 'cost': cost, 'clicks': clicks, 'impressions': impressions, 'conversions': conversions, 'daily_budget': 0.0, 'campaign_status': str(c.status.name if hasattr(c.status, 'name') else c.status), 'ad_group_status': str(g.status.name if hasattr(g.status, 'name') else g.status)})
    fallback = []
    for cid, row in campaign_rows.items():
        total = adgroup_totals.get(cid, {'cost': 0.0, 'clicks': 0, 'impressions': 0, 'conversions': 0.0})
        residual_cost = float(row['cost'] or 0) - float(total.get('cost') or 0)
        residual_clicks = int(row['clicks'] or 0) - int(total.get('clicks') or 0)
        residual_impressions = int(row['impressions'] or 0) - int(total.get('impressions') or 0)
        residual_conversions = float(row['conversions'] or 0) - float(total.get('conversions') or 0)
        if residual_cost > 0.000001 or residual_clicks > 0 or residual_impressions > 0:
            residual = dict(row)
            residual['cost'] = max(0.0, residual_cost)
            residual['clicks'] = max(0, residual_clicks)
            residual['impressions'] = max(0, residual_impressions)
            residual['conversions'] = max(0.0, residual_conversions)
            fallback.append(residual)
    return adgroup_rows + fallback

def repair_ads_daily_from_api(conn: sqlite3.Connection, service, *, account: dict, day: str, db_path: Path, checked_at: str) -> int:
    customer_id = clean_id(account.get("customer_id"))
    site = str(account.get("site") or account.get("name") or customer_id)
    rows = fetch_ads_rows_for_day(service, account=account, day=day)
    conn.execute("DELETE FROM ads_daily WHERE day=? AND customer_id=?", (day, customer_id))
    conn.executemany("""
        INSERT INTO ads_daily (
            day, account, customer_id, campaign_id, campaign, ad_group_id, ad_group_name,
            channel_type, attr_level, attr_id, site, cost, clicks, impressions, conversions,
            daily_budget, campaign_status, ad_group_status
        ) VALUES (
            :day, :account, :customer_id, :campaign_id, :campaign, :ad_group_id, :ad_group_name,
            :channel_type, :attr_level, :attr_id, :site, :cost, :clicks, :impressions, :conversions,
            :daily_budget, :campaign_status, :ad_group_status
        )
        ON CONFLICT(day, customer_id, campaign_id, ad_group_id, channel_type) DO UPDATE SET
            account=excluded.account, campaign=excluded.campaign, ad_group_name=excluded.ad_group_name,
            attr_level=excluded.attr_level, attr_id=excluded.attr_id, site=excluded.site, cost=excluded.cost,
            clicks=excluded.clicks, impressions=excluded.impressions, conversions=excluded.conversions,
            daily_budget=excluded.daily_budget, campaign_status=excluded.campaign_status, ad_group_status=excluded.ad_group_status
    """, rows)
    event(conn, now=checked_at, agent="consistencia-dados", priority="INFO", title="Investimento Ads reconciliado", message=f"{site} em {day}: ads_daily atualizado pela API do Google Ads com {len(rows)} linha(s).", entity_type="customer", entity_id=customer_id, project_id=site, metadata={"day": day, "customer_id": customer_id, "site": site, "rows": len(rows)}, impact_score=20, next_action="Manter conferencia horaria Google Ads real x plataforma.", auto_fix="ads_daily_repaired_from_google_ads_api")
    return len(rows)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--abs-tolerance", type=float, default=DEFAULT_ABS_TOLERANCE)
    parser.add_argument("--rel-tolerance", type=float, default=DEFAULT_REL_TOLERANCE)
    parser.add_argument("--repair", action="store_true")
    args = parser.parse_args()
    cfg = load_config(Path(args.config))
    db_path = Path(cfg.get("database", {}).get("path") or ROOT / "roas_cache.sqlite3")
    ads_cfg = cfg.get("google_ads") or {}
    accounts = [a for a in (ads_cfg.get("customers") or []) if valid_site(a.get("site") or a.get("name"))]
    checked_at = now_sp().isoformat(timespec="seconds")
    today = now_sp().date()
    days = [(today - timedelta(days=offset)).isoformat() for offset in range(max(1, args.days))]
    with sqlite3.connect(db_path, timeout=60) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=60000")
        ensure_table(conn)
        problems = 0
        try:
            client = google_ads_client(ads_cfg)
            service = client.get_service("GoogleAdsService")
        except Exception as exc:
            event(conn, now=checked_at, agent="consistencia-dados", priority="HIGH", title="Falha ao iniciar validador Google Ads", message=f"Validador nao conseguiu criar cliente Google Ads: {exc}", metadata={"error": str(exc)}, impact_score=90, next_action="Corrigir credenciais OAuth Google Ads na VPS antes de confiar no investimento consolidado.", auto_fix="google_ads_validation_client_error")
            conn.commit()
            print(json.dumps({"ok": False, "problems": 1, "error": str(exc)}, ensure_ascii=False))
            return 0
        for day in days:
            for account in accounts:
                before = problems
                problems += validate_account_day(conn, service, account=account, day=day, checked_at=checked_at, abs_tolerance=args.abs_tolerance, rel_tolerance=args.rel_tolerance)
                if args.repair and problems > before:
                    try:
                        repair_ads_daily_from_api(conn, service, account=account, day=day, db_path=db_path, checked_at=checked_at)
                        if validate_account_day(conn, service, account=account, day=day, checked_at=checked_at, abs_tolerance=args.abs_tolerance, rel_tolerance=args.rel_tolerance) == 0:
                            problems -= 1
                    except Exception as exc:
                        event(conn, now=checked_at, agent="consistencia-dados", priority="HIGH", title="Falha ao reconciliar investimento Ads", message=f"Nao foi possivel atualizar ads_daily por API para {account.get('site') or account.get('name')} em {day}: {exc}", entity_type="customer", entity_id=clean_id(account.get("customer_id")), project_id=str(account.get('site') or ''), metadata={"error": str(exc), "day": day}, impact_score=90, next_action="Corrigir reconciliacao antes de permitir decisao de ROI/investimento.", auto_fix="ads_daily_repair_failed")
        conn.commit()
    print(json.dumps({"ok": problems == 0, "problems": problems, "checked_at": checked_at, "days": days}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
