from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import secrets
import subprocess
import time
import yaml
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urlparse
import base64

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from agent_backlog import AGENT_BACKLOG, epic_summary, find_agent
from campaign_builder import apply_campaign_builder_draft, build_campaign_draft, country_targets, display_upload_sizes, google_ads_customers, model_campaign_prefill
from webapp_service import (
    app_today,
    exclude_project_campaign_placements,
    load_action_rows,
    load_agent_global_alerts,
    record_agent_alert_feedback,
    load_campaign_metric_history,
    load_dashboard_summary,
    load_gam_sync_rows,
    load_last_updated,
    load_nexus_agent_events,
    load_phase1_agent_dashboard,
    load_quality_os_dashboard,
    run_phase1_agent_observer,
    load_nexus_telegram_channels,
    load_nexus_telegram_messages,
    load_nexus_telegram_updates,
    load_project_campaign_blocks,
    load_project_campaign_placements,
    load_project_campaign_rows,
    load_attr_funnel,
    load_gam_top_countries,
    load_project_kpis,
    load_project_summary,
    traffic_session_project_periods,
    load_operational_health,
    google_ads_script_pending_actions,
    ingest_google_ads_script_placements,
    ingest_google_ads_script_settings,
    ingest_google_ads_script_spend,
    record_google_ads_script_result,
    record_google_ads_script_run,
    verify_google_ads_script_token,
    load_push_control_summary,
    load_push_installation_checks,
    load_push_optimization_summary,
    load_push_site_dashboard,
    record_gam_floor_manual_action,
    record_traffic_session,
    record_veraz_audio_lead,
    load_veraz_audio_leads,
    mark_veraz_audio_cta_click,
    record_campaign_bid_manual_action,
    record_campaign_budget_manual_action,
    record_campaign_status_manual_action,
    apply_initial_setup_google_ads,
    apply_initial_setup_ga4,
    apply_initial_setup_gtm,
    confirm_initial_setup_step,
    final_check_initial_setup,
    install_initial_setup_wordpress,
    load_initial_setup_detail,
    load_initial_setup_plans,
    is_julio_setup_domain,
    normalize_initial_setup_domain,
    save_initial_setup_plan,
    validate_initial_setup_wordpress,
    validate_initial_setup_clarity_api,
    load_runtime,
    _google_ads_client_from_runtime,
    _google_oauth_credentials_from_runtime,
    load_roi_control_actions,
    load_roi_control_latest_metrics,
    load_roi_control_outcomes,
    load_roi_control_runs,
    seed_nexus_war_room_messages,
)
from floor_bucket_service import floor_bucket_for_request, floor_bucket_javascript
from funnel_experiments import (
    assign_variant,
    create_experiment,
    experiment_javascript,
    experiment_history,
    get_experiment,
    list_experiments,
    pause_and_replace_variant,
    promote_variant,
    record_experiment_element_event,
    record_experiment_event,
    register_experiment_candidate,
    set_experiment_status,
    verify_proxy_request,
)
from clarity_service import get_clarity_snapshot
from telegram_notifier import flush_pending_telegram_messages
from telegram_notifier import process_telegram_update_payload
from telegram_notifier import process_telegram_updates
from telegram_notifier import queue_war_room_events
from telegram_notifier import sync_telegram_channels_from_updates
from quality_error_registry import register_exception


CONFIG_PATH = Path(os.getenv("ROAS_CONFIG_PATH", Path(__file__).with_name("config.yaml")))

AUTH_COOKIE_NAME = "roas_session"
AUTH_MAX_AGE = 60 * 60 * 12
AUTH_EXEMPT_PATHS = {"/health", "/login", "/logout", "/internal/agent-cycle", "/veraz-audio", "/veraz-audio/embed", "/veraz-audio/install-code", "/floor-bucket.js", "/push-tracker.js"}


def _auth_secret() -> str:
    secret = os.getenv("ROAS_AUTH_SECRET", "").strip()
    if not secret:
        raise RuntimeError("ROAS_AUTH_SECRET nao configurado no servico")
    return secret


def _password_hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _auth_password_hashes() -> dict[str, str]:
    raw = os.getenv("ROAS_AUTH_PASSWORD_HASHES", "")
    users: dict[str, str] = {}
    for item in raw.split(","):
        if ":" not in item:
            continue
        email, password_hash = item.split(":", 1)
        email = email.strip().lower()
        password_hash = password_hash.strip().lower()
        if email and password_hash:
            users[email] = password_hash
    if not users:
        raise RuntimeError("ROAS_AUTH_PASSWORD_HASHES nao configurado no servico")
    return users


def _allowed_auth_emails() -> set[str]:
    return set(_auth_password_hashes().keys())


def _sign_auth(payload: str) -> str:
    return hmac.new(_auth_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()


def _make_auth_cookie(email: str) -> str:
    expires = int(time.time()) + AUTH_MAX_AGE
    nonce = secrets.token_urlsafe(16)
    safe_email = email.strip().lower().replace(":", "")
    payload = f"{expires}:{nonce}:{safe_email}"
    return f"{payload}:{_sign_auth(payload)}"


def _is_authenticated(request: Request) -> bool:
    parts = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")
    if len(parts) != 4:
        return False
    expires, nonce, email, signature = parts
    if not expires.isdigit() or int(expires) < int(time.time()):
        return False
    safe_email = email.strip().lower()
    if safe_email not in _allowed_auth_emails():
        return False
    payload = f"{expires}:{nonce}:{safe_email}"
    return hmac.compare_digest(signature, _sign_auth(payload))


def _is_auth_exempt(path: str) -> bool:
    return path in AUTH_EXEMPT_PATHS or path.startswith("/telegram/webhook/") or path.startswith("/api/google-ads-script/") or path.startswith("/api/funnel-experiments/public/") or path in {"/api/veraz-audio/submit", "/api/veraz-audio/cta-click", "/api/floor-bucket", "/api/traffic-session", "/api/gam-top-countries", "/api/push/subscriber-event", "/api/customer-leads", "/funnel-experiments.js"}


def _login_page(error: str = "") -> str:
    error_html = f"<div class='login-error'>{html.escape(error)}</div>" if error else ""
    return f"""
    <!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
    <title>Login - ROAS Control Center</title>
    <style>body{{margin:0;min-height:100vh;display:grid;place-items:center;background:linear-gradient(135deg,#f3fbf5,#fff7df);font-family:Arial,sans-serif;color:#102018}}.login-card{{width:min(420px,calc(100vw - 32px));background:#fff;border:1px solid #d8e8dc;border-radius:24px;padding:32px;box-shadow:0 24px 70px rgba(16,32,24,.12)}}.eyebrow{{font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:#316b4b;font-weight:700}}h1{{margin:8px 0 20px;font-size:30px}}label{{display:block;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#607468;margin-bottom:8px}}input{{width:100%;box-sizing:border-box;border:1px solid #cbded1;border-radius:14px;padding:14px 16px;font-size:18px}}button{{width:100%;margin-top:18px;border:0;border-radius:999px;padding:14px 18px;background:#0f3b28;color:white;font-weight:800;cursor:pointer}}.login-error{{margin:0 0 14px;padding:10px 12px;border-radius:12px;background:#ffe8e4;color:#9b2d2d;font-weight:700}}</style>
    </head><body><form class='login-card' method='post' action='/login'><div class='eyebrow'>Acesso restrito</div><h1>ROAS Control Center</h1>{error_html}<label for='email'>Email</label><input id='email' name='email' type='email' autocomplete='username' autofocus required><label for='password'>Senha</label><input id='password' name='password' type='password' autocomplete='current-password' required><button type='submit'>Entrar</button></form></body></html>
    """

app = FastAPI(title="ROAS Control Center")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.wizseoservicesaustralia.com", "https://*.wizseoservicesaustralia.com", "https://*.foconadica.com", "https://lordkuma.com"],
    allow_origin_regex=r"https://([a-z0-9-]+\.)?(wizseoservicesaustralia\.com|foconadica\.com)$|https://lordkuma\.com",
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def require_login(request: Request, call_next):
    try:
        if not _is_auth_exempt(request.url.path) and not _is_authenticated(request):
            return RedirectResponse(url="/login", status_code=303)
        response = await call_next(request)
        if request.url.path == "/" or request.url.path.startswith("/projects") or request.url.path.startswith("/push-control"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response
    except Exception as exc:
        register_exception(
            "webapp",
            exc,
            scope=request.url.path,
            source="fastapi_middleware",
            evidence={"method": request.method, "path": request.url.path},
        )
        raise


def _money(value: float) -> str:
    return f"R${value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _percent(value: float) -> str:
    return f"{value * 100:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")


BR_TZ = ZoneInfo("America/Sao_Paulo")


def _to_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _br_datetime(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BR_TZ)
        else:
            dt = dt.astimezone(BR_TZ)
        return dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        return text


PUSH_EVENT_TYPES = {
    "pageview",
    "permission_state",
    "prompt_requested",
    "opt_in",
    "reject",
    "subscription_seen",
    "push_landing",
}


def _push_now_iso() -> str:
    return datetime.now(BR_TZ).replace(microsecond=0).isoformat()


def _push_db_path() -> Path:
    runtime = load_runtime(CONFIG_PATH)
    db_path_raw = str(runtime.get("db_path", "")).strip()
    if not db_path_raw:
        raise RuntimeError("db_path nao configurado")
    return Path(db_path_raw)


def _push_clean_str(value: object, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def _push_normalize_site(value: object) -> str:
    text = _push_clean_str(value, 300).lower()
    if not text:
        return ""
    if "://" in text:
        text = urlparse(text).netloc
    text = text.split("/")[0].split("?")[0].strip()
    if text.startswith("www."):
        text = text[4:]
    return text


def _push_hash(value: object) -> str:
    text = _push_clean_str(value, 1000)
    if not text:
        return ""
    salt = os.getenv("PUSH_TRACKING_SALT") or os.getenv("ROAS_AUTH_SECRET", "")
    return hmac.new(salt.encode("utf-8"), text.encode("utf-8"), hashlib.sha256).hexdigest()


def _ensure_push_table_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"pragma table_info({table})").fetchall()}
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"alter table {table} add column {name} {definition}")


def _ensure_push_subscriber_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists push_subscriber_events (
            id integer primary key autoincrement,
            created_at text not null,
            site text not null,
            subscriber_hash text not null,
            session_id text not null,
            event_type text not null,
            permission_state text,
            page_url text,
            page_path text,
            referrer text,
            utm_source text,
            utm_medium text,
            utm_campaign text,
            utm_content text,
            utm_term text,
            utm_creative text,
            push_provider text,
            push_website_id text,
            push_endpoint_hash text,
            payload_json text
        )
        """
    )
    conn.execute(
        """
        create table if not exists push_subscribers (
            subscriber_hash text primary key,
            site text not null,
            first_seen_at text not null,
            last_seen_at text not null,
            permission_state text,
            first_page_path text,
            last_page_path text,
            first_utm_source text,
            first_utm_medium text,
            first_utm_campaign text,
            last_utm_source text,
            last_utm_medium text,
            last_utm_campaign text,
            events_count integer not null default 0
        )
        """
    )
    _ensure_push_table_columns(
        conn,
        "push_subscriber_events",
        {
            "subscriber_hash": "text",
            "session_id": "text",
            "permission_state": "text",
            "page_url": "text",
            "page_path": "text",
            "referrer": "text",
            "utm_source": "text",
            "utm_medium": "text",
            "utm_campaign": "text",
            "utm_content": "text",
            "utm_term": "text",
            "utm_creative": "text",
            "push_provider": "text",
            "push_website_id": "text",
            "push_endpoint_hash": "text",
            "payload_json": "text",
        },
    )
    _ensure_push_table_columns(
        conn,
        "push_subscribers",
        {
            "subscriber_hash": "text",
            "site": "text",
            "first_seen_at": "text",
            "last_seen_at": "text",
            "permission_state": "text",
            "first_page_path": "text",
            "last_page_path": "text",
            "first_utm_source": "text",
            "first_utm_medium": "text",
            "first_utm_campaign": "text",
            "last_utm_source": "text",
            "last_utm_medium": "text",
            "last_utm_campaign": "text",
            "events_count": "integer default 0",
        },
    )
    conn.execute("create index if not exists idx_push_events_site_created on push_subscriber_events(site, created_at)")
    conn.execute("create index if not exists idx_push_events_subscriber on push_subscriber_events(subscriber_hash)")
    conn.execute("create index if not exists idx_push_events_type on push_subscriber_events(event_type)")
    conn.execute("create index if not exists idx_push_events_site_campaign on push_subscriber_events(site, utm_campaign)")


def _push_safe_payload(value: object) -> object:
    blocked = {"endpoint", "push_endpoint", "subscription", "auth", "p256dh", "keys"}

    def clean(obj: object) -> object:
        if isinstance(obj, dict):
            out: dict[str, object] = {}
            for key, item in obj.items():
                key_text = str(key)[:80]
                if key_text.lower() in blocked:
                    continue
                out[key_text] = clean(item)
            return out
        if isinstance(obj, list):
            return [clean(item) for item in obj[:20]]
        if isinstance(obj, str):
            return _push_clean_str(obj, 300)
        if isinstance(obj, (int, float, bool)) or obj is None:
            return obj
        return _push_clean_str(obj, 300)

    return clean(value)


def _push_period_range(period: str) -> tuple[str, str]:
    today = app_today()
    period = (period or "hoje").strip().lower()
    if period == "ontem":
        start = today - timedelta(days=1)
        end = today
    elif period in {"7d", "ultimos_7_dias", "ultimos-7-dias", "ultimos 7 dias"}:
        start = today - timedelta(days=6)
        end = today + timedelta(days=1)
    elif period == "mes":
        start = today.replace(day=1)
        end = today + timedelta(days=1)
    else:
        start = today
        end = today + timedelta(days=1)
    return start.isoformat(), end.isoformat()


@app.get("/push-tracker.js")
def push_tracker_js() -> Response:
    script = r"""
(function () {
  if (window.__kaizenPushTrackerLoaded) return;
  window.__kaizenPushTrackerLoaded = true;

  var ENDPOINT = "https://app.wizseoservicesaustralia.com/api/push/subscriber-event";
  var SITE = location.hostname.replace(/^www\./, "");
  var PROVIDER = detectProvider();

  function id(storage, key) {
    try {
      var current = storage.getItem(key);
      if (current) return current;
      var value = "kp_" + Date.now() + "_" + Math.random().toString(36).slice(2, 12);
      storage.setItem(key, value);
      return value;
    } catch (e) {
      return "kp_" + Date.now() + "_" + Math.random().toString(36).slice(2, 12);
    }
  }

  var subscriberId = id(window.localStorage, "kaizen_push_subscriber_id");
  var sessionId = id(window.sessionStorage, "kaizen_push_session_id");

  function utms() {
    var params = new URLSearchParams(location.search || "");
    return {
      utm_source: params.get("utm_source") || "",
      utm_medium: params.get("utm_medium") || "",
      utm_campaign: params.get("utm_campaign") || "",
      utm_content: params.get("utm_content") || "",
      utm_term: params.get("utm_term") || "",
      utm_creative: params.get("utm_creative") || ""
    };
  }

  function detectProvider() {
    if (window.PushAlertCo || window.PushAlert) return "PushAlert";
    if (window.OneSignal) return "OneSignal";
    return "unknown";
  }

  function post(eventType, extra) {
    var data = utms();
    data.event_type = eventType;
    data.site = SITE;
    data.subscriber_id = subscriberId;
    data.session_id = sessionId;
    data.permission_state = window.Notification ? Notification.permission : "";
    data.page_url = location.href;
    data.page_path = location.pathname || "/";
    data.referrer = document.referrer || "";
    data.push_provider = PROVIDER;
    data.created_at_client = new Date().toISOString();
    if (extra) {
      Object.keys(extra).forEach(function (key) { data[key] = extra[key]; });
    }
    var body = JSON.stringify(data);
    if (navigator.sendBeacon) {
      try {
        if (navigator.sendBeacon(ENDPOINT, new Blob([body], {type: "application/json"}))) return;
      } catch (e) {}
    }
    try {
      fetch(ENDPOINT, {method: "POST", headers: {"Content-Type": "application/json"}, body: body, keepalive: true, credentials: "omit"});
    } catch (e) {}
  }

  function isPushLanding() {
    var data = utms();
    var joined = [data.utm_source, data.utm_medium, document.referrer || ""].join(" ").toLowerCase();
    return /push|pushalert|notification|webpush|onesignal/.test(joined);
  }

  post("pageview");
  if (window.Notification) post("permission_state");
  if (isPushLanding()) post("push_landing");

  if ("serviceWorker" in navigator && navigator.serviceWorker.ready) {
    navigator.serviceWorker.ready.then(function (registration) {
      if (!registration.pushManager) return;
      return registration.pushManager.getSubscription();
    }).then(function (subscription) {
      if (!subscription) return;
      post("subscription_seen", {push_endpoint: subscription.endpoint || ""});
    }).catch(function () {});
  }

  if (window.Notification && typeof Notification.requestPermission === "function" && !Notification.__kaizenWrapped) {
    var original = Notification.requestPermission;
    Notification.requestPermission = function () {
      post("prompt_requested");
      var args = Array.prototype.slice.call(arguments);
      if (typeof args[0] === "function") {
        var callback = args[0];
        args[0] = function (permission) {
          post(permission === "granted" ? "opt_in" : "reject", {permission_state: permission});
          callback(permission);
        };
      }
      var result = original.apply(Notification, args);
      if (result && typeof result.then === "function") {
        return result.then(function (permission) {
          post(permission === "granted" ? "opt_in" : "reject", {permission_state: permission});
          return permission;
        });
      }
      return result;
    };
    Notification.__kaizenWrapped = true;
  }
})();
"""
    return Response(content=script.strip() + "\n", media_type="application/javascript", headers={"Cache-Control": "public, max-age=300"})


@app.post("/api/push/subscriber-event")
async def api_push_subscriber_event(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="JSON invalido") from exc

    event_type = _push_clean_str(payload.get("event_type"), 64)
    if event_type not in PUSH_EVENT_TYPES:
        raise HTTPException(status_code=400, detail="event_type invalido")

    page_url = _push_clean_str(payload.get("page_url"), 1000)
    parsed = urlparse(page_url) if page_url else None
    site = _push_normalize_site(payload.get("site") or (parsed.netloc if parsed else ""))
    if not site:
        raise HTTPException(status_code=400, detail="site ausente")

    session_id = _push_clean_str(payload.get("session_id"), 200)
    subscriber_id = _push_clean_str(payload.get("subscriber_id") or session_id or page_url, 500)
    subscriber_hash = _push_hash(subscriber_id)
    endpoint_hash = _push_hash(payload.get("push_endpoint") or payload.get("endpoint") or "")
    page_path = _push_clean_str(payload.get("page_path") or (parsed.path if parsed else "/"), 500) or "/"
    now = _push_now_iso()

    row = {
        "created_at": now,
        "site": site,
        "subscriber_hash": subscriber_hash,
        "session_id": session_id,
        "event_type": event_type,
        "permission_state": _push_clean_str(payload.get("permission_state"), 40),
        "page_url": page_url,
        "page_path": page_path,
        "referrer": _push_clean_str(payload.get("referrer"), 1000),
        "utm_source": _push_clean_str(payload.get("utm_source"), 200),
        "utm_medium": _push_clean_str(payload.get("utm_medium"), 200),
        "utm_campaign": _push_clean_str(payload.get("utm_campaign"), 200),
        "utm_content": _push_clean_str(payload.get("utm_content"), 200),
        "utm_term": _push_clean_str(payload.get("utm_term"), 200),
        "utm_creative": _push_clean_str(payload.get("utm_creative"), 200),
        "push_provider": _push_clean_str(payload.get("push_provider"), 80),
        "push_website_id": _push_clean_str(payload.get("push_website_id"), 200),
        "push_endpoint_hash": endpoint_hash,
        "payload_json": json.dumps(_push_safe_payload(payload), ensure_ascii=True),
    }

    try:
        with sqlite3.connect(_push_db_path()) as conn:
            _ensure_push_subscriber_tables(conn)
            conn.execute(
                """
                insert into push_subscriber_events (
                    created_at, site, subscriber_hash, session_id, event_type, permission_state, page_url, page_path,
                    referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term, utm_creative,
                    push_provider, push_website_id, push_endpoint_hash, payload_json
                ) values (
                    :created_at, :site, :subscriber_hash, :session_id, :event_type, :permission_state, :page_url, :page_path,
                    :referrer, :utm_source, :utm_medium, :utm_campaign, :utm_content, :utm_term, :utm_creative,
                    :push_provider, :push_website_id, :push_endpoint_hash, :payload_json
                )
                """,
                row,
            )
            exists = conn.execute("select 1 from push_subscribers where subscriber_hash = ?", (subscriber_hash,)).fetchone()
            if exists:
                conn.execute(
                    """
                    update push_subscribers
                       set last_seen_at = ?, permission_state = ?, last_page_path = ?,
                           last_utm_source = ?, last_utm_medium = ?, last_utm_campaign = ?,
                           events_count = events_count + 1
                     where subscriber_hash = ?
                    """,
                    (now, row["permission_state"], page_path, row["utm_source"], row["utm_medium"], row["utm_campaign"], subscriber_hash),
                )
            else:
                conn.execute(
                    """
                    insert into push_subscribers (
                        subscriber_hash, site, first_seen_at, last_seen_at, permission_state, first_page_path, last_page_path,
                        first_utm_source, first_utm_medium, first_utm_campaign, last_utm_source, last_utm_medium,
                        last_utm_campaign, events_count
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        subscriber_hash,
                        site,
                        now,
                        now,
                        row["permission_state"],
                        page_path,
                        page_path,
                        row["utm_source"],
                        row["utm_medium"],
                        row["utm_campaign"],
                        row["utm_source"],
                        row["utm_medium"],
                        row["utm_campaign"],
                    ),
                )
            conn.commit()
    except Exception as exc:
        print(f"PUSH_TELEMETRY_ERROR site={site} event={event_type}: {exc}")
        raise HTTPException(status_code=500, detail="erro ao registrar evento push") from exc

    print(f"PUSH_EVENT_RECORDED site={site} event={event_type}")
    return {"ok": True, "event_type": event_type}


@app.get("/api/push/subscriber-summary")
def api_push_subscriber_summary(site: str, period: str = "hoje") -> dict:
    site = _push_normalize_site(site)
    if not site:
        raise HTTPException(status_code=400, detail="site ausente")
    start, end = _push_period_range(period)

    with sqlite3.connect(_push_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_push_subscriber_tables(conn)
        counts = {
            row["event_type"]: int(row["total"])
            for row in conn.execute(
                """
                select event_type, count(*) as total
                  from push_subscriber_events
                 where site = ? and created_at >= ? and created_at < ?
                 group by event_type
                """,
                (site, start, end),
            ).fetchall()
        }
        subscribers = conn.execute(
            """
            select count(distinct subscriber_hash) as total
              from push_subscriber_events
             where site = ? and created_at >= ? and created_at < ?
            """,
            (site, start, end),
        ).fetchone()["total"]
        top_paths = [
            dict(row)
            for row in conn.execute(
                """
                select page_path, count(*) as events, count(distinct subscriber_hash) as subscribers
                  from push_subscriber_events
                 where site = ? and created_at >= ? and created_at < ?
                 group by page_path
                 order by events desc
                 limit 20
                """,
                (site, start, end),
            ).fetchall()
        ]
        top_campaigns = [
            dict(row)
            for row in conn.execute(
                """
                select utm_campaign, count(*) as events, count(distinct subscriber_hash) as subscribers
                  from push_subscriber_events
                 where site = ? and created_at >= ? and created_at < ? and coalesce(utm_campaign, '') <> ''
                 group by utm_campaign
                 order by events desc
                 limit 20
                """,
                (site, start, end),
            ).fetchall()
        ]

    prompts = counts.get("prompt_requested", 0)
    opt_ins = counts.get("opt_in", 0)
    rejects = counts.get("reject", 0)
    return {
        "site": site,
        "period": period,
        "date_start": start,
        "date_end": end,
        "subscribers": int(subscribers or 0),
        "events": counts,
        "opt_in_rate": (opt_ins / prompts) if prompts else 0,
        "reject_rate": (rejects / prompts) if prompts else 0,
        "top_paths": top_paths,
        "top_campaigns": top_campaigns,
    }


def _budget_manual_actions_latest(runtime: dict) -> dict[str, dict]:
    import sqlite3
    db_path = Path(runtime.get('db_path', ''))
    if not db_path.exists():
        return {}
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute("select name from sqlite_master where type='table' and name='campaign_budget_manual_actions'").fetchone()
            if not exists:
                return {}
            rows = conn.execute("select * from campaign_budget_manual_actions order by created_at desc limit 500").fetchall()
            output = {}
            for row in rows:
                data = dict(row)
                for key in (str(row['campaign_id'] or ''), str(row['attr_id'] or '')):
                    if key and key not in output:
                        output[key] = data
            return output
    except Exception:
        return {}


def _budget_increase_actions_today(runtime: dict) -> dict[str, dict]:
    import sqlite3
    db_path = Path(runtime.get('db_path', ''))
    if not db_path.exists():
        return {}
    today = app_today().isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute("select name from sqlite_master where type='table' and name='budget_increase_actions'").fetchone()
            if not exists:
                return {}
            rows = conn.execute("select campaign_id, old_budget, new_budget, status, created_at from budget_increase_actions where day=? and status in ('success', 'success_minimum_budget')", (today,)).fetchall()
            return {str(row['campaign_id']): dict(row) for row in rows}
    except Exception:
        return {}


def _roi_class(value: float) -> str:
    if value > 0:
        return "roi-positive"
    if value < 0:
        return "roi-negative"
    return "roi-neutral"


def _status_badge(status: str) -> str:
    normalized = str(status or "").strip().upper()
    css = "status-neutral"
    if normalized in {"OK", "EXECUTED_INTERNAL", "SYNCED_OK", "EXPORTED", "READY_TO_SYNC"}:
        css = "status-good"
    elif normalized in {"BLOCK", "CRITICAL", "SYNC_ERROR", "SKIPPED_RUN_LIMIT", "SKIPPED_COOLDOWN"}:
        css = "status-bad"
    elif normalized in {"WARN", "PENDING_INTERNAL", "DISPATCHED"}:
        css = "status-warn"
    return f"<span class='status-pill {css}'>{html.escape(status)}</span>"


def _base_styles(title: str) -> str:
    return f"""
    <html>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <title>{html.escape(title)}</title>
      <style>

      .path-explorer-v2{{margin:18px 0 22px;padding:18px;border:1px solid #d5e5d9;border-radius:22px;background:#fff;box-shadow:0 18px 45px rgba(17,52,41,.08);overflow:hidden}}
      .path-explorer-head{{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:16px}}
      .path-explorer-head h2{{margin:4px 0;font-size:28px;line-height:1;color:#071c18}}
      .path-explorer-head p{{margin:0;color:#557268;font-weight:600}}
      .path-board{{display:grid;grid-template-columns:repeat(5,minmax(170px,1fr));gap:18px;overflow-x:auto;padding:4px 2px 18px}}
      .path-column{{min-width:170px;position:relative}}
      .path-column:not(:last-child)::after{{content:'?';position:absolute;right:-16px;top:48px;color:#24443a;font-size:22px;font-weight:900}}
      .path-column header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}}
      .path-column header small{{text-transform:uppercase;letter-spacing:.08em;font-size:10px;font-weight:900;color:#18352c}}
      .path-column header button{{border:0;background:transparent;color:#18352c;font-weight:900;cursor:pointer}}
      .path-dimension{{display:flex;align-items:center;gap:7px;height:36px;padding:0 10px;border:1px solid #d4dee0;border-radius:4px;background:#fafafa;color:#394c4a;font-size:12px;margin-bottom:10px;box-shadow:0 1px 0 rgba(0,0,0,.03)}}
      .path-dimension span{{display:grid;place-items:center;width:18px;height:18px;background:#eef3f4;border-radius:2px;color:#50605f}}
      .path-dimension b{{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
      .path-items{{display:grid;gap:8px}}
      .path-row{{display:grid;grid-template-columns:10px 1fr;gap:8px;align-items:start;min-height:32px}}
      .path-row i{{display:block;width:10px;min-height:18px;background:#88b8ff;border-radius:1px}}
      .path-node{{display:grid;gap:2px;text-decoration:none;color:#122821;font-size:12px;line-height:1.15;padding:3px 0}}
      .path-node span{{overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;color:#28463d}}
      .path-node strong{{font-size:13px;color:#000;font-weight:900}}
      .path-node small{{font-size:10px;color:#557268;font-weight:800;line-height:1.2}}
      .path-node em{{font-style:normal;font-size:10px;color:#145d43;font-weight:900;line-height:1.25}}
      .path-node em.muted{{color:#7b8b86;font-weight:700}}
      .path-node mark{{display:inline-block;margin-top:3px;padding:3px 5px;border-radius:8px;background:#ffe2dc;color:#9b2f24;font-size:10px;font-weight:900}}
      .metric-scope{{color:#789089!important;font-weight:700!important}}
      .path-node:hover span{{color:#005bd3;text-decoration:underline}}
      .path-more span{{color:#005bd3;font-weight:800}}
      .path-start{{background:#f2f5ff;padding:6px 8px;border-left:4px solid #80adf5;min-height:170px;align-content:start}}
      @media(max-width:900px){{.path-explorer-head{{display:block}}.path-board{{grid-template-columns:repeat(5,190px)}}}}
        :root {{
          --ink: #112018;
          --muted: #587062;
          --line: #d9e4db;
          --paper: #f6f5ef;
          --paper-strong: #ffffff;
          --mint: #d9f0df;
          --sage: #93b09b;
          --forest: #1e6b43;
          --brick: #9b2d2d;
          --amber: #b07a15;
          --sky: #dceefc;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          font-family: "Segoe UI", Arial, sans-serif;
          color: var(--ink);
          background:
            radial-gradient(circle at top left, #eef7f0, transparent 28%),
            radial-gradient(circle at bottom right, #f8ead9, transparent 20%),
            var(--paper);
        }}
        .shell {{
          max-width: 1440px;
          margin: 0 auto;
          padding: 24px;
        }}
        .masthead {{
          display: block;
          margin-bottom: 22px;
        }}
        .hero, .aside, .panel {{
          background: rgba(255,255,255,0.92);
          border: 1px solid var(--line);
          border-radius: 20px;
          box-shadow: 0 16px 40px rgba(17, 32, 24, 0.08);
        }}
        .hero {{
          padding: 28px;
          background:
            linear-gradient(135deg, rgba(217,240,223,0.95), rgba(255,255,255,0.9)),
            #fff;
        }}
        .hero h1 {{
          margin: 0 0 10px;
          font-size: 34px;
          line-height: 1.05;
          letter-spacing: -0.03em;
        }}
        .hero p {{
          margin: 0;
          color: var(--muted);
          font-size: 15px;
          max-width: 720px;
        }}
        .project-hero-meta {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin: 16px 0 0;
        }}
        .project-hero-pill {{
          min-width: 180px;
          padding: 12px 14px;
          border: 1px solid rgba(17,32,24,0.10);
          border-radius: 16px;
          background: rgba(255,255,255,0.62);
        }}
        .project-hero-pill span {{
          display: block;
          font-size: 10px;
          letter-spacing: .14em;
          text-transform: uppercase;
          color: var(--muted);
          margin-bottom: 4px;
        }}
        .project-hero-pill strong {{
          display: block;
          font-size: 18px;
          line-height: 1.1;
        }}
        .aside {{
          padding: 18px 20px;
          display: flex;
          flex-direction: column;
          justify-content: flex-start;
          gap: 18px;
          align-self: start;
        }}
        .aside-label {{
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.14em;
          color: var(--muted);
        }}
        .aside strong {{
          display: block;
          font-size: 28px;
          margin-top: 6px;
        }}
        .nav {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin-top: 16px;
        }}
        .nav a {{
          text-decoration: none;
          color: var(--ink);
          padding: 10px 14px;
          border-radius: 999px;
          background: rgba(17,32,24,0.05);
          border: 1px solid rgba(17,32,24,0.08);
          font-size: 14px;
        }}
        .hero-actions {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin-top: 18px;
        }}
        .hero-actions form {{
          margin: 0;
        }}
        .action-button, .action-link {{
          appearance: none;
          border: none;
          cursor: pointer;
          padding: 11px 16px;
          border-radius: 999px;
          font-size: 14px;
          font-weight: 700;
          color: #fff;
          background: var(--forest);
          box-shadow: 0 10px 24px rgba(30, 107, 67, 0.18);
          display: inline-flex;
          align-items: center;
          justify-content: center;
          text-decoration: none;
        }}
        .action-button.secondary, .action-link.secondary {{
          background: var(--ink);
        }}
        .nav a.active {{
          background: var(--ink);
          color: #fff;
        }}
        .metrics {{
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 14px;
          margin-bottom: 22px;
        }}
        .metric {{
          background: rgba(255,255,255,0.88);
          border: 1px solid var(--line);
          border-radius: 18px;
          padding: 18px;
        }}
        .metric .label {{
          color: var(--muted);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.12em;
        }}
        .metric .value {{
          margin-top: 10px;
          font-size: 28px;
          font-weight: 700;
        }}
        .metric .sub {{
          margin-top: 8px;
          color: var(--muted);
          font-size: 13px;
        }}
        .project-summary-panel {{
          margin-bottom: 18px;
        }}
        .project-summary-table {{
          width: 100%;
          min-width: 860px;
          border-collapse: collapse;
        }}
        .project-summary-table th,
        .project-summary-table td {{
          padding: 14px 12px;
          border-bottom: 1px solid var(--line);
          vertical-align: middle;
        }}
        .project-summary-table thead th {{
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.12em;
          color: var(--muted);
          background: rgba(248,250,249,0.9);
        }}
        .project-summary-table tbody th {{
          font-size: 14px;
          font-weight: 700;
          color: var(--ink);
          text-transform: uppercase;
          letter-spacing: 0.08em;
          width: 170px;
        }}
        .project-summary-table .summary-cell {{
          font-size: 16px;
          font-weight: 700;
        }}
        .project-summary-table .summary-row-today td,
        .project-summary-table .summary-row-today th {{
          background: #fff8e8;
        }}
        .project-summary-table .summary-row-yesterday td,
        .project-summary-table .summary-row-yesterday th {{
          background: #eef6ff;
        }}
        .project-summary-table .summary-row-7d td,
        .project-summary-table .summary-row-7d th {{
          background: #eefaf2;
        }}
        .project-summary-table .summary-row-month td,
        .project-summary-table .summary-row-month th {{
          background: #f7f0ff;
        }}
        .project-summary-table .summary-label {{
          display: block;
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.11em;
          color: var(--muted);
          margin-bottom: 4px;
        }}
        .project-summary-table .summary-value {{
          display: block;
          font-size: 18px;
          font-weight: 700;
        }}
        .project-summary-table .summary-value.roi-positive {{
          color: var(--green);
        }}
        .project-summary-table .summary-value.roi-negative {{
          color: var(--red);
        }}
        .grid {{
          display: grid;
          grid-template-columns: 2fr 1fr;
          gap: 18px;
        }}
        .grid-full {{
          display: block;
        }}
        .panel {{
          padding: 18px;
          overflow: hidden;
          box-shadow: 0 14px 38px rgba(17, 32, 24, 0.07);
        }}
        .panel h2 {{
          margin: 0 0 4px;
          font-size: 20px;
        }}
        .panel p {{
          margin: 0 0 14px;
          color: var(--muted);
          font-size: 14px;
        }}
        .agent-alert-grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 12px;
          margin: 14px 0 18px;
        }}
        .agent-alert {{
          border: 1px solid #d7e5da;
          border-left: 7px solid var(--forest);
          border-radius: 16px;
          padding: 14px 15px;
          background: #f4fbf6;
          box-shadow: 0 10px 24px rgba(17,32,24,0.06);
        }}
        .agent-alert.high {{ border-left-color: var(--brick); background: #fff1ee; }}
        .agent-alert.medium {{ border-left-color: var(--amber); background: #fff8e8; }}
        .agent-alert.opportunity {{ border-left-color: var(--forest); background: #edf8f0; }}
        .agent-alert strong {{ display: block; font-size: 15px; margin: 6px 0; }}
        .agent-alert .agent-meta {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 8px; }}
        .agent-badge {{
          display: inline-flex;
          align-items: center;
          border-radius: 999px;
          padding: 4px 8px;
          font-size: 11px;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: .05em;
          background: rgba(17,32,24,.08);
          color: var(--ink);
        }}
        .agent-alert p {{ margin: 0; color: var(--ink); font-size: 13px; line-height: 1.35; }}
        .agent-alert small {{ display: block; margin-top: 8px; color: var(--muted); font-weight: 700; }}
        table {{
          width: 100%;
          border-collapse: collapse;
          font-size: 14px;
          table-layout: auto;
        }}
        .projects-table {{
          min-width: 1680px;
        }}
        .project-status-cell {{ min-width: 150px; vertical-align: top; }}
        .project-status-pill {{ display: inline-flex; border-radius: 999px; padding: 6px 10px; font-size: 11px; font-weight: 950; text-transform: uppercase; letter-spacing: .04em; }}
        .project-status-pill.good {{ background: #dcfce7; color: #14532d; }}
        .project-status-pill.warn {{ background: #fff3cd; color: #7a5300; }}
        .project-status-pill.bad {{ background: #fee2e2; color: #991b1b; }}
        .project-status-pill.neutral {{ background: #eef2f7; color: #475569; }}
        .project-action-cell {{ min-width: 120px; vertical-align: top; }}
        .project-open-button {{ display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; background: var(--forest); color: #fff; padding: 8px 12px; font-size: 12px; font-weight: 950; }}
        .project-open-button:hover {{ color: #fff; filter: brightness(.95); }}
        .table-scroll {{
          overflow-x: auto;
          border: 1px solid var(--line);
          border-radius: 18px;
          background: #fff;
          box-shadow: inset 0 0 0 1px rgba(255,255,255,0.55);
        }}
        .table-scroll table {{
          min-width: 980px;
        }}
        .campaign-table {{
          min-width: 0;
          width: 100%;
          table-layout: fixed;
        }}
        th, td {{
          padding: 11px 10px;
          border-bottom: 1px solid var(--line);
          text-align: left;
          vertical-align: middle;
          resize: horizontal;
          overflow: auto;
        }}
        th:first-child, td:first-child {{
          resize: horizontal;
        }}
        .project-name-cell {{
          min-width: 240px;
          max-width: 420px;
        }}
        .col-spend {{
          background: rgba(176, 122, 21, 0.08);
        }}
        .col-revenue {{
          background: rgba(30, 107, 67, 0.08);
        }}
        .col-roi {{
          background: rgba(220, 238, 252, 0.7);
        }}
        .period-title, .period-cell {{
          border-left: 1px solid rgba(17,32,24,0.08);
        }}
        .period-title {{
          text-align: center;
          font-size: 12px;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: #486052;
          border-bottom: 1px solid rgba(17,32,24,0.14);
          background: #f7faf6;
        }}
        .period-cell {{
          background: #fff;
          min-width: 0;
          width: 12%;
          resize: none;
          overflow: visible;
        }}
        .period-cell-today {{ background: #fff8e8; }}
        .period-cell-yesterday {{ background: #eef6ff; }}
        .period-cell-7d {{ background: #eefaf2; }}
        .period-cell-month {{ background: #f7f0ff; }}
        .period-title-today {{ background: #fff1c8; color: #6d4c00; }}
        .period-title-yesterday {{ background: #dbeafe; color: #1e3a8a; }}
        .period-title-7d {{ background: #dcfce7; color: #14532d; }}
        .period-title-month {{ background: #f3e8ff; color: #581c87; }}
        .campaign-table th:nth-child(1), .campaign-table td:nth-child(1) {{ width: 31%; }}
        .campaign-table th:nth-child(2), .campaign-table td:nth-child(2) {{ width: 13%; }}
        .campaign-table th:nth-child(3), .campaign-table td:nth-child(3) {{ width: 16%; }}
        .campaign-table th:nth-child(n+4), .campaign-table td:nth-child(n+4) {{ width: 10%; }}
        .period-stack {{
          display: grid;
          gap: 4px;
          font-size: 12px;
          line-height: 1.25;
        }}
        .period-line {{
          display: flex;
          justify-content: space-between;
          gap: 10px;
          color: #5d6b61;
        }}
        .period-line.profit-line {{ margin: 1px 0 2px; padding: 4px 0; border-top: 1px dashed rgba(17,32,24,0.12); border-bottom: 1px dashed rgba(17,32,24,0.08); }}
        .period-line strong {{
          color: #102017;
          font-size: 13px;
        }}
        .period-line .roi-positive, .period-line .roi-negative {{
          font-weight: 800;
        }}
        .pmr-line {{
          margin-top: 2px;
          padding-top: 5px;
          border-top: 1px solid rgba(17,32,24,0.08);
        }}
        .pmr-line strong {{
          color: #0f5132;
        }}
        .ecpm-line strong {{
          color: #334155;
        }}
        .metric-value {{
          cursor: help;
          text-decoration: underline dotted rgba(15,81,50,0.35);
          text-underline-offset: 3px;
        }}
        .metric-chart-popover {{
          position: fixed;
          z-index: 9999;
          width: 420px;
          padding: 18px;
          border: 1px solid #cfe4d5;
          border-radius: 16px;
          background: #ffffff;
          box-shadow: 0 18px 50px rgba(10,45,25,0.18);
          color: #102017;
          pointer-events: auto;
        }}
        .metric-chart-title {{ font-weight: 900; font-size: 15px; margin-bottom: 3px; }}
        .metric-chart-subtitle {{ font-size: 12px; color: #607468; margin-bottom: 12px; }}
        .metric-chart-stats {{ display: grid; grid-template-columns: 1fr 1fr; gap: 7px 16px; font-size: 12px; color: #607468; }}
        .metric-chart-stats strong {{ color: #102017; }}
        .metric-chart-svg {{ display:block; width: 100%; height: 150px; margin: 4px 0 12px; pointer-events: auto; }}
        .metric-chart-bar {{ pointer-events: auto; cursor: crosshair; transition: opacity .12s ease, filter .12s ease; }}
        .metric-chart-bar:hover {{ opacity: 1; filter: drop-shadow(0 0 2px rgba(15,81,50,.45)); }}
        .metric-chart-day-value {{ margin: 0 0 10px; padding: 7px 9px; border-radius: 12px; background: #f3fbf5; font-size: 12px; color: #607468; }}
        .metric-chart-day-value strong {{ color: #102017; }}


        .efficiency-cell {{
          min-width: 260px;
          max-width: 310px;
          background: #fbfdf9;
          vertical-align: top;
        }}
        .efficiency-stack {{
          display: grid;
          gap: 7px;
          font-size: 12px;
          line-height: 1.25;
        }}
        .efficiency-head {{
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 8px;
        }}
        .efficiency-goal {{
          font-weight: 950;
          color: #102017;
          white-space: normal;
          line-height: 1.15;
          max-width: 145px;
        }}
        .efficiency-metrics {{
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 6px;
        }}
        .efficiency-line {{
          display: grid;
          gap: 2px;
          padding: 6px 7px;
          border: 1px solid rgba(17,32,24,.08);
          border-radius: 10px;
          background: #ffffff;
          color: #607468;
        }}
        .efficiency-line span {{
          font-size: 10px;
          text-transform: uppercase;
          letter-spacing: .05em;
        }}
        .efficiency-line strong {{
          color: #102017;
          font-size: 13px;
        }}
        .efficiency-badge {{
          display: inline-flex;
          width: fit-content;
          align-items: center;
          border-radius: 999px;
          padding: 5px 8px;
          font-size: 10px;
          font-weight: 950;
          border: 1px solid rgba(17,32,24,.12);
          background: #eef4ef;
          color: #345044;
          white-space: nowrap;
        }}
        .efficiency-badge.good {{ background: #dcfce7; color: #11643c; border-color: #b7efc9; }}
        .efficiency-badge.warn {{ background: #fff3cd; color: #7a5300; border-color: #f4d06f; }}
        .efficiency-badge.bad {{ background: #fee2e2; color: #a61b1b; border-color: #fecaca; }}
        .efficiency-diagnosis {{
          color: #506559;
          font-size: 11px;
          background: #f3faf5;
          border-left: 3px solid #9bcfaf;
          border-radius: 9px;
          padding: 6px 7px;
        }}
        .efficiency-change {{
          display: grid;
          gap: 6px;
          color: #174532;
          font-size: 11px;
          background: linear-gradient(135deg, #ecfdf3 0%, #ffffff 100%);
          border: 1px solid #8bd7a8;
          border-radius: 12px;
          padding: 8px;
          box-shadow: 0 6px 18px rgba(18, 92, 54, .08);
        }}
        .efficiency-change-title {{
          font-size: 10px;
          font-weight: 950;
          color: #0f6b43;
          text-transform: uppercase;
          letter-spacing: .05em;
        }}
        .efficiency-change-grid {{
          display: grid;
          grid-template-columns: 1fr 18px 1fr;
          align-items: center;
          gap: 5px;
        }}
        .efficiency-change-item {{
          display: grid;
          gap: 2px;
          padding: 5px 6px;
          border-radius: 9px;
          background: #ffffff;
          border: 1px solid rgba(15, 107, 67, .10);
        }}
        .efficiency-change-item span {{
          color: #607468;
          font-size: 9px;
          font-weight: 900;
          text-transform: uppercase;
          letter-spacing: .04em;
        }}
        .efficiency-change-item strong {{
          color: #102017;
          font-size: 13px;
        }}
        .efficiency-change-arrow {{
          color: #0f6b43;
          font-weight: 950;
          text-align: center;
        }}
        .efficiency-change-time {{
          color: #506559;
          font-size: 10px;
        }}
        .efficiency-change-time strong {{ color: #174532; }}
        .efficiency-action-box {{
          display: grid;
          gap: 6px;
          margin-top: 1px;
          padding: 7px;
          border: 1px dashed rgba(19,115,70,.28);
          border-radius: 12px;
          background: #ffffff;
        }}
        .efficiency-action-title {{
          font-size: 10px;
          font-weight: 950;
          color: #137346;
          letter-spacing: .04em;
          text-transform: uppercase;
        }}
        .efficiency-action-row {{
          display: grid;
          grid-template-columns: 78px 1fr;
          gap: 6px;
          align-items: center;
        }}
        .efficiency-cpa-input {{
          width: 100%;
          border: 1px solid #cbded1;
          border-radius: 999px;
          padding: 6px 8px;
          font-size: 11px;
          font-weight: 850;
          background: #fff;
        }}
        .efficiency-reason-input {{
          width: 100%;
          min-height: 34px;
          border: 1px solid #cbded1;
          border-radius: 10px;
          padding: 6px 8px;
          font-size: 11px;
          resize: vertical;
          background: #fff;
        }}
        .efficiency-action-button {{
          border: 0;
          border-radius: 999px;
          background: #137346;
          color: #fff;
          font-weight: 950;
          font-size: 11px;
          padding: 7px 9px;
          cursor: pointer;
          box-shadow: 0 4px 10px rgba(19,115,70,.18);
        }}
        .efficiency-action-button.saved {{ background: #0ea77a; }}
        .efficiency-action-status {{
          font-size: 10px;
          color: #607468;
          font-weight: 800;
        }}


        .diagnosis-panel {{
          display: grid;
          gap: 14px;
        }}
        .diagnosis-hero {{
          border: 1px solid rgba(17,32,24,.10);
          border-radius: 18px;
          padding: 16px;
          background: linear-gradient(135deg, rgba(236,250,240,.95), rgba(255,255,255,.92));
        }}
        .diagnosis-hero.warn {{ background: linear-gradient(135deg, #fff8df, #ffffff); }}
        .diagnosis-hero.bad {{ background: linear-gradient(135deg, #fff0ed, #ffffff); }}
        .diagnosis-hero.good {{ background: linear-gradient(135deg, #e8f9ef, #ffffff); }}
        .diagnosis-kicker {{
          font-size: 11px;
          letter-spacing: .12em;
          text-transform: uppercase;
          color: #607468;
          font-weight: 900;
        }}
        .diagnosis-title {{
          margin-top: 5px;
          font-size: 20px;
          font-weight: 950;
          color: #102017;
        }}
        .diagnosis-next {{
          margin-top: 8px;
          color: #345044;
          font-weight: 800;
        }}
        .diagnosis-grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
          gap: 10px;
        }}
        .diagnosis-card {{
          border: 1px solid rgba(17,32,24,.10);
          border-radius: 16px;
          padding: 12px;
          background: rgba(255,255,255,.78);
        }}
        .diagnosis-card span {{
          display: block;
          font-size: 10px;
          letter-spacing: .10em;
          text-transform: uppercase;
          color: #607468;
          font-weight: 900;
          margin-bottom: 4px;
        }}
        .diagnosis-card strong {{ display: block; font-size: 14px; color: #102017; }}
        .diagnosis-card p {{ margin: 5px 0 0; font-size: 12px; color: #607468; line-height: 1.35; }}
        .diagnosis-steps {{
          display: grid;
          gap: 8px;
        }}
        .diagnosis-step {{
          display: grid;
          grid-template-columns: 22px 1fr;
          gap: 8px;
          align-items: start;
          font-size: 13px;
          color: #345044;
        }}
        .diagnosis-dot {{
          width: 18px;
          height: 18px;
          border-radius: 999px;
          display: inline-grid;
          place-items: center;
          background: #e5efe8;
          color: #0f5132;
          font-size: 11px;
          font-weight: 900;
        }}

        .sticky-campaign {{
          position: sticky;
          z-index: 2;
          background: rgba(255,255,255,0.97);
          left: 0;
          min-width: 390px;
          max-width: 590px;
        }}
        thead .sticky-campaign {{
          z-index: 3;
          background: #f8fbf6;
        }}
        .campaign-table tbody tr:hover td {{
          background-color: rgba(30, 107, 67, 0.055);
        }}
        .campaign-row {{
          cursor: pointer;
        }}
        .campaign-row td {{
          transition: background-color 0.16s ease;
        }}
        .campaign-row.is-open td {{
          background-color: rgba(30, 107, 67, 0.08);
        }}
        .campaign-row.agent-focus td {{
          background-color: #fff6d8 !important;
          box-shadow: inset 0 0 0 2px rgba(218, 151, 36, .32);
        }}
        .campaign-main-cell {{
          display: flex;
          align-items: flex-start;
          gap: 10px;
        }}
        .campaign-name-stack {{
          display: flex;
          flex-direction: column;
          gap: 7px;
        }}
        .campaign-ad-group-name {{
          color: #4c675f;
          font-size: 12px;
          font-weight: 800;
          line-height: 1.25;
        }}
        .campaign-title-link {{
          color: inherit;
          font-weight: 800;
          text-decoration: none;
        }}
        .campaign-title-link:hover {{
          color: var(--green);
          text-decoration: underline;
        }}
        .campaign-inline-actions {{
          display: flex;
          flex-wrap: wrap;
          gap: 7px;
        }}
        .campaign-meta-row {{ display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-top: 1px; }}
        .attr-chip {{ display: inline-flex; align-items: center; gap: 5px; border: 1px solid rgba(30,107,67,0.16); background: #eef8f1; color: #174532; border-radius: 999px; padding: 4px 8px; font-size: 10px; font-weight: 900; letter-spacing: .02em; }}
        .attr-copy-button {{ border: 0; background: #ffffff; color: var(--forest); border-radius: 999px; padding: 2px 6px; font-size: 9px; font-weight: 950; cursor: pointer; }}
        .campaign-inline-action {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border: 1px solid rgba(17, 32, 24, 0.14);
          background: #fff;
          color: var(--forest);
          border-radius: 999px;
          padding: 5px 9px;
          font-size: 11px;
          font-weight: 700;
          cursor: pointer;
          text-decoration: none;
        }}
        .campaign-inline-action:hover,
        .campaign-inline-action.active {{
          background: var(--forest);
          border-color: var(--forest);
          color: #fff;
        }}
        .campaign-status-action {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border: 1px solid rgba(17, 32, 24, 0.14);
          border-radius: 999px;
          padding: 5px 9px;
          font-size: 11px;
          font-weight: 850;
          cursor: pointer;
        }}
        .campaign-status-action.pause {{
          background: #fff7ed;
          color: #9a3412;
          border-color: #fed7aa;
        }}
        .campaign-status-action.enable {{
          background: #ecfdf5;
          color: #166534;
          border-color: #bbf7d0;
        }}
        .campaign-status-action.saved {{
          background: var(--forest);
          border-color: var(--forest);
          color: #fff;
        }}
        .campaign-caret {{
          width: 18px;
          height: 18px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border-radius: 999px;
          background: rgba(17, 32, 24, 0.08);
          color: var(--forest);
          flex: 0 0 auto;
          transition: transform 0.16s ease, background-color 0.16s ease;
        }}
        .campaign-row.is-open .campaign-caret {{
          transform: rotate(90deg);
          background: rgba(30, 107, 67, 0.16);
        }}
        .campaign-detail-row td {{
          padding: 0;
          background: #f7fbf8;
          border-bottom: 1px solid var(--line);
          max-width: 0;
        }}
        .campaign-detail-shell {{
          width: 100%;
          max-width: none;
          overflow: hidden;
          box-sizing: border-box;
          padding: 16px 18px 18px 54px;
          border-top: 1px solid rgba(30, 107, 67, 0.12);
          background:
            linear-gradient(180deg, rgba(240, 248, 242, 0.98), rgba(248, 252, 249, 0.98));
        }}
        .campaign-detail-toolbar {{
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 14px;
          margin-bottom: 12px;
          flex-wrap: wrap;
        }}
        .campaign-detail-title {{
          font-size: 14px;
          font-weight: 700;
          color: var(--ink);
        }}
        .campaign-detail-note {{
          color: var(--muted);
          font-size: 12px;
          margin-top: 3px;
        }}
        .campaign-detail-tabs {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }}
        .campaign-detail-actions {{
          display: flex;
          flex-wrap: wrap;
          justify-content: flex-end;
          gap: 8px;
        }}
        .campaign-detail-tab {{
          border: 1px solid rgba(17, 32, 24, 0.12);
          background: #fff;
          color: var(--ink);
          border-radius: 999px;
          padding: 8px 12px;
          font-size: 12px;
          font-weight: 700;
          cursor: pointer;
        }}
        .campaign-detail-tab.active {{
          background: var(--forest);
          color: #fff;
          border-color: transparent;
        }}
        .placement-exclude-button {{
          border: 1px solid rgba(153, 27, 27, 0.28);
          background: #fff;
          color: var(--brick);
          border-radius: 999px;
          padding: 8px 12px;
          font-size: 12px;
          font-weight: 800;
          cursor: pointer;
        }}
        .placement-exclude-button:hover {{
          background: var(--brick);
          color: #fff;
        }}
        .placement-filter-bar {{
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 4px 6px;
          border: 1px solid #d8e8dc;
          border-radius: 999px;
          background: #fff;
          white-space: nowrap;
        }}
        .placement-filter-bar label {{
          display: inline-flex;
          align-items: center;
          gap: 4px;
          font-size: 11px;
          font-weight: 700;
          color: #315848;
        }}
        .placement-filter-bar input {{
          width: 62px;
          border: 1px solid #bdd8c6;
          border-radius: 999px;
          padding: 5px 7px;
          font: inherit;
          font-size: 11px;
        }}
        .placement-filter-button {{
          border: 1px solid #0b6b3f;
          border-radius: 999px;
          background: #0f7a49;
          color: #fff;
          padding: 6px 9px;
          font-size: 11px;
          font-weight: 800;
          cursor: pointer;
        }}
        .placement-download-button {{
          border: 1px solid #0f7a49;
          border-radius: 999px;
          background: #eefbf3;
          color: #0f6b3d;
          padding: 6px 9px;
          font-size: 11px;
          font-weight: 800;
          cursor: pointer;
        }}
        .placement-row-filtered td {{
          background: #ecfdf3 !important;
        }}
        .placement-check-cell {{
          width: 28px;
          text-align: center;
        }}
        .campaign-block-table tr.placement-row-selected td {{
          background: #fff3a6 !important;
          box-shadow: inset 0 1px 0 rgba(120, 83, 0, 0.18), inset 0 -1px 0 rgba(120, 83, 0, 0.18);
        }}
        .campaign-block-table tr.placement-row-selected td:first-child {{
          box-shadow: inset 4px 0 0 #d97706, inset 0 1px 0 rgba(120, 83, 0, 0.18), inset 0 -1px 0 rgba(120, 83, 0, 0.18);
        }}


        .campaign-detail-date-filter {{
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 4px 6px;
          border: 1px solid #d8e8dc;
          border-radius: 999px;
          background: #fff;
          white-space: nowrap;
        }}
        .campaign-detail-date-filter input {{
          width: 124px;
          border: 0;
          background: transparent;
          font: inherit;
          font-size: 11px;
          color: #0c2f22;
        }}
        .detail-date-apply {{
          border: 0;
          border-radius: 999px;
          background: #0f7a49;
          color: #fff;
          padding: 6px 9px;
          font-size: 11px;
          font-weight: 800;
          cursor: pointer;
        }}

        .campaign-detail-table-scroll {{
          display: block;
          width: 100%;
          max-width: 100%;
          overflow-x: scroll;
          overflow-y: hidden;
          padding-bottom: 16px;
          margin-bottom: 8px;
          scrollbar-gutter: stable both-edges;
          scrollbar-width: auto;
          scrollbar-color: #0f7a49 #dcebe2;
          border-bottom: 1px solid #cfe3d5;
        }}
        .campaign-detail-table-scroll::-webkit-scrollbar {{
          height: 14px;
        }}
        .campaign-detail-table-scroll::-webkit-scrollbar-track {{
          background: #dcebe2;
          border-radius: 999px;
        }}
        .campaign-detail-table-scroll::-webkit-scrollbar-thumb {{
          background: #0f7a49;
          border-radius: 999px;
          border: 3px solid #dcebe2;
        }}
        .campaign-detail-table-scroll .campaign-block-table {{
          width: max-content;
          min-width: max-content;
          table-layout: auto;
        }}
        .floor-status-cell {{
          min-width: 190px;
          white-space: normal;
          line-height: 1.25;
          font-size: 12.5px;
        }}
        .floor-registered-cell {{
          min-width: 132px;
          white-space: nowrap;
        }}
        .floor-change-cell {{
          min-width: 58px;
          width: 58px;
          max-width: 58px;
          white-space: nowrap;
          font-weight: 900;
          font-size: 13px;
        }}
        .floor-change-cell.suggest-increase {{ color: #08783f; }}
        .floor-change-cell.suggest-decrease,
        .floor-change-cell.suggest-disable {{ color: #b91c1c; }}
        .floor-new-input.suggest-increase {{ border-color: #0f7a49; background: #eefbf3; }}
        .floor-new-input.suggest-decrease,
        .floor-new-input.suggest-disable {{ border-color: #b91c1c; background: #fff1f1; color: #991b1b; }}


        .campaign-detail-table-scroll .campaign-block-table th,
        .campaign-detail-table-scroll .campaign-block-table td {{
          padding: 6px 7px;
          font-size: 12px;
          line-height: 1.18;
          vertical-align: middle;
          resize: none !important;
          overflow: visible !important;
          background-image: none !important;
        }}
        .campaign-detail-table-scroll .placement-operational-table th,
        .campaign-detail-table-scroll .placement-operational-table td {{
          padding: 7px 8px;
          font-size: 11.5px;
        }}
        .placement-operational-table thead th {{
          position: sticky;
          top: 0;
          z-index: 3;
          background: #f9fcfa;
          box-shadow: inset 0 -1px 0 #cfdfd4;
        }}
        .placement-operational-table .metric-cell {{
          text-align: right;
          white-space: nowrap;
        }}
        .placement-operational-table .gam-missing {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 58px;
          color: #607365;
          font-size: 10.5px;
          font-weight: 800;
          line-height: 1.05;
        }}
        .campaign-detail-table-scroll .block-name-cell {{
          max-width: 210px;
          white-space: normal;
          font-size: 12.2px;
          font-weight: 800;
        }}
        .campaign-detail-table-scroll .no-cell-resize {{
          resize: none !important;
          overflow: visible !important;
        }}
        .campaign-detail-table-scroll .ctr-cell,
        .campaign-detail-table-scroll .pmr-cell,
        .campaign-detail-table-scroll .viewability-cell {{
          min-width: 54px;
          width: 54px;
          white-space: nowrap;
        }}
        .campaign-detail-table-scroll .floor-registered-cell {{
          min-width: 190px;
          white-space: nowrap;
        }}
        .campaign-detail-table-scroll .floor-action-cell {{
          min-width: 92px;
          white-space: nowrap;
        }}
        .campaign-detail-table-scroll .campaign-block-table tbody tr:hover td {{
          background: #dff4e9;
        }}
        .campaign-detail-table-scroll .campaign-block-table th::after,
        .campaign-detail-table-scroll .campaign-block-table td::after {{
          display: none !important;
          content: none !important;
        }}

        .campaign-detail-table-scroll .placement-check-cell {{
          width: 24px !important;
          min-width: 24px !important;
          max-width: 24px !important;
          text-align: center;
          padding-left: 2px !important;
          padding-right: 2px !important;
        }}
        .placement-operational-table .placement-check-cell,
        .placement-operational-table .block-name-cell {{
          position: sticky;
          z-index: 4;
          background: #f7fbf8 !important;
        }}
        .placement-operational-table .placement-check-cell {{
          left: 0;
        }}
        .placement-operational-table .block-name-cell {{
          left: 28px;
          min-width: 190px;
          max-width: 190px;
          box-shadow: 1px 0 0 #d7e6dc;
        }}
        .placement-operational-table thead .placement-check-cell,
        .placement-operational-table thead .block-name-cell {{
          z-index: 6;
          background: #f9fcfa !important;
        }}
        .campaign-detail-table-scroll input[type="checkbox"] {{
          width: 13px;
          height: 13px;
          transform: none;
          accent-color: #0f7a49;
        }}
        .campaign-detail-table-scroll .placement-roi-cell {{
          min-width: 76px;
          width: 76px;
          white-space: nowrap;
          text-align: right;
          padding-right: 12px !important;
        }}
        .campaign-detail-table-scroll .placement-pmr-cell {{
          min-width: 86px;
          width: 86px;
          white-space: nowrap;
          text-align: right;
        }}
        .campaign-detail-table-scroll .floor-status-cell {{
          min-width: 132px;
          width: 132px;
          max-width: 132px;
          white-space: normal;
          overflow: visible !important;
        }}
        .floor-summary-cell small {{
          display: block;
          margin-top: 3px;
          color: #587062;
          font-size: 10.5px;
          font-weight: 800;
        }}
        .floor-change-pill {{
          display: inline-flex;
          margin-top: 5px;
          padding: 2px 6px;
          border-radius: 999px;
          background: #eefbf3;
          color: #0f6b3d;
          font-size: 10.5px;
          font-weight: 900;
          white-space: nowrap;
        }}
        .floor-control-cell {{
          min-width: 236px;
          width: 236px;
        }}
        .floor-control-grid {{
          display: grid;
          grid-template-columns: 76px 76px 1fr;
          gap: 5px;
          align-items: center;
        }}
        .floor-control-grid .floor-status-rule-button {{
          grid-column: 2 / 4;
          width: 100%;
        }}
        .campaign-detail-table-scroll .floor-status-rule-cell {{
          min-width: 78px;
          width: 78px;
          white-space: nowrap;
        }}

        .placements-vertical-scroll {{
          max-height: 620px;
          overflow-y: auto;
          overflow-x: scroll;
          padding-left: 0;
          direction: ltr;
        }}
        .placements-vertical-scroll .campaign-block-table {{
          direction: ltr;
        }}
        .placement-detail-row {{
          display: none;
        }}
        .placement-detail-row.is-open {{
          display: table-row;
        }}
        .placement-detail-cell {{
          padding: 0 8px 12px 34px !important;
          background: transparent !important;
        }}
        .placement-detail-shell {{
          border: 1px solid #c6decf;
          border-radius: 16px;
          background:
            linear-gradient(135deg, rgba(255,255,255,.97), rgba(240,250,244,.98)),
            #fff;
          box-shadow: inset 0 1px 0 rgba(255,255,255,.9), 0 8px 24px rgba(11, 107, 63, .07);
          padding: 12px;
          margin: 0 6px 8px 18px;
        }}
        .placement-name-wrap {{
          display: flex;
          align-items: flex-start;
          gap: 10px;
        }}
        .placement-expand-button {{
          flex: 0 0 auto;
          width: 24px;
          height: 24px;
          border-radius: 999px;
          border: 1px solid #b9dfcf;
          background: #eff9f3;
          color: #0f7a49;
          font-weight: 900;
          cursor: pointer;
          line-height: 1;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          transition: transform .18s ease, background-color .18s ease, color .18s ease;
        }}
        .placement-row.is-open .placement-expand-button {{
          transform: rotate(90deg);
          background: #0f7a49;
          color: #fff;
        }}
        .placement-name-stack {{
          display: grid;
          gap: 4px;
        }}
        .placement-name-stack small {{
          color: #4f685d;
          font-size: 11.5px;
          font-weight: 700;
          line-height: 1.35;
        }}
        .placement-name-strong {{
          font-weight: 900;
          color: #0b2318;
        }}
        .placement-row {{
          cursor: pointer;
        }}
        .placements-vertical-scroll::-webkit-scrollbar {{
          width: 13px;
          height: 14px;
        }}
        .placements-vertical-scroll::-webkit-scrollbar-track {{
          background: #dcebe2;
          border-radius: 999px;
        }}
        .placements-vertical-scroll::-webkit-scrollbar-thumb {{
          background: #0f7a49;
          border-radius: 999px;
          border: 3px solid #dcebe2;
        }}


        @keyframes actionPulseGlow {{
          0%, 100% {{ background-color: #fff7cc; box-shadow: inset 0 0 0 2px rgba(217, 119, 6, 0.25); }}
          50% {{ background-color: #ffcf55; box-shadow: inset 0 0 0 3px rgba(180, 83, 9, 0.75); }}
        }}
        .campaign-block-table tr.action-pending > td {{
          animation: actionPulseGlow 0.85s ease-in-out infinite !important;
        }}
        .campaign-block-table tr.action-pending .floor-change-cell,
        .campaign-block-table tr.action-pending .floor-new-input {{
          outline: 3px solid rgba(180, 83, 9, 0.85) !important;
          outline-offset: 1px;
        }}
        .floor-current-input, .floor-new-input {{
          width: 96px;
          border: 1px solid #bdd8c6;
          border-radius: 9px;
          padding: 6px 8px;
          background: #fff;
          font: inherit;
        }}
        .floor-select {{
          min-width: 118px;
          max-width: 132px;
          cursor: pointer;
        }}
        .floor-register-button {{
          border: 1px solid #0b6b3f;
          border-radius: 999px;
          background: #0f7a49;
          color: #fff;
          padding: 7px 10px;
          font-weight: 800;
          cursor: pointer;
          white-space: nowrap;
        }}
        .floor-register-button.registered {{ background: #0ea77a; }}
        .floor-status-cell.rule-disabled {{
          background: #ffe8e5;
          color: #9f1d13;
          font-weight: 900;
          border-radius: 8px;
        }}
        .floor-status-cell small {{
          display: block;
          margin-top: 3px;
          color: #49675a;
          font-size: 11.5px;
          font-weight: 700;
        }}
        .floor-status-rule-button.create-state {{
          background: #fff3c4;
          border-color: #d7a500;
          color: #604200;
        }}
        .floor-status-rule-button.active-state {{
          background: #0f7a49;
          border-color: #0b6b3f;
          color: #fff;
        }}
        .floor-status-rule-button.disabled-state {{
          background: #ffe8e5;
          border-color: #df8b84;
          color: #9f1d13;
        }}
        .campaign-detail-state {{
          padding: 14px 12px;
          border-radius: 14px;
          background: rgba(255,255,255,0.92);
          border: 1px dashed rgba(17, 32, 24, 0.16);
          color: var(--muted);
          font-size: 14px;
        }}
        .campaign-block-table {{
          min-width: 1100px;
        }}
        .campaign-block-table th {{
          background: rgba(255,255,255,0.76);
        }}
        .campaign-block-table td {{
          background: rgba(255,255,255,0.88);
        }}
        .block-name-cell {{
          min-width: 220px;
          font-weight: 700;
        }}
        .block-summary {{
          display: flex;
          flex-direction: column;
          gap: 3px;
          font-size: 12px;
        }}
        .block-summary strong {{
          font-size: 14px;
        }}
        .project-kpi-strip .metric {{
          border: 1px solid rgba(30,107,67,0.12);
          background: linear-gradient(145deg, rgba(255,255,255,0.98), rgba(246,251,247,0.92));
        }}
        .metric.push-card {{
          background: linear-gradient(145deg, rgba(232,246,237,0.98), rgba(255,255,255,0.92));
          border-color: rgba(30,107,67,0.18);
        }}
        .metric.risk-card {{
          background: linear-gradient(145deg, rgba(255,248,225,0.98), rgba(255,255,255,0.92));
        }}
        .project-link {{
          font-weight: 700;
        }}
        .validation-note {{
          display: inline-flex;
          align-items: center;
          margin-left: 8px;
          padding: 3px 8px;
          border-radius: 999px;
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          background: rgba(176, 122, 21, 0.12);
          color: var(--amber);
          border: 1px solid rgba(176, 122, 21, 0.18);
        }}
        th {{
          color: var(--muted);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }}
        tr:last-child td {{
          border-bottom: none;
        }}
        a {{
          color: var(--forest);
          text-decoration: none;
        }}
        .roi-positive {{ color: var(--forest); font-weight: 700; }}
        .roi-negative {{ color: var(--brick); font-weight: 700; }}
        .roi-neutral {{ color: var(--muted); font-weight: 700; }}
        .budget-cell {{
          min-width: 190px;
          background: rgba(248, 252, 249, 0.94);
          vertical-align: top;
        }}
        .budget-stack {{
          display: grid;
          gap: 5px;
          font-size: 12px;
        }}
        .budget-stack span {{
          color: var(--muted);
          text-transform: uppercase;
          letter-spacing: 0.08em;
          font-size: 10px;
        }}
        .budget-stack strong {{
          display: block;
          font-size: 14px;
        }}
        .budget-alert {{
          display: inline-flex;
          width: fit-content;
          margin-top: 3px;
          border-radius: 999px;
          padding: 4px 8px;
          background: rgba(30,107,67,0.12);
          color: var(--forest);
          font-weight: 800;
          font-size: 10px;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }}
        .budget-alert.blink {{
          animation: budgetPulse 1s ease-in-out infinite;
        }}
        .budget-control-title {{ color: #0f6b43; font-size: 10px; font-weight: 950; text-transform: uppercase; letter-spacing: .06em; }}
        .budget-manual-box {{ display: grid; gap: 5px; margin-top: 6px; padding: 8px; border: 1px solid rgba(30,107,67,0.18); border-radius: 14px; background: #f4fbf6; }}
        .budget-manual-row {{ display: grid; grid-template-columns: 72px 1fr; gap: 5px; }}
        .budget-manual-input, .budget-manual-reason {{ width: 100%; border: 1px solid #b9d9c4; border-radius: 10px; padding: 5px 6px; font-size: 11px; font-weight: 800; }}
        .budget-manual-reason {{ grid-column: 1 / -1; font-weight: 700; }}
        .budget-manual-button {{ border: 0; border-radius: 999px; background: var(--forest); color: white; font-size: 10px; font-weight: 950; cursor: pointer; }}
        .budget-manual-button.saved {{ background: #0ea77a; }}
        .budget-manual-status {{ color: #506559; font-size: 10px; font-weight: 800; }}
        .optimization-countdown {{
          display: inline-flex;
          align-items: center;
          gap: 4px;
          margin-left: 8px;
          padding: 5px 10px;
          border-radius: 999px;
          font-size: 11px;
          font-weight: 800;
          letter-spacing: 0.02em;
          border: 1px solid rgba(176,122,21,0.22);
          background: rgba(255,246,221,0.92);
          color: #8a5b0a;
          vertical-align: middle;
        }}
        .optimization-countdown.ready {{
          border-color: rgba(30,107,67,0.22);
          background: rgba(30,107,67,0.12);
          color: var(--forest);
        }}
        .optimization-countdown.unknown {{
          border-color: rgba(17,32,24,0.12);
          background: rgba(17,32,24,0.06);
          color: var(--muted);
        }}
        @keyframes budgetPulse {{
          0%, 100% {{ box-shadow: 0 0 0 0 rgba(30,107,67,0.35); }}
          50% {{ box-shadow: 0 0 0 7px rgba(30,107,67,0); }}
        }}
        .status-pill {{
          display: inline-flex;
          align-items: center;
          padding: 6px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 700;
          border: 1px solid transparent;
        }}
        .status-good {{ background: rgba(30,107,67,0.12); color: var(--forest); border-color: rgba(30,107,67,0.18); }}
        .status-bad {{ background: rgba(155,45,45,0.12); color: var(--brick); border-color: rgba(155,45,45,0.18); }}
        .status-warn {{ background: rgba(176,122,21,0.12); color: var(--amber); border-color: rgba(176,122,21,0.18); }}
        .status-neutral {{ background: rgba(17,32,24,0.08); color: var(--ink); border-color: rgba(17,32,24,0.12); }}
        .stack {{
          display: grid;
          gap: 18px;
        }}
        .muted {{
          color: var(--muted);
        }}
        .mono {{
          font-family: Consolas, monospace;
          font-size: 12px;
        }}
        .nowrap {{ white-space: nowrap; }}
        .agent-global-alerts {{
          margin-top: 16px;
          display: grid;
          gap: 14px;
        }}
        .agent-command-center {{
          border: 1px solid rgba(17,32,24,.12);
          border-radius: 22px;
          background: linear-gradient(135deg, #071b12, #145b38 58%, #e9fff1);
          color: #fff;
          padding: 18px;
          box-shadow: 0 18px 38px rgba(17,32,24,.16);
          overflow: hidden;
          position: relative;
        }}
        .agent-command-center:after {{
          content: "";
          position: absolute;
          right: -70px;
          top: -90px;
          width: 230px;
          height: 230px;
          border-radius: 999px;
          background: rgba(255,255,255,.18);
        }}
        .agent-command-head {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; position: relative; z-index: 1; }}
        .agent-command-eyebrow {{ font-size: 11px; letter-spacing: .12em; text-transform: uppercase; font-weight: 900; opacity: .78; }}
        .agent-command-title {{ font-size: 22px; font-weight: 950; margin-top: 4px; }}
        .agent-command-subtitle {{ margin-top: 6px; color: rgba(255,255,255,.82); line-height: 1.35; max-width: 760px; }}
        .agent-command-score {{ min-width: 150px; border-radius: 18px; background: rgba(255,255,255,.14); padding: 12px 14px; text-align: center; backdrop-filter: blur(4px); }}
        .agent-command-score b {{ display: block; font-size: 30px; line-height: 1; }}
        .agent-command-score span {{ display: block; margin-top: 4px; font-size: 11px; text-transform: uppercase; letter-spacing: .08em; font-weight: 900; }}
        .agent-command-stats {{ display: grid; grid-template-columns: repeat(4, minmax(110px, 1fr)); gap: 10px; margin-top: 16px; position: relative; z-index: 1; }}
        .agent-stat {{ border-radius: 16px; background: rgba(255,255,255,.13); padding: 11px 12px; }}
        .agent-stat strong {{ display: block; font-size: 20px; }}
        .agent-stat span {{ display: block; font-size: 11px; text-transform: uppercase; letter-spacing: .06em; opacity: .8; font-weight: 900; }}
        .agent-global-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(285px, 1fr)); gap: 12px; }}
        .agent-global-summary {{
          border: 1px solid rgba(17,32,24,.12);
          border-left: 9px solid var(--forest);
          border-radius: 18px;
          background: linear-gradient(135deg, #f6fff8, #fffdf5);
          padding: 15px;
          box-shadow: 0 12px 28px rgba(17,32,24,.08);
        }}
        .agent-global-summary.critical {{ border-left-color: var(--brick); background: linear-gradient(135deg, #fff0ec, #fff8f4); }}
        .agent-global-summary.high {{ border-left-color: var(--brick); background: linear-gradient(135deg, #fff4ef, #fffaf7); }}
        .agent-global-summary.warn {{ border-left-color: var(--amber); background: linear-gradient(135deg, #fff8e8, #fffdf4); }}
        .agent-global-summary.opportunity {{ border-left-color: var(--forest); background: linear-gradient(135deg, #effaf2, #fbfff4); }}
        .agent-global-top {{ display: flex; flex-wrap: wrap; gap: 7px; align-items: center; margin-bottom: 9px; }}
        .agent-global-badge {{ display: inline-flex; border-radius: 999px; padding: 5px 9px; font-size: 10px; font-weight: 950; text-transform: uppercase; letter-spacing: .05em; background: rgba(17,32,24,.08); color: var(--ink); }}
        .agent-global-badge.block {{ color: #fff; background: var(--brick); }}
        .agent-global-summary strong {{ display: block; font-size: 17px; margin-bottom: 8px; }}
        .agent-global-summary p {{ margin: 5px 0; color: var(--ink); line-height: 1.42; }}
        .agent-global-summary small {{ display: block; margin-top: 9px; color: var(--muted); font-weight: 850; line-height: 1.35; }}
        .agent-global-actions {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 11px; }}
        .agent-global-actions a {{ text-decoration: none; color: #fff; background: var(--ink); border-radius: 999px; padding: 9px 12px; font-size: 12px; font-weight: 950; }}
        .agent-global-actions a.secondary {{ background: var(--forest); }}
        .agent-global-actions a.light {{ color: var(--ink); background: rgba(17,32,24,.08); }}
        .agent-feedback-box {{ margin-top: 12px; border-top: 1px solid rgba(17,32,24,.10); padding-top: 12px; display: grid; gap: 8px; }}
        .agent-feedback-row {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }}
        .agent-feedback-box textarea {{ width: 100%; min-height: 58px; resize: vertical; border: 1px solid rgba(17,32,24,.18); border-radius: 13px; padding: 10px; font: inherit; background: rgba(255,255,255,.78); }}
        .agent-feedback-box select {{ border: 1px solid rgba(17,32,24,.18); border-radius: 999px; padding: 9px 10px; font-weight: 800; background: #fff; }}
        .agent-feedback-box label {{ font-size: 12px; font-weight: 850; color: var(--ink); display: inline-flex; gap: 6px; align-items: center; }}
        .agent-feedback-button {{ border: 0; border-radius: 999px; padding: 9px 12px; background: var(--forest); color: #fff; font-weight: 950; cursor: pointer; }}
        .agent-feedback-button:disabled {{ opacity: .65; cursor: wait; }}
        .agent-feedback-status {{ font-size: 12px; font-weight: 900; color: var(--muted); }}
        #system-help-tooltip {{
          position: fixed;
          z-index: 2147483000;
          display: none;
          width: max-content;
          max-width: min(360px, calc(100vw - 24px));
          padding: 10px 12px;
          border: 1px solid rgba(255,255,255,.18);
          border-radius: 8px;
          background: #102e24;
          color: #fff;
          box-shadow: 0 12px 30px rgba(0,0,0,.28);
          font-size: 13px;
          font-weight: 650;
          line-height: 1.4;
          pointer-events: none;
          opacity: 0;
          transform: translateY(3px);
          transition: opacity .12s ease, transform .12s ease;
        }}
        #system-help-tooltip.is-visible {{
          display: block;
          opacity: 1;
          transform: translateY(0);
        }}
        .system-metric-definition {{
          margin: 7px 0 9px;
          padding: 8px 10px;
          border-radius: 7px;
          background: #eef8f1;
          color: #234438;
          font-size: 12px;
          font-weight: 700;
          line-height: 1.35;
        }}
        [data-system-help-active="1"] {{
          outline: 2px solid rgba(30,107,67,.22);
          outline-offset: 2px;
        }}
        @media (max-width: 760px) {{
          .agent-command-head {{ display: block; }}
          .agent-command-score {{ margin-top: 12px; }}
          .agent-command-stats {{ grid-template-columns: repeat(2, 1fr); }}
        }}
        @media (max-width: 1080px) {{
          .masthead, .grid {{ grid-template-columns: 1fr; }}
          .metrics {{ grid-template-columns: repeat(2, 1fr); }}
        }}
        @media (max-width: 640px) {{
          .shell {{ padding: 16px; }}
          .metrics {{ grid-template-columns: 1fr; }}
          .hero h1 {{ font-size: 28px; }}
          table {{ font-size: 13px; }}
        }}
      </style>
    </head>
    <body>
    """


def _global_help_layer() -> str:
    return r"""
    <div id="system-help-tooltip" role="tooltip" aria-hidden="true"></div>
    <script>
    (() => {
      const tooltip = document.getElementById('system-help-tooltip');
      if (!tooltip) return;

      const selector = [
        '[data-help]', '[data-metric]', '[title]', '[aria-label]', 'a', 'button', 'input',
        'select', 'textarea', 'summary', 'label', 'th', 'h1', 'h2', 'h3',
        '.status-pill', '.badge', '.optimization-countdown'
      ].join(',');
      const ignored = [
        '#system-help-tooltip', '[role="tooltip"]', '.metric-chart-popover',
        '.help-icon', 'script', 'style'
      ].join(',');
      const glossary = [
        [/\battr\s*id\b/i, 'Identificador exclusivo da campanha ou grupo usado para separar investimento, receita, funil e regras.'],
        [/\binvestimento\b|\binv\.\b/i, 'Valor gasto no Google Ads no periodo selecionado.'],
        [/\breceita\b|\badsense\b/i, 'Receita real registrada pelo GAM ou AdSense no periodo selecionado.'],
        [/\blucro\b|resultado liquido/i, 'Receita menos investimento. Valor negativo indica prejuizo no periodo.'],
        [/\broi\b/i, 'Retorno sobre o investimento: lucro dividido pelo investimento.'],
        [/\bpmr\b/i, 'Percentual de monetizacao real. Deve ser analisado junto com receita, investimento, ROI e volume.'],
        [/\becpm\b/i, 'Receita media por mil impressoes monetizadas no GAM.'],
        [/\brps\b/i, 'Receita por sessao: receita atribuida dividida pelas sessoes reais.'],
        [/\bctr\b/i, 'Percentual de impressoes que gerou clique.'],
        [/\bcpc\b/i, 'Custo medio pago por clique no Google Ads.'],
        [/\bcpa\b/i, 'Custo medio para gerar a conversao configurada.'],
        [/\bsessoes?\b/i, 'Visitas reais identificadas pelo rastreador first-party.'],
        [/\bimpressoes?\b/i, 'Quantidade de vezes que o anuncio ou bloco foi exibido.'],
        [/\bcliques?\b/i, 'Quantidade de interacoes registradas com o anuncio ou elemento.'],
        [/\bvisibilidade\b|\bviewability\b/i, 'Percentual de impressoes mensuraveis em que ao menos 50% do anuncio ficou visivel por pelo menos 1 segundo.'],
        [/\bfloor\b|preco minimo/i, 'Preco minimo configurado para o inventario. Nao garante que o eCPM medio fique acima desse valor.'],
        [/\bconversao\b/i, 'Percentual de usuarios que concluiu o avanco ou objetivo indicado.'],
        [/\borcamento\b/i, 'Limite diario de investimento configurado para a campanha.'],
        [/\bstatus\b/i, 'Situacao operacional atual deste item.'],
        [/\bhoje\b/i, 'Dados acumulados do dia atual; podem estar incompletos ou sujeitos ao atraso das APIs.'],
        [/\bontem\b/i, 'Dados do ultimo dia completo.'],
        [/ultimos 7 dias/i, 'Consolidado dos sete dias mais recentes para reduzir variacoes de curto prazo.'],
        [/\bmes\b/i, 'Consolidado do mes corrente ate a ultima atualizacao disponivel.'],
        [/\bexperimento\b/i, 'Teste controlado que compara variantes usando trafego e eventos reais.'],
        [/\bvolume\b/i, 'Quantidade de observacoes disponiveis. Volume insuficiente nao permite declarar uma vencedora.'],
        [/\buplift\b/i, 'Melhoria percentual da variante em relacao a campea ou controle.'],
        [/\bconfianca\b|limite sequencial/i, 'Faixa estatistica usada para evitar decidir com poucos dados ou oscilacao aleatoria.']
      ];
      const metricGlossary = {
        investment: 'Valor gasto no Google Ads no periodo selecionado.',
        revenue: 'Receita real atribuida pelo GAM ou AdSense no periodo selecionado.',
        roi: 'Retorno sobre o investimento: lucro dividido pelo investimento.',
        pmr: 'Percentual de monetizacao real, analisado junto com receita, investimento, ROI e volume.',
        ecpm: 'Receita media por mil impressoes monetizadas no GAM.',
        rps: 'Receita por sessao: receita atribuida dividida pelas sessoes reais.',
        cost_per_conversion: 'Custo medio para gerar a conversao configurada no Google Ads.'
      };
      let active = null;
      let showTimer = 0;

      const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
      const visibleText = (element) => clean(
        element.getAttribute('data-label') ||
        element.innerText ||
        element.textContent ||
        element.getAttribute('placeholder') ||
        element.getAttribute('name')
      );

      function explanation(element) {
        const metricExplanation = metricGlossary[element.getAttribute('data-metric') || ''];
        if (metricExplanation) return metricExplanation;
        const explicit = clean(
          element.getAttribute('data-help') ||
          element.getAttribute('aria-description') ||
          element.getAttribute('title') ||
          element.getAttribute('aria-label')
        );
        if (explicit) return explicit;

        const text = visibleText(element);
        if (!text) return '';
        const glossaryEntry = glossary.find(([pattern]) => pattern.test(text));
        if (glossaryEntry) return glossaryEntry[1];

        const tag = element.tagName.toLowerCase();
        if (tag === 'a') {
          return element.target === '_blank'
            ? `Abre "${text}" em uma nova guia.`
            : `Abre a tela ou detalhe "${text}".`;
        }
        if (tag === 'button') return `Executa a acao "${text}".`;
        if (tag === 'input' || tag === 'select' || tag === 'textarea') {
          const label = element.id
            ? document.querySelector(`label[for="${CSS.escape(element.id)}"]`)
            : null;
          return `Campo para ${visibleText(label) || text}.`;
        }
        if (tag === 'th') return `Coluna "${text}" da tabela.`;
        if (tag === 'h1' || tag === 'h2' || tag === 'h3') return `Secao "${text}".`;
        if (tag === 'summary') return `Expande ou recolhe "${text}".`;
        if (tag === 'label') return `Identifica o campo "${text}".`;
        if (element.matches('.status-pill,.badge,.optimization-countdown')) {
          return `Indicador operacional: ${text}.`;
        }
        return '';
      }

      function enrichMetricPopover(element) {
        const text = explanation(element);
        if (!text) return;
        let attempts = 0;
        const timer = window.setInterval(() => {
          attempts += 1;
          const popover = document.querySelector('.metric-chart-popover');
          if (
            popover &&
            getComputedStyle(popover).display !== 'none' &&
            popover.querySelector('.metric-chart-stats')
          ) {
            let definition = popover.querySelector('.system-metric-definition');
            if (!definition) {
              definition = document.createElement('div');
              definition.className = 'system-metric-definition';
              const title = popover.querySelector('.metric-chart-title');
              title?.insertAdjacentElement('afterend', definition);
            }
            definition.textContent = text;
            window.clearInterval(timer);
          } else if (attempts >= 30) {
            window.clearInterval(timer);
          }
        }, 100);
      }

      function position(element, pointer) {
        const rect = element.getBoundingClientRect();
        const tipRect = tooltip.getBoundingClientRect();
        const anchorX = pointer?.clientX || (rect.left + rect.width / 2);
        const preferredTop = rect.bottom + 9;
        const top = preferredTop + tipRect.height <= innerHeight - 8
          ? preferredTop
          : Math.max(8, rect.top - tipRect.height - 9);
        const left = Math.min(
          innerWidth - tipRect.width - 8,
          Math.max(8, anchorX - tipRect.width / 2)
        );
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
      }

      function show(element, pointer) {
        const text = explanation(element);
        if (!text) return;
        active?.removeAttribute('data-system-help-active');
        active = element;
        active.setAttribute('data-system-help-active', '1');
        tooltip.textContent = text;
        tooltip.classList.add('is-visible');
        tooltip.setAttribute('aria-hidden', 'false');
        position(element, pointer);
      }

      function hide() {
        window.clearTimeout(showTimer);
        active?.removeAttribute('data-system-help-active');
        active = null;
        tooltip.classList.remove('is-visible');
        tooltip.setAttribute('aria-hidden', 'true');
      }

      function targetFrom(node) {
        const element = node instanceof Element ? node.closest(selector) : null;
        if (!element || element.matches(ignored) || element.closest(ignored)) return null;
        return explanation(element) ? element : null;
      }

      document.addEventListener('pointerover', (event) => {
        const element = targetFrom(event.target);
        if (!element || element === active) return;
        if (element.matches('.metric-value')) {
          enrichMetricPopover(element);
          return;
        }
        window.clearTimeout(showTimer);
        showTimer = window.setTimeout(() => show(element, event), 220);
      });
      document.addEventListener('pointerout', (event) => {
        if (!active) return;
        const next = event.relatedTarget instanceof Element ? event.relatedTarget.closest(selector) : null;
        if (next === active) return;
        hide();
      });
      document.addEventListener('focusin', (event) => {
        const element = targetFrom(event.target);
        if (element) show(element);
      });
      document.addEventListener('focusout', hide);
      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') hide();
      });
      window.addEventListener('scroll', () => {
        if (active && active === document.activeElement) {
          position(active);
          return;
        }
        hide();
      }, {passive: true});
      window.addEventListener('resize', hide);
    })();
    </script>
    """



def _agent_global_alerts_html(active: str) -> str:
    try:
        runtime = load_runtime(CONFIG_PATH)
        alerts = load_agent_global_alerts(runtime, active=active, limit=4)
    except Exception:
        alerts = []
    if not alerts:
        return ""

    counts = {"critical": 0, "high": 0, "warn": 0, "opportunity": 0, "blocked": 0}
    for alert in alerts:
        key = str(alert.get("severity") or "INFO").lower()
        if key in counts:
            counts[key] += 1
        if bool(alert.get("automation_block")):
            counts["blocked"] += 1

    page_label = {
        "roi-control": "ROI e automacoes",
        "push": "Push e LTV",
        "push-control": "Push e LTV",
        "agents": "Agentes",
        "dashboard": "Dashboard",
    }.get(active, "operacao")
    priority = "Critico" if counts["critical"] else ("Alto" if counts["high"] else ("Atencao" if counts["warn"] else "Oportunidade"))
    command_center = (
        "<section class='agent-command-center'>"
        "<div class='agent-command-head'>"
        "<div>"
        "<div class='agent-command-eyebrow'>Central de Atencao dos Agentes</div>"
        f"<div class='agent-command-title'>{priority}: o sistema encontrou {len(alerts)} ponto(s) para decidir melhor</div>"
        f"<div class='agent-command-subtitle'>Esta faixa resume o que pode afetar lucro, volume ou confiabilidade nesta pagina de {html.escape(page_label)}. Se houver bloqueio, evite automatizar antes de validar o dado.</div>"
        "</div>"
        f"<div class='agent-command-score'><b>{counts['blocked']}</b><span>bloqueios</span></div>"
        "</div>"
        "<div class='agent-command-stats'>"
        f"<div class='agent-stat'><strong>{counts['critical']}</strong><span>criticos</span></div>"
        f"<div class='agent-stat'><strong>{counts['high']}</strong><span>alto risco</span></div>"
        f"<div class='agent-stat'><strong>{counts['warn']}</strong><span>observacao</span></div>"
        f"<div class='agent-stat'><strong>{counts['opportunity']}</strong><span>oportunidades</span></div>"
        "</div>"
        "</section>"
    )

    cards = []
    for alert in alerts:
        observation_key = html.escape(str(alert.get("observation_key") or ""))
        severity_raw = str(alert.get("severity") or "INFO")
        severity = html.escape(severity_raw)
        css_class = severity_raw.lower()
        title_raw = str(alert.get("title") or "Aviso dos agentes")
        title = html.escape(title_raw)
        details_raw = str(alert.get("details") or "")
        period_source = (title_raw + " " + details_raw).lower()
        if "7 dias" in period_source:
            focus_period = "7d"
            period_badge = "ultimos 7 dias"
        elif "ontem" in period_source:
            focus_period = "yesterday"
            period_badge = "ontem"
        elif "mes" in period_source or "m?s" in period_source:
            focus_period = "month"
            period_badge = "mes"
        else:
            focus_period = "today"
            period_badge = "hoje"
        agent_slug = html.escape(str(alert.get("agent_slug") or "agente"))
        project_raw = str(alert.get("project") or "")
        project = html.escape(project_raw or "geral")
        attr_raw = str(alert.get("attr_id") or "")
        attr_id = html.escape(attr_raw)
        problem_raw = str(alert.get("problem_type") or "operacional")
        problem_type = html.escape(problem_raw)
        hypothesis = html.escape(str(alert.get("hypothesis") or "Sem hipotese registrada."))
        next_action = html.escape(str(alert.get("next_action") or "Revisar antes de agir."))
        roi_context = html.escape(str(alert.get("roi_context") or ""))
        impact = float(alert.get("impact_value") or 0)
        automation_block = bool(alert.get("automation_block"))
        expires = html.escape(str(alert.get("expires") or ""))
        project_url = f"/projects/{quote(project_raw)}" if project_raw and project_raw != "geral" else "/"
        focus_detail = "blocks"
        if "placement" in problem_raw:
            primary_label = "Abrir placements"
            focus_detail = "placements"
        elif "floor" in problem_raw or "monetizacao" in problem_raw:
            primary_label = "Abrir blocos GAM"
            focus_detail = "blocks"
        elif "tracking" in problem_raw or "dados" in problem_raw:
            primary_label = "Conferir dados"
            focus_detail = "blocks"
        else:
            primary_label = "Abrir projeto"
            focus_detail = "blocks"
        if project_url != "/" and attr_raw:
            primary_href = f"{project_url}?focus_attr={quote(attr_raw)}&focus_detail={focus_detail}&focus_period={focus_period}"
        else:
            primary_href = project_url
        cards.append(
            f"<section class='agent-global-summary {css_class}'>"
            "<div class='agent-global-top'>"
            f"<span class='agent-global-badge'>{severity}</span>"
            f"<span class='agent-global-badge'>{agent_slug}</span>"
            f"<span class='agent-global-badge'>{problem_type}</span>"
            f"<span class='agent-global-badge'>periodo: {html.escape(period_badge)}</span>"
            + ("<span class='agent-global-badge block'>nao automatizar</span>" if automation_block else "")
            + "</div>"
            f"<strong>{title}</strong>"
            f"<p><b>Por que importa:</b> {hypothesis}</p>"
            f"<p><b>Proxima melhor acao:</b> {next_action}</p>"
            + (f"<p><b>Contexto ROI:</b> {roi_context}</p>" if roi_context else "")
            + f"<small>Projeto: {project} | Attr ID: {attr_id or '-'} | impacto monitorado: R${impact:,.2f} | validade: {expires}</small>"
            + f"<div class='agent-global-actions'><a class='secondary' href='{primary_href}'>{primary_label}</a><a href='/agents'>Ver agentes</a><a class='light' href='/roi-control'>ROI</a></div>"
            + "<div class='agent-feedback-box' data-observation-key='" + observation_key + "'>"
            + "<textarea class='agent-feedback-note' placeholder='Registre o que foi feito ou por que este aviso deve ser observado. Ex: campanha nova de Display ainda em aprendizado; manter observacao por mais alguns dias.'></textarea>"
            + "<div class='agent-feedback-row'><select class='agent-feedback-decision'><option value='invalid'>Ainda nao e valido, manter aviso</option><option value='valid'>Informacao valida, retirar aviso</option></select>"
            + "<label><input class='agent-feedback-pattern' type='checkbox'> pode virar padrao se o agente confirmar lucro/volume</label>"
            + "<button class='agent-feedback-button' type='button'>Registrar</button><span class='agent-feedback-status'></span></div>"
            + "</div>"
            "</section>"
        )
    return command_center


def _layout(title: str, content: str, active: str) -> str:
    nav = """
      <div class='nav'>
        <a class='{dashboard}' href='/'>Dashboard</a>
        <a class='{roi}' href='/roi-control'>Controle ROI</a>
        <a class='{push}' href='/push-control'>Push</a>
        <a class='{actions}' href='/actions'>Acoes IA</a>
        <a class='{sync}' href='/gam-sync'>Execucao GAM</a>
        <a class='{data_checks}' href='/data-checks'>Conferencia</a>
        <a class='{setup}' href='https://app.wizseoservicesaustralia.com/setup-inicial'>Setup Inicial</a>
        <a class='{campaign_builder}' href='/campaign-builder'>Criar Campanha</a>
        <a class='{experiments}' href='/experiments'>Experimentos</a>
        <a class='{leads}' href='/leads'>Leads</a>
        <a class='{agents}' href='/agents'>Agentes</a>
        <a class='{war_room}' href='/war-room'>Julio War Room</a>
        <a class='' href='/logout'>Sair</a>
      </div>
    """.format(
        dashboard="active" if active == "dashboard" else "",
        roi="active" if active == "roi-control" else "",
        push="active" if active == "push-control" else "",
        actions="active" if active == "actions" else "",
        sync="active" if active == "sync" else "",
        data_checks="active" if active == "data-checks" else "",
        setup="active" if active == "setup-inicial" else "",
        campaign_builder="active" if active == "campaign-builder" else "",
        experiments="active" if active == "experiments" else "",
        leads="active" if active == "leads" else "",
        agents="active" if active == "agents" else "",
        war_room="active" if active == "war-room" else "",
    )
    alerts = _agent_global_alerts_html(active)
    nav_with_alerts = nav + alerts
    return _base_styles(title) + content.replace("[[NAV]]", nav_with_alerts).replace("{[[NAV]]}", nav_with_alerts).replace("[[NAV]]", nav_with_alerts) + _global_help_layer() + "</body></html>"


def _veraz_audio_page() -> str:
    return """
    <!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <title>Orientacion inicial sobre Veraz</title><meta name="robots" content="noindex,nofollow">
    <style>
    :root{--ink:#13231a;--muted:#62756b;--green:#073f2a;--green2:#108456;--soft:#eef8ef;--line:#dbeade;--cream:#fff9ea;--red:#b82929}*{box-sizing:border-box}body{margin:0;background:linear-gradient(180deg,#fff9ea 0,#f1faef 48%,#fbfffb 100%);font-family:Arial,sans-serif;color:var(--ink)}.wrap{width:min(760px,100%);margin:0 auto;padding:18px 14px 92px}.hero{padding:8px 4px 14px}.eyebrow{display:inline-flex;border-radius:999px;background:#e2f8cf;color:#255b3a;padding:7px 10px;font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase}h1{font-family:Georgia,'Times New Roman',serif;font-size:clamp(30px,9vw,48px);line-height:1;margin:14px 0 10px}.lead{font-size:16px;line-height:1.45;color:#34483b;margin:0}.card{background:rgba(255,255,255,.94);border:1px solid var(--line);border-radius:24px;padding:18px;box-shadow:0 16px 46px rgba(19,35,26,.10);margin:14px 0}.step{display:flex;gap:10px;align-items:center;font-size:12px;font-weight:900;color:#2e6648;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px}.num{width:26px;height:26px;border-radius:50%;display:grid;place-items:center;background:#e2f8cf}.choices{display:grid;grid-template-columns:1fr;gap:9px}.choice{border:1px solid var(--line);background:#fbfefb;border-radius:16px;padding:13px 14px;text-align:left;font-weight:850;color:#203429;cursor:pointer}.choice small{display:block;font-weight:600;color:var(--muted);margin-top:3px}.choice.active{border-color:#159460;background:#ecfff2;box-shadow:0 0 0 3px rgba(21,148,96,.10)}.recordBox{text-align:center}.solutionBox,.transcriptBox{display:none}.solutionBox.show,.transcriptBox.show{display:block}.transcriptBox{margin-top:26px}.micRow{display:flex;align-items:center;justify-content:center;gap:12px}.mic{width:84px;height:84px;border-radius:50%;border:0;background:linear-gradient(145deg,#118a59,#075d39);color:white;font-size:12px;font-weight:950;box-shadow:0 14px 35px rgba(15,122,77,.26);cursor:pointer}.mic.recording{background:linear-gradient(145deg,#d43131,#8a1919);animation:pulse 1s infinite}@keyframes pulse{50%{transform:scale(1.04)}}.timer{font-size:26px;font-weight:950;color:#183c29;min-width:74px;text-align:left}.hint{font-size:13px;line-height:1.35;color:var(--muted);margin:12px 0}.error{display:none;background:#fff0ee;color:#91302d;border:1px solid #ffc6c0;border-radius:14px;padding:10px 12px;font-size:13px;font-weight:800;margin:10px 0}.tools{display:none;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}.audioPreview{display:none;width:100%;margin-top:12px}.smallBtn{border:1px solid var(--line);background:white;border-radius:999px;padding:11px 12px;font-weight:900;color:#18462f;cursor:pointer}textarea{width:100%;min-height:132px;background:#f8fcf9;border:1px solid var(--line);border-radius:16px;padding:13px;color:#24382e;font-size:15px;line-height:1.45;resize:vertical}.consentBox{display:none;margin:14px 0 0}.consentBox.show{display:block}.consentLead{background:linear-gradient(135deg,#ffcc2f,#ff8a00);color:#10251a;border-radius:18px;padding:12px 14px;font-size:13px;font-weight:950;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px;box-shadow:0 12px 28px rgba(255,138,0,.22)}.consent{display:flex;gap:10px;text-align:left;font-size:12.5px;line-height:1.35;color:#43564b;background:#fff;border:1px solid var(--line);border-radius:16px;padding:12px;margin:12px 0}.consent input{width:18px;height:18px;flex:0 0 auto}.consent.recordingCta{border:2px solid #ff9d00;background:#fff7df;box-shadow:0 14px 34px rgba(255,157,0,.22);animation:consentPulse 1.1s infinite}.consent.recordingCta span:before{content:'PROXIMA ETAPA: ';font-weight:950;color:#a34f00}@keyframes consentPulse{50%{transform:scale(1.015)}}.primary{width:100%;border:0;border-radius:999px;padding:18px 18px;background:linear-gradient(135deg,#ffcc2f,#ff8a00);color:#10251a;font-size:16px;font-weight:950;cursor:pointer;text-decoration:none;display:inline-block;text-align:center;box-shadow:0 14px 30px rgba(255,138,0,.28), inset 0 -2px 0 rgba(0,0,0,.12);letter-spacing:.02em}.primary:disabled{background:#d9e5dc;color:#6d8274;box-shadow:none;cursor:not-allowed}.secondary{width:100%;border:1px solid #b9dbc7;border-radius:999px;padding:13px;background:#fff;color:#0f6b44;font-weight:950;margin-top:8px;cursor:pointer}.solutionCta{width:100%;border:0;border-radius:20px;padding:18px 16px;background:linear-gradient(135deg,#0b6b3d,#0fb96f);color:#fff;font-size:16px;font-weight:950;text-decoration:none;text-align:center;box-shadow:0 16px 34px rgba(11,107,61,.28), inset 0 -2px 0 rgba(0,0,0,.14);letter-spacing:.02em}.answer{display:none;background:#efffed;border:1px solid #b7e7c4;border-radius:18px;padding:15px;font-size:15px;line-height:1.45;margin-top:12px}.resultTitle{font-size:13px;font-weight:950;color:#17623f;text-transform:uppercase;letter-spacing:.08em}.fixed{position:fixed;left:0;right:0;bottom:0;background:rgba(255,255,255,.94);border-top:1px solid var(--line);padding:10px 14px calc(10px + env(safe-area-inset-bottom));backdrop-filter:blur(10px)}.fixedInner{width:min(760px,100%);margin:0 auto}.proof{display:grid;grid-template-columns:1fr;gap:8px;font-size:13px;color:#42584b}.proof div{background:white;border:1px solid var(--line);border-radius:14px;padding:10px}.legal{font-size:11.5px;line-height:1.4;color:#6a7c70;margin:14px 4px}@media(min-width:700px){.choices{grid-template-columns:1fr 1fr}.wrap{padding-bottom:36px}.fixed{position:static;background:transparent;border:0;padding:0}.fixedInner{margin-top:0}}
    </style></head><body><main class="wrap"><section class="hero"><span class="eyebrow">Revision previa al credito</span><h1>Contanos tu caso con Veraz</h1><p class="lead">Elegi tu situacion, graba un audio corto y recibi una orientacion inicial con un proximo paso mas claro.</p></section>
    <section class="card"><div class="step"><span class="num">1</span> Que queres resolver?</div><div class="choices" id="choices"><button class="choice" data-cat="consultar_veraz">Saber si estoy en Veraz<small>Consulta preventiva antes de pedir credito.</small></button><button class="choice" data-cat="deuda_pagada">Ya pague una deuda<small>Quiero saber si sigue apareciendo.</small></button><button class="choice" data-cat="credito_rechazado">Me rechazaron un credito<small>Quiero entender que pudo pasar.</small></button><button class="choice" data-cat="pedir_credito">Quiero pedir un prestamo<small>Quiero evitar pedir a ciegas.</small></button><button class="choice" data-cat="score">Entender mi score<small>Quiero saber que senales revisar.</small></button></div></section>
    <section class="card recordBox" id="recordStep"><div class="step"><span class="num">2</span> Graba hasta 60 segundos</div><div class="micRow"><button id="recordBtn" class="mic">GRABAR<br>CASO</button><div id="timer" class="timer">00:00</div></div><div id="consentBox" class="consentBox"><div class="consentLead">Autoriza para recibir tu solucion al terminar</div><label class="consent"><input id="consent" type="checkbox"><span>Autorizo el uso de mi audio/texto para generar una orientacion inicial y analisis interno. No voy a informar DNI, CUIL ni datos bancarios.</span></label><a id="cta" class="solutionCta" style="display:none;margin-top:10px" href="#" rel="nofollow">VER CONTENIDO SOLUCION</a></div><p id="status" class="hint">Habla de forma general. No informes DNI, CUIL, tarjeta, banco ni documentos.</p><div id="error" class="error"></div><audio id="preview" class="audioPreview" controls playsinline preload="metadata"></audio><div class="tools" id="tools"><button class="smallBtn" id="listenBtn" type="button">Reproducir audio</button><button class="smallBtn" id="againBtn" type="button">Grabar de nuevo</button></div></section>
    <section class="card solutionBox" id="solutionStep"><div class="step"><span class="num">3</span> Escucha la solucion</div><button id="playBtn" class="primary" style="display:none;margin-bottom:10px">ESCUCHAR SOLUCION</button><button id="sendBtn" class="primary" disabled>RECIBIR SOLUCION</button><div id="answer" class="answer"></div></section><section class="card transcriptBox" id="transcriptStep"><div class="step"><span class="num">4</span> Texto entendido</div><textarea id="typedText" placeholder="Ejemplo: Me rechazaron un prestamo y quiero saber si aparezco en Veraz."></textarea></section>
    <section class="proof"><div><b>Seguro:</b> no pedimos documentos en esta etapa.</div><div><b>Util:</b> guardamos categoria, audio, texto, UTM y CTA para mejorar con agentes de IA.</div></section><p class="legal">Esta orientacion es inicial y no reemplaza una consulta oficial, asesoramiento financiero ni asesoramiento legal.</p></main><div class="fixed"><div class="fixedInner"><button id="fixedSend" class="primary" disabled>RECIBIR SOLUCION</button></div></div>
    <script>
    let mediaRecorder,chunks=[],audioBase64='',audioMime='',audioUrl='',finalTranscript='',answerText='',selectedCategory='',startedAt=0,timerId=null,duration=0,initialTranscript='',isSubmitting=false,ctaRevealTimer=null,consentRevealTimer=null;
    const $=id=>document.getElementById(id), recordBtn=$('recordBtn'), sendBtn=$('sendBtn'), fixedSend=$('fixedSend'), playBtn=$('playBtn'), listenBtn=$('listenBtn'), againBtn=$('againBtn'), statusEl=$('status'), typedText=$('typedText'), answerEl=$('answer'), consent=$('consent'), consentBox=$('consentBox'), cta=$('cta'), errorEl=$('error'), timerEl=$('timer'), tools=$('tools'), preview=$('preview'), solutionStep=$('solutionStep'), transcriptStep=$('transcriptStep');
    function notifyParentScroll(target){try{window.parent&&window.parent.postMessage({type:'veraz_scroll',target},'*')}catch(e){}}
    const SpeechRecognition=window.SpeechRecognition||window.webkitSpeechRecognition; let recog=null;
    function showError(msg){errorEl.textContent=msg;errorEl.style.display='block'} function clearError(){errorEl.style.display='none';errorEl.textContent=''}
    function fmt(sec){sec=Math.max(0,Math.floor(sec));return String(Math.floor(sec/60)).padStart(2,'0')+':'+String(sec%60).padStart(2,'0')}
    function getParam(k){return new URLSearchParams(location.search).get(k)||''}
    function chooseRecorderOptions(){const types=['audio/mp4;codecs=mp4a.40.2','audio/mp4','audio/aac','audio/webm;codecs=opus','audio/webm','audio/ogg;codecs=opus'];if(!window.MediaRecorder)return null;for(const type of types){try{if(MediaRecorder.isTypeSupported&&MediaRecorder.isTypeSupported(type))return {mimeType:type}}catch(e){}}return {};}
    function updateSend(){const hasText=typedText.value.trim().length>8;if(hasText)transcriptStep.classList.add('show');const ok=!!selectedCategory&&(!!audioBase64||hasText)&&consent.checked&&!isSubmitting;sendBtn.disabled=!ok;fixedSend.disabled=!ok}
    function hideSolutionCta(){if(ctaRevealTimer)clearTimeout(ctaRevealTimer);ctaRevealTimer=null;cta.style.display='none'}
    function hideConsentBox(){if(consentRevealTimer)clearTimeout(consentRevealTimer);consentRevealTimer=null;consentBox.classList.remove('show');consent.classList.remove('recordingCta')}
    function scheduleConsentBox(){hideConsentBox();consentRevealTimer=setTimeout(()=>{consentBox.classList.add('show');consent.classList.add('recordingCta');notifyParentScroll('recordStep')},2000)}
    function revealSolutionCta(){if(!cta.href||cta.href.endsWith('#'))return;cta.style.display='inline-block';fixedSend.textContent=cta.textContent||'VER CONTENIDO SOLUCION';fixedSend.disabled=false;fixedSend.onclick=()=>{location.href=cta.href}}
    function scheduleSolutionCta(delay){hideSolutionCta();consentBox.classList.add('show');ctaRevealTimer=setTimeout(revealSolutionCta,delay||3000)}
    document.querySelectorAll('.choice').forEach(btn=>btn.onclick=()=>{document.querySelectorAll('.choice').forEach(b=>b.classList.remove('active'));btn.classList.add('active');selectedCategory=btn.dataset.cat;clearError();updateSend();statusEl.textContent='Perfecto. Ahora graba un audio corto contando tu caso.';setTimeout(()=>{document.getElementById('recordStep').scrollIntoView({behavior:'smooth',block:'center'});notifyParentScroll('recordStep')},120)});
    function autoSubmitIfReady(){if(!answerText&&!isSubmitting&&consent.checked&&selectedCategory&&(audioBase64||typedText.value.trim().length>8))submitLead();else if(audioBase64&&!consent.checked)statusEl.textContent='Audio listo. Marca la autorizacion para recibir la solucion automaticamente.'}
    consent.onchange=()=>{consent.classList.remove('recordingCta');updateSend();autoSubmitIfReady()}; typedText.oninput=()=>{clearError();updateSend()}; fixedSend.onclick=()=>sendBtn.click();
    if(SpeechRecognition){recog=new SpeechRecognition();recog.lang='es-AR';recog.continuous=true;recog.interimResults=true;recog.onresult=e=>{let interim='';for(let i=e.resultIndex;i<e.results.length;i++){let t=e.results[i][0].transcript;if(e.results[i].isFinal)finalTranscript+=t+' ';else interim+=t;}typedText.value=(finalTranscript+interim).trim();initialTranscript=finalTranscript.trim();if(typedText.value.trim().length>8)transcriptStep.classList.add('show');updateSend();};}
    async function startRecording(){clearError(); if(!selectedCategory){showError('Primero elegi que queres resolver.');return} if(!navigator.mediaDevices||!navigator.mediaDevices.getUserMedia||!window.MediaRecorder){showError('Tu navegador no permite grabar audio aqui. Escribi tu caso en el campo de texto.');typedText.placeholder='Si no podes grabar, escribi brevemente tu caso aca antes de enviar.';return} chunks=[]; finalTranscript=''; answerText=''; isSubmitting=false; answerEl.style.display='none'; playBtn.style.display='none'; solutionStep.classList.remove('show'); transcriptStep.classList.remove('show'); hideSolutionCta(); hideConsentBox(); sendBtn.style.display='inline-block';sendBtn.textContent='RECIBIR SOLUCION'; fixedSend.textContent='RECIBIR SOLUCION'; sendBtn.onclick=submitLead; fixedSend.onclick=()=>sendBtn.click(); typedText.value=''; typedText.placeholder='Si no aparece texto automatico, escribi brevemente tu caso aca antes de enviar.'; duration=0; audioBase64=''; audioMime=''; preview.pause(); preview.removeAttribute('src'); preview.load(); preview.style.display='none'; tools.style.display='none'; let stream; try{stream=await navigator.mediaDevices.getUserMedia({audio:true})}catch(e){showError('No pudimos acceder al microfono. Revisa el permiso o escribi tu caso en el campo de texto.');return} const options=chooseRecorderOptions(); try{mediaRecorder=new MediaRecorder(stream,options||undefined)}catch(e){try{mediaRecorder=new MediaRecorder(stream)}catch(err){stream.getTracks().forEach(t=>t.stop());showError('Este navegador no permitio iniciar la grabacion. Escribi tu caso en el campo de texto.');return}} audioMime=mediaRecorder.mimeType||((options&&options.mimeType)||''); mediaRecorder.ondataavailable=e=>{if(e.data&&e.data.size)chunks.push(e.data)}; mediaRecorder.onstop=()=>{stream.getTracks().forEach(t=>t.stop()); clearInterval(timerId); recordBtn.classList.remove('recording'); consent.classList.remove('recordingCta'); recordBtn.innerHTML='GRABAR<br>DE NUEVO'; if(!chunks.length){showError('No se guardo audio. Intenta grabar de nuevo o escribi tu caso.');updateSend();return} const blob=audioMime?new Blob(chunks,{type:audioMime}):new Blob(chunks); if(audioUrl)URL.revokeObjectURL(audioUrl); audioUrl=URL.createObjectURL(blob); preview.onerror=()=>{preview.style.display='none';showError('Tu navegador grabo el audio, pero no permite escuchar la previa aqui. Podes enviarlo igual o escribir tu caso abajo.')}; preview.oncanplay=()=>clearError(); preview.src=audioUrl; preview.controls=true; preview.style.display='block'; preview.load(); tools.style.display='grid'; const reader=new FileReader(); reader.onload=()=>{audioBase64=String(reader.result); statusEl.textContent=consent.checked?'Audio listo. Enviando para generar tu solucion...':'Audio listo. Marca la autorizacion para recibir la solucion automaticamente.'; updateSend(); autoSubmitIfReady();}; reader.readAsDataURL(blob); try{recog&&recog.stop()}catch(e){}}; try{mediaRecorder.start(1000)}catch(e){stream.getTracks().forEach(t=>t.stop());showError('No se pudo iniciar la grabacion. Escribi tu caso en el campo de texto.');return} startedAt=Date.now(); timerId=setInterval(()=>{duration=(Date.now()-startedAt)/1000; timerEl.textContent=fmt(duration); if(duration>=60&&mediaRecorder.state==='recording')stopRecording();},250); try{recog&&recog.start()}catch(e){} recordBtn.classList.add('recording'); scheduleConsentBox(); recordBtn.innerHTML='DETENER'; statusEl.textContent='Sigue hablando. En unos segundos aparecera la autorizacion para recibir tu solucion.';}
    function stopRecording(){if(mediaRecorder&&mediaRecorder.state==='recording')mediaRecorder.stop()} recordBtn.onclick=()=>mediaRecorder&&mediaRecorder.state==='recording'?stopRecording():startRecording(); againBtn.onclick=()=>startRecording(); listenBtn.onclick=async()=>{if(!audioUrl){showError('Todavia no hay audio para reproducir. Graba primero o escribi tu caso.');return} try{preview.style.display='block';preview.currentTime=0;await preview.play();clearError()}catch(e){showError('Este navegador no permitio reproducir la previa. El audio quedo guardado; podes enviarlo o grabar de nuevo.')}};
    function primeVoice(){if(!('speechSynthesis' in window))return;try{speechSynthesis.cancel();const u=new SpeechSynthesisUtterance('Preparando tu orientacion.');u.lang='es-AR';u.volume=.01;u.rate=1.1;speechSynthesis.speak(u)}catch(e){}}
    async function submitLead(){if(isSubmitting)return; isSubmitting=true; clearError(); if(!selectedCategory){showError('Elegi una situacion antes de enviar.');return} if(!consent.checked){isSubmitting=false;showError('Marca la autorizacion para recibir la solucion.');updateSend();return} if(!audioBase64&&!typedText.value.trim()){isSubmitting=false;showError('Graba un audio o escribi tu caso.');updateSend();return} primeVoice(); sendBtn.disabled=true;fixedSend.disabled=true;sendBtn.textContent='GENERANDO SOLUCION...';fixedSend.textContent='GENERANDO SOLUCION...';statusEl.textContent='Analizando tu audio y preparando la solucion...'; const payload={consent:consent.checked,selected_category:selectedCategory,audio_duration_seconds:duration,transcript_was_edited:typedText.value.trim()!==initialTranscript,audio_base64:audioBase64,audio_mime:audioMime,transcript_text:finalTranscript.trim(),typed_text:typedText.value.trim(),page_slug:'veraz-audio',landing_url:location.href,source_url:document.referrer,utm_source:getParam('utm_source'),utm_medium:getParam('utm_medium'),utm_campaign:getParam('utm_campaign'),utm_term:getParam('utm_term'),utm_content:getParam('utm_content'),gclid:getParam('gclid'),client_meta:{language:navigator.language,timezone:Intl.DateTimeFormat().resolvedOptions().timeZone}}; try{const res=await fetch('/api/veraz-audio/submit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}); const data=await res.json(); if(!res.ok||!data.ok)throw new Error(data.detail||'send_failed'); answerText=data.response_text; solutionStep.classList.add('show'); answerEl.style.display='block'; answerEl.innerHTML='<div class="resultTitle">Tu caso parece: '+(data.recommended_path||data.intent||'revision')+'</div><p>'+answerText+'</p>'; playBtn.textContent='ESCUCHAR SOLUCION';playBtn.style.display='block'; hideSolutionCta(); if(data.cta_url){cta.href=data.cta_url;cta.textContent=data.cta_label||'VER CONTENIDO SOLUCION';cta.onclick=()=>{fetch('/api/veraz-audio/cta-click',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({lead_id:data.lead_id})}).catch(()=>{})}} sendBtn.style.display='none';fixedSend.textContent='ESCUCHAR SOLUCION';isSubmitting=false;fixedSend.disabled=false;fixedSend.onclick=()=>speakAnswer(false);statusEl.textContent='Solucion lista. Escucha el audio y despues toca el contenido recomendado.';setTimeout(()=>speakAnswer(true),80);}catch(e){isSubmitting=false;showError('No pudimos conectar. Intenta nuevamente.');sendBtn.style.display='inline-block';sendBtn.textContent='RECIBIR SOLUCION';fixedSend.textContent='RECIBIR SOLUCION';sendBtn.disabled=false;fixedSend.disabled=false;}}
    function speakAnswer(auto){if(!answerText)return; if(!('speechSynthesis' in window)){if(!auto){showError('Tu navegador no permite leer la respuesta en voz alta. Podes leerla en pantalla.');scheduleSolutionCta(3000)}return;} try{speechSynthesis.cancel(); const u=new SpeechSynthesisUtterance(answerText); u.lang='es-AR'; u.rate=.95; u.onstart=()=>{statusEl.textContent='Escuchando solucion...';scheduleSolutionCta(3000)}; u.onboundary=(event)=>{if(event.charIndex>answerText.length*.68)revealSolutionCta()}; u.onend=()=>{clearError();statusEl.textContent='Solucion lista. Toca el contenido recomendado para seguir.'}; u.onerror=()=>{hideSolutionCta();statusEl.textContent='Solucion lista. Toca ESCUCHAR SOLUCION para liberar el contenido recomendado.';if(!auto)showError('No pudimos reproducir la voz. Podes leer la orientacion en pantalla y tocar el boton recomendado.')}; speechSynthesis.speak(u);}catch(e){if(!auto)showError('No pudimos reproducir la voz. Podes leer la orientacion en pantalla.')}} sendBtn.onclick=submitLead; playBtn.onclick=()=>speakAnswer(false);
    </script></body></html>
    """


@app.get("/veraz-audio", response_class=HTMLResponse)
def veraz_audio_page() -> str:
    return _veraz_audio_page()




@app.get("/veraz-audio/embed", response_class=HTMLResponse)
def veraz_audio_embed_page() -> str:
    page = _veraz_audio_page()
    page = page.replace("<body><main", "<body class=\"embedded\"><main")
    page = page.replace("</style>", ".embedded .hero{display:none}.embedded .wrap{padding:0 10px 18px}.embedded .fixed{display:none}.embedded .proof,.embedded .legal{display:none}</style>")
    page = page.replace("page_slug:'veraz-audio'", "page_slug:'esp-g-quali-veraz-audio'")
    return page


@app.get("/veraz-audio/install-code", response_class=HTMLResponse)
def veraz_audio_install_code() -> str:
    code = html.escape("<iframe src=\"https://app.turnos.xyz/veraz-audio/embed?site=esp-g.quali.sonora123.com\" title=\"Orientacion inicial sobre Veraz\" style=\"width:100%;min-height:1180px;border:0;border-radius:24px;overflow:hidden;background:transparent;\" loading=\"lazy\" allow=\"microphone; autoplay\" referrerpolicy=\"strict-origin-when-cross-origin\"></iframe>")
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta name='robots' content='noindex,nofollow'><title>Codigo de instalacion Veraz Audio</title><style>body{{font-family:Arial,sans-serif;max-width:920px;margin:32px auto;padding:0 16px}}pre{{white-space:pre-wrap;background:#f5f5f5;border:1px solid #ddd;border-radius:12px;padding:16px}}</style></head><body><h1>Codigo para WordPress</h1><p>Inserir em um bloco HTML personalizado no esp-g.quali.sonora123.com.</p><pre>{code}</pre></body></html>"""

@app.post("/api/veraz-audio/submit")
async def api_veraz_audio_submit(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    client_host = request.client.host if request.client else ""
    try:
        return record_veraz_audio_lead(runtime, payload, {"user_agent": request.headers.get("user-agent", ""), "ip": client_host})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/veraz-audio/cta-click")
async def api_veraz_audio_cta_click(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    return mark_veraz_audio_cta_click(runtime, str(payload.get("lead_id") or ""))


@app.get("/api/veraz-audio/leads")
def api_veraz_audio_leads_list() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_veraz_audio_leads(runtime, limit=100)




@app.get("/floor-bucket.js")
def floor_bucket_script(request: Request) -> Response:
    base_url = str(request.base_url).rstrip("/")
    return Response(
        content=floor_bucket_javascript(base_url),
        media_type="application/javascript",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/floor-bucket")
def api_floor_bucket(request: Request, response: Response, site: str = "", attr_id: str = "", block: str = "", placement: str = "", href: str = "") -> dict[str, object]:
    origin = request.headers.get("origin") or "*"
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Vary"] = "Origin"
    runtime = load_runtime(CONFIG_PATH)
    resolved_site = site or origin or href
    return floor_bucket_for_request(runtime, resolved_site, attr_id, block, placement)


@app.get("/api/traffic-session")
def api_traffic_session(
    session_id: str = "",
    site: str = "",
    attr_id: str = "",
    placement: str = "",
    url_path: str = "/",
    event_type: str = "page_view",
    target_url: str = "",
) -> Response:
    runtime = load_runtime(CONFIG_PATH)
    try:
        record_traffic_session(
            runtime,
            {
                "session_id": session_id,
                "site": site,
                "attr_id": attr_id,
                "placement": placement,
                "url_path": url_path,
                "event_type": event_type,
                "target_url": target_url,
            },
        )
    except ValueError:
        return Response(status_code=204)
    return Response(status_code=204)


def _customer_lead_db(runtime: dict[str, object]) -> Path:
    return Path(str(runtime["db_path"]))


def _ensure_customer_lead_tables(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS customer_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                site TEXT NOT NULL,
                page_path TEXT NOT NULL,
                source_url TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                gclid TEXT NOT NULL DEFAULT '',
                gbraid TEXT NOT NULL DEFAULT '',
                wbraid TEXT NOT NULL DEFAULT '',
                experiment_id TEXT NOT NULL DEFAULT '',
                experiment_variant TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                email_sha256 TEXT NOT NULL,
                consent_text TEXT NOT NULL,
                consent_version TEXT NOT NULL DEFAULT 'lead-customer-match-v1',
                user_agent TEXT NOT NULL DEFAULT '',
                ip_hash TEXT NOT NULL DEFAULT '',
                google_ads_conversion_status TEXT NOT NULL DEFAULT 'queued',
                data_manager_status TEXT NOT NULL DEFAULT 'queued',
                data_manager_request_id TEXT NOT NULL DEFAULT '',
                data_manager_synced_at TEXT NOT NULL DEFAULT '',
                sync_error TEXT NOT NULL DEFAULT '',
                UNIQUE(site, email_sha256)
            );
            CREATE INDEX IF NOT EXISTS idx_customer_leads_status
            ON customer_leads(data_manager_status, google_ads_conversion_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_customer_leads_attr
            ON customer_leads(site, attr_id, created_at);
            CREATE TABLE IF NOT EXISTS customer_lead_sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT ''
            );
            """
        )
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(customer_leads)").fetchall()}
        for name in ("gclid", "gbraid", "wbraid"):
            if name not in columns:
                conn.execute(f"ALTER TABLE customer_leads ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        conn.commit()


def _lead_clean_text(value: object, limit: int = 160) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text[:limit]


def _lead_normalize_email(value: object) -> str:
    email_text = str(value or "").strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email_text):
        raise ValueError("email invalido")
    return email_text


def _lead_site(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^https?://", "", text).split("/", 1)[0].split(":", 1)[0]
    if not text or not re.fullmatch(r"[a-z0-9.-]+", text):
        raise ValueError("site invalido")
    return text


def _lead_path(value: object) -> str:
    text = str(value or "/").strip()
    parsed = urlparse(text)
    path = parsed.path if parsed.scheme or parsed.netloc else text.split("?", 1)[0].split("#", 1)[0]
    if not path.startswith("/"):
        path = "/" + path
    return ("/" + path.strip("/") + "/") if path.strip("/") else "/"


def _lead_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _lead_experiment_attribution(runtime: dict[str, object], site: str, page_path: str) -> tuple[str, str]:
    db_path = _customer_lead_db(runtime)
    if not Path(db_path).exists():
        return "", page_path
    try:
        with sqlite3.connect(db_path, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT e.id AS experiment_id, v.url_path
                FROM funnel_experiments e
                JOIN funnel_experiment_variants v ON v.experiment_id=e.id
                WHERE e.project=? AND v.url_path=? AND e.status='running' AND v.status='active'
                ORDER BY e.id DESC, v.id DESC
                LIMIT 1
                """,
                (site, page_path),
            ).fetchone()
    except sqlite3.Error:
        return "", page_path
    if not row:
        return "", page_path
    return str(row["experiment_id"] or ""), str(row["url_path"] or page_path)


def _lead_query_value(source_url: str, *names: str) -> str:
    try:
        parsed = urlparse(str(source_url or ""))
    except ValueError:
        return ""
    values = parse_qs(parsed.query)
    for name in names:
        found = values.get(name)
        if found:
            return _lead_clean_text(found[0], 120)
    return ""


def _record_customer_lead(runtime: dict[str, object], payload: dict[str, object], request: Request) -> dict[str, object]:
    if str(payload.get("website") or "").strip():
        return {"ok": True, "ignored": True}
    if payload.get("consent") is not True:
        raise ValueError("consentimento obrigatorio")
    name = _lead_clean_text(payload.get("name"), 80)
    if len(name) < 2:
        raise ValueError("nome obrigatorio")
    email_text = _lead_normalize_email(payload.get("email"))
    site = _lead_site(payload.get("site") or request.headers.get("origin") or "")
    page_path = _lead_path(payload.get("page_path") or "/")
    source_url = _lead_clean_text(payload.get("source_url"), 500)
    attr_id = _lead_clean_text(payload.get("attr_id"), 80) or _lead_query_value(source_url, "attr_id", "campaign")
    inferred_experiment_id, inferred_variant = _lead_experiment_attribution(runtime, site, page_path)
    experiment_variant = _lead_clean_text(payload.get("experiment_variant"), 120) or page_path
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    email_hash = _lead_hash(email_text)
    ip_value = request.client.host if request.client else ""
    row = {
        "created_at": now,
        "site": site,
        "page_path": page_path,
        "source_url": source_url,
        "attr_id": attr_id,
        "gclid": _lead_query_value(source_url, "gclid") or _lead_clean_text(payload.get("gclid"), 180),
        "gbraid": _lead_query_value(source_url, "gbraid") or _lead_clean_text(payload.get("gbraid"), 180),
        "wbraid": _lead_query_value(source_url, "wbraid") or _lead_clean_text(payload.get("wbraid"), 180),
        "experiment_id": _lead_clean_text(payload.get("experiment_id"), 80) or inferred_experiment_id,
        "experiment_variant": experiment_variant or inferred_variant,
        "name": name,
        "email": email_text,
        "email_sha256": email_hash,
        "consent_text": _lead_clean_text(payload.get("consent_text"), 500),
        "user_agent": _lead_clean_text(request.headers.get("user-agent"), 500),
        "ip_hash": _lead_hash(ip_value) if ip_value else "",
    }
    db_path = _customer_lead_db(runtime)
    _ensure_customer_lead_tables(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        cursor = conn.execute(
            """
            INSERT INTO customer_leads (
                created_at, site, page_path, source_url, attr_id, gclid, gbraid, wbraid, experiment_id, experiment_variant,
                name, email, email_sha256, consent_text, user_agent, ip_hash
            ) VALUES (
                :created_at, :site, :page_path, :source_url, :attr_id, :gclid, :gbraid, :wbraid, :experiment_id, :experiment_variant,
                :name, :email, :email_sha256, :consent_text, :user_agent, :ip_hash
            )
            ON CONFLICT(site, email_sha256) DO UPDATE SET
                page_path=excluded.page_path,
                source_url=excluded.source_url,
                attr_id=excluded.attr_id,
                gclid=excluded.gclid,
                gbraid=excluded.gbraid,
                wbraid=excluded.wbraid,
                experiment_id=excluded.experiment_id,
                experiment_variant=excluded.experiment_variant,
                name=excluded.name,
                consent_text=excluded.consent_text,
                user_agent=excluded.user_agent
            """,
            row,
        )
        conn.commit()
    return {"ok": True, "lead_id": int(cursor.lastrowid or 0), "data_manager_status": "queued"}


def _customer_lead_summary(runtime: dict[str, object], limit: int = 100) -> dict[str, object]:
    db_path = _customer_lead_db(runtime)
    _ensure_customer_lead_tables(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            """
            SELECT id, created_at, site, page_path, attr_id, experiment_variant, name, email,
                   data_manager_status, data_manager_request_id, data_manager_synced_at, sync_error
            FROM customer_leads
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()]
        counts = {
            str(row["data_manager_status"]): int(row["total"])
            for row in conn.execute(
                "SELECT data_manager_status, COUNT(*) AS total FROM customer_leads GROUP BY data_manager_status"
            ).fetchall()
        }
    return {"rows": rows, "counts": counts}


def _customer_lead_sync_allowed(db_path: str | Path) -> bool:
    min_hours = max(1.0, float(os.getenv("CUSTOMER_LEAD_SYNC_MIN_HOURS", "12") or 12))
    try:
        with sqlite3.connect(db_path, timeout=10) as conn:
            row = conn.execute(
                "SELECT created_at FROM customer_lead_sync_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
    except sqlite3.Error:
        return True
    if not row or not row[0]:
        return True
    try:
        last_run = datetime.fromisoformat(str(row[0]))
    except ValueError:
        return True
    return (datetime.now().astimezone() - last_run).total_seconds() >= min_hours * 3600


def _record_customer_lead_sync_run(db_path: str | Path, result: dict[str, object]) -> None:
    status = "ok" if result.get("ok") else "block"
    message = str(result.get("message") or result.get("request_id") or "")[:800]
    sent = int(result.get("sent") or 0)
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.execute(
            "INSERT INTO customer_lead_sync_runs (created_at, status, sent, message) VALUES (?, ?, ?, ?)",
            (datetime.now().astimezone().isoformat(timespec="seconds"), status, sent, message),
        )
        conn.commit()


def _customer_lead_agent_report(runtime: dict[str, object], *, attempt_sync: bool = False, force_sync: bool = False) -> dict[str, object]:
    db_path = _customer_lead_db(runtime)
    _ensure_customer_lead_tables(db_path)
    backfill = _backfill_customer_lead_attribution(runtime)
    now = datetime.now().astimezone()
    sync_result: dict[str, object] | None = None
    if attempt_sync and os.getenv("DATA_MANAGER_DESTINATIONS_JSON", "").strip() and (force_sync or _customer_lead_sync_allowed(db_path)):
        try:
            sync_result = _sync_customer_leads_to_data_manager(runtime, limit=200, validate_only=False)
            _record_customer_lead_sync_run(db_path, sync_result)
        except Exception as exc:
            sync_result = {"ok": False, "message": str(exc)}
            _record_customer_lead_sync_run(db_path, sync_result)
    elif attempt_sync and os.getenv("DATA_MANAGER_DESTINATIONS_JSON", "").strip():
        sync_result = {"ok": True, "sent": 0, "message": "Sincronizacao automatica aguardando janela de 12 horas."}
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            str(row["data_manager_status"]): int(row["total"])
            for row in conn.execute(
                "SELECT data_manager_status, COUNT(*) AS total FROM customer_leads GROUP BY data_manager_status"
            ).fetchall()
        }
        total = int(conn.execute("SELECT COUNT(*) FROM customer_leads").fetchone()[0] or 0)
        missing_attr = int(conn.execute(
            "SELECT COUNT(*) FROM customer_leads WHERE TRIM(attr_id)=''"
        ).fetchone()[0] or 0)
        missing_variant = int(conn.execute(
            "SELECT COUNT(*) FROM customer_leads WHERE TRIM(experiment_variant)=''"
        ).fetchone()[0] or 0)
        queued_rows = [dict(row) for row in conn.execute(
            """
            SELECT id, created_at, page_path, attr_id, experiment_variant, data_manager_status, sync_error
            FROM customer_leads
            WHERE data_manager_status IN ('queued','retry')
            ORDER BY datetime(created_at) ASC
            LIMIT 20
            """
        ).fetchall()]
    stale = 0
    for row in queued_rows:
        try:
            created = datetime.fromisoformat(str(row.get("created_at") or ""))
            stale += int((now - created).total_seconds() > 900)
        except ValueError:
            stale += 1
    destinations_ok = bool(os.getenv("DATA_MANAGER_DESTINATIONS_JSON", "").strip())
    agents = [
        {
            "agent": "agente-lead-captacao",
            "item": "P2 com captacao de lead",
            "status": "ok" if total > 0 else "warn",
            "detail": f"{total} lead(s) captado(s)." if total > 0 else "Formulario instalado, mas ainda sem lead captado para validar ponta a ponta.",
        },
        {
            "agent": "agente-lead-atribuicao",
            "item": "Attr ID e variante do experimento",
            "status": "block" if missing_variant else ("warn" if missing_attr else "ok"),
            "detail": f"{missing_variant} lead(s) sem variante e {missing_attr} sem Attr ID; variante bloqueia decisao do teste, Attr ID ausente fica como alerta para leads sem parametro." if missing_attr or missing_variant else "Todos os leads possuem atribuicao minima para experimento/campanha.",
        },
        {
            "agent": "agente-data-manager-sync",
            "item": "Fila Data Manager",
            "status": "block" if (not destinations_ok and (counts.get("queued") or counts.get("retry"))) or stale else ("warn" if counts.get("queued") or counts.get("retry") else "ok"),
            "detail": (
                "DATA_MANAGER_DESTINATIONS_JSON ausente; leads nao saem da fila."
                if not destinations_ok and (counts.get("queued") or counts.get("retry"))
                else f"{stale} lead(s) parados ha mais de 15 minutos."
                if stale
                else f"{int(counts.get('queued') or 0)} queued, {int(counts.get('retry') or 0)} retry."
            ),
        },
        {
            "agent": "agente-google-ads-customer-list",
            "item": "Envio para lista Google Ads",
            "status": "ok" if int(counts.get("sent") or 0) > 0 or int(counts.get("validated") or 0) > 0 else ("block" if total > 0 else "warn"),
            "detail": f"{int(counts.get('sent') or 0)} enviado(s), {int(counts.get('validated') or 0)} validado(s)." if total > 0 else "Sem lead para enviar ainda.",
        },
    ]
    return {
        "status": "block" if any(item["status"] == "block" for item in agents) else ("warn" if any(item["status"] == "warn" for item in agents) else "ok"),
        "counts": counts,
        "total": total,
        "missing_attr": missing_attr,
        "missing_variant": missing_variant,
        "backfill": backfill,
        "queued_sample": queued_rows,
        "sync_result": sync_result,
        "agents": agents,
    }


def _backfill_customer_lead_attribution(runtime: dict[str, object]) -> dict[str, object]:
    db_path = _customer_lead_db(runtime)
    if not Path(db_path).exists():
        return {"updated": 0}
    updated = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, site, page_path, source_url, attr_id, experiment_id, experiment_variant
            FROM customer_leads
            WHERE TRIM(experiment_variant)='' OR TRIM(experiment_id)='' OR TRIM(attr_id)=''
            ORDER BY id
            LIMIT 500
            """
        ).fetchall()
        for row in rows:
            experiment_id, variant = _lead_experiment_attribution(runtime, str(row["site"] or ""), str(row["page_path"] or "/"))
            attr_id = str(row["attr_id"] or "").strip() or _lead_query_value(str(row["source_url"] or ""), "utm_campaign", "attr_id", "campaign")
            next_experiment_id = str(row["experiment_id"] or "").strip() or experiment_id
            next_variant = str(row["experiment_variant"] or "").strip() or variant or str(row["page_path"] or "")
            if attr_id != str(row["attr_id"] or "") or next_experiment_id != str(row["experiment_id"] or "") or next_variant != str(row["experiment_variant"] or ""):
                conn.execute(
                    """
                    UPDATE customer_leads
                    SET attr_id=?, experiment_id=?, experiment_variant=?
                    WHERE id=?
                    """,
                    (attr_id, next_experiment_id, next_variant, int(row["id"])),
                )
                updated += 1
        conn.commit()
    return {"updated": updated}


def _data_manager_access_token(runtime: dict[str, object]) -> str:
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest

        credentials = _google_oauth_credentials_from_runtime(runtime, ["https://www.googleapis.com/auth/datamanager"])
        credentials.refresh(GoogleAuthRequest())
        return str(credentials.token or "")
    except Exception:
        return ""


def _sync_customer_leads_to_data_manager(runtime: dict[str, object], limit: int = 200, validate_only: bool = False) -> dict[str, object]:
    destinations_raw = os.getenv("DATA_MANAGER_DESTINATIONS_JSON", "").strip()
    if not destinations_raw:
        return {"ok": False, "message": "DATA_MANAGER_DESTINATIONS_JSON ausente; leads permanecem em fila."}
    try:
        destinations = json.loads(destinations_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("DATA_MANAGER_DESTINATIONS_JSON invalido") from exc
    if not isinstance(destinations, list) or not destinations:
        raise ValueError("destinos Data Manager ausentes")
    token = _data_manager_access_token(runtime)
    if not token:
        return {"ok": False, "message": "OAuth sem token valido para Data Manager."}
    db_path = _customer_lead_db(runtime)
    _ensure_customer_lead_tables(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, email_sha256
            FROM customer_leads
            WHERE data_manager_status IN ('queued','retry')
            ORDER BY datetime(created_at) ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    if not rows:
        return {"ok": True, "sent": 0, "message": "Sem leads pendentes."}
    members = [
        {
            "compositeData": {
                "userData": {
                    "userIdentifiers": [{"emailAddress": str(row["email_sha256"])}]
                }
            }
        }
        for row in rows
    ]
    body = {
        "destinations": destinations,
        "audienceMembers": members,
        "encoding": "HEX",
        "validateOnly": bool(validate_only),
        "consent": {
            "adUserData": "CONSENT_GRANTED",
            "adPersonalization": "CONSENT_GRANTED",
        },
        "termsOfService": {
            "customerMatchTermsOfServiceStatus": "ACCEPTED",
        },
    }
    request = urllib.request.Request(
        "https://datamanager.googleapis.com/v1/audienceMembers:ingest",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8") or "{}")
        request_id = str(result.get("requestId") or "")
        status = "validated" if validate_only else "sent"
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.executemany(
                "UPDATE customer_leads SET data_manager_status=?, google_ads_conversion_status=?, data_manager_request_id=?, data_manager_synced_at=?, sync_error='' WHERE id=?",
                [(status, status, request_id, now, int(row["id"])) for row in rows],
            )
            conn.commit()
        return {"ok": True, "sent": len(rows), "request_id": request_id, "validate_only": bool(validate_only)}
    except urllib.error.HTTPError as exc:
        error_text = exc.read().decode("utf-8", "replace")[:800]
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.executemany(
                "UPDATE customer_leads SET data_manager_status='retry', google_ads_conversion_status='retry', sync_error=? WHERE id=?",
                [(error_text, int(row["id"])) for row in rows],
            )
            conn.commit()
        return {"ok": False, "sent": 0, "message": error_text}


@app.post("/api/customer-leads")
async def api_customer_leads(request: Request, response: Response) -> dict[str, object]:
    origin = request.headers.get("origin") or "*"
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Vary"] = "Origin"
    runtime = load_runtime(CONFIG_PATH)
    try:
        payload = await request.json()
        return _record_customer_lead(runtime, payload, request)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


async def _experiment_proxy_payload(request: Request, db_path: str | Path) -> dict[str, object]:
    secret = os.getenv("FUNNEL_EXPERIMENT_PROXY_SECRET", "").strip()
    timestamp = str(request.headers.get("x-kaizen-timestamp") or "")
    nonce = str(request.headers.get("x-kaizen-nonce") or "")
    signature = str(request.headers.get("x-kaizen-signature") or "")
    body = await request.body()
    try:
        return verify_proxy_request(
            db_path,
            secret,
            request.client.host if request.client else "",
            timestamp,
            nonce,
            signature,
            body,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="proxy invalido") from exc


@app.get("/funnel-experiments.js")
def funnel_experiments_script(request: Request) -> Response:
    return Response(
        content=experiment_javascript(str(request.base_url).rstrip("/")),
        media_type="application/javascript",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.post("/api/funnel-experiments/public/assign")
async def api_funnel_experiment_assign(
    request: Request,
) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await _experiment_proxy_payload(request, runtime["db_path"])
    try:
        return assign_variant(
            runtime,
            str(payload.get("site") or ""),
            str(payload.get("source_path") or "/"),
            str(payload.get("visitor_id") or ""),
            str(payload.get("attr_id") or ""),
            _auth_secret(),
        )
    except ValueError:
        return {"active": False}


@app.post("/api/funnel-experiments/public/event")
async def api_funnel_experiment_event(
    request: Request,
    experiment_id: int = 0,
    cycle: int = 0,
    variant_id: int = 0,
    visitor_id: str = "",
    attr_id: str = "",
    event_type: str = "",
    page_path: str = "/",
    project: str = "",
    assignment_token: str = "",
    token_issued_at: str = "",
) -> Response:
    runtime = load_runtime(CONFIG_PATH)
    body = await _experiment_proxy_payload(request, runtime["db_path"])
    experiment_id = int(body.get("experiment_id") or experiment_id)
    cycle = int(body.get("cycle") or cycle)
    variant_id = int(body.get("variant_id") or variant_id)
    visitor_id = str(body.get("visitor_id") or visitor_id)
    attr_id = str(body.get("attr_id") or attr_id)
    event_type = str(body.get("event_type") or event_type)
    page_path = str(body.get("page_path") or page_path)
    project = str(body.get("project") or project)
    assignment_token = str(body.get("assignment_token") or assignment_token)
    token_issued_at = str(body.get("token_issued_at") or token_issued_at)
    try:
        record_experiment_event(
            runtime,
            {
                "experiment_id": experiment_id,
                "cycle": cycle,
                "variant_id": variant_id,
                "visitor_id": visitor_id,
                "attr_id": attr_id,
                "event_type": event_type,
                "page_path": page_path,
                "project": project,
                "assignment_token": assignment_token,
                "token_issued_at": token_issued_at,
            },
            _auth_secret(),
        )
    except ValueError:
        pass
    return Response(status_code=204)


@app.post("/api/funnel-experiments/public/element")
async def api_funnel_experiment_element(request: Request) -> Response:
    runtime = load_runtime(CONFIG_PATH)
    body = await _experiment_proxy_payload(request, runtime["db_path"])
    try:
        record_experiment_element_event(runtime, body, _auth_secret())
    except ValueError:
        pass
    return Response(status_code=204)


@app.get("/api/funnel-experiments")
def api_funnel_experiments(project: str = "") -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return list_experiments(runtime, project)


@app.post("/api/funnel-experiments")
async def api_create_funnel_experiment(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        payload = await request.json()
        actor = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")[2]
        return create_experiment(runtime, payload, actor)
    except (ValueError, IndexError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/funnel-experiments/{experiment_id}/promote/{variant_id}")
def api_promote_funnel_experiment(
    request: Request,
    experiment_id: int,
    variant_id: int,
) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        actor = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")[2]
        return promote_variant(runtime, experiment_id, variant_id, actor)
    except (ValueError, IndexError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/funnel-experiments/{experiment_id}/status/{status}")
def api_set_funnel_experiment_status(
    request: Request,
    experiment_id: int,
    status: str,
) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        actor = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")[2]
        return set_experiment_status(runtime, experiment_id, status, actor)
    except (ValueError, IndexError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _experiment_clarity_by_url(experiment: dict[str, object]) -> dict[str, dict[str, object]]:
    database = CONFIG_PATH.with_name("roas_control.sqlite3")
    project = str(experiment.get("project") or "")
    try:
        start_day = datetime.fromisoformat(str(experiment.get("started_at") or "")).date().isoformat()
    except (TypeError, ValueError):
        start_day = app_today()
    paths = {
        str(item.get("url_path") or "/"): {
            "coverage": "project_only",
            "page_visits": 0,
            "date_start": start_day,
            "date_end": app_today(),
            "message": "Clarity confirma visitas por URL; comportamento e elementos por URL nao estao disponiveis no Data Export.",
        }
        for item in experiment.get("variants") or []
    }
    if not database.exists() or not paths:
        return paths
    host = f"https://{project}"
    with sqlite3.connect(database, timeout=10) as conn:
        rows = conn.execute(
            """
            SELECT payload FROM clarity_snapshots
            WHERE project_id=? AND date_start BETWEEN ? AND ?
            ORDER BY collected_at
            """,
            (project, start_day, app_today()),
        ).fetchall()
    for row in rows:
        try:
            payload = json.loads(row[0] or "[]")
        except json.JSONDecodeError:
            continue
        popular = next(
            (
                item.get("information") or []
                for item in payload
                if item.get("metricName") == "PopularPages"
            ),
            [],
        )
        for item in popular:
            page_path = urlparse(str(item.get("url") or "")).path
            page_path = "/" + page_path.strip("/") + "/"
            if page_path in paths:
                paths[page_path]["page_visits"] += int(item.get("visitsCount") or 0)
    return paths


@app.get("/api/funnel-experiments/{experiment_id}/history")
def api_funnel_experiment_history(experiment_id: int) -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return experiment_history(runtime, experiment_id)


@app.post("/api/funnel-experiments/{experiment_id}/candidates")
async def api_register_funnel_experiment_candidate(
    request: Request,
    experiment_id: int,
) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        actor = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")[2]
        payload = await request.json()
        return register_experiment_candidate(runtime, experiment_id, payload, actor)
    except (ValueError, IndexError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/funnel-experiments/{experiment_id}/pause-variant/{variant_id}")
async def api_pause_funnel_experiment_variant(
    request: Request,
    experiment_id: int,
    variant_id: int,
) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        actor = request.cookies.get(AUTH_COOKIE_NAME, "").split(":")[2]
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        current = get_experiment(runtime, experiment_id)
        return pause_and_replace_variant(
            runtime,
            experiment_id,
            variant_id,
            actor,
            _experiment_clarity_by_url(current),
            str((payload or {}).get("reason") or ""),
        )
    except (ValueError, IndexError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/gam-top-countries")
def api_gam_top_countries(
    request: Request,
    response: Response,
    attr_id: str = "",
    days: int = 7,
) -> dict[str, object]:
    origin = request.headers.get("origin") or "*"
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Vary"] = "Origin"
    runtime = load_runtime(CONFIG_PATH)
    return {
        "attr_id": re.sub(r"\D+", "", str(attr_id or "")),
        "days": max(1, min(int(days or 7), 31)),
        "countries": load_gam_top_countries(runtime, attr_id, days),
    }


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    return _login_page()


@app.post("/login")
async def login(request: Request):
    body = (await request.body()).decode("utf-8", "ignore")
    form = parse_qs(body)
    email = form.get("email", [""])[0].strip().lower()
    password = form.get("password", [""])[0]
    password_hashes = _auth_password_hashes()
    expected_hash = password_hashes.get(email)
    if expected_hash is None:
        return HTMLResponse(_login_page("Email sem acesso"), status_code=401)
    if not hmac.compare_digest(_password_hash(password), expected_hash):
        return HTMLResponse(_login_page("Senha invalida"), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(AUTH_COOKIE_NAME, _make_auth_cookie(email), max_age=AUTH_MAX_AGE, httponly=True, secure=True, samesite="lax")
    return response


@app.get("/logout")
def logout() -> RedirectResponse:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/health/operational")
def api_operational_health() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_operational_health(runtime)


def _julio_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def _julio_table_count(conn: sqlite3.Connection, table: str) -> int | None:
    if not _julio_table_exists(conn, table):
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _julio_table_max(conn: sqlite3.Connection, table: str, column: str) -> str:
    if not _julio_table_exists(conn, table):
        return ""
    try:
        return str(conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0] or "")
    except sqlite3.Error:
        return ""


def _julio_gam_content_status(conn: sqlite3.Connection) -> dict[str, object]:
    if not _julio_table_exists(conn, "gam_content_daily"):
        return {
            "status": "block",
            "detail": "Tabela gam_content_daily ausente; como o GAM sempre tem dados, isto e falha critica de sync/conferencia do agente-gam.",
            "impressions": None,
            "revenue": None,
        }
    try:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(impressions), 0) AS impressions,
                COALESCE(SUM(revenue), 0) AS revenue,
                MAX(day) AS last_day
            FROM gam_content_daily
            """
        ).fetchone()
    except sqlite3.Error as exc:
        return {
            "status": "block",
            "detail": f"Falha ao ler gam_content_daily: {exc}",
            "impressions": None,
            "revenue": None,
        }
    impressions = int(row["impressions"] or 0)
    revenue = float(row["revenue"] or 0.0)
    last_day = str(row["last_day"] or "")
    if impressions <= 0:
        return {
            "status": "block",
            "detail": f"Sem impressoes GAM conciliadas; ultimo dia: {last_day or 'indisponivel'}. Como o GAM sempre tem dados, isto e falha critica de sync/conferencia.",
            "impressions": impressions,
            "revenue": revenue,
        }
    ecpm = revenue / impressions * 1000.0
    return {
        "status": "ok",
        "detail": f"GAM conciliado: {impressions} impressoes, eCPM {_money(ecpm)}; ultimo dia {last_day}.",
        "impressions": impressions,
        "revenue": revenue,
        "ecpm": ecpm,
        "last_day": last_day,
    }


def _julio_data_checks() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    db_path = Path(str(runtime.get("db_path") or CONFIG_PATH.with_name("roas_control.sqlite3")))
    now = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
    checks: list[dict[str, object]] = []
    counts: dict[str, object] = {"db_path": str(db_path)}
    if not db_path.exists():
        return {
            "status": "block",
            "generated_at": now,
            "counts": counts,
            "checks": [{
                "agent": "agente-consistencia-dados",
                "item": "Banco operacional",
                "status": "block",
                "detail": "Banco da Julio nao encontrado; nenhuma metrica deve ser usada para decisao.",
            }],
        }
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        traffic_tables = ("traffic_sessions", "traffic_session_events", "traffic_events")
        traffic_counts = {
            table: _julio_table_count(conn, table)
            for table in traffic_tables
            if _julio_table_count(conn, table) is not None
        }
        experiment_tables = (
            "funnel_experiment_assignments",
            "funnel_experiment_events",
            "funnel_experiment_element_events",
        )
        experiment_counts = {
            table: _julio_table_count(conn, table)
            for table in experiment_tables
            if _julio_table_count(conn, table) is not None
        }
        agent_count = _julio_table_count(conn, "agent_observations")
        clarity_count = _julio_table_count(conn, "clarity_snapshots")
        ads_counts = {
            table: _julio_table_count(conn, table)
            for table in (
                "google_ads_script_spend",
                "google_ads_spend_daily",
                "google_ads_daily",
                "google_ads_campaign_daily",
            )
            if _julio_table_count(conn, table) is not None
        }
        gam_status = _julio_gam_content_status(conn)
        lead_report = _customer_lead_agent_report(runtime, attempt_sync=False)
        counts.update({
            "traffic": traffic_counts,
            "experiments": experiment_counts,
            "agent_observations": agent_count,
            "clarity_snapshots": clarity_count,
            "ads": ads_counts,
            "gam": {
                "impressions": gam_status.get("impressions"),
                "revenue": gam_status.get("revenue"),
                "ecpm": gam_status.get("ecpm"),
            },
            "leads": {
                "total": lead_report.get("total"),
                "counts": lead_report.get("counts"),
                "missing_attr": lead_report.get("missing_attr"),
                "missing_variant": lead_report.get("missing_variant"),
            },
        })
    traffic_total = sum(int(value or 0) for value in traffic_counts.values())
    experiment_events = sum(int(value or 0) for value in experiment_counts.values())
    checks.append({
        "agent": "agente-utm-attrid",
        "item": "Visitas identificadas no sistema Julio",
        "status": "ok" if traffic_total > 0 else "block",
        "detail": (
            f"{traffic_total} registro(s) first-party no rastreador Julio."
            if traffic_total > 0
            else "GA4/Analytics pode mostrar visitas, mas o rastreador Julio nao tem eventos first-party conciliados."
        ),
    })
    checks.append({
        "agent": "agente-experimentos-funil",
        "item": "Eventos de split/funil",
        "status": "ok" if experiment_events > 0 else "block",
        "detail": (
            f"{experiment_events} evento(s) de experimento/funil registrados."
            if experiment_events > 0
            else "Sem eventos de experimento; P1/P2 podem estar com URL fixa ou sem cliente/proxy de split instalado."
        ),
    })
    checks.append({
        "agent": "agente-gam",
        "item": "eCPM GAM real",
        "status": gam_status["status"],
        "detail": gam_status["detail"],
    })
    checks.append({
        "agent": "agente-ads",
        "item": "Google Ads: investimento, CPA e conversoes",
        "status": "ok" if sum(int(value or 0) for value in ads_counts.values()) > 0 else "block",
        "detail": (
            f"{sum(int(value or 0) for value in ads_counts.values())} registro(s) locais de Google Ads encontrados."
            if sum(int(value or 0) for value in ads_counts.values()) > 0
            else "Sem snapshot local de Google Ads; investimento, CPA e conversoes nao podem ser conferidos para decisao."
        ),
    })
    checks.append({
        "agent": "agente-clarity",
        "item": "Cobertura Clarity",
        "status": "ok" if int(clarity_count or 0) > 0 else "block",
        "detail": (
            f"{clarity_count} snapshot(s) Clarity armazenados."
            if int(clarity_count or 0) > 0
            else "Sem snapshot Clarity local; nao atribuir causa visual sem dado por pagina."
        ),
    })
    checks.append({
        "agent": "agente-site",
        "item": "GTM, GA4, Clarity e Ad Inserter",
        "status": "ok" if int(traffic_total or 0) > 0 and int(clarity_count or 0) > 0 else "block",
        "detail": (
            "Tracking first-party e Clarity possuem dados locais; manter conferencia visual de GTM/GA4/Ad Inserter antes de trafego pago."
            if int(traffic_total or 0) > 0 and int(clarity_count or 0) > 0
            else "Tracking ou Clarity sem confirmacao local; GTM, GA4, Clarity e Ad Inserter devem ser validados antes de decisao."
        ),
    })
    for lead_check in lead_report.get("agents") or []:
        checks.append(dict(lead_check))
    checks.append({
        "agent": "agente-aprendizado",
        "item": "Observacoes dos agentes",
        "status": "ok" if int(agent_count or 0) > 0 else "block",
        "detail": (
            f"{agent_count} observacao(oes) de agente registradas."
            if int(agent_count or 0) > 0
            else "Sem observacoes registradas; erro recorrente precisa gerar aprendizado."
        ),
    })
    checks.append({
        "agent": "agente-consistencia-dados",
        "item": "Decisao automatica",
        "status": "block" if any(item["status"] == "block" for item in checks) else "ok",
        "detail": "Bloquear otimizacao se visitas, eventos Julio e eCPM GAM nao estiverem conciliados; GAM ausente no sistema e incidente, nao ausencia real de dados.",
    })
    overall = "ok"
    if any(item["status"] == "block" for item in checks):
        overall = "block"
    elif any(item["status"] == "warn" for item in checks):
        overall = "warn"
    return {
        "status": overall,
        "generated_at": now,
        "counts": counts,
        "risk_policy": {
            "baixo": "mudar texto interno ou registrar aprendizado",
            "medio": "criar teste AB ou mudar CTA",
            "alto": "alterar floor, pausar campanha, mudar orcamento ou mexer em GTM/GAM",
            "critico": "OAuth, APIs, tracking, Ad Inserter, dominio ou monetizacao",
            "required_for_high_critical": [
                "backup",
                "simulacao",
                "validacao antes/depois",
                "registro no graphify",
                "rollback documentado",
            ],
        },
        "checks": checks,
    }


@app.get("/api/data-checks")
def api_data_checks() -> dict[str, object]:
    return _julio_data_checks()


@app.get("/data-checks", response_class=HTMLResponse)
def data_checks_page() -> str:
    report = _julio_data_checks()
    rows = []
    for item in report["checks"]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item['agent']))}</td>"
            f"<td>{html.escape(str(item['item']))}</td>"
            f"<td>{_status_badge(str(item['status']))}</td>"
            f"<td>{html.escape(str(item['detail']))}</td>"
            "</tr>"
        )
    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Conferencia dos agentes</div>
          <h1>Integridade de visitas, funil e GAM</h1>
          <p>Esta tela responde se as visitas vistas no Analytics tambem foram identificadas pelo rastreador Julio e se o eCPM GAM esta realmente conciliado. Como o GAM sempre tem dados, ausencia de eCPM no sistema e incidente de sync/conferencia, nao dado zero.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div><div class='aside-label'>Status geral</div><strong>{html.escape(str(report['status']))}</strong></div>
          <div><div class='aside-label'>Gerado em</div><strong>{html.escape(str(report['generated_at']))}</strong></div>
        </div>
      </div>
      <div class='panel'>
        <table>
          <thead><tr><th>Agente</th><th>Item</th><th>Status</th><th>Detalhe</th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
      <div class='panel'>
        <h2>Regra operacional</h2>
        <p>Se GA4 mostra visitas, mas esta tela nao mostra eventos first-party ou eventos de funil, o trafego existe fora da conferencia do sistema. Se GAM nao tiver impressoes conciliadas, abrir incidente do agente-gam: o dado existe no GAM, mas nao foi conciliado pelo sistema.</p>
        <p><strong>Risco alto/critico:</strong> alterar floor, pausar campanha, mudar orcamento, GTM/GAM, OAuth, APIs, tracking, Ad Inserter, dominio ou monetizacao exige backup, simulacao, validacao antes/depois, registro no graphify e rollback documentado.</p>
      </div>
    </div>
    """
    return _layout("Conferencia de Dados", content, "data-checks")



def _script_token(request: Request) -> str:
    return str(request.query_params.get("token") or "")


@app.get("/api/google-ads-script/pending-actions")
def api_google_ads_script_pending_actions(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return google_ads_script_pending_actions(runtime, request.query_params.get("customer_id") or "")
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post("/api/google-ads-script/spend")
async def api_google_ads_script_spend(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return ingest_google_ads_script_spend(runtime, await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post("/api/google-ads-script/settings")
async def api_google_ads_script_settings(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return ingest_google_ads_script_settings(runtime, await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post("/api/google-ads-script/placements")
async def api_google_ads_script_placements(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return ingest_google_ads_script_placements(runtime, await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post("/api/google-ads-script/result")
async def api_google_ads_script_result(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return record_google_ads_script_result(runtime, await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/google-ads-script/log")
async def api_google_ads_script_log(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        verify_google_ads_script_token(runtime, _script_token(request))
        return record_google_ads_script_run(runtime, await request.json())
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

@app.get("/api/overview")
def api_overview() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_dashboard_summary(runtime, app_today())


@app.get("/api/projects")
def api_projects() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_project_kpis(runtime, app_today())


@app.get("/api/projects/{project}")
def api_project_detail(project: str) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    current_day = app_today()
    rows = load_project_campaign_rows(runtime, project, current_day)
    kpis = load_project_kpis(runtime, current_day)
    summary = load_project_summary(runtime, project, current_day, project_rows=rows, project_kpis=kpis)
    budget_actions = _budget_increase_actions_today(runtime)
    budget_manual_actions = _budget_manual_actions_latest(runtime)
    if not summary:
        raise HTTPException(status_code=404, detail="Projeto nao encontrado")
    return {"summary": summary, "rows": rows}


@app.get("/api/projects/{project}/campaigns/{attr_id}/gam-blocks")
def api_project_campaign_gam_blocks(project: str, attr_id: str, period: str = "today", start_date: str | None = None, end_date: str | None = None) -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return load_project_campaign_blocks(runtime, project, attr_id, period, app_today(), start_date=start_date, end_date=end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/agent-alert-feedback")
async def api_agent_alert_feedback(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    try:
        return record_agent_alert_feedback(runtime, dict(payload or {}))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/projects/{project}/campaigns/{attr_id}/gam-blocks/floor-action")
async def api_project_campaign_floor_action(project: str, attr_id: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    payload["project"] = project
    payload["attr_id"] = attr_id
    try:
        return record_gam_floor_manual_action(runtime, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/projects/{project}/campaigns/{attr_id}/bid-action")
async def api_project_campaign_bid_action(project: str, attr_id: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    payload["project"] = project
    payload["attr_id"] = attr_id
    try:
        return record_campaign_bid_manual_action(runtime, dict(payload or {}))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/projects/{project}/campaigns/{attr_id}/budget-action")
async def api_project_campaign_budget_action(project: str, attr_id: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    payload["project"] = project
    payload["attr_id"] = attr_id
    try:
        return record_campaign_budget_manual_action(runtime, dict(payload or {}))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/projects/{project}/campaigns/{attr_id}/status-action")
async def api_project_campaign_status_action(project: str, attr_id: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    payload["project"] = project
    payload["attr_id"] = attr_id
    try:
        return record_campaign_status_manual_action(runtime, dict(payload or {}))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/projects/{project}/campaigns/{attr_id}/placements")
def api_project_campaign_placements(project: str, attr_id: str, period: str = "today") -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return load_project_campaign_placements(runtime, project, attr_id, period, app_today())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/projects/{project}/campaigns/{attr_id}/metric-history")
def api_campaign_metric_history(project: str, attr_id: str, metric: str = "investment", days: int = 30, scope: str = "days") -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_campaign_metric_history(runtime, project, attr_id, metric, app_today(), days, scope)


@app.post("/api/projects/{project}/campaigns/{attr_id}/placements/exclude")
async def api_exclude_project_campaign_placements(project: str, attr_id: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    try:
        return exclude_project_campaign_placements(runtime, project, attr_id, list(payload.get("placements") or []))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/campaign-builder/agent-scoreboard")
def api_campaign_builder_agent_scoreboard() -> dict[str, object]:
    db_path = CONFIG_PATH.with_name("agent_memory.sqlite3")
    if not db_path.exists():
        return {"status": "ok", "agents": []}
    rows: list[dict[str, object]] = []
    study_tasks: list[dict[str, object]] = []
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        for row in conn.execute("""
            SELECT
                agent_slug,
                score_role,
                COALESCE(SUM(points), 0) AS score,
                COUNT(*) AS events,
                MAX(created_at) AS last_event
            FROM agent_score_events
            WHERE instance = 'campaign_builder'
            GROUP BY agent_slug, score_role
            ORDER BY score DESC, events DESC, agent_slug ASC
            LIMIT 30
        """):
            rows.append({
                "agent": row["agent_slug"],
                "role": row["score_role"],
                "score": int(row["score"] or 0),
                "events": int(row["events"] or 0),
                "last_event": row["last_event"] or "",
            })
        for row in conn.execute("""
            SELECT agent_slug, status, title, missing_evidence, new_rule_needed, created_at
            FROM agent_study_tasks
            WHERE instance = 'campaign_builder' AND status = 'OPEN'
            ORDER BY created_at DESC
            LIMIT 12
        """):
            study_tasks.append({
                "agent": row["agent_slug"],
                "status": row["status"],
                "title": row["title"],
                "missing_evidence": row["missing_evidence"],
                "new_rule_needed": row["new_rule_needed"],
                "created_at": row["created_at"],
            })
    return {"status": "ok", "agents": rows, "study_tasks": study_tasks}


@app.post("/api/campaign-builder/model-prefill")
async def api_campaign_builder_model_prefill(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    return model_campaign_prefill(
        CONFIG_PATH,
        str(payload.get("model_customer_id") or payload.get("customer_id") or ""),
        str(payload.get("model_campaign_id") or ""),
        str(payload.get("model_ad_group_id") or ""),
    )


@app.post("/api/campaign-builder/draft")
async def api_campaign_builder_draft(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    try:
        return build_campaign_draft(CONFIG_PATH, payload)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

def _campaign_builder_error_payload(exc: Exception) -> dict[str, object]:
    details: list[str] = []
    request_id = getattr(exc, "request_id", "")
    failure = getattr(exc, "failure", None)
    for err in getattr(failure, "errors", []) or []:
        message = getattr(err, "message", "") or ""
        trigger = ""
        try:
            trigger = getattr(getattr(err, "trigger", None), "string_value", "") or ""
        except Exception:
            trigger = ""
        policy = ""
        try:
            pvd = err.details.policy_violation_details
            policy = getattr(pvd, "external_policy_name", "") or getattr(pvd, "external_policy_description", "") or ""
            if getattr(pvd, "external_policy_description", ""):
                message = pvd.external_policy_description
        except Exception:
            pass
        item = message
        if policy and policy not in item:
            item = f"{policy}: {item}"
        if trigger:
            item = f"{item} | texto: {trigger}"
        if item:
            details.append(item)
    error = " | ".join(details) if details else str(exc)
    return {"status": "error", "error": error, "request_id": str(request_id or ""), "error_type": type(exc).__name__}


@app.post("/api/campaign-builder/apply")
async def api_campaign_builder_apply(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    try:
        return apply_campaign_builder_draft(CONFIG_PATH, payload)
    except Exception as exc:
        return _campaign_builder_error_payload(exc)


@app.get("/api/actions")
def api_actions() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_action_rows(runtime)


@app.get("/api/roi-control")
def api_roi_control() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return {
        "runs": load_roi_control_runs(runtime, limit=30),
        "actions": load_roi_control_actions(runtime, limit=80),
        "outcomes": load_roi_control_outcomes(runtime, limit=80),
        "metrics": load_roi_control_latest_metrics(runtime, limit=120),
    }


@app.get("/api/push-control")
def api_push_control() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_push_control_summary(runtime, app_today(), limit=120)


@app.get("/api/push-optimization")
def api_push_optimization() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_push_optimization_summary(runtime, app_today(), limit=120)


@app.get("/api/push-installation")
def api_push_installation() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_push_installation_checks(runtime)


@app.get("/api/gam-sync")
def api_gam_sync() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_gam_sync_rows(runtime)


@app.get("/api/agents")
def api_agents() -> dict[str, object]:
    return {"epics": epic_summary(), "agents": AGENT_BACKLOG}


@app.get("/api/agents/phase1")
def api_agents_phase1() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_phase1_agent_dashboard(runtime, limit=120)


@app.get("/api/agents/quality-os")
def api_agents_quality_os() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return load_quality_os_dashboard(runtime)


@app.get("/api/war-room")
def api_war_room() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return load_nexus_agent_events(runtime, limit=40, today=app_today())


@app.get("/api/telegram")
def api_telegram() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return {
        "channels": load_nexus_telegram_channels(runtime),
        "messages": load_nexus_telegram_messages(runtime, limit=40),
        "updates": load_nexus_telegram_updates(runtime, limit=20),
    }


@app.get("/api/approvals")
def api_approvals() -> list[dict[str, object]]:
    runtime = load_runtime(CONFIG_PATH)
    return [row for row in load_action_rows(runtime, limit=200) if str(row.get("status", "") or "").upper() == "AWAITING_APPROVAL"]


@app.post("/internal/agent-cycle")
def internal_agent_cycle() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    events = load_nexus_agent_events(runtime, limit=40, today=app_today())
    phase1 = run_phase1_agent_observer(runtime, "nexus", today=app_today())
    lead_agents = _customer_lead_agent_report(runtime, attempt_sync=True)
    processed_updates = process_telegram_updates(runtime["db_path"], runtime["config"], config_path=CONFIG_PATH, limit=25)
    flushed_improvements = flush_pending_telegram_messages(runtime["db_path"], runtime["config"], channel_key="agent-improvements", limit=5)
    approvals = [row for row in load_action_rows(runtime, limit=200) if str(row.get("status", "") or "").upper() == "AWAITING_APPROVAL"]
    return {"ok": True, "events": len(events), "phase1": phase1, "lead_agents": lead_agents, "telegram_updates": processed_updates, "queued": 0, "flushed": 0, "flushed_improvements": len(flushed_improvements), "approvals": len(approvals)}


def _run_sync(mode: str) -> None:
    command = ["python", "pull_roas.py", "--config", str(CONFIG_PATH), "--mode", mode]
    if Path("/etc/systemd/system/turnos-roas-fast-sync.service").exists():
        unit = "turnos-roas-fast-sync.service" if mode == "fast" else "turnos-roas-full-sync.service"
        command = ["systemctl", "start", unit]
    subprocess.Popen(command, cwd=CONFIG_PATH.parent)


@app.post("/run-fast")
def run_fast() -> RedirectResponse:
    _run_sync("fast")
    return RedirectResponse(url="/", status_code=303)


@app.post("/run-full")
def run_full() -> RedirectResponse:
    _run_sync("full")
    return RedirectResponse(url="/", status_code=303)


@app.post("/telegram/seed-war-room")
def telegram_seed_war_room() -> RedirectResponse:
    runtime = load_runtime(CONFIG_PATH)
    seed_nexus_war_room_messages(runtime, today=app_today())
    return RedirectResponse(url="/telegram", status_code=303)


@app.post("/telegram/flush")
def telegram_flush() -> RedirectResponse:
    runtime = load_runtime(CONFIG_PATH)
    flush_pending_telegram_messages(runtime["db_path"], runtime["config"], limit=20)
    return RedirectResponse(url="/telegram", status_code=303)


@app.post("/telegram/sync-updates")
def telegram_sync_updates() -> RedirectResponse:
    runtime = load_runtime(CONFIG_PATH)
    sync_telegram_channels_from_updates(runtime["db_path"], runtime["config"], limit=50)
    return RedirectResponse(url="/telegram", status_code=303)


@app.post("/telegram/process-updates")
def telegram_process_updates() -> RedirectResponse:
    runtime = load_runtime(CONFIG_PATH)
    process_telegram_updates(
        runtime["db_path"],
        runtime["config"],
        config_path=runtime["config_path"],
        limit=50,
    )
    return RedirectResponse(url="/telegram", status_code=303)


@app.post("/telegram/webhook/{bot_token}")
async def telegram_webhook(bot_token: str, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    configured_token = str((runtime["config"].get("telegram") or {}).get("bot_token", "") or "")
    if not configured_token or bot_token != configured_token:
        raise HTTPException(status_code=403, detail="Invalid webhook token")
    payload = await request.json()
    result = process_telegram_update_payload(
        runtime["db_path"],
        runtime["config"],
        payload,
        config_path=runtime["config_path"],
    )
    return {"ok": True, "result": result}


@app.get("/agents", response_class=HTMLResponse)
def agents_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    phase1_dashboard = load_phase1_agent_dashboard(runtime, limit=80)
    epic_rows = []
    for epic in epic_summary():
        epic_rows.append(
            "<tr>"
            f"<td>{html.escape(str(epic['epic']))}</td>"
            f"<td>{int(epic['agents'])}</td>"
            f"<td>{int(epic['high_priority_tasks'])}</td>"
            f"<td>{int(epic['total_tasks'])}</td>"
            "</tr>"
        )

    phase_learning_rows = []
    for row in phase1_dashboard.get("learnings", [])[:30]:
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        title = html.escape(str(row.get("title") or ""))
        confidence = float(row.get("confidence") or 0)
        updated_at = html.escape(str(row.get("updated_at") or ""))
        evidence = row.get("evidence_json") or "{}"
        try:
            evidence_obj = json.loads(evidence)
        except Exception:
            evidence_obj = {}
        project = html.escape(str(evidence_obj.get("project") or ""))
        hits = int(evidence_obj.get("hits") or 0)
        affected_attrs = int(evidence_obj.get("affected_attrs") or 0)
        rule = html.escape(str(evidence_obj.get("rule") or ""))
        phase_learning_rows.append(f"<tr><td>{agent_slug}</td><td>{title}</td><td>{project}</td><td>{hits}</td><td>{affected_attrs}</td><td>{confidence:.0%}</td><td>{rule}</td><td>{updated_at}</td></tr>")

    agent_alert_cards = []
    for row in phase1_dashboard.get("learnings", [])[:8]:
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        title = html.escape(str(row.get("title") or ""))
        confidence = float(row.get("confidence") or 0)
        evidence = row.get("evidence_json") or "{}"
        try:
            evidence_obj = json.loads(evidence)
        except Exception:
            evidence_obj = {}
        project = html.escape(str(evidence_obj.get("project") or ""))
        hits = int(evidence_obj.get("hits") or 0)
        affected_attrs = int(evidence_obj.get("affected_attrs") or 0)
        rule = html.escape(str(evidence_obj.get("rule") or ""))
        alert_class = "high" if confidence >= 0.85 else ("medium" if confidence >= 0.70 else "opportunity")
        agent_alert_cards.append(
            "<div class='agent-alert " + alert_class + "'>"
            "<div class='agent-meta'>"
            f"<span class='agent-badge'>{agent_slug}</span>"
            f"<span class='agent-badge'>{confidence:.0%} confianÃƒÂ§a</span>"
            f"<span class='agent-badge'>{hits} ocorrÃƒÂªncias</span>"
            "</div>"
            f"<strong>{title}</strong>"
            f"<p>{rule}</p>"
            f"<small>Projeto: {project or 'geral'} | Attr IDs afetados: {affected_attrs}</small>"
            "</div>"
        )

    phase_readiness = phase1_dashboard.get("phase_readiness") or {}
    quality_os = phase1_dashboard.get("quality_os") or {}
    quality_class = "opportunity" if quality_os.get("ok") else "high"
    quality_label = "Ativo" if quality_os.get("ok") else "Incompleto"
    quality_gate_rows = "".join(f"<tr><td>{html.escape(str(item))}</td></tr>" for item in (quality_os.get("publication_gate") or []))
    quality_missing_rows = "".join(f"<tr><td>{html.escape(str(item))}</td></tr>" for item in (quality_os.get("missing") or []))
    quality_os_panel = (
        "<div class='panel'>"
        "<div class='aside-label'>Quality OS</div>"
        "<h2>Sistema operacional de qualidade</h2>"
        "<div class='agent-alert " + quality_class + "'>"
        f"<div class='agent-meta'><span class='agent-badge'>{quality_label}</span>"
        f"<span class='agent-badge'>{int(quality_os.get('existing_operational_agent_count') or 0)} existentes integrados</span>"
        f"<span class='agent-badge'>{int(quality_os.get('total_integrated_agent_count') or 0)} total</span>"
        f"<span class='agent-badge'>{int(quality_os.get('agent_count') or 0)} agentes base</span>"
        f"<span class='agent-badge'>{int(quality_os.get('google_ads_agent_count') or 0)} Google Ads</span>"
        f"<span class='agent-badge'>{int(quality_os.get('monetization_agent_count') or 0)} monetizacao</span></div>"
        "<strong>Memoria, riscos, criticos e gates sincronizados</strong>"
        "<p>Todo trabalho relevante deve ler memoria obrigatoria, prever riscos, selecionar agentes aplicaveis, passar por critico independente e bloquear publicacao com risco alto ou critico aberto.</p>"
        "</div>"
        "<h3>Gate de publicacao</h3>"
        "<table><thead><tr><th>Criterio obrigatorio</th></tr></thead><tbody>"
        + (quality_gate_rows or "<tr><td>Nenhum criterio carregado.</td></tr>")
        + "</tbody></table>"
        "<h3>Pendencias estruturais</h3>"
        "<table><thead><tr><th>Arquivo faltante</th></tr></thead><tbody>"
        + (quality_missing_rows or "<tr><td>Nenhuma pendencia estrutural.</td></tr>")
        + "</tbody></table>"
        "</div>"
    )
    readiness_class = "opportunity" if phase_readiness.get("ready_for_next_phase") else "medium"
    readiness_label = "Pronto para prÃƒÂ³xima fase" if phase_readiness.get("ready_for_next_phase") else "Ainda em validaÃƒÂ§ÃƒÂ£o"
    readiness_card = (
        "<div class='agent-alert " + readiness_class + "'>"
        "<div class='agent-meta'>"
        f"<span class='agent-badge'>{readiness_label}</span>"
        f"<span class='agent-badge'>{int(phase_readiness.get('evaluated') or 0)} avaliadas</span>"
        f"<span class='agent-badge'>{int(phase_readiness.get('worse') or 0)} pioraram</span>"
        "</div>"
        "<strong>Momento de mudar de fase</strong>"
        f"<p>{html.escape(str(phase_readiness.get('criteria') or 'Aguardando dados suficientes.'))}</p>"
        f"<small>Total monitorado: {int(phase_readiness.get('total') or 0)} | Melhoraram: {int(phase_readiness.get('improved') or 0)}</small>"
        "</div>"
    )

    phase_outcome_rows = []
    for row in phase1_dashboard.get("outcomes", [])[:30]:
        result = html.escape(str(row.get("result") or "pending"))
        status = html.escape(str(row.get("status") or ""))
        action_type = html.escape(str(row.get("action_type") or ""))
        project = html.escape(str(row.get("project") or ""))
        attr_id = html.escape(str(row.get("attr_id") or ""))
        window = html.escape(str(row.get("evaluation_window") or ""))
        profit_delta = float(row.get("profit_delta") or 0)
        roi_delta = float(row.get("roi_delta") or 0)
        updated_at = html.escape(str(row.get("updated_at") or ""))
        phase_outcome_rows.append(f"<tr><td>{result}</td><td>{status}</td><td>{action_type}</td><td>{project}</td><td>{attr_id}</td><td>{window}</td><td>{profit_delta:.2f}</td><td>{roi_delta:.2f} pp</td><td>{updated_at}</td></tr>")

    improvement_rows = []
    for row in phase1_dashboard.get("improvements", [])[:40]:
        status = html.escape(str(row.get("status") or "pending"))
        priority = html.escape(str(row.get("priority") or ""))
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        title = html.escape(str(row.get("title") or ""))
        project = html.escape(str(row.get("project") or ""))
        attr_id = html.escape(str(row.get("attr_id") or ""))
        evidence = html.escape(str(row.get("evidence") or ""))
        proposal = html.escape(str(row.get("proposal") or ""))
        sent_at = html.escape(str(row.get("sent_at") or ""))
        short_id = html.escape(str(row.get("suggestion_key") or "")[:8])
        score_total = int(row.get("score_total") or 0)
        decision = html.escape(str(row.get("responsible_decision") or ""))
        owner = html.escape(str(row.get("responsible_owner") or ""))
        reason = html.escape(str(row.get("responsible_reason") or ""))
        profit_answer = html.escape(str(row.get("profit_answer") or ""))
        improvement_rows.append(f"<tr><td>{status}</td><td>{score_total}</td><td>{decision}</td><td>{owner}</td><td>{priority}</td><td>{agent_slug}</td><td>{short_id}</td><td>{title}</td><td>{project}</td><td>{attr_id}</td><td>{profit_answer}</td><td>{evidence}</td><td>{proposal}</td><td>{reason}</td><td>{sent_at or '-'}</td></tr>")

    responsible_accuracy_rows = []
    for row in phase1_dashboard.get("responsible_accuracy", [])[:20]:
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        sent = int(row.get("sent") or 0)
        approved = int(row.get("approved") or 0)
        rejected = int(row.get("rejected") or 0)
        rate = float(row.get("accuracy_rate") or 0)
        study = "SIM" if row.get("study_required") else "NAO"
        responsible_accuracy_rows.append(f"<tr><td>{agent_slug}</td><td>{sent}</td><td>{approved}</td><td>{rejected}</td><td>{rate:.2f}%</td><td>{study}</td></tr>")

    study_rows = []
    for row in phase1_dashboard.get("study_tasks", [])[:20]:
        status = html.escape(str(row.get("status") or "OPEN"))
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        title = html.escape(str(row.get("title") or ""))
        error_committed = html.escape(str(row.get("error_committed") or ""))
        missing_evidence = html.escape(str(row.get("missing_evidence") or ""))
        new_rule_needed = html.escape(str(row.get("new_rule_needed") or ""))
        created_at = html.escape(str(row.get("created_at") or ""))
        study_rows.append(f"<tr><td>{status}</td><td>{agent_slug}</td><td>{title}</td><td>{error_committed}</td><td>{missing_evidence}</td><td>{new_rule_needed}</td><td>{created_at}</td></tr>")

    phase_rows = []
    for row in phase1_dashboard.get("observations", [])[:30]:
        severity = html.escape(str(row.get("severity") or "INFO"))
        agent_slug = html.escape(str(row.get("agent_slug") or ""))
        title = html.escape(str(row.get("title") or ""))
        project = html.escape(str(row.get("project") or ""))
        attr_id = html.escape(str(row.get("attr_id") or ""))
        created_at = html.escape(str(row.get("created_at") or ""))
        phase_rows.append(f"<tr><td>{severity}</td><td>{agent_slug}</td><td>{title}</td><td>{project}</td><td>{attr_id}</td><td>{created_at}</td></tr>")
    phase1_panel = (
        "<div class='panel'>"
        "<div class='aside-label'>Fase 1</div>"
        "<h2>Memoria dos agentes observadores</h2>"
        "<p>Os agentes registram alertas, oportunidades e aprendizados em banco proprio, sem executar alteracoes automaticas.</p>"
        "<h3>Avisos importantes dos agentes</h3>"
        "<div class='agent-alert-grid'>"
        + readiness_card
        + ("".join(agent_alert_cards) if agent_alert_cards else "<div class='agent-alert'><strong>Nenhum aviso consolidado ainda</strong><p>Os agentes ainda estao reunindo dados suficientes.</p></div>")
        + "</div>"
        "<h3>Resultado das aÃƒÂ§ÃƒÂµes monitoradas</h3>"
        "<table><thead><tr><th>Resultado</th><th>Status</th><th>AÃƒÂ§ÃƒÂ£o</th><th>Projeto</th><th>Attr ID</th><th>Janela</th><th>Lucro ÃŽâ€</th><th>ROI ÃŽâ€</th><th>Atualizado</th></tr></thead><tbody>"
        + ("".join(phase_outcome_rows) if phase_outcome_rows else "<tr><td colspan='9'>Nenhuma aÃƒÂ§ÃƒÂ£o avaliada ainda.</td></tr>")
        + "</tbody></table>"
        "<h3>Aprendizados consolidados</h3>"
        "<table><thead><tr><th>Agente</th><th>Aprendizado</th><th>Projeto</th><th>Ocorrencias</th><th>Attrs</th><th>ConfianÃƒÂ§a</th><th>Regra</th><th>Atualizado</th></tr></thead><tbody>"
        + ("".join(phase_learning_rows) if phase_learning_rows else "<tr><td colspan='7'>Nenhum aprendizado consolidado ainda.</td></tr>")
        + "</tbody></table>"
        "<h3>Melhorias avaliadas pelo portao de lucro</h3>"
        "<p>Regra: se a sugestao nao responder como protege ou aumenta lucro, reprova. Score >= 70 vai para Luciano; 50-69 fica em observacao; < 50 reprova.</p>"
        "<table><thead><tr><th>Status</th><th>Score</th><th>Decisao</th><th>Responsavel</th><th>Prioridade</th><th>Agente</th><th>ID</th><th>Melhoria</th><th>Projeto</th><th>Attr ID</th><th>Lucro</th><th>Evidencia</th><th>Proposta</th><th>Motivo</th><th>Enviado</th></tr></thead><tbody>"
        + ("".join(improvement_rows) if improvement_rows else "<tr><td colspan='15'>Nenhuma melhoria avaliada ainda.</td></tr>")
        + "</tbody></table>"
        "<h3>Taxa de acerto do responsavel</h3>"
        "<p>Meta: poucos envios, alta aprovacao do Luciano e lucro comprovado. Abaixo de 60% com amostra suficiente entra em estudo.</p>"
        "<table><thead><tr><th>Responsavel</th><th>Envios</th><th>Aprovadas Luciano</th><th>Recusadas Luciano</th><th>Taxa</th><th>Estudo</th></tr></thead><tbody>"
        + ("".join(responsible_accuracy_rows) if responsible_accuracy_rows else "<tr><td colspan='6'>Sem envios suficientes ainda.</td></tr>")
        + "</tbody></table>"
        "<h3>Modo estudo obrigatorio</h3>"
        "<table><thead><tr><th>Status</th><th>Agente</th><th>Tarefa</th><th>Erro</th><th>Evidencia faltante</th><th>Nova regra</th><th>Criado</th></tr></thead><tbody>"
        + ("".join(study_rows) if study_rows else "<tr><td colspan='7'>Nenhum agente em modo estudo obrigatorio.</td></tr>")
        + "</tbody></table>"
        "<h3>Observacoes recentes</h3>"
        "<table><thead><tr><th>Severidade</th><th>Agente</th><th>Observacao</th><th>Projeto</th><th>Attr ID</th><th>Registrado</th></tr></thead><tbody>"
        + ("".join(phase_rows) if phase_rows else "<tr><td colspan='6'>Nenhuma observacao registrada ainda. Rode /internal/agent-cycle.</td></tr>")
        + "</tbody></table>"
        "</div>"
    )
    agent_cards = []
    for agent in AGENT_BACKLOG:
        tasks = agent.get("technical_backlog") or []
        high_tasks = sum(1 for task in tasks if str(task.get("priority", "")).lower() == "alta")
        agent_cards.append(
            "<div class='panel'>"
            f"<div class='aside-label'>{html.escape(str(agent['epic']))}</div>"
            f"<h2><a class='project-link' href='/agents/{quote(str(agent['slug']), safe='')}'>{html.escape(str(agent['name']))}</a></h2>"
            f"<p>{html.escape(str(agent['mission']))}</p>"
            "<table>"
            "<tbody>"
            f"<tr><td>Entregas-chave</td><td>{len(agent.get('deliveries') or [])}</td></tr>"
            f"<tr><td>Backlog tecnico</td><td>{len(tasks)}</td></tr>"
            f"<tr><td>Prioridade alta</td><td>{high_tasks}</td></tr>"
            "</tbody>"
            "</table>"
            f"<div class='hero-actions' style='margin-top:14px;'><a class='action-link secondary' href='/agents/{quote(str(agent['slug']), safe='')}'>ABRIR AGENTE</a></div>"
            "</div>"
        )



    def _path_link(path: str) -> str:
        path = str(path or "/")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return "https://" + project + (path if path.startswith("/") else "/" + path)

    def _short_path(path: str, limit: int = 34) -> str:
        path = str(path or "/")
        return path if len(path) <= limit else path[: limit - 1] + "?"

    routes_by_source = {}
    for route in metrics.get("routes") or []:
        source = str(route.get("source") or "/")
        routes_by_source.setdefault(source, []).append(route)
    for route_list in routes_by_source.values():
        route_list.sort(key=lambda row: int(row.get("sessions") or 0), reverse=True)
    stage_metrics = {str(stage.get("url") or "/"): stage for stage in (metrics.get("journey_stages") or [])}

    def _node_metrics(path: str, sessions: int) -> str:
        stage = stage_metrics.get(str(path or "/")) or {}
        pct = sessions / start_sessions if start_sessions else 0
        revenue = stage.get("revenue")
        ecpm = stage.get("ecpm")
        rps = stage.get("rps")
        scope = str(stage.get("metric_scope") or "sem dado real por etapa")
        iab_label = str(stage.get("iab_content_label") or "")
        commercial_signal = str(stage.get("commercial_signal_type") or "").replace("_", " ")
        commercial_score = _to_float(stage.get("commercial_intent_score"))
        intelligence_line = (
            f"<small class='path-intelligence'>IAB: {html.escape(iab_label)}<br>Sinal: {html.escape(commercial_signal)}"
            f"{' | Score ' + str(int(commercial_score)) if commercial_score else ''}</small>"
            if iab_label or commercial_signal or commercial_score else ""
        )
        money_parts = []
        if revenue is not None:
            money_parts.append(f"Receita {_money(revenue)}")
        if ecpm is not None:
            money_parts.append(f"eCPM {_money(ecpm)}")
        if rps is not None:
            money_parts.append(f"RPS {_money(rps)}")
        money_line = (
            f"<em>{' | '.join(money_parts)}</em>"
            if money_parts
            else "<em class='muted'>Sem receita/eCPM real por URL</em>"
        )
        return f"<small>{_percent(pct)} das sessoes</small>{intelligence_line}{money_line}<small class='metric-scope'>{html.escape(scope)}</small>"

    def _node_alert(path: str) -> str:
        route_list = routes_by_source.get(str(path or "/"), [])
        if not route_list:
            return ""
        source_total = sum(int(route.get("sessions") or 0) for route in route_list)
        if not source_total:
            return ""
        return_routes = [route for route in route_list if route.get("return_to_stage")]
        if return_routes:
            return_sessions = sum(int(route.get("sessions") or 0) for route in return_routes)
            return_stage = min(int(route.get("return_to_stage") or 1) for route in return_routes)
            return f"<mark>Retorno para etapa {return_stage}: {return_sessions} ({_percent(return_sessions / source_total)})</mark>"
        landing_returns = sum(int(route.get("sessions") or 0) for route in route_list if str(route.get("target") or "") == landing_path)
        if landing_returns:
            return f"<mark>Retorno para etapa 1: {landing_returns} ({_percent(landing_returns / source_total)})</mark>"
        top = route_list[0]
        return f"<small>Proximo: {html.escape(_short_path(str(top.get('target') or '/'), 24))} ({_percent(int(top.get('sessions') or 0) / source_total)})</small>"

    start_sessions = int(metrics.get("sessions") or 0)
    landing_path = str(metrics.get("primary_entry_path") or "")
    path_columns = []
    for column in metrics.get("journey_columns") or []:
        raw_items = list(column.get("items") or [])
        items = raw_items[:5]
        extra_count = max(len(raw_items) - 5, 0)
        extra_sessions = sum(int(item.get("sessions") or 0) for item in raw_items[5:])
        if extra_count:
            items.append({"path": f"Mais {extra_count}", "sessions": extra_sessions, "is_more": True})
        path_columns.append({"label": f"Etapa {int(column.get('position') or len(path_columns) + 1)}", "items": items})

    max_path_sessions = max((int(item.get("sessions") or 0) for col in path_columns for item in col["items"]), default=1)
    path_columns_html = ""
    for col in path_columns[:5]:
        items_html = ""
        for item in col["items"]:
            sessions = int(item.get("sessions") or 0)
            scale = sessions / max_path_sessions if max_path_sessions else 0
            bar_height = max(18.0, min(190.0, 28.0 + scale * 150.0))
            path = str(item.get("path") or "/")
            if item.get("is_more"):
                node = f"<div class='path-node path-more'><span>{html.escape(path)}</span><strong>{sessions}</strong></div>"
            elif item.get("is_start"):
                node = f"<div class='path-node path-start'><span>session_start</span><strong>{sessions}</strong><small>{_percent(1 if sessions else 0)} das sessoes</small></div>"
            else:
                url = html.escape(_path_link(path), quote=True)
                label = html.escape(_short_path(path))
                full = html.escape(path, quote=True)
                node = f"<a class='path-node' href='{url}' target='_blank' rel='noopener noreferrer' title='{full}'><span>{label}</span><strong>{sessions}</strong>{_node_metrics(path, sessions)}{_node_alert(path)}</a>"
            items_html += f"<div class='path-row'><i style='height:{bar_height:.1f}px'></i>{node}</div>"
        path_columns_html += (
            "<section class='path-column'>"
            f"<header><small>{html.escape(col['label'])}</small><button type='button' aria-label='Editar dimensao'>?</button></header>"
            "<div class='path-dimension'><span>?</span><b>Caminho da pagina e...</b></div>"
            f"<div class='path-items'>{items_html}</div>"
            "</section>"
        )
    path_explorer_html = (
        "<section class='path-explorer-v2 metric-help' tabindex='0' data-help='Exploracao visual de caminhos reais por Attr ID. Cada coluna representa uma etapa da jornada; clique na URL para abrir a pagina.'>"
        "<div class='path-explorer-head'><div><span class='cockpit-kicker'>Exploracao de caminho</span>"
        "<h2>Jornada por etapa</h2><p>Clique em qualquer URL para abrir a pagina correspondente. Dados reais do rastreador por Attr ID.</p></div>"
        f"<div class='campaign-inline-actions'>{tabs}</div></div>"
        f"<div class='path-board'>{path_columns_html}</div>"
        "</section>"
    )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Arquitetura Operacional</div>
          <h1>Agentes Julio</h1>
          <p>Mapa vivo dos agentes autonomos da Nexus, com missao, funcao, metricas, melhoria continua e backlog tecnico para transformar a estrategia da empresa em produto real.</p>
          <div class='hero-actions'>
            <a class='action-link secondary' href='/'>VOLTAR PARA DASHBOARD</a>
          </div>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
          <div class='aside-label'>Agentes Julio</div>
            <strong>{len(AGENT_BACKLOG)}</strong>
          </div>
          <div>
            <div class='aside-label'>Epicos ativos</div>
            <strong>{len(epic_summary())}</strong>
          </div>
        </div>
      </div>

      <div class='metrics'>
        <div class='metric'>
          <div class='label'>Prioridade agora</div>
          <div class='value'>Dados + Painel</div>
          <div class='sub'>Fundacao para nao perder dinheiro nem contexto</div>
        </div>
        <div class='metric'>
          <div class='label'>Backlog tecnico</div>
          <div class='value'>{sum(len(agent.get("technical_backlog") or []) for agent in AGENT_BACKLOG)}</div>
          <div class='sub'>Itens materializados por agente</div>
        </div>
        <div class='metric'>
          <div class='label'>Alta prioridade</div>
          <div class='value'>{sum(1 for agent in AGENT_BACKLOG for task in (agent.get("technical_backlog") or []) if str(task.get("priority", "")).lower() == "alta")}</div>
          <div class='sub'>Itens que puxam o roadmap imediato</div>
        </div>
        <div class='metric'>
          <div class='label'>Melhoria continua</div>
          <div class='value'>100%</div>
          <div class='sub'>Todo agente tem bloco formal de autoaprimoramento</div>
        </div>
      </div>

      {phase1_panel}
      {quality_os_panel}

      <div class='grid'>
        <div class='stack'>{''.join(agent_cards)}</div>
        <div class='stack'>
          <div class='panel'>
            <h2>Epicos</h2>
            <p>Resumo do backlog por frente estrutural da empresa.</p>
            <table>
              <thead>
                <tr><th>Epico</th><th>Agentes</th><th>Alta</th><th>Total</th></tr>
              </thead>
              <tbody>{''.join(epic_rows)}</tbody>
            </table>
          </div>
          <div class='panel'>
            <h2>Leitura do Roadmap</h2>
            <p>Ordem recomendada para evoluir o sistema sem perder dados nem controle.</p>
            <table>
              <tbody>
                <tr><td>Fase 1</td><td>Governanca, UTM, Painel e UX</td></tr>
                <tr><td>Fase 2</td><td>Trafego, Monetizacao e Receita</td></tr>
                <tr><td>Fase 3</td><td>Funil, Conteudo e Experimentacao</td></tr>
                <tr><td>Fase 4</td><td>Aprendizado, Orquestrador, Auditor e Chat</td></tr>
                <tr><td>Fase 5</td><td>Expansao internacional e autonomia parcial</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    """
    return _layout("Agentes Julio", content, "agents")


@app.get("/agents/{slug}", response_class=HTMLResponse)
def agent_detail_page(slug: str) -> str:
    agent = find_agent(slug)
    if not agent:
        raise HTTPException(status_code=404, detail="Agente nao encontrado")

    def _bullet_rows(items: list[str]) -> str:
        return "".join(f"<tr><td>{html.escape(item)}</td></tr>" for item in items)

    backlog_rows = []
    for task in agent.get("technical_backlog") or []:
        priority = str(task.get("priority", "") or "")
        backlog_rows.append(
            "<tr>"
            f"<td>{html.escape(str(task.get('title', '') or ''))}</td>"
            f"<td>{html.escape(priority)}</td>"
            f"<td>{html.escape(str(task.get('expected', '') or ''))}</td>"
            "</tr>"
        )
    operating_standard = agent.get("operating_standard") or {}
    analysis_contract = operating_standard.get("analysis_contract") if isinstance(operating_standard, dict) else []

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>{html.escape(str(agent['epic']))}</div>
          <h1>{html.escape(str(agent['name']))}</h1>
          <p>{html.escape(str(agent['mission']))}</p>
          <div class='hero-actions'>
            <a class='action-link secondary' href='/agents'>VOLTAR PARA AGENTES</a>
            <a class='action-link secondary' href='/'>VOLTAR PARA DASHBOARD</a>
          </div>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Entregas</div>
            <strong>{len(agent.get('deliveries') or [])}</strong>
          </div>
          <div>
            <div class='aside-label'>Backlog tecnico</div>
            <strong>{len(agent.get('technical_backlog') or [])}</strong>
          </div>
        </div>
      </div>

      <div class='metrics'>
        <div class='metric'>
          <div class='label'>Missao</div>
          <div class='value' style='font-size:20px;'>{html.escape(str(agent['mission']))}</div>
          <div class='sub'>Objetivo central do agente</div>
        </div>
        <div class='metric'>
          <div class='label'>Responsavel</div>
          <div class='value' style='font-size:20px;'>{html.escape(str(agent.get('responsible_to') or 'Luciano'))}</div>
          <div class='sub'>{html.escape(str(agent.get('reporting_cadence') or 'reportar sempre que houver risco ou oportunidade'))}</div>
        </div>
        <div class='metric'>
          <div class='label'>Funcao</div>
          <div class='value'>{len(agent.get('function') or [])}</div>
          <div class='sub'>Responsabilidades principais</div>
        </div>
        <div class='metric'>
          <div class='label'>Metricas</div>
          <div class='value'>{len(agent.get('metrics') or [])}</div>
          <div class='sub'>KPIs formais do agente</div>
        </div>
        <div class='metric'>
          <div class='label'>Autoaprimoramento</div>
          <div class='value'>{len(agent.get('continuous_improvement') or [])}</div>
          <div class='sub'>Rotinas de melhoria continua</div>
        </div>
      </div>

      <div class='grid'>
        <div class='stack'>
          <div class='panel'>
            <h2>Funcao</h2>
            <p>O que esse agente precisa fazer todos os dias para a empresa funcionar melhor.</p>
            <table><tbody>{_bullet_rows(agent.get('function') or [])}</tbody></table>
          </div>
          <div class='panel'>
            <h2>Contrato Operacional</h2>
            <p>Como esse agente analisa, reporta e aprende com foco em lucro.</p>
            <table><tbody>{_bullet_rows(list(analysis_contract or []))}</tbody></table>
          </div>
          <div class='panel'>
            <h2>Entregas</h2>
            <p>O que deve aparecer no produto quando esse agente estiver bem implementado.</p>
            <table><tbody>{_bullet_rows(agent.get('deliveries') or [])}</tbody></table>
          </div>
          <div class='panel'>
            <h2>Backlog Tecnico</h2>
            <p>Itens concretos para tirar o agente do papel.</p>
            <table>
              <thead>
                <tr><th>Tarefa</th><th>Prioridade</th><th>Entrega esperada</th></tr>
              </thead>
              <tbody>{''.join(backlog_rows)}</tbody>
            </table>
          </div>
        </div>
        <div class='stack'>
          <div class='panel'>
            <h2>Metricas</h2>
            <p>Como saber se esse agente esta funcionando bem.</p>
            <table><tbody>{_bullet_rows(agent.get('metrics') or [])}</tbody></table>
          </div>
          <div class='panel'>
            <h2>Melhoria Continua</h2>
            <p>Como esse agente deve estudar, aprender e se autoaprimorar constantemente.</p>
            <table><tbody>{_bullet_rows(agent.get('continuous_improvement') or [])}</tbody></table>
          </div>
        </div>
      </div>
    </div>
    """
    return _layout(str(agent["name"]), content, "agents")


@app.get("/war-room", response_class=HTMLResponse)
def war_room_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    events = load_nexus_agent_events(runtime, limit=40, today=app_today())

    event_rows = []
    for row in events:
        priority = str(row.get("priority", "") or "").upper()
        css = "status-neutral"
        if priority in {"HIGH", "CRITICAL"}:
            css = "status-bad"
        elif priority in {"MEDIUM", "WARN"}:
            css = "status-warn"
        else:
            css = "status-good"
        project_link = ""
        if str(row.get("project_id", "") or ""):
            project = str(row["project_id"])
            project_link = f"<a class='project-link' href='/projects/{quote(project, safe='')}'>{html.escape(project)}</a>"
        event_rows.append(
            "<tr>"
            f"<td><span class='status-pill {css}'>{html.escape(priority or 'INFO')}</span></td>"
            f"<td>{html.escape(str(row['agent_slug']))}</td>"
            f"<td>{html.escape(str(row['title']))}</td>"
            f"<td>{html.escape(str(row['message']))}</td>"
            f"<td>{project_link or '-'}</td>"
            f"<td class='nowrap'>{html.escape(str(row['created_at']))}</td>"
            "</tr>"
        )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Julio Ops Bot</div>
          <h1>Julio War Room</h1>
          <p>Timeline viva da atuacao dos agentes Nexus. Aqui voce acompanha alertas, decisoes, execucoes e sinais de divergencia antes de plugar o Telegram em producao.</p>
          <div class='hero-actions'>
            <a class='action-link secondary' href='/agents'>VER NEXUS AGENTS</a>
            <a class='action-link secondary' href='/'>VOLTAR PARA DASHBOARD</a>
          </div>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Eventos visiveis</div>
            <strong>{len(events)}</strong>
          </div>
          <div>
            <div class='aside-label'>Canal alvo</div>
            <strong>Julio War Room</strong>
          </div>
        </div>
      </div>

      <div class='metrics'>
        <div class='metric'>
          <div class='label'>Data Guardian</div>
          <div class='value'>{sum(1 for row in events if str(row.get('agent_slug', '')) == 'governanca-dados')}</div>
          <div class='sub'>Eventos de validacao e divergencia</div>
        </div>
        <div class='metric'>
          <div class='label'>Monetizacao</div>
          <div class='value'>{sum(1 for row in events if str(row.get('agent_slug', '')) == 'monetizacao')}</div>
          <div class='sub'>Acoes e recomendacoes de pricing</div>
        </div>
        <div class='metric'>
          <div class='label'>Auditoria</div>
          <div class='value'>{sum(1 for row in events if str(row.get('agent_slug', '')) == 'auditor')}</div>
          <div class='sub'>Fila GAM e rastreabilidade operacional</div>
        </div>
        <div class='metric'>
          <div class='label'>Orquestrador</div>
          <div class='value'>{sum(1 for row in events if str(row.get('agent_slug', '')) == 'orquestrador')}</div>
          <div class='sub'>Resumo consolidado do momento</div>
        </div>
      </div>

      <div class='panel'>
        <h2>Timeline dos Agentes</h2>
        <p>Fluxo visivel da comunicacao entre os Agentes Julio antes do envio para Telegram.</p>
        <table>
          <thead>
            <tr><th>Prioridade</th><th>Agente</th><th>Titulo</th><th>Mensagem</th><th>Projeto</th><th>Quando</th></tr>
          </thead>
          <tbody>{''.join(event_rows) or "<tr><td colspan='6' class='muted'>Sem eventos ainda.</td></tr>"}</tbody>
        </table>
      </div>
    </div>
    """
    return _layout("Julio War Room", content, "war-room")


@app.get("/telegram", response_class=HTMLResponse)
def telegram_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    channels = load_nexus_telegram_channels(runtime)
    messages = load_nexus_telegram_messages(runtime, limit=50)
    updates = load_nexus_telegram_updates(runtime, limit=20)
    war_room_channel = next((row for row in channels if str(row.get("channel_key", "") or "") == "nexus-war-room"), None)

    channel_rows = []
    for row in channels:
        channel_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['channel_name']))}</td>"
            f"<td>{html.escape(str(row['channel_type']))}</td>"
            f"<td class='mono'>{html.escape(str(row['chat_id']) or '-')}</td>"
            f"<td>{_status_badge('READY' if row['is_enabled'] else 'DISABLED')}</td>"
            "</tr>"
        )

    message_rows = []
    for row in messages:
        message_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['channel_key']))}</td>"
            f"<td>{_status_badge(str(row['status']))}</td>"
            f"<td class='mono'>{html.escape(str(row['telegram_message_id']) or '-')}</td>"
            f"<td>{html.escape(str(row['body'])[:140])}</td>"
            f"<td class='nowrap'>{html.escape(str(row['sent_at']) or '-')}</td>"
            "</tr>"
        )

    update_rows = []
    for row in updates:
        update_rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(str(row['update_id']))}</td>"
            f"<td>{html.escape(str(row['sender_name']))}</td>"
            f"<td>{html.escape(str(row['text'])[:140])}</td>"
            f"<td>{_status_badge(str(row['status']))}</td>"
            f"<td class='nowrap'>{html.escape(str(row['processed_at']) or '-')}</td>"
            "</tr>"
        )

    pending_count = sum(1 for row in messages if str(row.get("status", "")).upper() == "PENDING")

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Julio Ops Bot</div>
          <h1>Telegram Julio War Room</h1>
          <p>Painel de preparacao do fluxo externo do Julio Ops Bot. Aqui ficam o canal configurado, a fila de mensagens e o disparo real para Telegram.</p>
          <div class='hero-actions'>
            <form method='post' action='/telegram/seed-war-room'>
              <button class='action-button secondary' type='submit'>PREPARAR FILA WAR ROOM</button>
            </form>
            <form method='post' action='/telegram/sync-updates'>
              <button class='action-button secondary' type='submit'>SINCRONIZAR UPDATES DO TELEGRAM</button>
            </form>
            <form method='post' action='/telegram/flush'>
              <button class='action-button' type='submit'>ENVIAR FILA PARA TELEGRAM</button>
            </form>
          </div>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Canais</div>
            <strong>{len(channels)}</strong>
          </div>
          <div>
            <div class='aside-label'>Pendentes</div>
            <strong>{pending_count}</strong>
          </div>
        </div>
      </div>

      <div class='metrics'>
        <div class='metric'>
          <div class='label'>Canal padrao</div>
          <div class='value'>Julio War Room</div>
          <div class='sub'>Destino principal do Julio Ops Bot</div>
        </div>
        <div class='metric'>
          <div class='label'>Fila gerada</div>
          <div class='value'>{len(messages)}</div>
          <div class='sub'>Mensagens registradas no sistema</div>
        </div>
        <div class='metric'>
          <div class='label'>Status atual</div>
          <div class='value'>{'Pronto para envio' if channels else 'Configurar bot primeiro'}</div>
          <div class='sub'>Webhook do bot em tempo real</div>
        </div>
        <div class='metric'>
          <div class='label'>Proxima etapa</div>
          <div class='value'>{'Grupo detectado' if war_room_channel and str(war_room_channel.get('channel_type', '')) == 'group' else 'Entrar no grupo'}</div>
          <div class='sub'>{'O canal Julio War Room ja pode receber mensagens da equipe' if war_room_channel and str(war_room_channel.get('channel_type', '')) == 'group' else 'Adicione o bot a um grupo chamado Julio War Room e envie uma mensagem la'}</div>
        </div>
      </div>

      <div class='grid'>
        <div class='stack'>
          <div class='panel'>
            <h2>Canais Telegram</h2>
            <p>Configuracao dos destinos do Julio Ops Bot.</p>
            <table>
              <thead>
                <tr><th>Canal</th><th>Tipo</th><th>Chat ID</th><th>Status</th></tr>
              </thead>
              <tbody>{''.join(channel_rows) or "<tr><td colspan='4' class='muted'>Sem canais configurados.</td></tr>"}</tbody>
            </table>
          </div>
        </div>
        <div class='stack'>
          <div class='panel'>
            <h2>Fila de Mensagens</h2>
            <p>Mensagens preparadas a partir dos eventos do Julio War Room.</p>
            <table>
              <thead>
                <tr><th>Canal</th><th>Status</th><th>Msg ID</th><th>Corpo</th><th>Quando</th></tr>
              </thead>
              <tbody>{''.join(message_rows) or "<tr><td colspan='5' class='muted'>Sem mensagens na fila.</td></tr>"}</tbody>
            </table>
          </div>
          <div class='panel'>
            <h2>Comandos Recebidos</h2>
            <p>Leitura do que o grupo pediu ao Julio Ops Bot e como o sistema respondeu.</p>
            <table>
              <thead>
                <tr><th>Update</th><th>Remetente</th><th>Texto</th><th>Status</th><th>Processado</th></tr>
              </thead>
              <tbody>{''.join(update_rows) or "<tr><td colspan='5' class='muted'>Nenhum comando recebido ainda.</td></tr>"}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    """
    return _layout("Telegram Julio War Room", content, "war-room")


@app.get("/experiments", response_class=HTMLResponse)
def experiments_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    experiments = list_experiments(runtime)
    cards = []
    for experiment in experiments:
        recommendation = experiment.get("recommendation") or {}
        recommendation_text = {
            "candidate": f"Recomendacao: promover {recommendation.get('variant_name')}",
            "collecting": f"Em apuracao: {recommendation.get('reason') or 'volume ou evidencia insuficiente'}",
            "blocked": f"Bloqueado: {recommendation.get('reason') or 'configuracao incompleta'}",
        }.get(str(recommendation.get("status") or ""), "Sem recomendacao")
        rows = []
        for variant in experiment.get("variants") or []:
            role = "Campea" if variant.get("role") == "champion" else "Desafiante"
            stage_label = {
                "discovery": "Descoberta",
                "diagnosis": "Diagnostico",
                "problem_identified": "Problema identificado",
                "comparison_decision": "Comparacao / decisao",
                "high_intent": "Alta intencao",
            }.get(str(variant.get("cognitive_stage") or ""), "Nao registrado")
            promote = ""
            if (
                variant.get("role") != "champion"
                and experiment.get("status") == "running"
                and recommendation.get("status") == "candidate"
                and int(recommendation.get("variant_id") or 0) == int(variant.get("id") or 0)
            ):
                promote = (
                    f"<button class='experiment-action' data-action='promote' data-experiment='{int(experiment['id'])}' "
                    f"data-variant='{int(variant['id'])}'>Promover a campea</button>"
                )
            replace = ""
            if (
                variant.get("role") == "challenger"
                and experiment.get("status") in {"running", "paused"}
            ):
                replace = (
                    f"<button class='experiment-action danger' data-action='pause-variant' "
                    f"data-experiment='{int(experiment['id'])}' data-variant='{int(variant['id'])}'>"
                    "Pausar e substituir</button>"
                )
            gam_ecpm = variant.get("gam_ecpm")
            gam_period = ""
            if variant.get("gam_period_start") and variant.get("gam_period_end"):
                gam_period = (
                    f"{html.escape(str(variant.get('gam_period_start')))} a "
                    f"{html.escape(str(variant.get('gam_period_end')))}"
                )
            gam_scope = html.escape(str(variant.get("gam_metric_scope") or ""))
            gam_provisional = bool(variant.get("gam_is_provisional"))
            gam_label = "parcial" if gam_provisional else "confirmado"
            gam_ecpm_cell = (
                f"<strong>{_money(float(gam_ecpm))}</strong>"
                f"<small>{gam_label}: {gam_period} | {int(variant.get('gam_impressions') or 0)} impressoes</small>"
                f"<small>{gam_scope}</small>"
                if gam_ecpm is not None
                else f"<span class='muted'>Sem dado</span><small>{gam_scope or 'GAM sem impressoes no ciclo ou no parcial disponivel'}</small>"
            )
            rows.append(
                "<tr>"
                f"<td><strong>{html.escape(str(variant.get('name') or ''))}</strong><small>{role} | {html.escape(stage_label)}</small><small>Sinal: {html.escape(str(variant.get('commercial_signal') or 'Nao registrado'))}</small><small>Hipotese: {html.escape(str(variant.get('hypothesis') or 'Nao registrada'))}</small></td>"
                f"<td><a href='https://{html.escape(str(experiment['project']))}{html.escape(str(variant.get('url_path') or '/'), quote=True)}' target='_blank' rel='noopener noreferrer'>{html.escape(str(variant.get('url_path') or '/'))}</a></td>"
                f"<td>{int(variant.get('weight') or 0)}%</td>"
                f"<td>{int(variant.get('exposures') or 0)}</td>"
                f"<td>{int(variant.get('conversions') or 0)}</td>"
                f"<td><strong>{_percent(float(variant.get('conversion_rate') or 0))}</strong></td>"
                f"<td>{gam_ecpm_cell}</td>"
                f"<td>{_percent(float(variant.get('confidence_lower') or 0))} a {_percent(float(variant.get('confidence_upper') or 1))}</td>"
                f"<td>{_percent(float(variant.get('relative_uplift') or 0)) if variant.get('role') != 'champion' else '-'}</td>"
                f"<td>{html.escape(str(variant.get('volume_status') or 'insuficiente'))}</td>"
                f"<td><div class='row-actions'>{promote}{replace}</div></td>"
                "</tr>"
            )
        history_rows = []
        for item in experiment_history(runtime, int(experiment["id"])):
            metrics = item.get("metrics") or {}
            clarity = item.get("clarity") or {}
            elements = item.get("element_metrics") or {}
            history_rows.append(
                "<tr>"
                f"<td>{int(item.get('cycle') or 0)}</td>"
                f"<td>{html.escape(str(item.get('name') or ''))}<small>{html.escape(str(item.get('url_path') or ''))}</small></td>"
                f"<td>{html.escape(str(item.get('outcome') or ''))}</td>"
                f"<td>{_percent(float(metrics.get('conversion_rate') or 0))}</td>"
                f"<td>{_money(float(metrics.get('gam_ecpm') or 0)) if metrics.get('gam_ecpm') is not None else 'Sem dado'}</td>"
                f"<td>{int(clarity.get('page_visits') or 0)}<small>{html.escape(str(clarity.get('coverage') or 'sem cobertura'))}</small></td>"
                f"<td>{int(elements.get('cta_visible') or 0)} / {int(elements.get('cta_click') or 0)}</td>"
                f"<td>{html.escape(str(item.get('failure_reason') or ''))}</td>"
                "</tr>"
            )
        history_html = (
            "<details class='experiment-history'><summary>Historico: campea e todas as perdedoras</summary>"
            "<div class='table-wrap'><table><thead><tr><th>Ciclo</th><th>Variante</th><th>Resultado</th><th>Conversao</th><th>eCPM</th><th>Clarity</th><th>CTA visto/clicado</th><th>Diagnostico</th></tr></thead>"
            f"<tbody>{''.join(history_rows)}</tbody></table></div></details>"
            if history_rows
            else ""
        )
        status_buttons = ""
        if experiment.get("status") == "running":
            status_buttons = (
                f"<button class='experiment-action secondary' data-action='status' data-status='paused' data-experiment='{int(experiment['id'])}'>Pausar todo o ciclo</button>"
            )
        elif experiment.get("status") in {"paused", "draft"}:
            status_buttons = (
                f"<button class='experiment-action' data-action='status' data-status='running' data-experiment='{int(experiment['id'])}'>Ativar modo protecao</button>"
            )
        cards.append(
            "<section class='experiment-card'>"
            "<div class='experiment-head'>"
            f"<div><span class='experiment-status'>{html.escape(str(experiment.get('status') or ''))}</span>"
            f"<h2>{html.escape(str(experiment.get('name') or ''))}</h2>"
            f"<p>{html.escape(str(experiment.get('project') or ''))}{html.escape(str(experiment.get('source_path') or '/'))} · ciclo {int(experiment.get('cycle') or 1)}</p></div>"
            f"<div class='experiment-recommendation'>{html.escape(recommendation_text)}</div>"
            "</div>"
            "<div class='experiment-rules'>"
            f"<span>Metrica: conversao P2 para P3</span><span>Volume: {int(experiment.get('min_exposures') or 0)} visitas e {int(experiment.get('min_conversions') or 0)} conversoes por variante</span>"
            f"<span>Prazo: {float(experiment.get('elapsed_days') or 0):.1f} de {int(experiment.get('min_days') or 0)} dias</span>"
            f"<span>Uplift minimo: {float(experiment.get('min_relative_uplift') or 0) * 100:.0f}%</span>"
            f"<span>Confianca sequencial: {float(experiment.get('confidence') or 0) * 100:.0f}% valida mesmo consultando continuamente</span>"
            "</div>"
            "<div class='table-wrap'><table><thead><tr><th>Variante</th><th>URL P2</th><th>Trafego</th><th>Visitas P2</th><th>Avancos P3</th><th>Conversao</th><th>eCPM GAM</th><th>Limite sequencial</th><th>Uplift</th><th>Volume</th><th>Acao</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
            f"{history_html}"
            f"<div class='experiment-buttons'>{status_buttons}</div>"
            "</section>"
        )
    content = f"""
    <div class='shell'>
      [[NAV]]
      <section class='experiment-hero'>
        <div><span>Melhoria continua</span><h1>Experimentos de funil</h1><p>Modo protecao: campea com 60% do trafego e quatro desafiantes com 10% cada. A decisao principal usa conversao real da P2 para a P3, protegendo eCPM validado e sinais de Clarity.</p></div>
        <button id='open-experiment-form' type='button'>Novo experimento</button>
      </section>
      <section id='experiment-form-panel' class='experiment-form-panel' hidden>
        <h2>Novo ciclo</h2>
        <div class='experiment-form-grid'>
          <label>Nome<input id='exp-name' value='Teste continuo P2 para P3'></label>
          <label>Projeto<input id='exp-project' placeholder='exemplo.com'></label>
          <label>URL P1<input id='exp-source' placeholder='/pagina-p1/'></label>
          <label>Seletor do botao<input id='exp-selector' placeholder='.cta-principal'></label>
          <label>URL P3 de conversao<input id='exp-conversion' placeholder='/pagina-p3/'></label>
        </div>
        <div class='variant-grid'>
          <fieldset><legend>Campea - 60%</legend><input id='v1-name' placeholder='Nome'><input id='v1-path' placeholder='/url-p2/'><input id='v1-post' type='number' min='0' placeholder='ID post'><select id='v1-stage'><option value='diagnosis'>Diagnostico</option><option value='discovery'>Descoberta</option><option value='problem_identified'>Problema identificado</option><option value='comparison_decision'>Comparacao / decisao</option><option value='high_intent'>Alta intencao</option></select><input id='v1-signal' placeholder='Sinal comercial editorial'><textarea id='v1-hypothesis' placeholder='Hipotese testada'></textarea></fieldset>
          <fieldset><legend>Desafiante A - 10%</legend><input id='v2-name' placeholder='Nome'><input id='v2-path' placeholder='/url-p2-a/'><input id='v2-post' type='number' min='0' placeholder='ID post'><select id='v2-stage'><option value='diagnosis'>Diagnostico</option><option value='discovery'>Descoberta</option><option value='problem_identified'>Problema identificado</option><option value='comparison_decision'>Comparacao / decisao</option><option value='high_intent'>Alta intencao</option></select><input id='v2-signal' placeholder='Sinal comercial editorial'><textarea id='v2-hypothesis' placeholder='Hipotese testada'></textarea></fieldset>
          <fieldset><legend>Desafiante B - 10%</legend><input id='v3-name' placeholder='Nome'><input id='v3-path' placeholder='/url-p2-b/'><input id='v3-post' type='number' min='0' placeholder='ID post'><select id='v3-stage'><option value='diagnosis'>Diagnostico</option><option value='discovery'>Descoberta</option><option value='problem_identified'>Problema identificado</option><option value='comparison_decision'>Comparacao / decisao</option><option value='high_intent'>Alta intencao</option></select><input id='v3-signal' placeholder='Sinal comercial editorial'><textarea id='v3-hypothesis' placeholder='Hipotese testada'></textarea></fieldset>
          <fieldset><legend>Desafiante C - 10%</legend><input id='v4-name' placeholder='Nome'><input id='v4-path' placeholder='/url-p2-c/'><input id='v4-post' type='number' min='0' placeholder='ID post'><select id='v4-stage'><option value='diagnosis'>Diagnostico</option><option value='discovery'>Descoberta</option><option value='problem_identified'>Problema identificado</option><option value='comparison_decision'>Comparacao / decisao</option><option value='high_intent'>Alta intencao</option></select><input id='v4-signal' placeholder='Sinal comercial editorial'><textarea id='v4-hypothesis' placeholder='Hipotese testada'></textarea></fieldset>
          <fieldset><legend>Desafiante D - 10%</legend><input id='v5-name' placeholder='Nome'><input id='v5-path' placeholder='/url-p2-d/'><input id='v5-post' type='number' min='0' placeholder='ID post'><select id='v5-stage'><option value='diagnosis'>Diagnostico</option><option value='discovery'>Descoberta</option><option value='problem_identified'>Problema identificado</option><option value='comparison_decision'>Comparacao / decisao</option><option value='high_intent'>Alta intencao</option></select><input id='v5-signal' placeholder='Sinal comercial editorial'><textarea id='v5-hypothesis' placeholder='Hipotese testada'></textarea></fieldset>
        </div>
        <div class='experiment-buttons'><button id='save-experiment' type='button'>Salvar como rascunho</button><span id='experiment-form-status'></span></div>
      </section>
      {''.join(cards) if cards else "<section class='experiment-empty'><h2>Nenhum experimento cadastrado</h2><p>Cadastre a URL de origem, uma campea e quatro desafiantes. O trafego somente sera dividido depois da ativacao manual.</p></section>"}
    </div>
    <style>
      .experiment-hero{{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;padding:28px 0 22px}}
      .experiment-hero span{{font-size:11px;font-weight:900;text-transform:uppercase;color:#23764b}}
      .experiment-hero h1{{margin:5px 0 7px;font-size:34px}}.experiment-hero p{{margin:0;color:#52675b;max-width:760px}}
      .experiment-hero button,.experiment-buttons button,.experiment-action{{border:0;border-radius:6px;background:#19764b;color:#fff;padding:10px 13px;font-weight:800;cursor:pointer}}
      .experiment-action.secondary,.experiment-buttons .secondary{{background:#fff;color:#175c3e;border:1px solid #b9d8c5}}
      .experiment-action.danger{{background:#a53a2a}}.row-actions{{display:flex;flex-wrap:wrap;gap:6px}}
      .experiment-history{{margin-top:14px;border-top:1px solid #e0e9e3;padding-top:12px}}.experiment-history summary{{font-weight:900;cursor:pointer;color:#174b33}}
      .experiment-form-panel,.experiment-card,.experiment-empty{{background:#fff;border:1px solid #d5e4d9;border-radius:8px;padding:20px;margin-bottom:18px}}
      .experiment-form-grid{{display:grid;grid-template-columns:repeat(5,minmax(150px,1fr));gap:10px}}
      .experiment-form-panel label{{font-size:11px;font-weight:850;text-transform:uppercase;color:#52675b}}
      .experiment-form-panel input,.experiment-form-panel select,.experiment-form-panel textarea{{display:block;width:100%;box-sizing:border-box;margin-top:6px;border:1px solid #c9dace;border-radius:6px;padding:10px;font-size:14px;font-family:inherit}}
      .variant-grid{{display:grid;grid-template-columns:repeat(5,minmax(180px,1fr));gap:12px;margin:14px 0}}
      .variant-grid fieldset{{border:1px solid #d5e4d9;border-radius:7px;padding:12px}}.variant-grid legend{{font-weight:900;color:#174b33}}
      .experiment-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:18px}}.experiment-head h2{{margin:6px 0 4px;font-size:21px}}.experiment-head p{{margin:0;color:#687a70}}
      .experiment-status{{display:inline-block;padding:4px 7px;border-radius:4px;background:#e7f4eb;color:#17633f;font-size:10px;font-weight:900;text-transform:uppercase}}
      .experiment-recommendation{{background:#fff8dd;border:1px solid #efd17a;border-radius:6px;padding:10px 12px;font-weight:800;max-width:340px}}
      .experiment-rules{{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0}}.experiment-rules span{{background:#f3f7f4;border-radius:4px;padding:6px 8px;font-size:12px;color:#506359}}
      .experiment-card table{{width:100%;border-collapse:collapse;font-size:13px}}.experiment-card th,.experiment-card td{{padding:10px 8px;border-bottom:1px solid #e0e9e3;text-align:left;vertical-align:middle}}
      .experiment-card td small{{display:block;color:#6c7e74;margin-top:3px}}.experiment-card a{{color:#176d46}}
      .experiment-buttons{{display:flex;align-items:center;gap:8px;margin-top:14px}}.table-wrap{{overflow-x:auto}}
      @media(max-width:900px){{.experiment-form-grid,.variant-grid{{grid-template-columns:1fr}}.experiment-head,.experiment-hero{{align-items:stretch;flex-direction:column}}}}
    </style>
    <script>
    (function(){{
      const panel=document.getElementById('experiment-form-panel');
      const status=document.getElementById('experiment-form-status');
      document.getElementById('open-experiment-form').addEventListener('click',()=>{{panel.hidden=!panel.hidden}});
      async function send(url,options){{
        const response=await fetch(url,Object.assign({{method:'POST',headers:{{'Content-Type':'application/json'}}}},options||{{}}));
        if(!response.ok){{const body=await response.json().catch(()=>({{}}));throw new Error(body.detail||'Nao foi possivel concluir')}}
        return response.json();
      }}
      document.getElementById('save-experiment').addEventListener('click',async()=>{{
        status.textContent='Salvando...';
        const payload={{
          name:document.getElementById('exp-name').value,
          project:document.getElementById('exp-project').value,
          source_path:document.getElementById('exp-source').value,
          source_selector:document.getElementById('exp-selector').value,
          conversion_paths:[document.getElementById('exp-conversion').value],
          status:'draft',
          variants:[
            {{name:document.getElementById('v1-name').value,url_path:document.getElementById('v1-path').value,post_id:Number(document.getElementById('v1-post').value||0),cognitive_stage:document.getElementById('v1-stage').value,commercial_signal:document.getElementById('v1-signal').value,hypothesis:document.getElementById('v1-hypothesis').value,role:'champion',weight:60}},
            {{name:document.getElementById('v2-name').value,url_path:document.getElementById('v2-path').value,post_id:Number(document.getElementById('v2-post').value||0),cognitive_stage:document.getElementById('v2-stage').value,commercial_signal:document.getElementById('v2-signal').value,hypothesis:document.getElementById('v2-hypothesis').value,role:'challenger',weight:10}},
            {{name:document.getElementById('v3-name').value,url_path:document.getElementById('v3-path').value,post_id:Number(document.getElementById('v3-post').value||0),cognitive_stage:document.getElementById('v3-stage').value,commercial_signal:document.getElementById('v3-signal').value,hypothesis:document.getElementById('v3-hypothesis').value,role:'challenger',weight:10}},
            {{name:document.getElementById('v4-name').value,url_path:document.getElementById('v4-path').value,post_id:Number(document.getElementById('v4-post').value||0),cognitive_stage:document.getElementById('v4-stage').value,commercial_signal:document.getElementById('v4-signal').value,hypothesis:document.getElementById('v4-hypothesis').value,role:'challenger',weight:10}},
            {{name:document.getElementById('v5-name').value,url_path:document.getElementById('v5-path').value,post_id:Number(document.getElementById('v5-post').value||0),cognitive_stage:document.getElementById('v5-stage').value,commercial_signal:document.getElementById('v5-signal').value,hypothesis:document.getElementById('v5-hypothesis').value,role:'challenger',weight:10}}
          ]
        }};
        try{{await send('/api/funnel-experiments',{{body:JSON.stringify(payload)}});location.reload()}}catch(error){{status.textContent=error.message}}
      }});
      document.addEventListener('click',async(event)=>{{
        const button=event.target.closest('.experiment-action');if(!button)return;
        const id=button.dataset.experiment;
        let url='';
        if(button.dataset.action==='promote'){{
          if(!confirm('Promover esta variante a campea e iniciar um novo ciclo em modo protecao 60/10/10/10/10?'))return;
          url='/api/funnel-experiments/'+id+'/promote/'+button.dataset.variant;
        }}else if(button.dataset.action==='pause-variant'){{
          const reason=prompt('Motivo da pausa: risco claro ou derrota estatistica? Descreva em poucas palavras.');
          if(!reason)return;
          url='/api/funnel-experiments/'+id+'/pause-variant/'+button.dataset.variant;
          button.dataset.reason=reason;
        }}else{{
          if(!confirm('Confirmar esta alteracao?'))return;
          url='/api/funnel-experiments/'+id+'/status/'+button.dataset.status;
        }}
        button.disabled=true;
        const body=button.dataset.reason?JSON.stringify({{reason:button.dataset.reason}}):undefined;
        try{{await send(url,body?{{body}}:undefined);location.reload()}}catch(error){{alert(error.message);button.disabled=false}}
      }});
    }})();
    </script>
    """
    return _layout("Experimentos de funil", content, "experiments")


@app.post("/api/customer-leads/sync")
async def api_customer_leads_sync(request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    payload = await request.json()
    try:
        result = _sync_customer_leads_to_data_manager(
            runtime,
            limit=int(payload.get("limit") or 200),
            validate_only=bool(payload.get("validate_only")),
        )
        _record_customer_lead_sync_run(_customer_lead_db(runtime), result)
        return result
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/customer-leads/agents")
def api_customer_lead_agents() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return _customer_lead_agent_report(runtime, attempt_sync=False)


@app.post("/internal/customer-lead-agent-cycle")
def internal_customer_lead_agent_cycle() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return _customer_lead_agent_report(runtime, attempt_sync=True)


@app.get("/leads", response_class=HTMLResponse)
def customer_leads_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    summary = _customer_lead_summary(runtime)
    agent_report = _customer_lead_agent_report(runtime, attempt_sync=False)
    counts = summary.get("counts") or {}
    agent_cards = []
    for item in agent_report.get("agents") or []:
        status = str(item.get("status") or "")
        agent_cards.append(
            f"<article class='lead-agent lead-agent-{html.escape(status)}'>"
            f"<span>{html.escape(str(item.get('agent') or ''))}</span>"
            f"<strong>{html.escape(str(item.get('item') or ''))}</strong>"
            f"<small>{html.escape(str(item.get('detail') or ''))}</small>"
            "</article>"
        )
    rows = []
    for row in summary.get("rows") or []:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('created_at') or ''))}</td>"
            f"<td><strong>{html.escape(str(row.get('name') or ''))}</strong><small>{html.escape(str(row.get('email') or ''))}</small></td>"
            f"<td>{html.escape(str(row.get('site') or ''))}<small>{html.escape(str(row.get('page_path') or ''))}</small></td>"
            f"<td>{html.escape(str(row.get('attr_id') or ''))}<small>{html.escape(str(row.get('experiment_variant') or ''))}</small></td>"
            f"<td><strong>{html.escape(str(row.get('data_manager_status') or ''))}</strong><small>{html.escape(str(row.get('data_manager_request_id') or row.get('sync_error') or ''))}</small></td>"
            "</tr>"
        )
    content = f"""
    <div class='shell'>
      [[NAV]]
      <section class='lead-hero'>
        <div><span>Customer Match</span><h1>Leads e Data Manager</h1><p>Leads captados com consentimento nas P2. O envio usa hash SHA-256 e fica em fila ate haver destino Data Manager configurado.</p></div>
        <div class='lead-actions'><button id='lead-validate' type='button'>Validar Data Manager</button><button id='lead-sync' type='button'>Enviar pendentes</button></div>
      </section>
      <section class='lead-stats'>
        <article><span>Fila</span><strong>{int(counts.get('queued') or 0)}</strong></article>
        <article><span>Enviados</span><strong>{int(counts.get('sent') or 0)}</strong></article>
        <article><span>Retry</span><strong>{int(counts.get('retry') or 0)}</strong></article>
      </section>
      <section class='lead-agent-grid'>
        {''.join(agent_cards)}
      </section>
      <section class='lead-card'>
        <h2>Ultimos leads</h2>
        <div id='lead-status' class='lead-status'></div>
        <div class='table-wrap'><table><thead><tr><th>Data</th><th>Lead</th><th>Origem</th><th>Campanha/teste</th><th>Data Manager</th></tr></thead><tbody>{''.join(rows) or "<tr><td colspan='5'>Sem leads captados ainda.</td></tr>"}</tbody></table></div>
      </section>
    </div>
    <style>
      .lead-hero{{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;padding:28px 0 22px}}
      .lead-hero span{{font-size:11px;font-weight:900;text-transform:uppercase;color:#23764b}}
      .lead-hero h1{{margin:5px 0 7px;font-size:34px}}.lead-hero p{{margin:0;color:#52675b;max-width:760px}}
      .lead-actions{{display:flex;gap:8px;flex-wrap:wrap}}.lead-actions button{{border:0;border-radius:6px;background:#19764b;color:#fff;padding:10px 13px;font-weight:800;cursor:pointer}}
      .lead-actions button:first-child{{background:#fff;color:#175c3e;border:1px solid #b9d8c5}}
      .lead-stats{{display:grid;grid-template-columns:repeat(3,minmax(120px,1fr));gap:12px;margin-bottom:16px}}
      .lead-stats article,.lead-card{{background:#fff;border:1px solid #d5e4d9;border-radius:8px;padding:18px}}
      .lead-agent-grid{{display:grid;grid-template-columns:repeat(4,minmax(160px,1fr));gap:10px;margin-bottom:16px}}
      .lead-agent{{background:#fff;border:1px solid #d5e4d9;border-radius:8px;padding:13px}}
      .lead-agent span{{display:block;color:#536b5c;font-size:11px;font-weight:900;text-transform:uppercase}}
      .lead-agent strong{{display:block;margin:5px 0;color:#082216}}.lead-agent small{{color:#52675b;line-height:1.35}}
      .lead-agent-block{{border-color:#cf3c2f;background:#fff0ee}}.lead-agent-warn{{border-color:#d6ad37;background:#fff8df}}.lead-agent-ok{{border-color:#91d3a6;background:#f4fbf6}}
      .lead-stats span{{display:block;color:#5d7267;font-size:12px;text-transform:uppercase;font-weight:800}}.lead-stats strong{{font-size:28px}}
      .lead-card h2{{margin-top:0}}.lead-card table{{width:100%;border-collapse:collapse;font-size:13px}}.lead-card th,.lead-card td{{padding:10px 8px;border-bottom:1px solid #e0e9e3;text-align:left;vertical-align:top}}.lead-card small{{display:block;color:#6c7e74;margin-top:3px}}
      .lead-status{{margin:0 0 12px;color:#174b33;font-weight:800}}.table-wrap{{overflow-x:auto}}
      @media(max-width:800px){{.lead-hero{{align-items:stretch;flex-direction:column}}.lead-stats,.lead-agent-grid{{grid-template-columns:1fr}}}}
    </style>
    <script>
    (function(){{
      const status=document.getElementById('lead-status');
      async function sync(validateOnly){{
        status.textContent=validateOnly?'Validando destino Data Manager...':'Enviando pendentes...';
        const response=await fetch('/api/customer-leads/sync',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{limit:200,validate_only:validateOnly}})}});
        const body=await response.json().catch(()=>({{}}));
        if(!response.ok){{status.textContent=body.detail||'Falha na sincronizacao';return;}}
        status.textContent=body.message||('OK: '+(body.sent||0)+' lead(s), request '+(body.request_id||'sem id'));
        if(body.ok) window.setTimeout(()=>location.reload(),1200);
      }}
      document.getElementById('lead-validate').addEventListener('click',()=>sync(true));
      document.getElementById('lead-sync').addEventListener('click',()=>sync(false));
    }})();
    </script>
    """
    return _layout("Leads e Data Manager", content, "leads")


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    runtime = load_runtime(CONFIG_PATH)
    current_day = app_today()
    summary = load_dashboard_summary(runtime, current_day)
    projects = load_project_kpis(runtime, current_day)
    project_session_cache = {
        str(item.get("Projeto") or ""): traffic_session_project_periods(
            runtime, str(item.get("Projeto") or ""), current_day
        )
        for item in projects
        if str(item.get("Projeto") or "")
    }

    def project_period_values(item: dict, period_key: str) -> dict[str, float]:
        project_sessions = project_session_cache.get(str(item.get("Projeto") or ""), {})
        if period_key == "hoje":
            values = {"spend": _to_float(item.get("Investimento Hoje")), "revenue": _to_float(item.get("Receita Hoje")), "roi": _to_float(item.get("ROI Hoje")), "pmr": _to_float(item.get("Taxa de correspondencia do Ad Exchange Hoje")), "ecpm": _to_float(item.get("eCPM Hoje")), "mgid": _to_float(item.get("Receita MGID Hoje"))}
            sessions = _to_float(project_sessions.get("hoje"))
            return {**values, "sessions": sessions, "rps": values["revenue"] / sessions if sessions > 0 else 0.0}
        if period_key == "ontem":
            values = {"spend": _to_float(item.get("Investimento Ontem")), "revenue": _to_float(item.get("Receita Ontem")), "roi": _to_float(item.get("ROI Ontem")), "pmr": _to_float(item.get("Taxa de correspondencia do Ad Exchange Ontem")), "ecpm": _to_float(item.get("eCPM Ontem")), "mgid": _to_float(item.get("Receita MGID Ontem"))}
            sessions = _to_float(project_sessions.get("ontem"))
            return {**values, "sessions": sessions, "rps": values["revenue"] / sessions if sessions > 0 else 0.0}
        if period_key == "7d":
            values = {"spend": _to_float(item.get("Investimento Ultimos 7 Dias")), "revenue": _to_float(item.get("Receita Ultimos 7 Dias")), "roi": _to_float(item.get("ROI Ultimos 7 Dias")), "pmr": _to_float(item.get("Taxa de correspondencia do Ad Exchange Ultimos 7 Dias")), "ecpm": _to_float(item.get("eCPM Ultimos 7 Dias")), "mgid": _to_float(item.get("Receita MGID 7 Dias"))}
            sessions = _to_float(project_sessions.get("7d"))
            return {**values, "sessions": sessions, "rps": values["revenue"] / sessions if sessions > 0 else 0.0}
        values = {"spend": _to_float(item.get("Investimento Mes")), "revenue": _to_float(item.get("Receita Mes")), "roi": _to_float(item.get("ROI Mes")), "pmr": _to_float(item.get("Taxa de correspondencia do Ad Exchange Mes")), "ecpm": _to_float(item.get("eCPM Mes")), "mgid": _to_float(item.get("Receita MGID Mes"))}
        sessions = _to_float(project_sessions.get("mes"))
        return {**values, "sessions": sessions, "rps": values["revenue"] / sessions if sessions > 0 else 0.0}

    def project_has_data(item: dict) -> bool:
        for period in ("hoje", "ontem", "7d", "mes"):
            values = project_period_values(item, period)
            if values["spend"] > 0:
                return True
        return False

    def project_status(item: dict) -> tuple[str, str, float]:
        today = project_period_values(item, "hoje")
        week = project_period_values(item, "7d")
        profit_today = today["revenue"] - today["spend"]
        profit_week = week["revenue"] - week["spend"]
        if today["spend"] <= 0 and week["spend"] <= 0:
            return ("Sem dados", "neutral", 0)
        if today["spend"] > 0 and today["roi"] < 0:
            return ("Perdendo hoje", "bad", 400000 + abs(profit_today) + today["spend"])
        if week["spend"] > 0 and week["roi"] < 0:
            return ("7d negativo", "warn", 300000 + abs(profit_week) + week["spend"])
        if today["roi"] > 0 and week["roi"] >= 0:
            return ("Escalar", "good", 200000 + profit_today + today["spend"])
        return ("Monitorar", "neutral", 100000 + today["spend"] + week["spend"])

    projects = [item for item in projects if project_has_data(item)]
    projects.sort(key=lambda item: project_status(item)[2], reverse=True)

    def dashboard_period_cell(item: dict, period_key: str, title_class: str) -> str:
        values = project_period_values(item, period_key)
        spend = values["spend"]
        revenue = values["revenue"]
        roi = values["roi"]
        pmr = values["pmr"]
        ecpm = values["ecpm"]
        mgid = values["mgid"]
        sessions = values["sessions"]
        rps = values["rps"]
        gam_unavailable = period_key == "hoje" and str(item.get("GAM Hoje Indisponivel") or "").lower() == "sim"
        profit = revenue - spend
        if gam_unavailable:
            message = html.escape(str(item.get("GAM Snapshot Message") or "GAM sem snapshot hoje."))
            return (
                f"<td class='period-cell {title_class}'>"
                "<div class='period-stack'>"
                f"<div class='period-line'><span>Inv.</span><strong>{_money(_to_float(spend))}</strong></div>"
                "<div class='period-line'><span>Receita</span><strong>GAM sem sync</strong></div>"
                "<div class='period-line profit-line'><span>Lucro</span><strong>-</strong></div>"
                "<div class='period-line'><span>ROI</span><strong>-</strong></div>"
                f"<small class='metric-scope'>{message}</small>"
                "</div>"
                "</td>"
            )
        mgid_line = f"<div class='period-line'><span>MGID</span><strong>{_money(mgid)}</strong></div>" if mgid > 0 else ""
        return (
            f"<td class='period-cell {title_class}'>"
            "<div class='period-stack'>"
            f"<div class='period-line'><span>Inv.</span><strong>{_money(_to_float(spend))}</strong></div>"
            f"<div class='period-line'><span>Receita</span><strong>{_money(_to_float(revenue))}</strong></div>"
            f"{mgid_line}"
            f"<div class='period-line profit-line'><span>Lucro</span><strong class='{_roi_class(profit)}'>{_money(profit)}</strong></div>"
            f"<div class='period-line'><span>ROI</span><strong class='{_roi_class(_to_float(roi))}'>{_percent(_to_float(roi))}</strong></div>"
            f"<div class='period-line pmr-line'><span>PMR</span><strong>{_percent(_to_float(pmr))}</strong></div>"
            f"<div class='period-line ecpm-line'><span>eCPM</span><strong>{_money(_to_float(ecpm))}</strong></div>"
            f"<div class='period-line'><span>Sessoes</span><strong>{int(sessions)}</strong></div>"
            f"<div class='period-line'><span>RPS</span><strong>{_money(rps) if sessions > 0 else '-'}</strong></div>"
            "</div>"
            "</td>"
        )

    project_rows = []
    for item in projects:
        project = str(item["Projeto"])
        status_label, status_class, _priority = project_status(item)
        project_url = f"/projects/{quote(project, safe='')}"
        project_rows.append(
            "<tr>"
            f"<td class='project-name-cell'><a class='project-link' href='{project_url}'>{html.escape(project)}</a></td>"
            f"<td class='project-status-cell'><span class='project-status-pill {status_class}'>{html.escape(status_label)}</span></td>"
            + dashboard_period_cell(item, "hoje", "period-cell-today")
            + dashboard_period_cell(item, "ontem", "period-cell-yesterday")
            + dashboard_period_cell(item, "7d", "period-cell-7d")
            + dashboard_period_cell(item, "mes", "period-cell-month")
            + f"<td class='project-action-cell'><a class='project-open-button' href='{project_url}'>Abrir</a></td>"
            + "</tr>"
        )

    def summary_row(
        label: str,
        spend: object,
        revenue: object,
        roi: object,
        row_class: str,
    ) -> str:
        spend_value = _to_float(spend)
        revenue_value = _to_float(revenue)
        roi_value = _to_float(roi)
        net_value = revenue_value - spend_value
        blocked = row_class == "summary-row-today" and bool(summary.get("gam_hoje_indisponivel"))
        if blocked:
            message = html.escape(str(summary.get("gam_hoje_message") or "GAM sem snapshot hoje."))
            return (
                f"<tr class='summary-period-row {row_class}'>"
                f"<th>{html.escape(label)}</th>"
                "<td><span class='summary-label'>Investimento</span>"
                f"<span class='summary-value'>{_money(spend_value)}</span></td>"
                "<td><span class='summary-label'>Receita</span>"
                f"<span class='summary-value'>GAM sem sync</span><small class='metric-scope'>{message}</small></td>"
                "<td><span class='summary-label'>Resultado liquido</span><span class='summary-value'>-</span></td>"
                "<td><span class='summary-label'>ROI</span><span class='summary-value'>-</span></td>"
                "</tr>"
            )
        return (
            f"<tr class='summary-period-row {row_class}'>"
            f"<th>{html.escape(label)}</th>"
            "<td><span class='summary-label'>Investimento</span>"
            f"<span class='summary-value'>{_money(spend_value)}</span></td>"
            "<td><span class='summary-label'>Receita</span>"
            f"<span class='summary-value'>{_money(revenue_value)}</span></td>"
            "<td><span class='summary-label'>Resultado liquido</span>"
            f"<span class='summary-value'>{_money(net_value)}</span></td>"
            "<td><span class='summary-label'>ROI</span>"
            f"<span class='summary-value {_roi_class(roi_value)}'>{_percent(roi_value)}</span></td>"
            "</tr>"
        )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Controle Operacional</div>
          <h1>ROAS Control Center</h1>
          <p>Este painel jÃƒÂ¡ lÃƒÂª o SQLite como fonte principal, centraliza projetos, decisÃƒÂµes da IA e a fila operacional do GAM. Ãƒâ€° a base para a transiÃƒÂ§ÃƒÂ£o da planilha para o site.</p>
          <div class='hero-actions'>
            <form method='post' action='/run-fast'>
              <button class='action-button' type='submit'>ATUALIZAR HOJE</button>
            </form>
            <form method='post' action='/run-full'>
              <button class='action-button secondary' type='submit'>ATUALIZAR TUDO</button>
            </form>
            <a class='action-link secondary' href='https://app.wizseoservicesaustralia.com/setup-inicial'>SETUP INICIAL</a>
          </div>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Ultima atualizacao</div>
            <strong>{html.escape(summary['updated_at'])}</strong>
          </div>
          <div>
            <div class='aside-label'>Projetos ativos</div>
            <strong>{summary['total_projects']}</strong>
          </div>
        </div>
      </div>

      <div class='panel project-summary-panel'>
        <h2>Resumo geral</h2>
        <p>Investimento, receita, resultado liquido e ROI por periodo.</p>
        <div class='table-scroll'>
          <table class='project-summary-table'>
            <thead>
              <tr>
                <th>Periodo</th>
                <th>Investimento</th>
                <th>Receita</th>
                <th>Resultado liquido</th>
                <th class="placement-roi-cell">ROI</th>
              </tr>
            </thead>
            <tbody>
              {summary_row('Hoje', summary.get('investimento_hoje', 0), summary.get('receita_hoje', 0), summary.get('roi_hoje', 0), 'summary-row-today')}
              {summary_row('Ontem', summary.get('investimento_ontem', 0), summary.get('receita_ontem', 0), summary.get('roi_ontem', 0), 'summary-row-yesterday')}
              {summary_row('Ultimos 7 Dias', summary.get('investimento_7d', 0), summary.get('receita_7d', 0), summary.get('roi_7d', 0), 'summary-row-7d')}
              {summary_row('Mes', summary.get('investimento_mes', 0), summary.get('receita_mes', 0), summary.get('roi_mes', 0), 'summary-row-month')}
            </tbody>
          </table>
        </div>
      </div>

      <div class='grid-full'>
        <div class='panel'>
          <h2>Projetos</h2>
          <p>Visao executiva por prioridade: projetos zerados ficam ocultos; cada periodo mostra investimento, receita, lucro, ROI, PMR e eCPM.</p>
          <div class='table-scroll'>
          <table class='projects-table'>
            <thead>
              <tr>
                <th>Projeto</th>
                <th>Status</th>
                <th class='period-title period-title-today'>Hoje</th>
                <th class='period-title period-title-yesterday'>Ontem</th>
                <th class='period-title period-title-7d'>Ultimos 7 Dias</th>
                <th class='period-title period-title-month'>Mes</th>
                <th>Acao</th>
              </tr>
            </thead>
            <tbody>{''.join(project_rows)}</tbody>
          </table>
          </div>
        </div>
      </div>
    </div>
    """
    return _layout("ROAS Control Center", content, "dashboard")




def _clarity_funnel_audit(project: str, attr_id: str) -> dict:
    day = app_today()
    return get_clarity_snapshot(
        str(CONFIG_PATH.with_name("roas_control.sqlite3")),
        project_id=project,
        date_start=day,
        date_end=day,
        metrics=["project-live-insights"],
        dimensions=["project", "attr_id"],
        essential=False,
        allow_api=False,
    )


@app.get("/projects/{project}/funnels/{attr_id}", response_class=HTMLResponse)
def attr_funnel_page(project: str, attr_id: str, period: str = "hoje") -> str:
    runtime = load_runtime(CONFIG_PATH)
    funnel = load_attr_funnel(runtime, project, attr_id, app_today())
    if not funnel:
        raise HTTPException(status_code=404, detail="Attr ID nao encontrado")
    period = period if period in {"hoje", "ontem", "7d", "mes"} else "hoje"
    metrics = funnel["periods"][period]
    campaign = funnel["campaign"]
    clarity_audit = _clarity_funnel_audit(project, attr_id)
    tabs = "".join(
        f"<a class='campaign-inline-action{' active' if key == period else ''}' href='/projects/{quote(project, safe='')}/funnels/{quote(attr_id, safe='')}?period={key}'>{label}</a>"
        for key, label in (("hoje", "Hoje"), ("ontem", "Ontem"), ("7d", "Ultimos 7 dias"), ("mes", "Mes"))
    )
    destinations = "".join(
        "<tr>"
        f"<td><a href='https://{html.escape(project)}{html.escape(str(row.get('url_path') or '/'), quote=True)}' target='_blank' rel='noopener noreferrer'>{html.escape(str(row.get('url_path') or '/'))}</a></td>"
        f"<td>{int(row.get('sessions') or 0)}</td>"
        f"<td>{_percent((int(row.get('sessions') or 0) / metrics['sessions']) if metrics['sessions'] else 0)}</td>"
        "</tr>"
        for row in metrics["destinations"]
    ) or "<tr><td colspan='3'>Nenhum avanço para outra pagina registrado neste periodo.</td></tr>"
    first_seen = _br_datetime(funnel.get("tracking_first_seen_at"))
    partial_warning = ""
    if period == "hoje" and first_seen and "00:00" not in first_seen:
        partial_warning = (
            "<div class='funnel-warning'><strong>Cobertura parcial:</strong> o rastreador de sessoes comecou em "
            f"{html.escape(first_seen)}. Os cliques do Google Ads consideram o dia inteiro; a perda desta janela ainda nao e conclusiva.</div>"
        )
    google_ads_url = str(campaign.get("google_ads_url") or "").strip()
    landing_url = str(campaign.get("landing_url") or "").strip()
    links = ""
    if google_ads_url:
        links += f"<a class='action-button' href='{html.escape(google_ads_url, quote=True)}' target='_blank' rel='noopener noreferrer'>Google Ads</a>"
    if landing_url:
        links += f"<a class='action-button secondary' href='{html.escape(landing_url, quote=True)}' target='_blank' rel='noopener noreferrer'>Abrir artigo</a>"
    ads_session_rate = metrics["sessions"] / metrics["ads_clicks"] if metrics["ads_clicks"] else 0
    no_advance_rate = 1 - metrics["session_button_rate"] if metrics["sessions"] else 0
    session_delta = metrics["sessions"] - metrics["ads_clicks"]
    if session_delta > 0:
        session_delta_value = f"+{session_delta}"
        session_delta_label = f"{_percent(session_delta / metrics['ads_clicks'])} acima dos cliques" if metrics["ads_clicks"] else "sessoes sem clique Ads comparavel"
        session_delta_class = "is-info"
    elif session_delta < 0:
        session_delta_value = str(session_delta)
        session_delta_label = f"{_percent(abs(session_delta) / metrics['ads_clicks'])} de perda" if metrics["ads_clicks"] else "sem base de comparacao"
        session_delta_class = "is-loss"
    else:
        session_delta_value = "0"
        session_delta_label = "sem diferenca"
        session_delta_class = "is-neutral"
    event_coverage = _to_float(metrics.get("event_coverage"))
    detail_warning = ""
    if event_coverage < 0.95:
        detail_warning = (
            "<div class='funnel-warning'><strong>Jornada parcialmente reconstruida:</strong> "
            f"{_percent(event_coverage)} das sessoes deste periodo possuem eventos detalhados. "
            "Os dados antigos mostram entrada e ultimo destino; as novas sessoes registram todas as etapas.</div>"
        )
    off_landing_sessions = int(metrics.get("off_landing_sessions") or 0)
    if off_landing_sessions:
        excluded_entries = ", ".join(
            f"{str(item.get('url') or '/')}: {int(item.get('sessions') or 0)}"
            for item in (metrics.get("off_landing_entries") or [])
        )
        detail_warning += (
            "<div class='funnel-warning'><strong>Sessoes excluidas do funil oficial:</strong> "
            f"{off_landing_sessions} sessao(oes) reutilizaram este Attr ID, mas entraram fora da URL final "
            f"configurada no Google Ads ({html.escape(str(funnel.get('landing_path') or '/'))}). "
            f"Entradas observadas: {html.escape(excluded_entries)}.</div>"
        )
    journey_cards = [
        "<div class='journey-card metric-help' tabindex='0' data-help='Origem do funil: cliques registrados pelo Google Ads.'>"
        f"<span class='journey-index'>Origem</span><strong>Google Ads</strong><b>{metrics['ads_clicks']} cliques</b></div>"
    ]
    for index, stage in enumerate(metrics.get("journey_stages") or [], start=1):
        stage_url = str(stage.get("url") or "/")
        stage_kind = "Destino externo" if stage.get("external") else ("Pagina de entrada" if index == 1 else f"Etapa {index}")
        intelligence_label = str(stage.get("iab_content_label") or "IAB nao classificado")
        commercial_signal = str(stage.get("commercial_signal_type") or "sem sinal comercial classificado").replace("_", " ")
        commercial_score = _to_float(stage.get("commercial_intent_score"))
        intelligence_line = (
            f"<small class='journey-intelligence'>IAB: {html.escape(intelligence_label)}<br>"
            f"Sinal: {html.escape(commercial_signal)}"
            f"{' | Score ' + str(int(commercial_score)) if commercial_score else ''}</small>"
        )
        has_stage_gam = stage.get("ecpm") is not None or stage.get("rps") is not None or stage.get("revenue") is not None
        stage_money_parts = []
        if stage.get("revenue") is not None:
            stage_money_parts.append(f"Receita {_money(stage.get('revenue'))}")
        if stage.get("ecpm") is not None:
            stage_money_parts.append(f"eCPM {_money(stage.get('ecpm'))}")
        if stage.get("rps") is not None:
            stage_money_parts.append(f"RPS {_money(stage.get('rps'))}")
        stage_money = (
            f"<em>{' | '.join(stage_money_parts)}</em>"
            if has_stage_gam else
            "<em class='muted'>Sem receita/eCPM real por etapa ainda</em>"
        )
        stage_help = (
            f"{stage_kind}: {_percent(stage.get('landing_rate'))} das sessoes chegaram aqui. IAB classifica o tema da pagina; sinal comercial mostra a intencao economica usada para comparar RPS/eCPM. Receita/eCPM/RPS exibidos somente quando o GAM reporta kaizen_stage ou kaizen_post_id real."
        )
        journey_cards.append(
            "<div class='journey-card metric-help' tabindex='0' "
            f"data-help='{html.escape(stage_help, quote=True)}'>"
            f"<span class='journey-index'>{html.escape(stage_kind)}</span>"
            f"<strong title='{html.escape(stage_url, quote=True)}'>{html.escape(stage_url)}</strong>"
            f"<b>{int(stage.get('sessions') or 0)} sessoes</b>"
            f"<small>{_percent(stage.get('landing_rate'))} da entrada</small>"
            f"{intelligence_line}"
            f"{stage_money}</div>"
        )
    journey_html = "<div class='journey-track'>" + "<span class='journey-connector'>→</span>".join(journey_cards) + "</div>"
    route_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('source') or '/'))}</td>"
        f"<td>{html.escape(str(row.get('target') or '/'))}</td>"
        f"<td><strong>{int(row.get('sessions') or 0)}</strong></td>"
        f"<td>{_percent(row.get('conversion_rate'))}</td>"
        f"<td>{int(row.get('drop') or 0)}</td>"
        "</tr>"
        for row in (metrics.get("routes") or [])
    ) or "<tr><td colspan='5'>As novas sessoes ainda nao registraram uma transicao completa.</td></tr>"
    bottleneck = metrics.get("bottleneck") or {}
    bottleneck_rate = _to_float(bottleneck.get("drop_rate"))
    bottleneck_level = "Alta" if bottleneck_rate >= 0.5 else ("Media" if bottleneck_rate >= 0.25 else "Baixa")
    daily_rows_data = metrics.get("daily") or []
    max_rps = max((_to_float(row.get("rps")) for row in daily_rows_data), default=0.0)
    daily_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('day') or ''))}</td>"
        f"<td>{int(row.get('ads_clicks') or 0)}</td>"
        f"<td>{int(row.get('sessions') or 0)}</td>"
        f"<td>{int(row.get('advances') or 0)}</td>"
        f"<td>{_percent(row.get('session_advance_rate'))}</td>"
        f"<td>{_money(row.get('revenue'))}</td>"
        f"<td>{_money(row.get('ecpm'))}</td>"
        "<td><div class='rps-cell'>"
        f"<strong>{_money(row.get('rps'))}</strong>"
        f"<i style='width:{min(100.0, (_to_float(row.get('rps')) / max_rps * 100.0) if max_rps else 0.0):.1f}%'></i>"
        "</div></td></tr>"
        for row in daily_rows_data
    ) or "<tr><td colspan='8'>Sem dados diarios neste periodo.</td></tr>"
    rps_days = [row for row in daily_rows_data if int(row.get("sessions") or 0) > 0]
    rps_trend = "Sem comparacao diaria"
    rps_trend_class = "is-neutral"
    if len(rps_days) >= 2:
        previous_rps = _to_float(rps_days[-2].get("rps"))
        current_rps = _to_float(rps_days[-1].get("rps"))
        if previous_rps > 0:
            rps_change = (current_rps - previous_rps) / previous_rps
            rps_trend = f"{'+' if rps_change >= 0 else ''}{_percent(rps_change)} vs. dia anterior"
            rps_trend_class = "is-up" if rps_change >= 0 else "is-down"

    def _path_link(path: str) -> str:
        path = str(path or "/")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return "https://" + project + (path if path.startswith("/") else "/" + path)

    def _short_path(path: str, limit: int = 34) -> str:
        path = str(path or "/")
        return path if len(path) <= limit else path[: limit - 1] + "?"

    routes_by_source = {}
    for route in metrics.get("routes") or []:
        source = str(route.get("source") or "/")
        routes_by_source.setdefault(source, []).append(route)
    for route_list in routes_by_source.values():
        route_list.sort(key=lambda row: int(row.get("sessions") or 0), reverse=True)
    stage_metrics = {str(stage.get("url") or "/"): stage for stage in (metrics.get("journey_stages") or [])}

    def _node_metrics(path: str, sessions: int) -> str:
        stage = stage_metrics.get(str(path or "/")) or {}
        pct = sessions / start_sessions if start_sessions else 0
        revenue = stage.get("revenue")
        ecpm = stage.get("ecpm")
        rps = stage.get("rps")
        scope = str(stage.get("metric_scope") or "sem dado real por etapa")
        iab_label = str(stage.get("iab_content_label") or "")
        commercial_signal = str(stage.get("commercial_signal_type") or "").replace("_", " ")
        commercial_score = _to_float(stage.get("commercial_intent_score"))
        intelligence_line = (
            f"<small class='path-intelligence'>IAB: {html.escape(iab_label)}<br>Sinal: {html.escape(commercial_signal)}"
            f"{' | Score ' + str(int(commercial_score)) if commercial_score else ''}</small>"
            if iab_label or commercial_signal or commercial_score else ""
        )
        money_parts = []
        if revenue is not None:
            money_parts.append(f"Receita {_money(revenue)}")
        if ecpm is not None:
            money_parts.append(f"eCPM {_money(ecpm)}")
        if rps is not None:
            money_parts.append(f"RPS {_money(rps)}")
        money_line = (
            f"<em>{' | '.join(money_parts)}</em>"
            if money_parts
            else "<em class='muted'>Sem receita/eCPM real por URL</em>"
        )
        return f"<small>{_percent(pct)} das sessoes</small>{intelligence_line}{money_line}<small class='metric-scope'>{html.escape(scope)}</small>"

    def _node_alert(path: str) -> str:
        route_list = routes_by_source.get(str(path or "/"), [])
        if not route_list:
            return ""
        source_total = sum(int(route.get("sessions") or 0) for route in route_list)
        if not source_total:
            return ""
        return_routes = [route for route in route_list if route.get("return_to_stage")]
        if return_routes:
            return_sessions = sum(int(route.get("sessions") or 0) for route in return_routes)
            return_stage = min(int(route.get("return_to_stage") or 1) for route in return_routes)
            return f"<mark>Retorno para etapa {return_stage}: {return_sessions} ({_percent(return_sessions / source_total)})</mark>"
        landing_returns = sum(int(route.get("sessions") or 0) for route in route_list if str(route.get("target") or "") == landing_path)
        if landing_returns:
            return f"<mark>Retorno para etapa 1: {landing_returns} ({_percent(landing_returns / source_total)})</mark>"
        top = route_list[0]
        return f"<small>Proximo: {html.escape(_short_path(str(top.get('target') or '/'), 24))} ({_percent(int(top.get('sessions') or 0) / source_total)})</small>"

    start_sessions = int(metrics.get("sessions") or 0)
    landing_path = str(metrics.get("primary_entry_path") or "")
    path_columns = []
    for column in metrics.get("journey_columns") or []:
        raw_items = list(column.get("items") or [])
        items = raw_items[:5]
        extra_count = max(len(raw_items) - 5, 0)
        extra_sessions = sum(int(item.get("sessions") or 0) for item in raw_items[5:])
        if extra_count:
            items.append({"path": f"Mais {extra_count}", "sessions": extra_sessions, "is_more": True})
        path_columns.append({"label": f"Etapa {int(column.get('position') or len(path_columns) + 1)}", "items": items})

    max_path_sessions = max((int(item.get("sessions") or 0) for col in path_columns for item in col["items"]), default=1)
    path_columns_html = ""
    for col in path_columns[:5]:
        items_html = ""
        for item in col["items"]:
            sessions = int(item.get("sessions") or 0)
            scale = sessions / max_path_sessions if max_path_sessions else 0
            bar_height = max(18.0, min(190.0, 28.0 + scale * 150.0))
            path = str(item.get("path") or "/")
            if item.get("is_more"):
                node = f"<div class='path-node path-more'><span>{html.escape(path)}</span><strong>{sessions}</strong></div>"
            elif item.get("is_start"):
                node = f"<div class='path-node path-start'><span>session_start</span><strong>{sessions}</strong><small>{_percent(1 if sessions else 0)} das sessoes</small></div>"
            else:
                url = html.escape(_path_link(path), quote=True)
                label = html.escape(_short_path(path))
                full = html.escape(path, quote=True)
                node = f"<a class='path-node' href='{url}' target='_blank' rel='noopener noreferrer' title='{full}'><span>{label}</span><strong>{sessions}</strong>{_node_metrics(path, sessions)}{_node_alert(path)}</a>"
            items_html += f"<div class='path-row'><i style='height:{bar_height:.1f}px'></i>{node}</div>"
        path_columns_html += (
            "<section class='path-column'>"
            f"<header><small>{html.escape(col['label'])}</small><button type='button' aria-label='Editar dimensao'>?</button></header>"
            "<div class='path-dimension'><span>?</span><b>Caminho da pagina e...</b></div>"
            f"<div class='path-items'>{items_html}</div>"
            "</section>"
        )
    path_explorer_html = (
        "<section class='path-explorer-v2 metric-help' tabindex='0' data-help='Exploracao visual de caminhos reais por Attr ID. Cada coluna representa uma etapa da jornada; clique na URL para abrir a pagina.'>"
        "<div class='path-explorer-head'><div><span class='cockpit-kicker'>Exploracao de caminho</span>"
        "<h2>Jornada por etapa</h2><p>Clique em qualquer URL para abrir a pagina correspondente. Dados reais do rastreador por Attr ID.</p></div>"
        f"<div class='campaign-inline-actions'>{tabs}</div></div>"
        f"<div class='path-board'>{path_columns_html}</div>"
        "</section>"
    )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Funil por Attr ID</div>
          <h1>{html.escape(str(campaign.get('campaign') or attr_id))}</h1>
          <p>Attr ID <strong>{html.escape(attr_id)}</strong> | {html.escape(project)}</p>
          <div class='campaign-inline-actions'><a class='campaign-inline-action' href='/projects/{quote(project, safe='')}'>Voltar ao projeto</a>{links}</div>
        </div>
      </div>
      <div class='panel'>
        <div class='panel-head'><div><div class='aside-label'>Analise de funil</div><h2>Google Ads → sessoes reais → clique no botao</h2><p>Passe o mouse, toque ou use Tab em qualquer card para entender a metrica.</p></div><div class='campaign-inline-actions'>{tabs}</div></div>
        {partial_warning}
        {detail_warning}
        {path_explorer_html}
        <div class='funnel-grid'>
          <div class='funnel-stage metric-help' tabindex='0' data-help='Cliques cobrados ou registrados pelo Google Ads para esta campanha e periodo. As impressoes mostram quantas vezes o anuncio apareceu.'><span>1. Cliques Google Ads <b class='help-icon' aria-hidden='true'>?</b></span><strong>{metrics['ads_clicks']}</strong><small>{metrics['ads_impressions']} impressoes</small></div>
          <div class='funnel-arrow metric-help {session_delta_class}' tabindex='0' data-help='Compara cliques do Google Ads com sessoes first-party. Valor negativo indica perda antes do carregamento; valor positivo pode ocorrer por retorno, recarga, diferenca de janela ou cobertura parcial.'><strong>{session_delta_value}</strong><span>{session_delta_label}</span></div>
          <div class='funnel-stage metric-help' tabindex='0' data-help='Visitas reais identificadas pelo rastreador first-party na pagina de destino, vinculadas a este Attr ID. Uma pessoa pode gerar mais de uma sessao.'><span>2. Sessoes reais <b class='help-icon' aria-hidden='true'>?</b></span><strong>{metrics['sessions']}</strong><small>{_percent(ads_session_rate)} dos cliques</small></div>
          <div class='funnel-arrow metric-help is-loss' tabindex='0' data-help='Sessoes que chegaram a pagina, mas nao avancaram por nenhum botao rastreado. Quanto menor este numero, melhor a continuidade do funil.'><strong>-{max(metrics['sessions'] - metrics['button_clicks'], 0)}</strong><span>{_percent(no_advance_rate)} sem avanço</span></div>
          <div class='funnel-stage metric-help' tabindex='0' data-help='Sessoes que clicaram em um botao rastreado e chegaram a proxima URL. Mede avancos confirmados, nao apenas o clique visual.'><span>3. Cliques/avancos <b class='help-icon' aria-hidden='true'>?</b></span><strong>{metrics['button_clicks']}</strong><small>{_percent(metrics['session_button_rate'])} das sessoes</small></div>
        </div>
        <div class='funnel-summary'>
          <div class='metric-help' tabindex='0' data-help='Percentual de sessoes reais em relacao aos cliques do Google Ads. Pode passar de 100% por retornos, recargas, diferenca de janela ou cobertura parcial.'><span>Conversao Ads → sessao <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_percent(ads_session_rate)}</strong></div>
          <div class='metric-help' tabindex='0' data-help='Percentual das sessoes reais que avancou por um botao rastreado para a proxima pagina.'><span>Conversao sessao → botao <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_percent(metrics['session_button_rate'])}</strong></div>
          <div class='metric-help' tabindex='0' data-help='Percentual dos cliques do Google Ads que terminou em um avanco confirmado no funil.'><span>Conversao final Ads → botao <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_percent(metrics['ads_button_rate'])}</strong></div>
          <div class='metric-help' tabindex='0' data-help='Investimento registrado pelo Google Ads para esta campanha no periodo selecionado.'><span>Custo Google Ads <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_money(metrics['ads_cost'])}</strong></div>
          <div class='metric-help' tabindex='0' data-help='Receita GAM atribuida a este Attr ID no periodo selecionado.'><span>Receita atribuida <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_money(metrics.get('revenue'))}</strong></div>
          <div class='metric-help' tabindex='0' data-help='eCPM real do GAM para este Attr ID no periodo: receita atribuida dividida pelas impressoes GAM e multiplicada por 1000. Nao e estimado por etapa.'><span>eCPM GAM <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_money(metrics.get('ecpm'))}</strong><small>{int(_to_float(metrics.get('gam_impressions')))} impressoes GAM</small></div>
          <div class='metric-help' tabindex='0' data-help='Receita por sessao: receita atribuida dividida pelas sessoes reais. Use para comparar a qualidade economica do trafego entre dias.'><span>RPS <b class='help-icon' aria-hidden='true'>?</b></span><strong>{_money(metrics.get('rps'))}</strong><small class='{rps_trend_class}'>{html.escape(rps_trend)}</small></div>
        </div>
      </div>

      <div class='panel clarity-audit-panel'>
        <div class='panel-head'><div><h2>Auditoria Clarity</h2><p>Sinais comportamentais reais para validar gargalos: gravacoes, scroll, dead clicks e rage clicks. Quando a API falhar, o sistema nao deve usar Clarity para decisao.</p></div></div>
        <div class='funnel-summary'>
          <div class='metric-help' tabindex='0' data-help='Estado da conexao Clarity Data Export API para este funil. OK permite usar sinais comportamentais; quota/erro bloqueia conclusoes pelo Clarity.'><span>Status Clarity <b class='help-icon' aria-hidden='true'>?</b></span><strong>{html.escape(str(clarity_audit.get('status') or ''))}</strong><small>{'cache' if clarity_audit.get('cached') else 'consulta'}</small></div>
          <div class='metric-help' tabindex='0' data-help='Mensagem operacional da API Clarity. Se houver quota excedida, aguarde renovacao ou use somente o ultimo cache valido.'><span>Leitura <b class='help-icon' aria-hidden='true'>?</b></span><strong>{html.escape(str(clarity_audit.get('message') or ''))}</strong></div>
        </div>
        <div class='real-data-note'>Sem inventar dados: dead clicks, rage clicks e scroll depth so serao exibidos quando vierem da API/cache Clarity. Se o status for quota, a auditoria visual continua, mas a decisao fica marcada como sem cobertura Clarity.</div>
      </div>

      <div class='decision-grid'>
        <div class='panel bottleneck-panel'>
          <div class='aside-label'>Gargalo principal</div>
          <div class='bottleneck-head'><h2>{html.escape(str(bottleneck.get('stage') or 'Sem gargalo conclusivo'))}</h2><span class='priority-badge priority-{bottleneck_level.lower()}'>{bottleneck_level}</span></div>
          <div class='bottleneck-numbers'><strong>{int(bottleneck.get('drop') or 0)}</strong><span>pessoas nao avancaram<br>{_percent(bottleneck_rate)} de perda nesta etapa</span></div>
          <p class='decision-action'>{html.escape(str(metrics.get('recommendation') or 'Aguardar mais dados.'))}</p>
        </div>
        <div class='panel decision-panel'>
          <div class='aside-label'>Leitura para decisao</div>
          <h2>Onde agir primeiro</h2>
          <ol>
            <li>Corrija a etapa com maior perda absoluta.</li>
            <li>Compare a conversao e o RPS antes e depois da mudanca.</li>
            <li>Evite escalar investimento se o RPS estiver caindo.</li>
          </ol>
        </div>
      </div>
      <div class='panel'>
        <div class='panel-head'><div><h2>Jornada completa</h2><p>Paginas e destinos percorridos pelas sessoes deste Attr ID, na ordem observada. Receita/eCPM por etapa so aparecem quando houver dado real atribuido a etapa.</p></div></div>
        {journey_html}
        <div class='real-data-note'>Sem inventar dados: eCPM/RPS por etapa fica oculto enquanto nao existir atribuicao real por URL/etapa. O eCPM exibido acima e real por Attr ID no GAM.</div>
        <div class='table-wrap route-table'><table><thead><tr><th>De</th><th>Para</th><th>Sessoes</th><th>Conversao</th><th>Nao seguiram esta rota</th></tr></thead><tbody>{route_rows}</tbody></table></div>
      </div>
      <div class='panel'>
        <div class='panel-head'><div><h2>Evolucao diaria e RPS</h2><p>Compare volume, avanço, receita e valor economico de cada sessao ao longo do tempo.</p></div></div>
        <div class='table-wrap'><table class='daily-funnel-table'><thead><tr><th>Dia</th><th>Cliques Ads</th><th>Sessoes</th><th>Avancos</th><th>Conv. sessao</th><th>Receita</th><th>eCPM GAM</th><th>RPS</th></tr></thead><tbody>{daily_rows}</tbody></table></div>
      </div>
      <div class='panel'>
        <div class='panel-head'><div><h2>Destino dos cliques</h2><p>Mostra para onde as pessoas avançaram depois da pagina inicial e qual caminho concentra mais interesse.</p></div></div>
        <div class='table-wrap'><table><thead><tr><th>URL seguinte</th><th>Sessoes</th><th>% das sessoes</th></tr></thead><tbody>{destinations}</tbody></table></div>
      </div>
    </div>
    <div id='metric-tooltip' role='tooltip' aria-hidden='true'></div>
    <style>

      .path-explorer-v2{{margin:18px 0 22px;padding:18px;border:1px solid #d5e5d9;border-radius:22px;background:#fff;box-shadow:0 18px 45px rgba(17,52,41,.08);overflow:hidden}}
      .path-explorer-head{{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:16px}}
      .path-explorer-head h2{{margin:4px 0;font-size:28px;line-height:1;color:#071c18}}
      .path-explorer-head p{{margin:0;color:#557268;font-weight:600}}
      .path-board{{display:grid;grid-template-columns:repeat(5,minmax(170px,1fr));gap:18px;overflow-x:auto;padding:4px 2px 18px}}
      .path-column{{min-width:170px;position:relative}}
      .path-column:not(:last-child)::after{{content:'?';position:absolute;right:-16px;top:48px;color:#24443a;font-size:22px;font-weight:900}}
      .path-column header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}}
      .path-column header small{{text-transform:uppercase;letter-spacing:.08em;font-size:10px;font-weight:900;color:#18352c}}
      .path-column header button{{border:0;background:transparent;color:#18352c;font-weight:900;cursor:pointer}}
      .path-dimension{{display:flex;align-items:center;gap:7px;height:36px;padding:0 10px;border:1px solid #d4dee0;border-radius:4px;background:#fafafa;color:#394c4a;font-size:12px;margin-bottom:10px;box-shadow:0 1px 0 rgba(0,0,0,.03)}}
      .path-dimension span{{display:grid;place-items:center;width:18px;height:18px;background:#eef3f4;border-radius:2px;color:#50605f}}
      .path-dimension b{{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
      .path-items{{display:grid;gap:8px}}
      .path-row{{display:grid;grid-template-columns:10px 1fr;gap:8px;align-items:start;min-height:32px}}
      .path-row i{{display:block;width:10px;min-height:18px;background:#88b8ff;border-radius:1px}}
      .path-node{{display:grid;gap:2px;text-decoration:none;color:#122821;font-size:12px;line-height:1.15;padding:3px 0}}
      .path-node span{{overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;color:#28463d}}
      .path-node strong{{font-size:13px;color:#000;font-weight:900}}
      .path-node:hover span{{color:#005bd3;text-decoration:underline}}
      .path-more span{{color:#005bd3;font-weight:800}}
      .path-start{{background:#f2f5ff;padding:6px 8px;border-left:4px solid #80adf5;min-height:170px;align-content:start}}
      @media(max-width:900px){{.path-explorer-head{{display:block}}.path-board{{grid-template-columns:repeat(5,190px)}}}}

      .funnel-cockpit-v2{{margin:18px 0 22px;padding:22px;border-radius:28px;background:linear-gradient(135deg,#071c18 0%,#0d3b2d 46%,#f7edcf 100%);box-shadow:0 26px 60px rgba(7,28,24,.18);color:#f7fff8;overflow:hidden;position:relative}}
      .funnel-cockpit-v2:before{{content:"";position:absolute;inset:-40% -10% auto auto;width:380px;height:380px;border-radius:999px;background:radial-gradient(circle,rgba(255,208,82,.42),rgba(255,208,82,0) 66%);pointer-events:none}}
      .cockpit-head{{display:flex;gap:18px;justify-content:space-between;align-items:flex-start;position:relative;z-index:1}}
      .cockpit-kicker{{display:inline-flex;letter-spacing:.14em;text-transform:uppercase;font-size:11px;font-weight:900;color:#99f6c9}}
      .cockpit-head h2{{max-width:720px;margin:8px 0 8px;font-size:clamp(24px,3.1vw,42px);line-height:.98;color:#fff}}
      .cockpit-head p{{max-width:720px;margin:0;color:rgba(255,255,255,.76);font-size:14px}}
      .cockpit-period{{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px;min-width:250px}}
      .cockpit-flow{{display:grid;grid-template-columns:minmax(180px,1fr) 110px minmax(180px,1fr) 110px minmax(180px,1fr);gap:14px;align-items:stretch;margin-top:22px;position:relative;z-index:1}}
      .cockpit-node{{background:rgba(255,255,255,.94);border:1px solid rgba(255,255,255,.64);border-radius:24px;padding:18px;color:#061c16;box-shadow:0 16px 40px rgba(0,0,0,.12)}}
      .cockpit-node small,.cockpit-decision-strip small{{display:block;text-transform:uppercase;letter-spacing:.12em;font-size:10px;font-weight:900;color:#0f7a52}}
      .cockpit-node strong{{display:block;margin:8px 0 2px;font-size:48px;line-height:1;color:#02130f}}
      .cockpit-node span{{display:block;font-weight:900;font-size:14px}}
      .cockpit-node em{{display:block;margin-top:10px;font-style:normal;color:#416158;font-size:12px}}
      .cockpit-arrow{{align-self:center;min-height:86px;border-radius:22px;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;background:rgba(255,255,255,.16);border:1px solid rgba(255,255,255,.2);color:#fff}}
      .cockpit-arrow b{{font-size:22px;line-height:1}}.cockpit-arrow span{{font-size:12px;max-width:86px}}.cockpit-arrow.is-loss b{{color:#ffb4a8}}.cockpit-arrow.is-info b{{color:#99f6c9}}
      .cockpit-decision-strip{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-top:14px;position:relative;z-index:1}}
      .cockpit-decision-strip>div{{background:rgba(255,255,255,.9);color:#071c18;border-radius:18px;padding:13px 14px;min-height:74px}}
      .cockpit-decision-strip b{{display:block;margin-top:6px;font-size:18px;line-height:1.15;word-break:break-word}}
      @media(max-width:900px){{.cockpit-head{{display:block}}.cockpit-period{{justify-content:flex-start;margin-top:14px}}.cockpit-flow{{grid-template-columns:1fr}}.cockpit-arrow{{min-height:54px}}.cockpit-decision-strip{{grid-template-columns:1fr 1fr}}.funnel-cockpit-v2{{padding:16px;border-radius:22px}}.cockpit-node strong{{font-size:40px}}}}
      .funnel-warning{{margin:14px 0;padding:12px 14px;border:1px solid #e2a64b;border-radius:14px;background:#fff6dc;color:#69420b}}
      .funnel-grid{{display:grid;grid-template-columns:1fr auto 1fr auto 1fr;gap:14px;align-items:center;margin:22px 0}}
      .funnel-stage{{min-height:130px;padding:20px;border:1px solid var(--line);border-radius:20px;background:linear-gradient(145deg,#f4fbf6,#fffaf0);display:grid;align-content:center;gap:8px}}
      .funnel-stage span,.funnel-summary span{{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:800}}
      .funnel-stage strong{{font-size:42px;line-height:1}}
      .funnel-arrow{{display:grid;text-align:center;gap:3px;color:#a63d2f;min-width:92px}}
      .funnel-arrow.is-info{{color:#176f57}}
      .funnel-arrow.is-neutral{{color:var(--muted)}}
      .funnel-summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:16px 0}}
      .funnel-summary div{{padding:15px;border:1px solid var(--line);border-radius:15px;display:grid;gap:6px}}
      .funnel-summary strong{{font-size:22px}}
      .metric-help{{position:relative;cursor:help;transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease;outline:none}}
      .metric-help:hover,.metric-help:focus,.metric-help:focus-within{{transform:translateY(-2px);border-color:#2f855f;box-shadow:0 12px 28px rgba(15,72,50,.13);z-index:20}}
      .metric-help::after,.metric-help::before{{display:none!important}}
      #metric-tooltip{{position:fixed;z-index:10000;display:none;max-width:min(340px,calc(100vw - 24px));padding:12px 14px;border-radius:12px;background:#102e24;color:#fff;font-size:13px;font-weight:600;line-height:1.45;text-align:left;box-shadow:0 14px 34px rgba(0,0,0,.28);pointer-events:none}}
      #metric-tooltip.is-visible{{display:block}}
      .help-icon{{display:inline-grid;place-items:center;width:18px;height:18px;margin-left:4px;border:1px solid #8bb8a4;border-radius:50%;color:#176f57;font-size:11px;line-height:1;vertical-align:middle}}
      .funnel-arrow.metric-help::after{{bottom:calc(100% + 16px)}}
      .funnel-warning{{position:relative;padding-left:42px}}
      .funnel-warning::before{{content:'i';position:absolute;left:14px;top:50%;transform:translateY(-50%);display:grid;place-items:center;width:20px;height:20px;border:1px solid currentColor;border-radius:50%;font-weight:900}}
      .decision-grid{{display:grid;grid-template-columns:1.35fr .65fr;gap:16px}}
      .bottleneck-panel{{background:linear-gradient(145deg,#fff8e9,#fff)}}
      .bottleneck-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}}
      .bottleneck-head h2{{overflow-wrap:anywhere}}
      .priority-badge{{padding:6px 10px;border-radius:999px;font-size:11px;font-weight:900;text-transform:uppercase;letter-spacing:.06em}}
      .priority-alta{{background:#ffd9d5;color:#8d2018}}.priority-media{{background:#ffedbd;color:#79520a}}.priority-baixa{{background:#dff4e7;color:#17633f}}
      .bottleneck-numbers{{display:flex;align-items:center;gap:14px;margin:18px 0}}
      .bottleneck-numbers strong{{font-size:48px;line-height:1;color:#9b2f24}}
      .bottleneck-numbers span{{font-weight:700;color:var(--muted)}}
      .decision-action{{margin:0;padding:13px 15px;border-left:4px solid #d8892b;border-radius:8px;background:#fff;font-weight:700}}
      .decision-panel ol{{margin:14px 0 0;padding-left:21px;display:grid;gap:12px}}
      .journey-track{{display:flex;align-items:stretch;gap:10px;overflow-x:auto;padding:8px 4px 18px;scroll-snap-type:x proximity}}
      .journey-card{{flex:0 0 220px;min-height:126px;padding:16px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(150deg,#f2fbf6,#fff);display:grid;align-content:center;gap:7px;scroll-snap-align:start}}
      .journey-card strong{{font-size:14px;line-height:1.35;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}}
      .journey-card b{{font-size:22px}}.journey-card small{{color:var(--muted);font-weight:700}}
      .journey-card .journey-intelligence,.path-intelligence{{font-size:10px;line-height:1.35;color:#315d4a;background:#eef8f2;border:1px solid #d7eadf;border-radius:10px;padding:6px 8px}}
      .journey-card em{{font-style:normal;font-size:11px;line-height:1.35;color:#176f57;font-weight:800}}.journey-card em.muted{{color:#7a8d84;font-weight:700}}
      .journey-index{{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#176f57;font-weight:900}}
      .journey-connector{{align-self:center;color:#176f57;font-size:22px;font-weight:900}}
      .route-table{{margin-top:14px}}
      .route-table td:first-child,.route-table td:nth-child(2){{max-width:330px;overflow-wrap:anywhere}}
      .daily-funnel-table th,.daily-funnel-table td{{white-space:nowrap}}
      .real-data-note{{margin:10px 0 14px;padding:10px 12px;border:1px dashed #9ac5ae;border-radius:12px;background:#f4fbf7;color:#315d4a;font-weight:700;font-size:13px}}
      .rps-cell{{min-width:135px;display:grid;gap:5px}}
      .rps-cell i{{display:block;height:5px;min-width:2px;border-radius:999px;background:linear-gradient(90deg,#1f7a54,#e1ad32)}}
      .is-up{{color:#167047!important;font-weight:800}}.is-down{{color:#ae3025!important;font-weight:800}}
      @media(max-width:900px){{.funnel-grid{{grid-template-columns:1fr}}.funnel-arrow{{padding:4px}}.funnel-summary{{grid-template-columns:1fr 1fr}}}}
      @media(max-width:900px){{.decision-grid{{grid-template-columns:1fr}}}}
      @media(max-width:600px){{.funnel-summary{{grid-template-columns:1fr}}.journey-card{{flex-basis:82vw}}#metric-tooltip{{left:12px!important;right:12px!important;bottom:12px!important;top:auto!important;max-width:none}}}}

      /* Hybrid Analytics UX override */
      .shell:has(.funnel-grid) .masthead{{background:radial-gradient(circle at top left,#dff8ea,#fff 56%),linear-gradient(135deg,#fff,#f6faf7);border:1px solid #d2e6d8;border-radius:28px}}
      .shell:has(.funnel-grid) .hero h1{{font-size:clamp(26px,3.1vw,44px);line-height:1.02;max-width:920px}}
      .shell:has(.funnel-grid) .panel{{border-radius:26px;box-shadow:0 18px 42px rgba(15,72,50,.08)}}
      .funnel-grid{{grid-template-columns:minmax(210px,1fr) 90px minmax(210px,1fr) 90px minmax(210px,1fr)!important;gap:16px!important;padding:10px 0 4px}}
      .funnel-stage{{position:relative;overflow:hidden;min-height:154px!important;border-radius:24px!important;background:linear-gradient(145deg,#fff,#f2fbf6)!important;box-shadow:0 18px 34px rgba(16,36,27,.08)}}
      .funnel-stage::before{{content:'';position:absolute;left:0;top:0;bottom:0;width:10px;background:linear-gradient(180deg,#0e7a4f,#e4b33d)}}
      .funnel-stage::after{{content:'';position:absolute;right:-34px;top:-34px;width:110px;height:110px;border-radius:999px;background:rgba(14,122,79,.07)}}
      .funnel-stage strong{{font-size:clamp(38px,4vw,58px)!important;color:#10241b}}
      .funnel-arrow{{align-self:center;padding:10px;border-radius:18px;background:linear-gradient(90deg,rgba(14,122,79,.08),rgba(14,122,79,.02));min-width:90px!important}}
      .funnel-arrow::before{{content:'?';display:block;font-size:30px;color:#0e7a4f;line-height:1}}
      .funnel-arrow strong{{font-size:20px}}
      .funnel-summary div{{border-radius:18px!important;background:linear-gradient(145deg,#fff,#f8fbf7);box-shadow:0 12px 26px rgba(16,36,27,.055)}}
      .funnel-summary div:nth-last-child(-n+3){{border-color:#b8dfc6;background:linear-gradient(145deg,#f5fff8,#fff)}}
      .bottleneck-panel{{border-color:#f0c37b!important;background:radial-gradient(circle at top right,#fff0cf,#fff 50%)!important}}
      .journey-track{{padding:14px 6px 24px!important;gap:12px!important;background:linear-gradient(180deg,#f8fcfa,#fff);border-radius:22px}}
      .journey-card{{flex-basis:245px!important;border-radius:22px!important;background:#fff!important;box-shadow:0 18px 34px rgba(16,36,27,.08);position:relative;overflow:hidden}}
      .journey-card::before{{content:'';position:absolute;left:0;top:0;height:100%;width:32px;background:linear-gradient(180deg,rgba(14,122,79,.16),rgba(228,179,61,.1));pointer-events:none}}
      .journey-card b{{font-size:30px!important;color:#10241b}}
      .journey-connector{{font-size:28px!important;align-self:center;padding:0 2px}}
      .real-data-note{{font-size:14px!important;border-radius:16px!important;background:#f0faf4!important}}
      .daily-funnel-table tbody tr:hover,.route-table tbody tr:hover{{background:#f0faf4}}
      .rps-cell i{{height:7px!important;background:linear-gradient(90deg,#0e7a4f,#e4b33d)!important}}
      @media(max-width:900px){{.funnel-grid{{grid-template-columns:1fr!important}}.funnel-arrow::before{{content:'?'}}.journey-card{{flex-basis:82vw!important}}}}
      @media(prefers-reduced-motion:reduce){{.metric-help,.metric-help::after{{transition:none}}}}
    </style>
    <script>
      (() => {{
        const tooltip = document.getElementById('metric-tooltip');
        const cards = Array.from(document.querySelectorAll('.metric-help[data-help]'));
        let activeCard = null;
        const place = (card) => {{
          if (!tooltip || !card) return;
          const rect = card.getBoundingClientRect();
          const margin = 10;
          const width = tooltip.offsetWidth;
          const height = tooltip.offsetHeight;
          let left = rect.left + (rect.width - width) / 2;
          left = Math.max(12, Math.min(left, window.innerWidth - width - 12));
          let top = rect.top - height - margin;
          if (top < 12) top = rect.bottom + margin;
          tooltip.style.left = left + 'px';
          tooltip.style.top = top + 'px';
        }};
        const show = (card) => {{
          if (!tooltip || !card) return;
          activeCard = card;
          tooltip.textContent = card.getAttribute('data-help') || '';
          tooltip.classList.add('is-visible');
          tooltip.setAttribute('aria-hidden', 'false');
          requestAnimationFrame(() => place(card));
        }};
        const hide = () => {{
          activeCard = null;
          tooltip?.classList.remove('is-visible');
          tooltip?.setAttribute('aria-hidden', 'true');
        }};
        cards.forEach((card) => {{
          card.addEventListener('mouseenter', () => show(card));
          card.addEventListener('mouseleave', hide);
          card.addEventListener('focus', () => show(card));
          card.addEventListener('blur', hide);
          card.addEventListener('click', () => activeCard === card ? hide() : show(card));
        }});
        window.addEventListener('scroll', () => activeCard && place(activeCard), true);
        window.addEventListener('resize', () => activeCard && place(activeCard));
      }})();
    </script>
    """
    return _layout("Funil por Attr ID", content, "dashboard")


@app.get("/projects/{project}", response_class=HTMLResponse)
def project_page(project: str) -> str:
    runtime = load_runtime(CONFIG_PATH)
    current_day = app_today()
    rows = load_project_campaign_rows(runtime, project, current_day)
    kpis = load_project_kpis(runtime, current_day)
    summary = load_project_summary(runtime, project, current_day, project_rows=rows, project_kpis=kpis)
    budget_actions = _budget_increase_actions_today(runtime)
    budget_manual_actions = _budget_manual_actions_latest(runtime)
    if not summary:
        raise HTTPException(status_code=404, detail="Projeto nao encontrado")

    def period_cell(
        spend: Any,
        revenue: Any,
        roi: Any,
        pmr: Any,
        ecpm: Any,
        sessions: Any,
        rps: Any,
        extra_class: str = "",
        adsense: Any = None,
        push: Any = None,
        mgid: Any = None,
    ) -> str:
        adsense_value = _to_float(revenue if adsense is None else adsense)
        push_value = _to_float(push)
        mgid_value = _to_float(mgid)
        total_value = _to_float(revenue)
        net_value = total_value - _to_float(spend)
        push_line = f"<div class='period-line'><span>Push</span><strong>{_money(push_value)}</strong></div>" if push_value > 0 else ""
        mgid_line = f"<div class='period-line'><span>MGID</span><strong>{_money(mgid_value)}</strong></div>" if mgid_value > 0 else ""
        metric_period_attr = " data-period='today'" if "today" in str(extra_class or "") else ""
        return (
            f"<td class='period-cell {extra_class}'>"
            "<div class='period-stack'>"
            f"<div class='period-line'><span>Inv.</span><strong class='metric-value' data-metric='investment'{metric_period_attr}>{_money(_to_float(spend))}</strong></div>"
            f"<div class='period-line'><span>Adsense</span><strong>{_money(adsense_value)}</strong></div>"
            f"{push_line}"
            f"{mgid_line}"
            f"<div class='period-line'><span>Total</span><strong class='metric-value' data-metric='revenue'{metric_period_attr}>{_money(total_value)}</strong></div>"
            f"<div class='period-line profit-line'><span>Lucro</span><strong class='{_roi_class(net_value)}'>{_money(net_value)}</strong></div>"
            f"<div class='period-line'><span>ROI</span><strong class='{_roi_class(_to_float(roi))} metric-value' data-metric='roi'{metric_period_attr}>{_percent(_to_float(roi))}</strong></div>"
            f"<div class='period-line pmr-line'><span>PMR GAM</span><strong class='metric-value' data-metric='pmr'{metric_period_attr}>{_percent(_to_float(pmr))}</strong></div>"
            f"<div class='period-line ecpm-line'><span>eCPM</span><strong class='metric-value' data-metric='ecpm'{metric_period_attr}>{_money(_to_float(ecpm))}</strong></div>"
            f"<div class='period-line'><span>Sessoes</span><strong>{int(_to_float(sessions))}</strong></div>"
            f"<div class='period-line'><span>RPS</span><strong>{_money(_to_float(rps)) if _to_float(sessions) > 0 else '-'}</strong></div>"
            "</div>"
            "</td>"
        )

    def efficiency_cell(row: dict[str, Any]) -> str:
        data = row.get("efficiency") or {}
        state = html.escape(str(data.get("state") or "neutral"), quote=True)
        goal = html.escape(str(data.get("goal") or "Meta nao detectada"))
        badge = html.escape(str(data.get("badge") or "Sem dados"))
        diagnosis = html.escape(str(data.get("diagnosis") or ""))
        target_cpa = _to_float(data.get("target_cpa"))
        avg_cpc = _to_float(data.get("avg_cpc"))
        cost_per_conversion = _to_float(data.get("cost_per_conversion"))
        strategy = html.escape(str(data.get("bidding_strategy_type") or ""))
        change = data.get("target_cpa_change") or {}
        change_old = _to_float(change.get("old_value")) if isinstance(change, dict) else 0.0
        change_new = _to_float(change.get("new_value")) if isinstance(change, dict) else 0.0
        change_html = ""
        if change_old > 0 and change_new > 0:
            old_source = html.escape(str(change.get("old_source") or "anterior").title())
            new_source = html.escape(str(change.get("new_source") or "atual").title())
            changed_at = html.escape(_br_datetime(change.get("changed_at")))
            change_html = (
                "<div class='efficiency-change'>"
                "<div class='efficiency-change-title'>Historico CPA</div>"
                "<div class='efficiency-change-grid'>"
                f"<div class='efficiency-change-item'><span>{old_source}</span><strong>{_money(change_old)}</strong></div>"
                "<div class='efficiency-change-arrow'>&rarr;</div>"
                f"<div class='efficiency-change-item'><span>{new_source}</span><strong>{_money(change_new)}</strong></div>"
                "</div>"
                f"<div class='efficiency-change-time'>Horario de Brasilia: <strong>{changed_at}</strong></div>"
                "</div>"
            )
        title = html.escape(f"{diagnosis} | Estrategia: {strategy} | Snapshot: {data.get('settings_day') or ''}", quote=True)
        action_box = (
            "<div class='efficiency-action-box'>"
            "<div class='efficiency-action-title'>Ajustar CPA</div>"
            "<div class='efficiency-action-row'>"
            f"<input class='efficiency-cpa-input' type='number' step='0.01' min='0' placeholder='Novo CPA' aria-label='Novo CPA desejado para {html.escape(str(row.get('attr_id') or ''), quote=True)}'>"
            f"<button class='efficiency-action-button' type='button' "
            f"data-campaign-id='{html.escape(str(row.get('campaign_id') or ''), quote=True)}' "
            f"data-campaign='{html.escape(str(row.get('campaign') or ''), quote=True)}' "
            f"data-goal='{html.escape(str(data.get('goal') or ''), quote=True)}' "
            f"data-old-value='{target_cpa}' data-avg-cpc='{avg_cpc}' data-cost-per-conversion='{cost_per_conversion}' "
            f"data-roi-today='{_to_float(data.get('roi_today'))}' data-roi-7d='{_to_float(data.get('roi_7d'))}' "
            f"data-pmr-today='{_to_float(data.get('pmr_today'))}' data-pmr-7d='{_to_float(data.get('pmr_7d'))}'>Registrar</button>"
            "</div>"
            "<textarea class='efficiency-reason-input' placeholder='Motivo do ajuste'></textarea>"
            "<div class='efficiency-action-status'>Pendente para aplicar no Google Ads.</div>"
            "</div>"
        )
        return (
            f"<td class='efficiency-cell' title='{title}'>"
            "<div class='efficiency-stack'>"
            "<div class='efficiency-head'>"
            f"<div class='efficiency-goal'>{goal}</div>"
            f"<span class='efficiency-badge {state}'>{badge}</span>"
            "</div>"
            "<div class='efficiency-metrics'>"
            f"<div class='efficiency-line'><span>CPA alvo</span><strong>{_money(target_cpa) if target_cpa > 0 else '-'}</strong></div>"
            f"<div class='efficiency-line'><span>Custo/conv.</span><strong class='metric-value' data-metric='cost_per_conversion' data-days='7'>{_money(cost_per_conversion) if cost_per_conversion > 0 else '-'}</strong></div>"
            "</div>"
            f"{change_html}"
            f"<div class='efficiency-diagnosis'>{diagnosis}</div>"
            f"{action_box}"
            "</div></td>"
        )

    def budget_cell(row: dict[str, Any]) -> str:
        budget = _to_float(row.get("daily_budget"))
        spend = _to_float(row.get("investimento_hoje"))
        roi = _to_float(row.get("roi_hoje"))
        ratio = (spend / budget) if budget > 0 else 0.0
        alert = ""
        campaign_id = str(row.get("campaign_id") or "")
        attr_id = str(row.get("attr_id") or "")
        action = budget_actions.get(campaign_id)
        manual_action = budget_manual_actions.get(campaign_id) or budget_manual_actions.get(attr_id)
        channel_type = str(row.get("channel_type") or "").upper()
        lock = row.get("optimization_lock") or {}
        display_locked = channel_type == "DISPLAY" and lock and not lock.get("can_optimize")
        if action:
            old_budget = _to_float(action.get('old_budget'))
            new_budget = _to_float(action.get('new_budget'))
            if new_budget > 0:
                budget = new_budget
            alert = (
                "<div class='budget-alert done'>Aumentada hoje "
                f"{_money(old_budget)} -> {_money(new_budget)}</div>"
            )
        elif not display_locked and budget > 0 and roi > 0 and ratio >= 0.8:
            blink = " blink" if ratio >= 1 else ""
            label = "Aumentar orcamento" if ratio >= 1 else "Perto do limite"
            alert = f"<div class='budget-alert{blink}'>{label}</div>"
        manual_history = ""
        if manual_action:
            old_budget = _to_float(manual_action.get('old_budget'))
            new_budget = _to_float(manual_action.get('new_budget'))
            raw_status = str(manual_action.get('status') or '')
            if raw_status == 'applied' and new_budget > 0:
                budget = max(budget, new_budget)
            created_at = _br_datetime(manual_action.get('applied_at') or manual_action.get('created_at'))
            status = html.escape(raw_status)
            manual_history = f"<div class='budget-manual-status'>{status}: {_money(old_budget)} -> {_money(new_budget)} em {html.escape(created_at)}</div>"
        manual_status_html = manual_history or "<div class='budget-manual-status'>Aplica no Google Ads na proxima atualizacao.</div>"
        manual_box = (
            "<div class='budget-manual-box'>"
            "<div class='budget-control-title'>Ajustar orcamento</div>"
            "<div class='budget-manual-row'>"
            f"<input class='budget-manual-input' type='number' step='0.01' min='0' placeholder='Novo' aria-label='Novo orcamento para {html.escape(attr_id, quote=True)}'>"
            f"<button class='budget-manual-button' type='button' data-campaign-id='{html.escape(campaign_id, quote=True)}' data-customer-id='{html.escape(str(row.get('customer_id') or ''), quote=True)}' data-campaign='{html.escape(str(row.get('campaign') or ''), quote=True)}' data-old-budget='{budget}' data-roi-today='{_to_float(row.get('roi_hoje'))}' data-roi-7d='{_to_float(row.get('roi_7d'))}' data-pmr-today='{_to_float(row.get('pmr_hoje'))}' data-pmr-7d='{_to_float(row.get('pmr_7d'))}'>Registrar</button>"
            "</div>"
            "<input class='budget-manual-reason' type='text' placeholder='Motivo opcional'>"
            "<div class='budget-control-title'>Historico</div>"
            f"{manual_status_html}"
            "</div>"
        )
        return (
            "<td class='budget-cell'>"
            "<div class='budget-stack'>"
            f"<div><span>Orcamento</span><strong>{_money(budget)}</strong></div>"
            f"{alert}"
            f"{manual_box}"
            "</div>"
            "</td>"
        )

    def optimization_countdown_badge(row: dict[str, Any]) -> str:
        lock = row.get("optimization_lock") or {}
        if not lock:
            return ""
        label = str(lock.get("label") or "").strip()
        if not label:
            return ""
        css = "ready" if lock.get("can_optimize") else "locked"
        if lock.get("days_remaining") is None:
            css = "unknown"
        title = f"Inicio: {lock.get('start_date') or 'indisponivel'} | Liberacao: {lock.get('unlock_date') or 'indisponivel'} | Fonte: {lock.get('source') or ''}"
        return f"<span class='optimization-countdown {css}' title='{html.escape(title, quote=True)}'>{html.escape(label)}</span>"

    def summary_row(
        label: str,
        spend: Any,
        revenue: Any,
        roi: Any,
        sessions: Any,
        rps: Any,
        row_class: str,
    ) -> str:
        spend_value = _to_float(spend)
        revenue_value = _to_float(revenue)
        net_value = revenue_value - spend_value
        roi_value = _to_float(roi)
        return (
            f"<tr class='summary-period-row {row_class}'>"
            f"<th scope='row'>{html.escape(label)}</th>"
            "<td><span class='summary-label'>Investimento</span>"
            f"<span class='summary-value'>{_money(spend_value)}</span></td>"
            "<td><span class='summary-label'>Receita</span>"
            f"<span class='summary-value'>{_money(revenue_value)}</span></td>"
            "<td><span class='summary-label'>Resultado liquido</span>"
            f"<span class='summary-value'>{_money(net_value)}</span></td>"
            "<td><span class='summary-label'>ROI</span>"
            f"<span class='summary-value {_roi_class(roi_value)}'>{_percent(roi_value)}</span></td>"
            "<td><span class='summary-label'>Sessoes</span>"
            f"<span class='summary-value'>{int(_to_float(sessions))}</span></td>"
            "<td><span class='summary-label'>RPS</span>"
            f"<span class='summary-value'>{_money(_to_float(rps)) if _to_float(sessions) > 0 else '-'}</span></td>"
            "</tr>"
        )

    snapshot_alert = ""
    if summary.get("google_ads_snapshot_status") == "stale":
        if summary.get("google_ads_snapshot_source") == "apps_script_missing" or not summary.get("google_ads_snapshot_at"):
            snapshot_text = (
                "ADS DESATUALIZADO: nenhum snapshot de sucesso do Google Ads foi recebido hoje para esta conta. "
                "O painel pode estar mostrando apenas dado parcial da API. Nao usar investimento/ROI ate o Apps Script rodar."
            )
        else:
            snapshot_text = (
                f"ADS DESATUALIZADO: ultimo snapshot do Google Ads ha {int(_to_float(summary.get('google_ads_snapshot_age_minutes')))} min. "
                "Nao usar investimento/ROI para decisao ate o Apps Script rodar novamente."
            )
        snapshot_alert = (
            "<div style='margin:12px 0;padding:12px 14px;border-radius:14px;background:#fff1e8;border:1px solid #e08a5b;color:#7d2e13;font-weight:900'>"
            f"{snapshot_text}</div>"
        )
    if summary.get("gam_snapshot_status") == "stale":
        snapshot_alert += (
            "<div style='margin:12px 0;padding:12px 14px;border-radius:14px;background:#fff1e8;border:1px solid #e08a5b;color:#7d2e13;font-weight:900'>"
            f"GAM DESATUALIZADO: {html.escape(str(summary.get('gam_snapshot_message') or 'snapshot GAM indisponivel'))}</div>"
        )

    line_rows = []
    for row in rows:
        row_json = html.escape(json.dumps(row, ensure_ascii=False), quote=True)
        detail_buttons = "<button class='campaign-inline-action active' type='button' data-detail-type='blocks'>Blocos de Anuncios</button>"
        if str(row.get("channel_type", "")).upper() == "DISPLAY":
            detail_buttons += "<button class='campaign-inline-action' type='button' data-detail-type='placements'>Placements</button>"
            detail_buttons += optimization_countdown_badge(row)
        campaign_links = ""
        google_ads_url = str(row.get("google_ads_url") or "").strip()
        landing_url = str(row.get("landing_url") or "").strip()
        if google_ads_url:
            campaign_links += f"<a class='campaign-inline-action' href='{html.escape(google_ads_url, quote=True)}' target='_blank' rel='noopener noreferrer'>Google Ads</a>"
        if landing_url:
            campaign_links += f"<a class='campaign-inline-action' href='{html.escape(landing_url, quote=True)}' target='_blank' rel='noopener noreferrer'>Artigo</a>"
        ad_group_name = str(row.get("ad_group") or row.get("ad_group_name") or "").strip()
        ad_group_html = f"<div class='campaign-ad-group-name'>Grupo: {html.escape(ad_group_name)}</div>" if ad_group_name else ""
        campaign_links += (
            f"<button class='campaign-status-action pause' type='button' data-new-status='PAUSED' data-campaign-id='{html.escape(str(row.get('campaign_id') or row.get('attr_id') or ''), quote=True)}' data-customer-id='{html.escape(str(row.get('customer_id') or ''), quote=True)}' data-campaign='{html.escape(str(row.get('campaign') or ''), quote=True)}' data-old-status='{html.escape(str(row.get('campaign_status') or ''), quote=True)}'>Pausar</button>"
            f"<button class='campaign-status-action enable' type='button' data-new-status='ENABLED' data-campaign-id='{html.escape(str(row.get('campaign_id') or row.get('attr_id') or ''), quote=True)}' data-customer-id='{html.escape(str(row.get('customer_id') or ''), quote=True)}' data-campaign='{html.escape(str(row.get('campaign') or ''), quote=True)}' data-old-status='{html.escape(str(row.get('campaign_status') or ''), quote=True)}'>Ativar</button>"
        )
        line_rows.append(
            f"<tr class='campaign-row' data-project='{html.escape(project, quote=True)}' data-attr-id='{html.escape(str(row['attr_id']), quote=True)}' data-row-json='{row_json}'>"
            f"<td class='sticky-campaign'><div class='campaign-main-cell'><span class='campaign-caret' aria-hidden='true'>&rsaquo;</span><div class='campaign-name-stack'><a class='campaign-title-link' href='/projects/{quote(project, safe='')}/funnels/{quote(str(row['attr_id']), safe='')}'>{html.escape(str(row['campaign']))}</a>{ad_group_html}<div class='campaign-meta-row'><span class='attr-chip'>Attr ID: <strong class='mono'>{html.escape(str(row['attr_id']))}</strong><button class='attr-copy-button' type='button' data-copy-attr='{html.escape(str(row['attr_id']), quote=True)}'>Copiar</button></span></div><div class='campaign-inline-actions'>{campaign_links}{detail_buttons}</div></div></div></td>"
            + budget_cell(row)
            + efficiency_cell(row)
            + period_cell(row["investimento_hoje"], row["receita_hoje"], row["roi_hoje"], row.get("pmr_hoje"), row.get("ecpm_hoje"), row.get("sessoes_hoje"), row.get("rps_hoje"), "period-cell-today", row.get("receita_adsense_hoje"), row.get("receita_push_hoje"), row.get("receita_mgid_hoje"))
            + period_cell(row["investimento_ontem"], row["receita_ontem"], row["roi_ontem"], row.get("pmr_ontem"), row.get("ecpm_ontem"), row.get("sessoes_ontem"), row.get("rps_ontem"), "period-cell-yesterday", row.get("receita_adsense_ontem"), row.get("receita_push_ontem"), row.get("receita_mgid_ontem"))
            + period_cell(row["investimento_7d"], row["receita_7d"], row["roi_7d"], row.get("pmr_7d"), row.get("ecpm_7d"), row.get("sessoes_7d"), row.get("rps_7d"), "period-cell-7d", row.get("receita_adsense_7d"), row.get("receita_push_7d"), row.get("receita_mgid_7d"))
            + period_cell(row["investimento_mes"], row["receita_mes"], row["roi_mes"], row.get("pmr_mes"), row.get("ecpm_mes"), row.get("sessoes_mes"), row.get("rps_mes"), "period-cell-month", row.get("receita_adsense_mes"), row.get("receita_push_mes"), row.get("receita_mgid_mes"))
            + "</tr>"
        )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Projeto</div>
          <h1>{html.escape(project)}</h1>
          <p>Painel dedicado para substituir a aba especifica do projeto, com campanhas ordenadas por investimento de hoje.</p>
          <div class='project-hero-meta'>
            <div class='project-hero-pill'><span>Ultima atualizacao</span><strong>{html.escape(summary['updated_at'])}</strong></div>
            <div class='project-hero-pill'><span>Campanhas monitoradas</span><strong>{summary['campaigns']}</strong></div>
          </div>
          <div class='hero-actions'>
            <a class='action-link secondary' href='/'>VOLTAR PARA HOME</a>
            <form method='post' action='/run-fast'>
              <button class='action-button' type='submit'>ATUALIZAR HOJE</button>
            </form>
            <form method='post' action='/run-fast'>
              <button class='action-button secondary' type='submit'>ATUALIZAR DADOS HOJE</button>
            </form>
          </div>
          [[NAV]]
        </div>
      </div>

      <div class='panel project-summary-panel'>
        <h2>Resumo do projeto</h2>
        <p>Investimento, receita, resultado liquido e ROI por periodo.</p>
        {snapshot_alert}
        <div class='table-scroll'>
          <table class='project-summary-table'>
            <thead>
              <tr>
                <th>Periodo</th>
                <th>Investimento</th>
                <th>Receita</th>
                <th>Resultado liquido</th>
                <th>ROI</th>
                <th>Sessoes</th>
                <th>RPS</th>
              </tr>
            </thead>
            <tbody>
              {summary_row('Hoje', summary['investimento_hoje'], summary['receita_hoje'], summary['roi_hoje'], summary['sessoes_hoje'], summary['rps_hoje'], 'summary-row-today')}
              {summary_row('Ontem', summary['investimento_ontem'], summary['receita_ontem'], summary['roi_ontem'], summary['sessoes_ontem'], summary['rps_ontem'], 'summary-row-yesterday')}
              {summary_row('Ultimos 7 Dias', summary['investimento_7d'], summary['receita_7d'], summary['roi_7d'], summary['sessoes_7d'], summary['rps_7d'], 'summary-row-7d')}
              {summary_row('Mes', summary['investimento_mes'], summary['receita_mes'], summary['roi_mes'], summary['sessoes_mes'], summary['rps_mes'], 'summary-row-month')}
            </tbody>
          </table>
        </div>
      </div>

      <div class='metrics project-kpi-strip'>
        <div class='metric push-card'>
          <div class='label'>Receita Push Hoje</div>
          <div class='value'>{_money(summary.get('receita_push_hoje', 0))}</div>
          <div class='sub'>Somente UTMs Push confirmadas no GAM</div>
        </div>
        <div class='metric push-card'>
          <div class='label'>Receita Push Mes</div>
          <div class='value'>{_money(summary.get('receita_push_mes', 0))}</div>
          <div class='sub'>{html.escape(summary.get('campanhas_push_exemplo', '') or 'sem UTM Push confirmada')}</div>
        </div>
        <div class='metric push-card'>
          <div class='label'>Receita MGID Hoje</div>
          <div class='value'>{_money(summary.get('receita_mgid_hoje', 0))}</div>
          <div class='sub'>Fonte independente por instancia</div>
        </div>
        <div class='metric push-card'>
          <div class='label'>Receita MGID Mes</div>
          <div class='value'>{_money(summary.get('receita_mgid_mes', 0))}</div>
          <div class='sub'>Somada ao total operacional</div>
        </div>
      </div>

        <div class='panel'>
          <h2>Campanhas</h2>
        <p>Leitura operacional por campanha. Cada periodo mostra investimento, receita, lucro, ROI e PMR GAM.</p>
        <div class='table-scroll'>
        <table class='campaign-table'>
          <thead>
            <tr>
              <th class='sticky-campaign'>Campanha</th>
              <th>Controle</th>
              <th>Eficiencia</th>
              <th class='period-title period-title-today'>Hoje</th>
              <th class='period-title period-title-yesterday'>Ontem</th>
              <th class='period-title period-title-7d'>Ultimos 7 Dias</th>
              <th class='period-title period-title-month'>Mes</th>
            </tr>
          </thead>
          <tbody>{''.join(line_rows)}</tbody>
        </table>
        </div>
      </div>
      <script>
        (() => {{
          const periodMeta = {{
            today: {{ label: 'Hoje', key: 'receita_hoje' }},
            yesterday: {{ label: 'Ontem', key: 'receita_ontem' }},
            '7d': {{ label: 'Ultimos 7 Dias', key: 'receita_7d' }},
            month: {{ label: 'Mes', key: 'receita_mes' }},
          }};
          const rowCache = new Map();
          let openRow = null;
          let openDetailRow = null;
          let activePeriod = 'today';
          let activeDetailType = 'blocks';
          let activeCustomStart = '';
          let activeCustomEnd = '';

          const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (ch) => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
          const money = (value) => `R${{(Number(value) || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}`;
          const usdMoney = (value) => `US$ ${{(Number(value) || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}`;
          const blockEcpm = (block) => block.ecpm_currency === 'USD' ? usdMoney(block.ecpm) : money(block.ecpm);
          const placementEcpm = (item) => item.ecpm_currency === 'USD' ? usdMoney(item.ecpm) : money(item.ecpm);
          const pct = (value) => `${{(Number(value) || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}%`;
          const num = (value) => Number(value || 0).toLocaleString('pt-BR');
          const roiClass = (value) => Number(value || 0) > 0 ? 'roi-positive' : (Number(value || 0) < 0 ? 'roi-negative' : 'roi-neutral');
\n          const metricHistoryCache = new Map();\n          const metricTooltip = document.createElement('div');\n          metricTooltip.className = 'metric-chart-popover';\n          metricTooltip.style.display = 'none';\n          metricTooltip.addEventListener('mouseenter', () => {{ metricTooltip.dataset.inside = '1'; }});\n          metricTooltip.addEventListener('mouseleave', () => {{ metricTooltip.dataset.inside = ''; hideMetricTooltip(); }});\n          document.body.appendChild(metricTooltip);\n\n          const formatMetricValue = (value, unit) => unit === 'percent' ? pct(value) : money(value);\n          function metricSvg(points) {{\n            const values = points.map((point) => Number(point.value) || 0);\n            const min = Math.min(...values, 0);\n            const max = Math.max(...values, 0);\n            const span = max - min || 1;\n            const chartWidth = 360;\n            const baseY = 118;\n            const barWidth = Math.max(4.8, Math.min(10, (chartWidth - 24) / Math.max(values.length, 1) * 0.68));\n            const gap = Math.max(1.4, ((chartWidth - 24) - (barWidth * Math.max(values.length, 1))) / Math.max(values.length - 1, 1));\n            const positive = values[values.length - 1] >= values[0];\n            const bars = values.map((value, index) => {{\n              const x = 12 + index * (barWidth + gap);\n              const height = Math.max(3, ((value - min) * 96 / span));\n              const y = baseY - height;\n              const fill = value >= 0 ? (positive ? '#137346' : '#64748b') : '#b42318';\n              return `<rect class="metric-chart-bar" data-index="${{index}}" x="${{x.toFixed(1)}}" y="${{y.toFixed(1)}}" width="${{barWidth}}" height="${{height.toFixed(1)}}" rx="2" fill="${{fill}}" opacity="${{index === values.length - 1 ? '1' : '0.72'}}" />`;\n            }}).join('');\n            return `<svg class="metric-chart-svg" width="360" height="130" viewBox="0 0 360 130" aria-label="Grafico de barras do periodo selecionado">\n              <line x1="12" y1="118" x2="348" y2="118" stroke="#d8e8dc" stroke-width="1" />\n              ${{bars}}\n            </svg>`;\n          }}\n          function positionMetricTooltip(target) {{\n            const rect = target.getBoundingClientRect();\n            const left = Math.min(window.innerWidth - 460, Math.max(12, rect.left + 12));\n            const top = Math.min(window.innerHeight - 320, Math.max(12, rect.bottom + 8));\n            metricTooltip.style.left = `${{left}}px`;\n            metricTooltip.style.top = `${{top}}px`;\n          }}\n          async function showMetricTooltip(target) {{\n            const row = target.closest('.campaign-row');\n            if (!row) return;\n            const metric = target.dataset.metric;\n            const scope = target.dataset.period === 'today' ? 'today' : 'days';\n            const historyDays = Number(target.dataset.days || 30) || 30;\n            const cacheKey = `${{row.dataset.project}}|${{row.dataset.attrId}}|${{metric}}|${{scope}}|${{historyDays}}`;\n            positionMetricTooltip(target);\n            metricTooltip.style.display = 'block';\n            metricTooltip.innerHTML = '<div class="metric-chart-title">Carregando historico...</div>';\n            if (!metricHistoryCache.has(cacheKey)) {{\n              const response = await fetch(`/api/projects/${{encodeURIComponent(row.dataset.project)}}/campaigns/${{encodeURIComponent(row.dataset.attrId)}}/metric-history?metric=${{encodeURIComponent(metric)}}&days=${{encodeURIComponent(historyDays)}}&scope=${{encodeURIComponent(scope)}}`, {{headers: {{'Accept': 'application/json'}}}});\n              if (!response.ok) throw new Error(`HTTP ${{response.status}}`);\n              metricHistoryCache.set(cacheKey, await response.json());\n            }}\n            const data = metricHistoryCache.get(cacheKey);\n            const summary = data.summary || {{}};\n            const historyPeriodLabel = data.period_label || 'ultimos 30 dias';\n            metricTooltip.innerHTML = `\n              <div class="metric-chart-title">${{esc(data.label || metric)}} - ${{esc(historyPeriodLabel)}}</div>\n              <div class="metric-chart-subtitle">${{esc(row.dataset.attrId || '')}}</div>\n              ${{metricSvg(data.points || [])}}\n              <div class="metric-chart-day-value">Passe o mouse nas barras para ver hora e valor.</div>\n              <div class="metric-chart-stats">\n                <span>Atual <strong>${{formatMetricValue(summary.current, data.unit)}}</strong></span>\n                <span>Media <strong>${{formatMetricValue(summary.average, data.unit)}}</strong></span>\n                <span>Min <strong>${{formatMetricValue(summary.minimum, data.unit)}}</strong></span>\n                <span>Max <strong>${{formatMetricValue(summary.maximum, data.unit)}}</strong></span>\n              </div>`;\n            positionMetricTooltip(target);\n            metricTooltip.querySelectorAll('.metric-chart-bar').forEach((bar) => {{\n              bar.addEventListener('mousemove', () => {{\n                const point = (data.points || [])[Number(bar.dataset.index) || 0] || {{}};\n                const dayBox = metricTooltip.querySelector('.metric-chart-day-value');\n                if (dayBox) {{\n                  dayBox.innerHTML = `${{esc(point.day || '')}}: <strong>${{formatMetricValue(point.value, data.unit)}}</strong>`;\n                }}\n              }});\n            }});\n          }}\n          function hideMetricTooltip() {{\n            metricTooltip.style.display = 'none';\n          }}\n          document.querySelectorAll('.metric-value').forEach((target) => {{\n            target.addEventListener('mouseenter', () => showMetricTooltip(target).catch(() => {{ metricTooltip.innerHTML = '<div class="metric-chart-title">Falha ao carregar historico</div>'; }}));\n            target.addEventListener('mouseleave', () => setTimeout(() => {{ if (!metricTooltip.dataset.inside) hideMetricTooltip(); }}, 120));\n          }});\n


          document.querySelectorAll('.efficiency-action-button').forEach((button) => {{
            button.addEventListener('click', async () => {{
              const box = button.closest('.efficiency-action-box');
              const row = button.closest('.campaign-row');
              if (!box || !row) return;
              const input = box.querySelector('.efficiency-cpa-input');
              const reasonInput = box.querySelector('.efficiency-reason-input');
              const status = box.querySelector('.efficiency-action-status');
              const newValue = Number(String(input?.value || '').replace(',', '.')) || 0;
              const reason = String(reasonInput?.value || '').trim();
              if (newValue <= 0) {{ alert('Informe o novo CPA desejado.'); return; }}
              if (reason.length < 8) {{ alert('Informe o motivo do ajuste.'); return; }}
              const payload = {{
                campaign_id: button.dataset.campaignId || '',
                campaign: button.dataset.campaign || '',
                conversion_goal: button.dataset.goal || '',
                action_type: 'target_cpa_change',
                old_value: Number(button.dataset.oldValue || 0) || 0,
                new_value: newValue,
                reason,
                roi_today: Number(button.dataset.roiToday || 0) || 0,
                roi_7d: Number(button.dataset.roi7d || 0) || 0,
                pmr_today: Number(button.dataset.pmrToday || 0) || 0,
                pmr_7d: Number(button.dataset.pmr7d || 0) || 0,
                avg_cpc: Number(button.dataset.avgCpc || 0) || 0,
              }};
              button.disabled = true;
              const oldText = button.textContent;
              button.textContent = 'Registrando...';
              try {{
                const response = await fetch(`/api/projects/${{encodeURIComponent(row.dataset.project)}}/campaigns/${{encodeURIComponent(row.dataset.attrId)}}/bid-action`, {{
                  method: 'POST',
                  headers: {{'Content-Type': 'application/json'}},
                  body: JSON.stringify(payload),
                }});
                const data = await response.json().catch(() => ({{}}));
                if (!response.ok) throw new Error(data.detail || 'Falha ao registrar ajuste');
                button.textContent = 'Registrado';
                button.classList.add('saved');
                if (status) status.textContent = `Pendente para aplicar: ${{newValue.toLocaleString('pt-BR', {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}}`;
              }} catch (error) {{
                alert(error.message || 'Falha ao registrar ajuste');
                button.textContent = oldText;
                button.disabled = false;
              }}
            }});
          }});

          document.querySelectorAll('.budget-manual-button').forEach((button) => {{
            button.addEventListener('click', async () => {{
              const box = button.closest('.budget-manual-box');
              const row = button.closest('.campaign-row');
              if (!box || !row) return;
              const input = box.querySelector('.budget-manual-input');
              const reasonInput = box.querySelector('.budget-manual-reason');
              const status = box.querySelector('.budget-manual-status');
              const oldBudget = Number(button.dataset.oldBudget || 0) || 0;
              const newBudget = Number(String(input?.value || '').replace(',', '.')) || 0;
              if (newBudget <= 0) {{ alert('Informe o novo orcamento.'); return; }}
              if (newBudget <= oldBudget) {{ alert('O novo orcamento deve ser maior que o atual.'); return; }}
              const oldText = button.textContent;
              button.disabled = true;
              button.textContent = 'Registrando...';
              try {{
                const response = await fetch(`/api/projects/${{encodeURIComponent(row.dataset.project)}}/campaigns/${{encodeURIComponent(row.dataset.attrId)}}/budget-action`, {{
                  method: 'POST',
                  headers: {{'Content-Type': 'application/json'}},
                  body: JSON.stringify({{
                    campaign_id: button.dataset.campaignId || '',
                    customer_id: button.dataset.customerId || '',
                    campaign: button.dataset.campaign || '',
                    old_budget: oldBudget,
                    new_budget: newBudget,
                    reason: String(reasonInput?.value || '').trim(),
                    roi_today: Number(button.dataset.roiToday || 0) || 0,
                    roi_7d: Number(button.dataset.roi7d || 0) || 0,
                    pmr_today: Number(button.dataset.pmrToday || 0) || 0,
                    pmr_7d: Number(button.dataset.pmr7d || 0) || 0,
                  }}),
                }});
                const data = await response.json().catch(() => ({{}}));
                if (!response.ok) throw new Error(data.detail || 'Falha ao registrar orcamento');
                button.textContent = 'Registrado';
                button.classList.add('saved');
                if (status) status.textContent = `Pendente Google Ads: R${{oldBudget.toLocaleString('pt-BR', {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}} -> R${{newBudget.toLocaleString('pt-BR', {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}}`;
              }} catch (error) {{
                alert(error.message || 'Falha ao registrar orcamento');
                button.textContent = oldText;
                button.disabled = false;
              }}
            }});
          }});

          document.querySelectorAll('.agent-feedback-box').forEach((box) => {{
            const button = box.querySelector('.agent-feedback-button');
            if (!button) return;
            button.addEventListener('click', async () => {{
              const note = box.querySelector('.agent-feedback-note')?.value || '';
              const decision = box.querySelector('.agent-feedback-decision')?.value || 'invalid';
              const makePattern = !!box.querySelector('.agent-feedback-pattern')?.checked;
              const status = box.querySelector('.agent-feedback-status');
              if (note.trim().length < 8) {{
                if (status) status.textContent = 'Descreva melhor antes de registrar.';
                return;
              }}
              button.disabled = true;
              if (status) status.textContent = 'Registrando...';
              try {{
                const response = await fetch('/api/agent-alert-feedback', {{
                  method: 'POST',
                  headers: {{'Content-Type': 'application/json', 'Accept': 'application/json'}},
                  body: JSON.stringify({{
                    observation_key: box.dataset.observationKey || '',
                    decision: decision,
                    note: note,
                    make_pattern: makePattern
                  }})
                }});
                const data = await response.json().catch(() => ({{}}));
                if (!response.ok) throw new Error(data.detail || 'Falha ao registrar');
                if (status) status.textContent = data.hidden ? 'Registrado. O agente vai medir; aviso removido por enquanto.' : 'Registrado. Aviso mantido para acompanhamento.';
                if (data.hidden) {{
                  const card = box.closest('.agent-global-summary');
                  if (card) setTimeout(() => {{ card.style.display = 'none'; }}, 700);
                }}
              }} catch (error) {{
                if (status) status.textContent = error.message || String(error);
              }} finally {{
                button.disabled = false;
              }}
            }});
          }});

          function setRowDetailButtons(row, detailType) {{
            row.querySelectorAll('.campaign-inline-action').forEach((button) => {{
              button.classList.toggle('active', button.dataset.detailType === detailType);
            }});
          }}

          function closeActive() {{
            if (openDetailRow) {{
              openDetailRow.remove();
              openDetailRow = null;
            }}
            if (openRow) {{
              openRow.classList.remove('is-open');
              setRowDetailButtons(openRow, 'blocks');
            }}
            openRow = null;
            activePeriod = 'today';
            activeDetailType = 'blocks';
            activeCustomStart = '';
            activeCustomEnd = '';
          }}

          function renderTabs(selectedPeriod) {{
            return Object.entries(periodMeta).map(([period, meta]) => `
              <button class="campaign-detail-tab ${{period === selectedPeriod ? 'active' : ''}}" data-period="${{period}}" type="button">
                ${{meta.label}}
              </button>
            `).join('');
          }}

          function periodLabel(period) {{
            if (period === 'custom' && activeCustomStart && activeCustomEnd) {{
              return activeCustomStart === activeCustomEnd ? activeCustomStart : `${{activeCustomStart}} a ${{activeCustomEnd}}`;
            }}
            return (periodMeta[period] || periodMeta.today).label;
          }}


          function diagnosisPercent(value) {{
            const number = Number(value || 0);
            if (!Number.isFinite(number)) return 0;
            return Math.abs(number) <= 1 && number !== 0 ? number * 100 : number;
          }}

          function diagnosisMoney(value) {{
            const number = Number(value || 0);
            return number.toLocaleString('pt-BR', {{ style: 'currency', currency: 'BRL' }});
          }}

          function diagnosisPanelHtml(campaignData) {{
            const channel = String(campaignData.channel_type || '').toUpperCase();
            const efficiency = campaignData.efficiency || {{}};
            const lock = campaignData.optimization_lock || {{}};
            const isDisplay = channel === 'DISPLAY';
            const roi7 = diagnosisPercent(campaignData.roi_7d);
            const roiToday = diagnosisPercent(campaignData.roi_hoje);
            const pmrToday = diagnosisPercent(campaignData.pmr_hoje);
            const pmrYesterday = diagnosisPercent(campaignData.pmr_ontem);
            const pmr7 = diagnosisPercent(campaignData.pmr_7d);
            const spend7 = Number(campaignData.investimento_7d || 0);
            const revenueToday = Number(campaignData.receita_total_hoje || campaignData.receita_hoje || 0);
            const revenue7 = Number(campaignData.receita_total_7d || campaignData.receita_7d || 0);
            const targetCpa = Number(efficiency.target_cpa || 0);
            const avgCpc = Number(efficiency.avg_cpc || 0);
            const ratio = targetCpa > 0 && avgCpc > 0 ? (avgCpc / targetCpa) * 100 : 0;
            let state = 'warn';
            let title = 'Monitorar antes de alterar CPA/CPC';
            let next = 'Validar dados, GAM e placements antes de qualquer ajuste.';
            let cpaDecision = 'Nao mexer agora';
            let gamDecision = pmr7 > 0 ? 'PMR em observacao' : 'Sem PMR suficiente';
            let eligibility = 'Elegivel para diagnostico';

            if (isDisplay && lock && lock.can_optimize === false) {{
              state = 'warn';
              title = 'Display em trava de aprendizado';
              next = `Aguardar: ${{lock.label || 'campanha ainda nao liberada para otimizar'}}.`;
              eligibility = 'Bloqueada por aprendizado inicial';
              cpaDecision = 'Nao alterar campanha durante aprendizado';
            }} else if (((roiToday < 0 && (pmrToday >= 90 || pmrYesterday >= 90)) || (roi7 < 0 && pmr7 >= 90)) && (revenueToday > 0 || revenue7 > 0)) {{
              state = 'warn';
              title = 'Priorizar regra de preco GAM';
              next = 'PMR alto com ROI negativo. Avaliar floor por bloco antes de reduzir CPA/CPC.';
              gamDecision = 'Priorizar floor price';
              cpaDecision = 'Nao reduzir CPA/CPC antes de validar floor';
            }} else if (roi7 >= 40 && pmr7 >= 80) {{
              state = 'good';
              title = 'Campanha saudavel para escala controlada';
              next = 'Priorizar volume, orcamento e manter CPA/CPC estaveis.';
              cpaDecision = 'Nao reduzir CPA/CPC; risco de perder volume bom';
            }} else if (roi7 < 0 && pmr7 < 80 && pmr7 > 0) {{
              state = 'bad';
              title = 'Rever GAM antes de mexer no Ads';
              next = 'PMR fraco pode estar reduzindo conversoes AdView/Interstitial/Reward.';
              gamDecision = 'Investigar floor price e blocos GAM';
            }} else if (roi7 < 0 && ratio >= 80) {{
              state = 'bad';
              title = 'CPA/CPC pode estar pressionando ROI';
              next = 'Sugerir apenas reducao gradual apos validar GAM, floor e placements.';
              cpaDecision = 'Avaliar reducao leve de 5% a 10%';
            }} else if (spend7 < 30) {{
              state = 'warn';
              title = 'Volume ainda baixo para decisao forte';
              next = 'Aguardar mais dados antes de alterar lances.';
              eligibility = 'Volume insuficiente';
            }}

            return `
              <div class="diagnosis-panel">
                <div class="diagnosis-hero ${{state}}">
                  <div class="diagnosis-kicker">Diagnostico da campanha</div>
                  <div class="diagnosis-title">${{esc(title)}}</div>
                  <div class="diagnosis-next">Proxima melhor acao: ${{esc(next)}}</div>
                </div>
                <div class="diagnosis-grid">
                  <div class="diagnosis-card"><span>Elegibilidade</span><strong>${{esc(eligibility)}}</strong><p>${{isDisplay && lock && lock.label ? esc(lock.label) : 'Sem bloqueio de aprendizado aplicado.'}}</p></div>
                  <div class="diagnosis-card"><span>Lucro</span><strong>ROI 7d ${{roi7.toFixed(2)}}%</strong><p>Hoje: ${{roiToday.toFixed(2)}}% | Receita 7d: ${{diagnosisMoney(revenue7)}} | Inv. 7d: ${{diagnosisMoney(spend7)}}</p></div>
                  <div class="diagnosis-card"><span>GAM / Conversoes</span><strong>${{esc(gamDecision)}}</strong><p>PMR 7d: ${{pmr7.toFixed(2)}}%. Conversoes Ads dependem de AdView, Interstitial ou Reward quando a meta usa GAM.</p></div>
                  <div class="diagnosis-card"><span>CPA/CPC</span><strong>${{esc(cpaDecision)}}</strong><p>CPA alvo: ${{targetCpa > 0 ? diagnosisMoney(targetCpa) : '-'}} | CPC medio: ${{avgCpc > 0 ? diagnosisMoney(avgCpc) : '-'}} | CPC/CPA: ${{ratio ? ratio.toFixed(1) + '%' : '-'}}</p></div>
                </div>
                <div class="diagnosis-steps">
                  <div class="diagnosis-step"><span class="diagnosis-dot">1</span><div>Validar se a campanha ja pode ser avaliada e se nao houve alteracao recente.</div></div>
                  <div class="diagnosis-step"><span class="diagnosis-dot">2</span><div>Conferir ROI, receita e volume antes de sugerir reducao de CPA/CPC.</div></div>
                  <div class="diagnosis-step"><span class="diagnosis-dot">3</span><div>Se PMR/floor GAM estiver causando queda de conversoes, corrigir GAM antes de mexer no Google Ads.</div></div>
                  <div class="diagnosis-step"><span class="diagnosis-dot">4</span><div>Se tudo estiver validado, qualquer ajuste de CPA/CPC deve ser gradual e medido em 24h, 48h e 7 dias.</div></div>
                </div>
              </div>
            `;
          }}

          function dateFilterHtml() {{
            return `
              <div class="campaign-detail-date-filter">
                <input class="detail-date-start" type="date" value="${{esc(activeCustomStart)}}">
                <span>atÃƒÂ©</span>
                <input class="detail-date-end" type="date" value="${{esc(activeCustomEnd)}}">
                <button class="detail-date-apply" type="button">Aplicar</button>
              </div>
            `;
          }}

          async function registerFloorAction(button) {{
            const row = button.closest('tr');
            const currentInput = row.querySelector('.floor-current-input');
            const newInput = row.querySelector('.floor-new-input');
            const floorInputToValue = (value) => {{
              const text = String(value || '').trim().toLowerCase();
              const bucketMatch = text.match(/^fl[_-]?(\\d+)$/);
              if (bucketMatch) return Number(bucketMatch[1]) / 100;
              const cleaned = text.replace(/^(r\\$|us\\$|\\$)\\s*/, '');
              const normalized = cleaned.includes(',') && cleaned.includes('.')
                ? cleaned.replace(/\\./g, '').replace(',', '.')
                : cleaned.replace(',', '.');
              const parsed = Number(normalized);
              return Number.isFinite(parsed) ? parsed : NaN;
            }};
            const blockName = button.dataset.blockName || '';
            const placement = button.dataset.placement || '';
            const scope = button.dataset.scope || '';
            const project = button.dataset.project || '';
            const attrId = button.dataset.attrId || '';
            const campaign = button.dataset.campaign || '';
            const period = button.dataset.period || activePeriod || 'today';
            const payload = {{
              block_name: blockName,
              placement: placement,
              scope: scope,
              campaign: campaign,
              period: period,
              currency: floorCurrencyForProject(project),
              current_floor_bucket: currentInput.value || '',
              new_floor_bucket: newInput.value || '',
              current_floor: floorInputToValue(currentInput.value),
              new_floor: floorInputToValue(newInput.value),
              pmr: Number(String(button.dataset.pmr || '').replace(',', '.')) || 0,
              status: button.dataset.actionStatus || 'registered'
            }};
            if (payload.status === 'disabled') {{
              payload.new_floor = payload.current_floor;
            }}
            if (!['campaign_block', 'placement_block'].includes(payload.scope) || (payload.scope === 'placement_block' && !payload.placement) || !payload.block_name || !Number.isFinite(payload.current_floor) || !Number.isFinite(payload.new_floor) || payload.current_floor < 0 || payload.new_floor < 0) {{
              alert('Informe os valores de floor corretamente.');
              return;
            }}
            button.disabled = true;
            const oldText = button.textContent;
            button.textContent = payload.status === 'disabled' ? 'Desligando...' : 'Registrando...';
            try {{
              const response = await fetch(`/api/projects/${{encodeURIComponent(project)}}/campaigns/${{encodeURIComponent(attrId)}}/gam-blocks/floor-action`, {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(payload)
              }});
              const data = await response.json().catch(() => ({{}}));
              if (!response.ok) throw new Error(data.detail || data.error || 'Falha ao registrar acao');
              button.textContent = payload.status === 'disabled' ? 'Desligada' : 'Registrado';
              button.classList.add('registered');
              currentInput.value = payload.new_floor ? String(payload.new_floor) : currentInput.value;
              const statusCell = row.querySelector('.floor-status-cell');
              const registeredText = `${{new Date().toLocaleString('pt-BR', {{ timeZone: 'America/Sao_Paulo' }})}} | PMR ${{pct(data.pmr_at_registration ?? payload.pmr)}}`;
              if (statusCell) {{
                const statusText = payload.status === 'disabled' ? `Regra desligada ${{payload.current_floor.toFixed(2)}}` : `Registrada ${{payload.current_floor.toFixed(2)}} -> ${{payload.new_floor.toFixed(2)}}`;
                statusCell.innerHTML = `${{esc(statusText)}}<br><small>${{esc(registeredText)}}</small>`;
                statusCell.classList.toggle('rule-disabled', payload.status === 'disabled');
              }}
              const statusRuleButton = row.querySelector('.floor-status-rule-button');
              if (statusRuleButton) {{
                statusRuleButton.classList.remove('create-state', 'active-state', 'disabled-state', 'registered');
                if (payload.status === 'disabled') {{
                  statusRuleButton.textContent = 'Desligado';
                  statusRuleButton.dataset.actionStatus = 'disabled';
                  statusRuleButton.classList.add('disabled-state');
                }} else {{
                  statusRuleButton.textContent = 'Ativa';
                  statusRuleButton.dataset.actionStatus = 'disabled';
                  statusRuleButton.classList.add('active-state');
                }}
                statusRuleButton.disabled = false;
              }}
              const changeCell = row.querySelector('.floor-change-cell');
              if (changeCell) changeCell.textContent = `${{payload.new_floor >= payload.current_floor ? '+' : ''}}${{pct(((payload.new_floor - payload.current_floor) / payload.current_floor) * 100)}}`;
              if (button.classList.contains('floor-status-rule-button')) {{
                button.disabled = false;
                button.classList.remove('registered');
              }} else {{
                setTimeout(() => {{ button.textContent = oldText; button.disabled = false; button.classList.remove('registered'); }}, 1800);
              }}
            }} catch (error) {{
              alert(`Falha ao registrar floor: ${{error.message || error}}`);
              button.textContent = oldText;
              button.disabled = false;
            }}
          }}

          const manualFloorBuckets = [
            100, 125, 150, 200, 250, 300, 400, 500, 750, 1000,
            1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000,
            5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000,
            11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000,
            21000, 22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000, 30000,
            31000, 32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000, 40000,
            41000, 42000, 43000, 44000, 45000, 46000, 47000, 48000, 49000, 50000,
            55000, 60000, 65000, 70000, 75000, 80000, 85000, 90000, 95000, 100000
          ];

          function floorCurrencyForProject(project) {{
            const text = String(project || '').toLowerCase();
            if (text.includes('sonora123.com')) return 'BRL';
            return 'USD';
          }}

          function floorCurrencyLabel(currency) {{
            return String(currency || '').toUpperCase() === 'BRL' ? 'R$' : 'US$';
          }}

          function floorSelectOptions(selectedValue, placeholder, currency) {{
            const selected = Number(String(selectedValue ?? '').replace(',', '.')) || 0;
            const selectedCents = selected > 0 ? Math.round(selected * 100) : 0;
            const currencyLabel = floorCurrencyLabel(currency);
            const placeholderOption = `<option value="${{esc(selected || '')}}">${{esc(placeholder || 'Atual')}}</option>`;
            const options = manualFloorBuckets.map((bucket) => {{
              const value = bucket / 100;
              const label = `fl_${{bucket}} - ${{currencyLabel}} ${{value.toFixed(2).replace('.', ',')}}`;
              return `<option value="${{esc(value)}}" ${{selectedCents === bucket ? 'selected' : ''}}>${{esc(label)}}</option>`;
            }}).join('');
            return placeholderOption + options;
          }}

          function blockTableHtml(blocks, campaignRevenue, row, campaignData, period, placementName = '') {{
            if (!blocks.length) {{
              const emptyLabel = period === 'custom' ? `Sem dados GAM de blocos no periodo ${{esc(periodLabel(period))}}.` : 'Nenhum bloco encontrado para este periodo.';
              return `<div class="campaign-detail-state">${{emptyLabel}}</div>`;
            }}

            const totalImpressions = blocks.reduce((sum, block) => sum + (Number(block.impressions) || 0), 0);
            const rows = blocks.map((block) => {{
              const revenue = Number(block.net_revenue) || 0;
              const impressions = Number(block.impressions) || 0;
              const revenueShare = campaignRevenue > 0 ? (revenue / campaignRevenue) * 100 : 0;
              const ruleStatus = String(block.floor_rule_status || '');
              const isRuleDisabled = ruleStatus.startsWith('Regra desligada');
              const hasRegisteredRule = ruleStatus.startsWith('Registrada') || isRuleDisabled;
              const statusRuleLabel = isRuleDisabled ? 'Desligado' : (hasRegisteredRule ? 'Ativa' : 'Criar Regra');
              const statusRuleAction = isRuleDisabled ? 'disabled' : (hasRegisteredRule ? 'disabled' : 'registered');
              const statusRuleClass = isRuleDisabled ? 'disabled-state' : (hasRegisteredRule ? 'active-state' : 'create-state');
              const suggestionAction = String(block.floor_suggestion_action || '');
              const suggestionChange = block.floor_suggestion_change_pct;
              const suggestionValue = block.floor_suggested_value;
              const hasPendingFloorAction = Boolean(suggestionAction) || (suggestionValue !== null && suggestionValue !== undefined && Number(suggestionValue) !== Number(block.floor_current_value));
              const changeText = suggestionAction === 'disable'
                ? 'Desligar'
                : (suggestionChange === null || suggestionChange === undefined
                  ? (block.floor_change_pct === null || block.floor_change_pct === undefined ? '' : `${{Number(block.floor_change_pct) >= 0 ? '+' : ''}}${{pct(block.floor_change_pct)}}`)
                  : `${{Number(suggestionChange) >= 0 ? '+' : ''}}${{pct(suggestionChange)}}`);
              const suggestionClass = hasPendingFloorAction ? `suggest-${{suggestionAction || 'change'}} action-pending` : '';
              const newFloorValue = suggestionValue === null || suggestionValue === undefined ? '' : suggestionValue;
              const floorCurrency = floorCurrencyForProject(row?.dataset.project || '');
              const floorScope = placementName ? 'placement_block' : 'campaign_block';
              return `
                <tr class="${{suggestionClass}}">
                  <td class="placement-check-cell"><input class="gam-block-select" type="checkbox" value="${{esc(block.block_name)}}"></td>
                  <td class="block-name-cell">${{esc(block.block_name)}}</td>
                  <td>${{num(block.impressions)}}</td>
                  <td>${{num(block.clicks)}}</td>
                  <td class="ctr-cell no-cell-resize">${{pct(block.ctr)}}</td>
                  <td>${{money(block.cpc)}}</td>
                  <td>${{money(block.net_revenue)}}</td>
                  <td>${{pct(revenueShare)}}</td>
                  <td class="pmr-cell no-cell-resize">${{pct(block.pmr)}}</td>
                  <td class="viewability-cell no-cell-resize">${{pct(block.viewability)}}</td>
                  <td>${{blockEcpm(block)}}</td>
                  <td class="floor-status-cell no-cell-resize ${{isRuleDisabled ? 'rule-disabled' : ''}}">${{esc(block.floor_rule_status || 'Sem regra registrada')}}${{block.floor_registered_at ? `<br><small>${{esc(`${{new Date(block.floor_registered_at).toLocaleString('pt-BR', {{ timeZone: 'America/Sao_Paulo' }})}}${{block.floor_registered_pmr === null || block.floor_registered_pmr === undefined ? '' : ` | PMR ${{pct(block.floor_registered_pmr)}}`}}`)}}</small>` : ''}}</td>
                  <td class="floor-change-cell">${{block.floor_change_pct === null || block.floor_change_pct === undefined ? '' : `${{Number(block.floor_change_pct) >= 0 ? '+' : ''}}${{pct(block.floor_change_pct)}}`}}</td>
                  <td><select class="floor-current-input floor-select">${{floorSelectOptions(block.floor_current_value, 'Atual', floorCurrency)}}</select></td>
                  <td><select class="floor-new-input floor-select">${{floorSelectOptions('', 'Novo', floorCurrency)}}</select></td>
                  <td class="floor-action-cell no-cell-resize"><button class="floor-register-button" type="button" data-action-status="registered" data-scope="${{esc(floorScope)}}" data-placement="${{esc(placementName)}}" data-project="${{esc(row?.dataset.project || '')}}" data-attr-id="${{esc(row?.dataset.attrId || '')}}" data-campaign="${{esc(campaignData?.campaign || '')}}" data-period="${{esc(period || 'today')}}" data-block-name="${{esc(block.block_name)}}" data-pmr="${{esc(block.pmr ?? '')}}">Registrar</button></td>
                  <td class="floor-status-rule-cell"><button class="floor-register-button floor-status-rule-button ${{statusRuleClass}}" type="button" data-action-status="${{statusRuleAction}}" data-scope="${{esc(floorScope)}}" data-placement="${{esc(placementName)}}" data-project="${{esc(row?.dataset.project || '')}}" data-attr-id="${{esc(row?.dataset.attrId || '')}}" data-campaign="${{esc(campaignData?.campaign || '')}}" data-period="${{esc(period || 'today')}}" data-block-name="${{esc(block.block_name)}}" data-pmr="${{esc(block.pmr ?? '')}}">${{statusRuleLabel}}</button></td>
                </tr>
              `;
            }}).join('');

            return `
              <div class="campaign-detail-table-scroll">
              <table class="campaign-block-table">
                <thead>
                  <tr>
                    <th class="placement-check-cell"></th>
                    <th>Bloco</th>
                    <th>ImpressÃƒÂµes</th>
                    <th>Cliques</th>
                    <th class="ctr-cell no-cell-resize">CTR</th>
                    <th>CPC</th>
                    <th>Receita</th>
                    <th>% da receita</th>
                    <th class="pmr-cell no-cell-resize">PMR</th>
                    <th class="viewability-cell no-cell-resize">Visibilidade</th>
                    <th>eCPM GAM</th>
                    <th class="floor-status-cell no-cell-resize">Status Floor</th>
                    <th>AlteraÃƒÂ§ÃƒÂ£o</th>
                    <th>Floor atual</th>
                    <th>Novo floor</th>
                    <th>AÃƒÂ§ÃƒÂ£o</th>
                    <th>Status Regra</th>
                  </tr>
                </thead>
                <tbody>${{rows}}</tbody>
              </table>
              <div class="campaign-detail-scroll-hint" aria-hidden="true"></div>
              </div>
            `;
          }}

          function placementTableHtml(rows, row, campaignData, period) {{
            if (!rows.length) {{
              return '<div class="campaign-detail-state">Nenhum placement encontrado para este periodo.</div>';
            }}
            const floorCurrency = floorCurrencyForProject(row?.dataset.project || '');
            const tableRows = rows.map((item) => {{
              const hasExactGam = Boolean(item.gam_data_exact || item.gam_metric_source === 'content_exact');
              const gamMissing = '<span class="gam-missing" title="Sem dado exato de placement no GAM">Sem dado</span>';
              const gamNumber = (value) => hasExactGam ? num(value) : gamMissing;
              const gamMoney = (value) => hasExactGam ? money(value) : gamMissing;
              const gamPercent = (value) => hasExactGam ? pct(value) : gamMissing;
              const placementName = item.placement || item.display_name || '';
              const placementKey = String(placementName || item.display_name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
              const ruleStatus = String(item.floor_rule_status || '');
              const isRuleDisabled = ruleStatus.startsWith('Regra desligada');
              const hasRegisteredRule = ruleStatus.startsWith('Registrada') || isRuleDisabled;
              const statusRuleLabel = isRuleDisabled ? 'Desligado' : (hasRegisteredRule ? 'Ativa' : 'Criar Regra');
              const statusRuleAction = isRuleDisabled ? 'disabled' : (hasRegisteredRule ? 'disabled' : 'registered');
              const statusRuleClass = isRuleDisabled ? 'disabled-state' : (hasRegisteredRule ? 'active-state' : 'create-state');
              const changeText = item.floor_change_pct === null || item.floor_change_pct === undefined ? '' : `${{Number(item.floor_change_pct) >= 0 ? '+' : ''}}${{pct(item.floor_change_pct)}}`;
              const registeredText = item.floor_registered_at
                ? `<br><small>${{esc(`${{new Date(item.floor_registered_at).toLocaleString('pt-BR', {{ timeZone: 'America/Sao_Paulo' }})}}${{item.floor_registered_pmr === null || item.floor_registered_pmr === undefined ? '' : ` | PMR ${{pct(item.floor_registered_pmr)}}`}}`)}}</small>`
                : '';
              const changeBadge = changeText ? `<span class="floor-change-pill">${{esc(changeText)}}</span>` : '';
              const blocks = Array.isArray(item.ad_blocks) ? item.ad_blocks : [];
              const blockTable = blocks.length
                ? blockTableHtml(blocks, item.revenue, row, campaignData, period, placementName)
                : '<div class="campaign-detail-state">Sem bloco GAM identificado para este placement.</div>';
              return `
              <tr class="placement-row" data-placement-key="${{esc(placementKey)}}" data-clicks="${{esc(item.clicks ?? 0)}}" data-roi="${{esc(item.roi ?? 0)}}">
                <td class="placement-check-cell"><input class="placement-select" type="checkbox" value="${{esc(placementName)}}"></td>
                <td class="block-name-cell">
                  <div class="placement-name-wrap">
                    <button class="placement-expand-button" type="button" aria-label="Abrir blocos deste placement" aria-expanded="false">â€º</button>
                    <div class="placement-name-stack">
                      <div class="placement-name-strong">${{esc(placementName)}}</div>
                      <small>Clique para configurar o floor individual de cada bloco neste placement.</small>
                    </div>
                  </div>
                </td>
                <td class="metric-cell">${{money(item.cost)}}</td>
                <td class="metric-cell">${{num(item.clicks)}}</td>
                <td class="metric-cell">${{num(item.impressions)}}</td>
                <td class="metric-cell">${{pct(item.ctr)}}</td>
                <td class="metric-cell">${{money(item.cpc)}}</td>
                <td class="metric-cell">${{pct(item.conversion_rate)}}</td>
                <td class="metric-cell">${{num(item.conversions)}}</td>
                <td class="metric-cell">${{gamMoney(item.revenue)}}</td>
                <td class="metric-cell placement-roi-cell ${{roiClass(item.roi)}}">${{pct(item.roi)}}</td>
                <td class="metric-cell">${{gamNumber(item.gam_impressions)}}</td>
                <td class="metric-cell">${{gamNumber(item.gam_clicks)}}</td>
                <td class="metric-cell placement-pmr-cell">${{gamPercent(item.pmr)}}</td>
                <td class="metric-cell placement-ecpm-cell">${{hasExactGam ? placementEcpm(item) : gamMissing}}</td>
                <td class="metric-cell">${{num(item.sessions)}}</td>
                <td class="metric-cell">${{Number(item.sessions || 0) > 0 ? money(item.rps) : '-'}}</td>
                <td class="floor-status-cell floor-summary-cell no-cell-resize ${{isRuleDisabled ? 'rule-disabled' : ''}}">${{esc(item.floor_rule_status || 'Sem regra registrada')}}${{registeredText}}${{changeBadge}}</td>
                <td class="floor-control-cell no-cell-resize">Abra para configurar por bloco</td>
              </tr>
              <tr class="placement-detail-row" data-placement-detail="${{esc(placementKey)}}">
                <td class="placement-detail-cell" colspan="19">
                  <div class="placement-detail-shell">
                    <div class="campaign-detail-state" style="margin-bottom: 10px;">
                      Blocos GAM deste placement: <strong>${{esc(placementName)}}</strong>
                    </div>
                    ${{blockTable}}
                  </div>
                </td>
              </tr>
            `;
            }}).join('');
            return `
              <div class="campaign-detail-table-scroll placements-vertical-scroll">
              <table class="campaign-block-table placement-operational-table">
                <thead>
                  <tr>
                    <th class="placement-check-cell"></th>
                    <th class="block-name-cell">Placement</th>
                    <th>Inv.</th>
                    <th>Cliques Ads</th>
                    <th>Impr. Ads</th>
                    <th>CTR Ads</th>
                    <th>CPC Ads</th>
                    <th>Taxa conv. Ads</th>
                    <th>Conv.</th>
                    <th>Receita GAM</th>
                    <th>ROI</th>
                    <th>Impr. GAM</th>
                    <th>Cliques GAM</th>
                    <th class="placement-pmr-cell">PMR GAM</th>
                    <th class="placement-ecpm-cell">eCPM GAM</th>
                    <th>Sessoes</th>
                    <th>RPS</th>
                    <th class="floor-status-cell no-cell-resize">Status Floor</th>
                    <th class="floor-control-cell no-cell-resize">Controle Floor</th>
                  </tr>
                </thead>
                <tbody>${{tableRows}}</tbody>
              </table>
              <div class="campaign-detail-scroll-hint" aria-hidden="true"></div>
              </div>
            `;
          }}

          async function loadPeriod(row, detailShell, period, detailType = activeDetailType) {{
            const project = row.dataset.project;
            const attrId = row.dataset.attrId;
            const campaignData = JSON.parse(row.dataset.rowJson || '{{}}');
            const isPlacements = detailType === 'placements';
            const isDiagnosis = detailType === 'diagnosis';
            const title = isDiagnosis ? 'Diagnostico' : (isPlacements ? 'Placements' : 'Blocos de anuncio do GAM');
            const loadingText = isPlacements ? 'Carregando placements...' : 'Carregando blocos do GAM...';
            const failText = isPlacements ? 'Falha ao carregar placements. Tente novamente.' : 'Falha ao carregar blocos. Tente novamente.';
            const endpoint = isPlacements ? 'placements' : 'gam-blocks';
            if (isDiagnosis) {{
              activePeriod = period;
              activeDetailType = detailType;
              setRowDetailButtons(row, detailType);
              detailShell.innerHTML = `
                <div class="campaign-detail-toolbar">
                  <div>
                    <div class="campaign-detail-title">Diagnostico</div>
                    <div class="campaign-detail-note">${{esc(campaignData.campaign || '')}}</div>
                  </div>
                </div>
                ${{diagnosisPanelHtml(campaignData)}}
              `;
              return;
            }}
            if (period !== 'custom') {{
              activeCustomStart = '';
              activeCustomEnd = '';
            }}
            const customKey = period === 'custom' ? `${{activeCustomStart}}|${{activeCustomEnd}}` : '';
            const cacheKey = `${{project}}|${{attrId}}|${{detailType}}|${{period}}|${{customKey}}`;
            const placementTools = isPlacements ? '<div class="placement-filter-bar"><label>Cliques >= <input class="placement-filter-clicks" type="number" min="0" step="1" placeholder="0"></label><label>ROI >= <input class="placement-filter-roi" type="number" step="0.01" placeholder="0"></label><button class="placement-filter-button" type="button">Selecionar canais</button><button class="placement-download-button" type="button">Download</button></div>' : '';
            const excludeButton = isPlacements ? '<button class="placement-exclude-button" type="button">Excluir placements selecionados</button>' : '';
            activePeriod = period;
            activeDetailType = detailType;
            setRowDetailButtons(row, detailType);

            detailShell.innerHTML = `
              <div class="campaign-detail-toolbar">
                <div>
                  <div class="campaign-detail-title">${{title}}</div>
                  <div class="campaign-detail-note">${{esc(campaignData.campaign || '')}} | ${{periodLabel(period)}}</div>
                </div>
                <div class="campaign-detail-actions">
                  ${{!isPlacements ? dateFilterHtml() : ''}}
                  ${{placementTools}}
                  <div class="campaign-detail-tabs">${{renderTabs(period)}}</div>
                  ${{excludeButton}}
                </div>
              </div>
              <div class="campaign-detail-state">${{loadingText}}</div>
            `;

            const targetButton = detailShell.querySelector(`.campaign-detail-tab[data-period="${{period}}"]`);
            if (targetButton) {{
              targetButton.classList.add('active');
            }}

            if (!rowCache.has(cacheKey)) {{
              try {{
                const dateQuery = period === 'custom' ? `&start_date=${{encodeURIComponent(activeCustomStart)}}&end_date=${{encodeURIComponent(activeCustomEnd)}}` : '';
                const response = await fetch(`/api/projects/${{encodeURIComponent(project)}}/campaigns/${{encodeURIComponent(attrId)}}/${{endpoint}}?period=${{encodeURIComponent(period)}}${{dateQuery}}`, {{
                  headers: {{ 'Accept': 'application/json' }},
                }});
                if (!response.ok) {{
                  throw new Error(`HTTP ${{response.status}}`);
                }}
                const payload = await response.json();
                rowCache.set(cacheKey, payload);
              }} catch (error) {{
                if (openRow !== row) {{
                  return;
                }}
                detailShell.innerHTML = `
                  <div class="campaign-detail-toolbar">
                    <div>
                      <div class="campaign-detail-title">${{title}}</div>
                      <div class="campaign-detail-note">${{esc(campaignData.campaign || '')}} | ${{periodLabel(period)}}</div>
                    </div>
                    <div class="campaign-detail-actions">
                      ${{!isPlacements ? dateFilterHtml() : ''}}
                  ${{placementTools}}
                  <div class="campaign-detail-tabs">${{renderTabs(period)}}</div>
                      ${{excludeButton}}
                    </div>
                  </div>
                  <div class="campaign-detail-state">${{failText}}</div>
                `;
                wireTabs(row, detailShell);
                return;
              }}
            }}

            if (openRow !== row) {{
              return;
            }}
            const detailRows = rowCache.get(cacheKey) || [];
            const campaignRevenue = Number((detailRows[0] && detailRows[0].campaign_revenue) || campaignData[(periodMeta[period] || periodMeta.today).key] || 0);
            detailShell.innerHTML = `
              <div class="campaign-detail-toolbar">
                <div>
                  <div class="campaign-detail-title">${{title}}</div>
                  <div class="campaign-detail-note">${{esc(campaignData.campaign || '')}} | ${{periodLabel(period)}}</div>
                </div>
                <div class="campaign-detail-actions">
                  ${{!isPlacements ? dateFilterHtml() : ''}}
                  ${{placementTools}}
                  <div class="campaign-detail-tabs">${{renderTabs(period)}}</div>
                  ${{excludeButton}}
                </div>
              </div>
              ${{isPlacements ? placementTableHtml(detailRows, row, campaignData, period) : blockTableHtml(detailRows, campaignRevenue, row, campaignData, period)}}
            `;
            wireTabs(row, detailShell);
            wirePlacementFilters(detailShell);
            wirePlacementSelection(detailShell);
            wirePlacementExpansion(detailShell);
            wirePlacementExclude(row, detailShell);
          }}

          function wireTabs(row, detailShell) {{
            const applyDate = detailShell.querySelector('.detail-date-apply');
            if (applyDate) {{
              applyDate.addEventListener('click', (event) => {{
                event.preventDefault();
                event.stopPropagation();
                const startInput = detailShell.querySelector('.detail-date-start');
                const endInput = detailShell.querySelector('.detail-date-end');
                activeCustomStart = startInput ? startInput.value : '';
                activeCustomEnd = endInput ? endInput.value : activeCustomStart;
                if (!activeCustomStart) {{
                  alert('Selecione a data inicial.');
                  return;
                }}
                if (!activeCustomEnd) activeCustomEnd = activeCustomStart;
                loadPeriod(row, detailShell, 'custom', 'blocks');
              }});
            }}
            detailShell.querySelectorAll('.floor-register-button').forEach((button) => {{
              button.addEventListener('click', () => registerFloorAction(button));
            }});
            detailShell.querySelectorAll('.campaign-detail-tab').forEach((button) => {{
              button.addEventListener('click', (event) => {{
                event.preventDefault();
                event.stopPropagation();
                const period = button.dataset.period;
                loadPeriod(row, detailShell, period, activeDetailType);
              }});
            }});
          }}

          function placementRowsMatchingFilters(detailShell) {{
            const minClicks = Number(String(detailShell.querySelector('.placement-filter-clicks')?.value || '0').replace(',', '.')) || 0;
            const minRoi = Number(String(detailShell.querySelector('.placement-filter-roi')?.value || '0').replace(',', '.')) || 0;
            return Array.from(detailShell.querySelectorAll('.placement-row')).filter((placementRow) => {{
              const clicks = Number(placementRow.dataset.clicks || 0) || 0;
              const roi = Number(placementRow.dataset.roi || 0) || 0;
              return clicks >= minClicks && roi >= minRoi;
            }});
          }}

          function syncPlacementFilterHighlights(detailShell) {{
            const matching = new Set(placementRowsMatchingFilters(detailShell));
            detailShell.querySelectorAll('.placement-row').forEach((placementRow) => {{
              placementRow.classList.toggle('placement-row-filtered', matching.has(placementRow));
            }});
          }}

          function downloadSelectedPlacements(detailShell) {{
            const selectedRows = Array.from(detailShell.querySelectorAll('.placement-select:checked')).map((input) => input.closest('.placement-row')).filter(Boolean);
            if (!selectedRows.length) {{
              alert('Selecione pelo menos um canal para download.');
              return;
            }}
            const headers = Array.from(detailShell.querySelectorAll('.campaign-block-table thead th')).slice(1).map((th) => th.textContent.trim());
            const csvRows = [headers];
            selectedRows.forEach((row) => {{
              csvRows.push(Array.from(row.querySelectorAll('td')).slice(1).map((td) => td.textContent.trim().replace(/\s+/g, ' ')));
            }});
            const csv = csvRows.map((cols) => cols.map((value) => '"' + String(value).replace(/"/g, '""') + '"').join(';')).join(String.fromCharCode(10));
            const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'placements-' + Date.now() + '.csv';
            document.body.appendChild(link);
            link.click();
            link.remove();
            URL.revokeObjectURL(url);
          }}

          function wirePlacementFilters(detailShell) {{
            const filterBar = detailShell.querySelector('.placement-filter-bar');
            if (!filterBar) return;
            filterBar.querySelectorAll('input').forEach((input) => {{
              input.addEventListener('input', () => syncPlacementFilterHighlights(detailShell));
            }});
            const selectButton = filterBar.querySelector('.placement-filter-button');
            if (selectButton) {{
              selectButton.addEventListener('click', (event) => {{
                event.preventDefault();
                const matches = placementRowsMatchingFilters(detailShell);
                detailShell.querySelectorAll('.placement-select').forEach((input) => {{
                  input.checked = false;
                  const placementRow = input.closest('.placement-row');
                  if (placementRow) placementRow.classList.remove('placement-row-selected');
                }});
                matches.forEach((placementRow) => {{
                  const input = placementRow.querySelector('.placement-select');
                  if (input) {{
                    input.checked = true;
                    placementRow.classList.add('placement-row-selected');
                  }}
                }});
                syncPlacementFilterHighlights(detailShell);
              }});
            }}
            const downloadButton = filterBar.querySelector('.placement-download-button');
            if (downloadButton) {{
              downloadButton.addEventListener('click', (event) => {{
                event.preventDefault();
                downloadSelectedPlacements(detailShell);
              }});
            }}
            syncPlacementFilterHighlights(detailShell);
          }}

          function wirePlacementSelection(detailShell) {{
            detailShell.querySelectorAll('.placement-select').forEach((input) => {{
              const syncRow = () => {{
                const placementRow = input.closest('.placement-row');
                if (placementRow) {{
                  placementRow.classList.toggle('placement-row-selected', input.checked);
                }}
              }};
              input.addEventListener('change', syncRow);
              syncRow();
            }});
          }}

          function wirePlacementExpansion(detailShell) {{
            const togglePlacement = (placementRow) => {{
              const detailRow = placementRow.nextElementSibling;
              if (!detailRow || !detailRow.classList.contains('placement-detail-row')) {{
                return;
              }}
              const isOpen = detailRow.classList.toggle('is-open');
              placementRow.classList.toggle('is-open', isOpen);
              const button = placementRow.querySelector('.placement-expand-button');
              if (button) {{
                button.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
              }}
            }};
            detailShell.querySelectorAll('.placement-row').forEach((placementRow) => {{
              const detailRow = placementRow.nextElementSibling;
              if (!detailRow || !detailRow.classList.contains('placement-detail-row')) {{
                return;
              }}
              placementRow.addEventListener('click', (event) => {{
                if (event.target.closest('input, button, a, select, textarea, label')) {{
                  return;
                }}
                togglePlacement(placementRow);
              }});
              const button = placementRow.querySelector('.placement-expand-button');
              if (button) {{
                button.addEventListener('click', (event) => {{
                  event.preventDefault();
                  event.stopPropagation();
                  togglePlacement(placementRow);
                }});
              }}
            }});
          }}

          function wirePlacementExclude(row, detailShell) {{
            const button = detailShell.querySelector('.placement-exclude-button');
            if (!button) {{
              return;
            }}
            button.addEventListener('click', async (event) => {{
              event.preventDefault();
              event.stopPropagation();
              const selected = Array.from(detailShell.querySelectorAll('.placement-select:checked')).map((input) => input.value);
              if (!selected.length) {{
                alert('Selecione pelo menos um placement.');
                return;
              }}
              if (!confirm(`Excluir ${{selected.length}} placement(s) desta campanha no Google Ads?`)) {{
                return;
              }}
              button.disabled = true;
              button.textContent = 'Excluindo...';
              try {{
                const response = await fetch(`/api/projects/${{encodeURIComponent(row.dataset.project)}}/campaigns/${{encodeURIComponent(row.dataset.attrId)}}/placements/exclude`, {{
                  method: 'POST',
                  headers: {{ 'Accept': 'application/json', 'Content-Type': 'application/json' }},
                  body: JSON.stringify({{ placements: selected }}),
                }});
                const payload = await response.json();
                if (!response.ok) {{
                  throw new Error(payload.detail || `HTTP ${{response.status}}`);
                }}
                rowCache.delete(`${{row.dataset.project}}|${{row.dataset.attrId}}|placements|${{activePeriod}}`);
                await loadPeriod(row, detailShell, activePeriod, 'placements');
                const errors = (payload.errors || []).filter(Boolean);
                alert(errors.length ? `Exclusao enviada com avisos: ${{errors.join(' | ')}}` : 'Placements enviados para exclusao no Google Ads.');
              }} catch (error) {{
                alert(`Falha ao excluir placements: ${{error.message || error}}`);
                button.disabled = false;
                button.textContent = 'Excluir placements selecionados';
              }}
            }});
          }}

          async function openCampaignRow(row, detailType = 'blocks', period = 'today') {{
            closeActive();
            activePeriod = period || 'today';
            activeDetailType = detailType;
            openRow = row;
            row.classList.add('is-open');
            setRowDetailButtons(row, detailType);
            const detailRow = document.createElement('tr');
            detailRow.className = 'campaign-detail-row';
            detailRow.innerHTML = '<td colspan="7"><div class="campaign-detail-shell"><div class="campaign-detail-state">Carregando...</div></div></td>';
            row.insertAdjacentElement('afterend', detailRow);
            openDetailRow = detailRow;
            const detailShell = detailRow.querySelector('.campaign-detail-shell');
            await loadPeriod(row, detailShell, activePeriod, detailType);
          }}

          const focusParams = new URLSearchParams(window.location.search);
          const focusAttr = focusParams.get('focus_attr') || '';
          const focusDetail = focusParams.get('focus_detail') || 'blocks';
          const focusPeriod = focusParams.get('focus_period') || 'today';

          document.querySelectorAll('.attr-copy-button').forEach((button) => {{
            button.addEventListener('click', async (event) => {{
              event.stopPropagation();
              const value = button.dataset.copyAttr || '';
              try {{ await navigator.clipboard.writeText(value); button.textContent = 'Copiado'; setTimeout(() => button.textContent = 'Copiar', 900); }}
              catch (_) {{ button.textContent = value; }}
            }});
          }});

          document.querySelectorAll('.campaign-status-action').forEach((button) => {{
            button.addEventListener('click', async (event) => {{
              event.preventDefault();
              event.stopPropagation();
              const row = button.closest('.campaign-row');
              if (!row) return;
              const newStatus = String(button.dataset.newStatus || '').toUpperCase();
              const label = newStatus === 'PAUSED' ? 'pausar' : 'ativar';
              const campaign = button.dataset.campaign || '';
              if (!confirm(`Confirmar ${{label}} esta campanha no Google Ads?\\n${{campaign}}`)) return;
              const oldText = button.textContent;
              button.disabled = true;
              button.textContent = 'Registrando...';
              try {{
                const response = await fetch(`/api/projects/${{encodeURIComponent(row.dataset.project)}}/campaigns/${{encodeURIComponent(row.dataset.attrId)}}/status-action`, {{
                  method: 'POST',
                  headers: {{'Content-Type': 'application/json'}},
                  body: JSON.stringify({{
                    campaign_id: button.dataset.campaignId || '',
                    customer_id: button.dataset.customerId || '',
                    campaign: campaign,
                    old_status: button.dataset.oldStatus || '',
                    new_status: newStatus,
                    reason: `Acao manual pelo painel: ${{label}} campanha`,
                  }}),
                }});
                const data = await response.json().catch(() => ({{}}));
                if (!response.ok) throw new Error(data.detail || 'Falha ao registrar status');
                button.textContent = newStatus === 'PAUSED' ? 'Pausar registrado' : 'Ativar registrado';
                button.classList.add('saved');
              }} catch (error) {{
                alert(error.message || 'Falha ao registrar status');
                button.textContent = oldText;
                button.disabled = false;
              }}
            }});
          }});

          document.querySelectorAll('.campaign-row').forEach((row) => {{
            row.querySelectorAll('.campaign-inline-action').forEach((button) => {{
              button.addEventListener('click', async (event) => {{
                if (!button.dataset.detailType) {{
                  event.stopPropagation();
                  return;
                }}
                event.preventDefault();
                event.stopPropagation();
                const detailType = button.dataset.detailType || 'blocks';
                if (openRow === row) {{
                  const detailShell = openDetailRow?.querySelector('.campaign-detail-shell');
                  if (detailShell) {{
                    await loadPeriod(row, detailShell, activePeriod, detailType);
                  }}
                  return;
                }}
                await openCampaignRow(row, detailType);
              }});
            }});

            row.addEventListener('click', async () => {{
              if (openRow === row) {{
                closeActive();
                return;
              }}
              await openCampaignRow(row, 'blocks');
            }});
          }});

          if (focusAttr) {{
            const focusRow = Array.from(document.querySelectorAll('.campaign-row')).find((row) => String(row.dataset.attrId || '') === String(focusAttr));
            if (focusRow) {{
              focusRow.classList.add('agent-focus');
              openCampaignRow(focusRow, focusDetail === 'placements' ? 'placements' : 'blocks', focusPeriod).then(() => {{
                focusRow.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                setTimeout(() => focusRow.classList.remove('agent-focus'), 6500);
              }});
            }}
          }}
        }})();
      </script>
    </div>
    """
    return _layout(project, content, "dashboard")


@app.get("/roi-control", response_class=HTMLResponse)
def roi_control_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    runs = load_roi_control_runs(runtime, limit=20)
    actions = load_roi_control_actions(runtime, limit=80)
    outcomes = load_roi_control_outcomes(runtime, limit=80)
    metrics = load_roi_control_latest_metrics(runtime, limit=80)

    run_rows = []
    for row in runs:
        run_rows.append(
            "<tr>"
            f"<td class='nowrap'>{html.escape(str(row.get('generated_at', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('target_slug', '') or ''))}</td>"
            f"<td>{int(row.get('campaigns_reviewed') or 0)}</td>"
            f"<td>{int(row.get('groups_reviewed') or 0)}</td>"
            f"<td>{int(row.get('bid_actions') or 0)}</td>"
            f"<td>{int(row.get('google_ads_operation_count') or 0)}</td>"
            f"<td>{_status_badge('APLICADO' if int(row.get('google_ads_applied') or 0) else 'SEM AJUSTE')}</td>"
            f"<td>{int(row.get('telegram_sent') or 0)}</td>"
            "</tr>"
        )

    action_rows = []
    for row in actions:
        action_rows.append(
            "<tr>"
            f"<td class='nowrap'>{html.escape(str(row.get('generated_at', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('campaign_id', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('group_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('group_name', '') or row.get('campaign_name', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('direction', '') or ''))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('current_value') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('proposed_value') or 0))}</td>"
            f"<td class='{_roi_class(float(row.get('roi') or 0))}'>{_percent(float(row.get('roi') or 0))}</td>"
            f"<td>{html.escape(str(row.get('reason', '') or ''))}</td>"
            f"<td>{_status_badge(str(row.get('google_ads_status', '') or row.get('status', '') or ''))}</td>"
            "</tr>"
        )

    metric_rows = []
    for row in metrics:
        metric_rows.append(
            "<tr>"
            f"<td class='nowrap'>{html.escape(str(row.get('generated_at', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('target_slug', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('entity_type', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('entity_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('entity_name', '') or ''))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('cost') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('revenue') or 0))}</td>"
            f"<td class='{_roi_class(float(row.get('roi') or 0))}'>{_percent(float(row.get('roi') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('target_cpa') or row.get('target_cpc') or 0))}</td>"
            "</tr>"
        )

    outcome_rows = []
    for row in outcomes:
        verdict = str(row.get("verdict", "") or "")
        outcome_rows.append(
            "<tr>"
            f"<td class='nowrap'>{html.escape(str(row.get('measured_at', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('window_label', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('campaign_id', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('group_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('direction', '') or ''))}</td>"
            f"<td class='{_roi_class(float(row.get('before_roi') or 0))}'>{_percent(float(row.get('before_roi') or 0))}</td>"
            f"<td class='{_roi_class(float(row.get('after_roi') or 0))}'>{_percent(float(row.get('after_roi') or 0))}</td>"
            f"<td class='{_roi_class(float(row.get('delta_roi') or 0))}'>{_percent(float(row.get('delta_roi') or 0))}</td>"
            f"<td>{_status_badge(verdict)}</td>"
            f"<td>{'sim' if int(row.get('delivery_stalled') or 0) else 'nao'}</td>"
            "</tr>"
        )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Controle de ROI</div>
          <h1>ROI Control Center</h1>
          <p>Historico gravado no banco do site a cada ciclo de 30 minutos. Aqui o agente consulta campanha, grupo, ROI, CPA/CPC, acoes aplicadas e avisos enviados.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Ultima atualizacao do banco</div>
            <strong>{html.escape(load_last_updated(runtime))}</strong>
          </div>
          <div>
            <div class='aside-label'>Ciclos gravados</div>
            <strong>{len(runs)}</strong>
          </div>
        </div>
      </div>
      <div class='panel'>
        <h2>Ciclos do watcher</h2>
        <p>Cada linha e uma revisao automatica gravada no SQLite do site.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Horario</th><th>Projeto</th><th>Campanhas</th><th>Grupos</th><th>Ajustes</th><th>Ops Google Ads</th><th>Status</th><th>Telegram</th></tr></thead>
          <tbody>{''.join(run_rows)}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Acoes de ROI</h2>
        <p>Ajustes propostos/aplicados por campanha ou grupo, com motivo e status de Google Ads.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Horario</th><th>Campanha ID</th><th>Grupo ID</th><th>Nome</th><th>Direcao</th><th>Atual</th><th>Novo</th><th>ROI</th><th>Motivo</th><th>Status</th></tr></thead>
          <tbody>{''.join(action_rows) if action_rows else "<tr><td colspan='10'>Nenhuma acao elegivel no ultimo ciclo.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Resultado pos-acao</h2>
        <p>Comparacao automatica depois de 1h, 2h e 24h: mostra se o ROI melhorou, piorou ou se a entrega travou.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Medido em</th><th>Janela</th><th>Campanha ID</th><th>Grupo ID</th><th>Direcao</th><th>ROI Antes</th><th>ROI Depois</th><th>Delta</th><th>Veredito</th><th>Travou?</th></tr></thead>
          <tbody>{''.join(outcome_rows) if outcome_rows else "<tr><td colspan='10'>Ainda nao ha acoes antigas o suficiente para medir 1h/2h/24h.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Metricas gravadas para analise</h2>
        <p>Base que permite analisar no site sem abrir Google Ads: custo, receita, ROI e CPA/CPC atual por entidade.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Horario</th><th>Projeto</th><th>Tipo</th><th>ID</th><th>Nome</th><th>Invest.</th><th>Receita</th><th>ROI Hoje</th><th>CPA/CPC</th></tr></thead>
          <tbody>{''.join(metric_rows)}</tbody>
        </table></div>
      </div>
    </div>
    """
    return _layout("ROI Control Center", content, "roi-control")


PUSH_DAY_LABELS = {
    "segunda": "Segunda",
    "terca": "Terca",
    "quarta": "Quarta",
    "quinta": "Quinta",
    "sexta": "Sexta",
    "sabado": "Sabado",
    "domingo": "Domingo",
    "sem_data": "Sem data",
}


def _push_risk_badge(value: str) -> str:
    normalized = str(value or "").strip().lower()
    css = "status-good"
    if normalized == "alto":
        css = "status-bad"
    elif normalized in {"medio", "mÃƒÂ©dio"}:
        css = "status-warn"
    return f"<span class='status-pill {css}'>{html.escape(value or 'Baixo')}</span>"


def _push_tone_badge(value: str) -> str:
    normalized = str(value or "").strip().lower()
    css = "status-neutral"
    if "ordem" in normalized:
        css = "status-good"
    elif "hard" in normalized:
        css = "status-warn"
    return f"<span class='status-pill {css}'>{html.escape(value or 'Sem copy')}</span>"


def _push_site_url(project_id: str) -> str:
    return f"/push-control/site/{quote(project_id, safe='')}"


def _render_push_distribution(rows: list[dict[str, object]], key: str) -> str:
    if not rows:
        return "<div class='muted'>Sem dados suficientes.</div>"
    total = max(sum(int(row.get("value") or 0) for row in rows), 1)
    items = []
    for row in rows[:8]:
        value = int(row.get("value") or 0)
        pct = value / total * 100
        items.append(
            "<div class='push-rank-row'>"
            f"<div><strong>{html.escape(str(row.get('label') or 'Nao informado'))}</strong><span>{value} envios</span></div>"
            f"<div class='push-rank-bar'><i style='width:{pct:.1f}%'></i></div>"
            "</div>"
        )
    return "".join(items)


def _render_push_site_dashboard(project_id: str, data: dict[str, object]) -> str:
    kpis = data.get("kpis") or {}
    by_day = data.get("by_day") or []
    messages = data.get("messages") or []
    max_pushes = max([int(row.get("pushes") or 0) for row in by_day] or [1])
    day_count = max(len([row for row in by_day if int(row.get("pushes") or 0) > 0]), 1)
    domains = data.get("domains") or []
    topics = data.get("topics") or []
    winners = data.get("winners") or []
    losers = data.get("losers") or []
    pending_base = data.get("pending_base") or []
    recommended_variations = data.get("recommended_variations") or []

    def decision_badge(value: str) -> str:
        normalized = str(value or "").strip().lower()
        css = "status-neutral"
        label = "Observar"
        if normalized == "manter_replicar":
            css = "status-good"
            label = "Manter / replicar"
        elif normalized == "pausar_testar_novo":
            css = "status-bad"
            label = "Pausar / trocar"
        elif normalized == "aguardar_base":
            css = "status-warn"
            label = "Aguardar base"
        return f"<span class='status-pill {css}'>{label}</span>"

    def push_action_row(row: dict[str, object], action_label: str) -> str:
        day = str(row.get("day") or "sem_data")
        title = str(row.get("title") or "Sem copy no banco")
        return (
            "<tr>"
            f"<td>{html.escape(action_label)}</td>"
            f"<td><strong>{PUSH_DAY_LABELS.get(day, day)}</strong><br><span class='muted'>{html.escape(str(row.get('time') or ''))}</span></td>"
            f"<td class='mono'>{html.escape(str(row.get('utm_campaign') or ''))}</td>"
            f"<td>{html.escape(title)}</td>"
            f"<td class='nowrap'>{_money(_to_float(row.get('revenue_today')))}</td>"
            f"<td class='nowrap'>{int(_to_float(row.get('impressions_today')))}</td>"
            f"<td class='nowrap'>{_money(_to_float(row.get('ecpm_today')))}</td>"
            f"<td class='nowrap'>{_percent(_to_float(row.get('pmr_today')) / 100 if _to_float(row.get('pmr_today')) > 1 else _to_float(row.get('pmr_today')))}</td>"
            f"<td>{decision_badge(str(row.get('decision') or ''))}<br><span class='muted'>{html.escape(str(row.get('decision_reason') or ''))}</span></td>"
            "</tr>"
        )

    def rows_for_day(day_key: str) -> list[dict[str, object]]:
        return [row for row in messages if str(row.get("day") or "sem_data") == day_key]

    def avg_len(rows: list[dict[str, object]], key: str) -> float:
        if not rows:
            return 0.0
        values = [len(str(row.get(key) or "")) for row in rows if str(row.get(key) or "")]
        return (sum(values) / len(values)) if values else 0.0

    day_buttons = ["<button class='push-filter active' data-day='todos'>Todos</button>"]
    for row in by_day:
        day = str(row.get("day") or "sem_data")
        day_buttons.append(f"<button class='push-filter' data-day='{html.escape(day)}'>{PUSH_DAY_LABELS.get(day, day)}</button>")

    day_bars = []
    for row in by_day:
        day = str(row.get("day") or "sem_data")
        pushes = int(row.get("pushes") or 0)
        height = max((pushes / max_pushes) * 100, 4)
        day_bars.append(
            "<div class='push-bar-col'>"
            f"<div class='push-bar-track'><i style='height:{height:.1f}%'></i></div>"
            f"<span>{PUSH_DAY_LABELS.get(day, day)[:3]}</span>"
            f"<strong>{pushes}</strong>"
            "</div>"
        )

    day_rows = []
    for row in by_day:
        day = str(row.get("day") or "sem_data")
        day_messages = rows_for_day(day)
        avg_title = avg_len(day_messages, "title")
        avg_body = avg_len(day_messages, "body")
        avg_button = avg_len(day_messages, "button")
        day_rows.append(
            "<tr>"
            f"<td><strong>{PUSH_DAY_LABELS.get(day, day)}</strong></td>"
            f"<td class='nowrap'>{int(row.get('pushes') or 0)}</td>"
            f"<td class='nowrap'>{html.escape(str(row.get('start') or ''))} - {html.escape(str(row.get('end') or ''))}</td>"
            f"<td class='nowrap'>{avg_title:.1f}</td>"
            f"<td class='nowrap'>{avg_body:.1f}</td>"
            f"<td class='nowrap'>{avg_button:.1f}</td>"
            f"<td class='nowrap accent-good'>{int(row.get('order') or 0)}</td>"
            f"<td class='nowrap accent-bad'>{int(row.get('hard') or 0)}</td>"
            f"<td class='nowrap'>{int(row.get('high_risk') or 0)}</td>"
            "</tr>"
        )

    detail_rows = []
    for row in messages:
        day = str(row.get("day") or "sem_data")
        link = str(row.get("link") or "")
        link_html = f"<a href='{html.escape(link)}' target='_blank'>abrir</a>" if link else "<span class='muted'>sem link</span>"
        detail_rows.append(
            f"<tr data-day='{html.escape(day)}'>"
            f"<td><strong>{PUSH_DAY_LABELS.get(day, day)}</strong></td>"
            f"<td class='nowrap'>{html.escape(str(row.get('time') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('utm_campaign') or ''))}</td>"
            f"<td>{html.escape(str(row.get('title') or 'Sem copy no banco'))}</td>"
            f"<td>{html.escape(str(row.get('button') or ''))}</td>"
            f"<td>{html.escape(str(row.get('topic') or ''))}</td>"
            f"<td>{html.escape(str(row.get('host') or ''))}</td>"
            f"<td class='nowrap'>{_money(_to_float(row.get('revenue_today')))}</td>"
            f"<td class='nowrap'>{int(_to_float(row.get('impressions_today')))}</td>"
            f"<td class='nowrap'>{_money(_to_float(row.get('ecpm_today')))}</td>"
            f"<td class='nowrap'>{_percent(_to_float(row.get('pmr_today')) / 100 if _to_float(row.get('pmr_today')) > 1 else _to_float(row.get('pmr_today')))}</td>"
            f"<td>{decision_badge(str(row.get('decision') or ''))}</td>"
            f"<td>{_push_tone_badge(str(row.get('tone') or ''))}</td>"
            f"<td>{_push_risk_badge(str(row.get('risk') or ''))}</td>"
            "</tr>"
        )

    winner_rows = [push_action_row(row, "Manter e usar como base") for row in winners[:5]]
    loser_rows = [push_action_row(row, "Pausar e substituir") for row in losers[:3]]
    pending_rows = [push_action_row(row, "Aguardar 100 impressoes") for row in pending_base[:8]]
    variation_rows = []
    for row in recommended_variations[:3]:
        variation_rows.append(
            "<tr>"
            f"<td class='nowrap'>V{int(row.get('slot') or 0)}</td>"
            f"<td class='mono'>{html.escape(str(row.get('base_utm') or ''))}</td>"
            f"<td>{html.escape(str(row.get('topic') or ''))}</td>"
            f"<td>{html.escape(str(row.get('host') or ''))}</td>"
            f"<td>{html.escape(str(row.get('title') or 'Sem copy no banco'))}</td>"
            f"<td>{html.escape(str(row.get('reason') or ''))}</td>"
            "</tr>"
        )

    tone_rows = []
    max_tone = max([int(row.get("pushes") or 0) for row in by_day] or [1])
    for row in by_day:
        day = str(row.get("day") or "sem_data")
        order = int(row.get("order") or 0)
        hard = int(row.get("hard") or 0)
        other = max(int(row.get("pushes") or 0) - order - hard, 0)
        order_h = order / max_tone * 100
        hard_h = hard / max_tone * 100
        other_h = other / max_tone * 100
        tone_rows.append(
            "<div class='push-tone-col'>"
            "<div class='push-stacked'>"
            f"<i class='tone-order' style='height:{order_h:.1f}%'></i>"
            f"<i class='tone-hard' style='height:{hard_h:.1f}%'></i>"
            f"<i class='tone-other' style='height:{other_h:.1f}%'></i>"
            "</div>"
            f"<span>{PUSH_DAY_LABELS.get(day, day)[:3]}</span>"
            "</div>"
        )

    char_rows = []
    max_char = 70.0
    for row in by_day:
        day = str(row.get("day") or "sem_data")
        day_messages = rows_for_day(day)
        char_rows.append(
            "<div class='char-line'>"
            f"<span>{PUSH_DAY_LABELS.get(day, day)[:3]}</span>"
            f"<i class='char-title' style='width:{min(avg_len(day_messages, 'title') / max_char * 100, 100):.1f}%'></i>"
            f"<i class='char-body' style='width:{min(avg_len(day_messages, 'body') / max_char * 100, 100):.1f}%'></i>"
            f"<i class='char-button' style='width:{min(avg_len(day_messages, 'button') / max_char * 100, 100):.1f}%'></i>"
            "</div>"
        )

    top_order = max(by_day, key=lambda row: int(row.get("order") or 0), default={})
    top_risk = max(by_day, key=lambda row: int(row.get("high_risk") or 0), default={})
    longest_window = max(by_day, key=lambda row: len(str(row.get("start") or "") + str(row.get("end") or "")), default={})

    return f"""
    <style>
      body {{
        background: #070d13 !important;
        color: #e7f6ff !important;
      }}
      .push-command * {{
        scrollbar-color: #6b7280 #08111d;
      }}
      .push-command {{
        max-width: 1500px;
        display: grid;
        gap: 20px;
        color: #e7f6ff;
        background: #070d13;
        padding-top: 14px;
      }}
      .push-top {{
        display: flex;
        justify-content: space-between;
        gap: 20px;
        align-items: flex-start;
      }}
      .push-breadcrumb {{
        color: #12d6f3;
        font-size: 13px;
        font-weight: 800;
      }}
      .push-title {{
        margin: 8px 0 8px;
        font-size: 36px;
        line-height: 1;
        color: #fff;
        letter-spacing: -0.04em;
      }}
      .push-subtitle {{
        color: #a8d7f4;
        margin: 0;
        font-size: 14px;
      }}
      .push-badges {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        justify-content: flex-end;
      }}
      .push-badges span {{
        background: #0e9dc5;
        color: #fff;
        border-radius: 999px;
        padding: 8px 12px;
        font-size: 12px;
        font-weight: 900;
      }}
      .push-command .nav {{
        margin: 4px 0 0;
      }}
      .push-command .nav a {{
        background: #111b2d;
        color: #b7d8f6;
        border-color: #263b5c;
      }}
      .push-command .nav a.active {{
        background: #0496c7;
        color: #fff;
      }}
      .push-toolbar {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
        padding: 14px;
        border-radius: 16px;
        border: 1px solid #294261;
        background: #10192b;
      }}
      .push-toolbar strong {{ color: #fff; margin-right: 6px; }}
      .push-filter {{
        border: 1px solid #223856;
        background: #17233a;
        color: #d3ebff;
        border-radius: 9px;
        padding: 10px 18px;
        font-weight: 800;
        cursor: pointer;
      }}
      .push-filter.active {{
        background: #0797c7;
        color: #fff;
      }}
      .push-command .metrics {{
        grid-template-columns: repeat(6, 1fr);
        gap: 16px;
        margin-bottom: 0;
        display: none;
      }}
      .push-command .metric {{
        min-height: 104px;
        padding: 16px;
        background: #10192b !important;
        border: 1px solid #294261 !important;
        color: #fff;
        box-shadow: none;
      }}
      .push-command .metric .label {{
        color: #7fbee7;
      }}
      .push-command .metric .value {{
        color: #fff;
        font-size: 24px;
      }}
      .push-command .metric .sub {{
        color: #9cc7e6;
      }}
      .push-kpi-icon {{
        float: right;
        display: inline-flex;
        width: 38px;
        height: 38px;
        align-items: center;
        justify-content: center;
        border-radius: 12px;
        background: #13243d;
        border: 1px solid #275071;
      }}
      .push-grid {{
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 16px;
      }}
      .push-command-center {{
        display: grid;
        grid-template-columns: 1.15fr .85fr;
        gap: 16px;
      }}
      .push-command-panel {{
        border: 1px solid #294261;
        border-radius: 18px;
        background:
          radial-gradient(circle at top right, rgba(34, 211, 238, .16), transparent 34%),
          #10192b;
        padding: 18px;
      }}
      .push-command-panel h2 {{
        margin: 0 0 6px;
        color: #fff;
        font-size: 20px;
      }}
      .push-command-panel .lead {{
        color: #b7d8f6;
        margin: 0 0 16px;
        line-height: 1.45;
      }}
      .push-decision-list {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .push-money-strip {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        margin-bottom: 14px;
      }}
      .push-money-strip div {{
        border: 1px solid #223856;
        border-radius: 12px;
        background: rgba(7, 13, 25, .52);
        padding: 12px;
      }}
      .push-money-strip span {{
        display: block;
        color: #8fc9ee;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: .12em;
        margin-bottom: 7px;
      }}
      .push-money-strip strong {{
        color: #fff;
        font-size: 20px;
      }}
      .push-decision-item {{
        border: 1px solid #223856;
        border-radius: 14px;
        background: rgba(7, 13, 25, .72);
        padding: 14px;
      }}
      .push-decision-item span {{
        display: block;
        color: #8fc9ee;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: .12em;
        margin-bottom: 8px;
      }}
      .push-decision-item strong {{
        display: block;
        color: #fff;
        font-size: 24px;
      }}
      .push-action-tabs {{
        display: grid;
        gap: 12px;
      }}
      .push-action-card {{
        border: 1px solid #223856;
        border-radius: 14px;
        background: rgba(7, 13, 25, .72);
        padding: 14px;
      }}
      .push-action-card b {{
        display: block;
        color: #fff;
        margin-bottom: 6px;
      }}
      .push-section-title {{
        display: flex;
        align-items: end;
        justify-content: space-between;
        gap: 12px;
        margin: 6px 0 12px;
      }}
      .push-section-title h2 {{
        margin: 0;
        color: #fff;
      }}
      .push-section-title p {{
        margin: 4px 0 0;
        color: #9cc7e6;
        font-size: 12px;
      }}
      .push-chart-card {{
        grid-column: span 4;
        padding: 18px;
        border: 1px solid #294261;
        border-radius: 16px;
        background: #10192b;
        box-shadow: none;
      }}
      .push-chart-card.wide {{ grid-column: span 6; }}
      .push-chart-card.full {{ grid-column: 1 / -1; }}
      .push-chart-card h2 {{
        margin: 0 0 4px;
        color: #fff;
        font-size: 16px;
      }}
      .push-command .muted, .push-chart-card p {{
        color: #9cc7e6;
        font-size: 12px;
      }}
      .push-bars, .push-tone-bars {{
        height: 260px;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(44px, 1fr));
        gap: 14px;
        align-items: end;
        border-left: 1px solid #314763;
        border-bottom: 1px solid #314763;
        padding: 18px 8px 8px 14px;
        background:
          linear-gradient(#17223a 1px, transparent 1px) 0 0 / 100% 25%,
          transparent;
      }}
      .push-bar-col, .push-tone-col {{
        height: 100%;
        display: grid;
        grid-template-rows: 1fr auto auto;
        gap: 10px;
        text-align: center;
        color: #b7d8f6;
        font-size: 11px;
      }}
      .push-bar-track, .push-stacked {{
        height: 100%;
        display: flex;
        align-items: end;
        justify-content: center;
      }}
      .push-bar-track i {{
        width: 100%;
        max-width: 58px;
        display: block;
        border-radius: 8px 8px 2px 2px;
        background: linear-gradient(180deg, #38d9ec, #19b7cf);
      }}
      .push-stacked {{
        flex-direction: column-reverse;
        overflow: hidden;
        border-radius: 8px 8px 2px 2px;
        max-width: 58px;
        width: 100%;
        margin: 0 auto;
      }}
      .push-stacked i {{ display: block; width: 100%; }}
      .tone-order {{ background: #38d9ec; }}
      .tone-hard {{ background: #fb7185; }}
      .tone-other {{ background: #8b5cf6; }}
      .push-rank-bar {{
        height: 9px;
        border-radius: 999px;
        background: #1d2a40;
        overflow: hidden;
      }}
      .push-rank-bar i {{
        display: block;
        height: 100%;
        border-radius: inherit;
        background: linear-gradient(90deg, #38d9ec, #34d399);
      }}
      .push-rank-row {{
        display: grid;
        gap: 7px;
        padding: 9px 0;
        border-bottom: 1px solid #223551;
      }}
      .push-rank-row strong {{
        display: block;
        color: #fff;
      }}
      .push-rank-row span {{
        color: #9cc7e6;
        font-size: 12px;
      }}
      .char-panel {{
        display: grid;
        gap: 12px;
      }}
      .char-line {{
        display: grid;
        grid-template-columns: 70px 1fr;
        gap: 6px 12px;
        align-items: center;
      }}
      .char-line span {{
        grid-row: span 3;
        color: #b7d8f6;
        font-size: 12px;
      }}
      .char-line i {{
        height: 8px;
        border-radius: 999px;
        display: block;
      }}
      .char-title {{ background: #38d9ec; }}
      .char-body {{ background: #a78bfa; }}
      .char-button {{ background: #22c55e; }}
      .decision-grid {{
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
      }}
      .decision-card {{
        border: 1px solid #294261;
        border-radius: 16px;
        background: #10192b;
        padding: 18px;
      }}
      .decision-card .label {{
        color: #9cc7e6;
        text-transform: uppercase;
        letter-spacing: .12em;
        font-size: 11px;
      }}
      .decision-card strong {{
        display: block;
        color: #fff;
        font-size: 20px;
        margin: 10px 0;
      }}
      .push-command .table-scroll {{
        border: 1px solid #294261;
        border-radius: 16px;
        background: #10192b;
      }}
      .push-command table {{
        color: #dff3ff;
      }}
      .push-command th {{
        background: #070d19;
        color: #95a8c2;
      }}
      .push-command td {{
        border-color: #223551;
      }}
      .push-command tr:hover td {{
        background: #13203a;
      }}
      .push-command .status-pill {{
        border: 0;
        color: #fff;
        padding: 5px 9px;
        font-size: 11px;
      }}
      .push-command .status-good {{ background: #0ea77a; color: #fff; }}
      .push-command .status-bad {{ background: #dc2626; color: #fff; }}
      .push-command .status-warn {{ background: #f59e0b; color: #fff; }}
      .push-command .status-neutral {{ background: #0891b2; color: #fff; }}
      .accent-good {{ color: #38d9ec; font-weight: 900; }}
      .accent-bad {{ color: #fb7185; font-weight: 900; }}
      .push-detail-table th, .push-detail-table td {{
        vertical-align: top;
      }}
      .push-detail-table {{
        min-width: 1180px;
      }}
      @media (max-width: 1100px) {{
        .push-command .metrics {{ grid-template-columns: repeat(2, 1fr); }}
        .push-command-center {{ grid-template-columns: 1fr; }}
        .push-decision-list {{ grid-template-columns: 1fr; }}
        .push-money-strip {{ grid-template-columns: 1fr 1fr; }}
        .decision-grid {{ grid-template-columns: 1fr; }}
        .push-chart-card, .push-chart-card.wide {{ grid-column: 1 / -1; }}
      }}
    </style>
    <div class='shell push-command'>
      <div class='push-top'>
        <div>
          <div class='push-breadcrumb'>Dashboards / PUSH por Dia / {html.escape(project_id)}</div>
          <h1 class='push-title'>Grade de Push - Visao por Dia da Semana</h1>
          <p class='push-subtitle'>Analise da programacao por dia: volume, horarios, tipo de copy, risco editorial, dominios, topicos e resultado GAM.</p>
        </div>
        <div class='push-badges'>
          <span>{int(kpis.get('pushes') or 0)} pushes</span>
          <span>{day_count} dias</span>
          <span>Media {(int(kpis.get('pushes') or 0) / day_count):.1f}/dia</span>
        </div>
      </div>
      [[NAV]]
      <div class='push-toolbar'>
        <strong>Filtros</strong>
        {''.join(day_buttons)}
        <a class='push-filter' href='/push-control'>Sites</a>
      </div>
      <div class='push-command-center'>
        <div class='push-command-panel'>
          <h2>Decisao operacional de hoje</h2>
          <p class='lead'>O painel prioriza o que aumenta receita: manter mensagens que provaram eCPM/PMR, pausar as 3 piores com 100+ impressoes e criar 3 variacoes baseadas nos vencedores.</p>
          <div class='push-money-strip'>
            <div><span>Receita hoje</span><strong>{_money(_to_float(kpis.get('revenue_today')))}</strong></div>
            <div><span>eCPM hoje</span><strong>{_money(_to_float(kpis.get('ecpm_today')))}</strong></div>
            <div><span>Pushes</span><strong>{int(kpis.get('pushes') or 0)}</strong></div>
            <div><span>100+ imp.</span><strong>{int(kpis.get('eligible') or 0)}</strong></div>
          </div>
          <div class='push-decision-list'>
            <div class='push-decision-item'><span>Manter/replicar</span><strong>{len(winners[:5])}</strong><p class='muted'>usar como base de titulo, topico, horario e destino.</p></div>
            <div class='push-decision-item'><span>Pausar/substituir</span><strong>{len(losers[:3])}</strong><p class='muted'>piores elegiveis por eCPM, receita e PMR.</p></div>
            <div class='push-decision-item'><span>Aguardando base</span><strong>{int(kpis.get('pending_base') or 0)}</strong><p class='muted'>ainda sem 100 impressoes no GAM.</p></div>
            <div class='push-decision-item'><span>Novas paginas</span><strong>{len(recommended_variations[:3])}</strong><p class='muted'>variacoes sugeridas a partir dos melhores.</p></div>
          </div>
        </div>
        <div class='push-command-panel push-action-tabs'>
          <div class='push-action-card'><b>UX validou</b><span class='muted'>A pagina precisa responder: o que continua, o que sai, o que criar e o que ainda nao tem dado.</span></div>
          <div class='push-action-card'><b>Orquestrador validou</b><span class='muted'>Toda acao protege receita: so pausar/criar com base minima, UTM identificada e leitura GAM.</span></div>
          <div class='push-action-card'><b>Proximo ciclo</b><span class='muted'>Comparar eCPM, PMR, receita e topico para decidir os 8 envios do dia equivalente da proxima semana.</span></div>
        </div>
      </div>
      <div class='metrics'>
        <div class='metric'><span class='push-kpi-icon'>Ã°Å¸â€â€</span><div class='label'>Pushes</div><div class='value'>{int(kpis.get('pushes') or 0)}</div><div class='sub'>volume total da grade</div></div>
        <div class='metric'><span class='push-kpi-icon'>Ã°Å¸â€œâ€¦</span><div class='label'>Media por dia</div><div class='value'>{(int(kpis.get('pushes') or 0) / day_count):.1f}</div><div class='sub'>cadencia media semanal</div></div>
        <div class='metric'><span class='push-kpi-icon'>Ã°Å¸Â§Â­</span><div class='label'>Copy de ordem</div><div class='value'>{int(kpis.get('order') or 0)}</div><div class='sub'>diagnostico, erro, processo</div></div>
        <div class='metric'><span class='push-kpi-icon'>Ã°Å¸â€Â¥</span><div class='label'>Copy hard</div><div class='value'>{int(kpis.get('hard') or 0)}</div><div class='sub'>urgencia, promessa forte, cash</div></div>
        <div class='metric'><span class='push-kpi-icon'>Ã¢Å¡Â Ã¯Â¸Â</span><div class='label'>Risco alto</div><div class='value'>{int(kpis.get('high_risk') or 0)}</div><div class='sub'>termos agressivos / sensiveis</div></div>
        <div class='metric'><span class='push-kpi-icon'>Ã°Å¸Å’Â</span><div class='label'>Dominios</div><div class='value'>{len(domains)}</div><div class='sub'>hosts diferentes usados</div></div>
      </div>
      <div class='push-grid'>
        <div class='push-chart-card full'>
          <div class='push-section-title'><div><h2>Fila de decisao</h2><p>Vencedores, piores elegiveis e mensagens ainda sem base suficiente.</p></div><span class='status-pill status-neutral'>operacional</span></div>
          <div class='table-scroll'><table>
            <thead><tr><th>Acao</th><th>Dia/Hora</th><th>UTM</th><th>Title</th><th>Receita</th><th>Imp.</th><th>eCPM</th><th>PMR</th><th>Motivo</th></tr></thead>
            <tbody>
              {''.join(winner_rows) if winner_rows else "<tr><td colspan='9'>Ainda nao ha vencedores com base minima.</td></tr>"}
              {''.join(loser_rows) if loser_rows else "<tr><td colspan='9'>Ainda nao ha piores elegiveis para pausa.</td></tr>"}
              {''.join(pending_rows) if pending_rows else ""}
            </tbody>
          </table></div>
        </div>
        <div class='push-chart-card full'>
          <div class='push-section-title'><div><h2>3 novas variacoes sugeridas</h2><p>Baseadas nos melhores conteudos, para substituir os 3 piores quando elegiveis.</p></div><span class='status-pill status-good'>90/10</span></div>
          <div class='table-scroll'><table>
            <thead><tr><th>Slot</th><th>UTM base</th><th>Topico</th><th>Dominio</th><th>Title base</th><th>Motivo</th></tr></thead>
            <tbody>{''.join(variation_rows) if variation_rows else "<tr><td colspan='6'>Sem base vencedora suficiente para sugerir novas variacoes.</td></tr>"}</tbody>
          </table></div>
        </div>
        <div class='push-chart-card wide'>
          <h2>Pushes por Dia</h2>
          <p class='muted'>Volume programado/observado por dia da semana.</p>
          <div class='push-bars'>{''.join(day_bars) if day_bars else "<p class='muted'>Sem dados de grade para este site.</p>"}</div>
        </div>
        <div class='push-chart-card'>
          <h2>Tom da Copy por Dia</h2>
          <p class='muted'>Hard/urgencia vs ordem/diagnostico.</p>
          <div class='push-tone-bars'>{''.join(tone_rows)}</div>
        </div>
        <div class='push-chart-card'>
          <h2>Dominios</h2>
          <p class='muted'>Distribuicao de destino no filtro atual.</p>
          {_render_push_distribution(domains, 'domains')}
        </div>
        <div class='push-chart-card wide'>
          <h2>Comprimento Medio da Copy por Dia</h2>
          <p class='muted'>Controle de title, message e button contra limites operacionais.</p>
          <div class='char-panel'>{''.join(char_rows)}</div>
        </div>
        <div class='push-chart-card wide'>
          <h2>Topicos no Filtro Atual</h2>
          <p class='muted'>Para entender saturacao de oferta e repeticao tematica.</p>
          {_render_push_distribution(topics, 'topics')}
        </div>
      </div>

      <div class='decision-grid'>
        <div class='decision-card'><div class='label'>Dia mais limpo para escalar</div><strong>{PUSH_DAY_LABELS.get(str(top_order.get('day') or ''), str(top_order.get('day') or 'Sem dado'))}</strong><p class='muted'>Mais foco em ordem/diagnostico: <b class='accent-good'>{int(top_order.get('order') or 0)}</b> pushes.</p></div>
        <div class='decision-card'><div class='label'>Dia de maior risco editorial</div><strong>{PUSH_DAY_LABELS.get(str(top_risk.get('day') or ''), str(top_risk.get('day') or 'Sem dado'))}</strong><p class='muted'>Concentra <b class='accent-bad'>{int(top_risk.get('high_risk') or 0)}</b> pushes de risco alto. Revisar antes de escalar.</p></div>
        <div class='decision-card'><div class='label'>Janela mais longa</div><strong>{PUSH_DAY_LABELS.get(str(longest_window.get('day') or ''), str(longest_window.get('day') or 'Sem dado'))}</strong><p class='muted'>Cobertura de {html.escape(str(longest_window.get('start') or '--'))} ate {html.escape(str(longest_window.get('end') or '--'))}.</p></div>
        <div class='decision-card'><div class='label'>Decisao operacional</div><strong>Migrar para qualidade</strong><p class='muted'>Priorizar copys sustentaveis e usar hard copy so quando eCPM/receita justificarem.</p></div>
      </div>
      <div class='push-grid'>
        <div class='push-chart-card full'>
          <h2>Tabela por Dia - Pushes Programados</h2>
          <div class='table-scroll'><table>
            <thead><tr><th>Dia</th><th>Pushes</th><th>Janela</th><th>Title medio</th><th>Message medio</th><th>Button medio</th><th>Ordem</th><th>Hard</th><th>Risco alto</th></tr></thead>
            <tbody>{''.join(day_rows) if day_rows else "<tr><td colspan='9'>Sem dados por dia.</td></tr>"}</tbody>
          </table></div>
        </div>
        <div class='push-chart-card full'>
          <h2>Detalhe dos Pushes</h2>
          <p class='muted'>Campanha, horario, copy, destino, tom e risco.</p>
          <div class='table-scroll'><table class='push-detail-table'>
            <thead><tr><th>Dia</th><th>Hora</th><th>UTM</th><th>Title</th><th>Button</th><th>Topico</th><th>Dominio</th><th>Receita</th><th>Imp.</th><th>eCPM</th><th>PMR</th><th>Decisao</th><th>Tom</th><th>Risco</th></tr></thead>
            <tbody>{''.join(detail_rows) if detail_rows else "<tr><td colspan='14'>Sem mensagens para este site.</td></tr>"}</tbody>
          </table></div>
        </div>
      </div>
    </div>
    <script>
      document.querySelectorAll('.push-filter').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          var day = btn.getAttribute('data-day');
          document.querySelectorAll('.push-filter').forEach(function(item) {{ item.classList.remove('active'); }});
          btn.classList.add('active');
          document.querySelectorAll('.push-detail-table tbody tr').forEach(function(row) {{
            row.style.display = day === 'todos' || row.getAttribute('data-day') === day ? '' : 'none';
          }});
        }});
      }});
    </script>
    """


@app.get("/push-control", response_class=HTMLResponse)
def push_control_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    data = load_push_control_summary(runtime, app_today(), limit=120)
    optimization = load_push_optimization_summary(runtime, app_today(), limit=120)
    installation_checks = load_push_installation_checks(runtime, [row.get("project_id", "") for row in data.get("projects", [])])
    sync = data.get("sync") or {}
    project_rows = []
    for row in data.get("projects", []):
        project_id = str(row.get("project_id", "") or "")
        project_rows.append(
            "<tr>"
            f"<td><a class='project-link' href='{_push_site_url(project_id)}'>{html.escape(project_id)}</a></td>"
            f"<td>{int(row.get('messages') or 0)}</td>"
            f"<td>{int(row.get('scheduled') or 0)}</td>"
            f"<td>{int(row.get('sent') or 0)}</td>"
            f"<td>{int(row.get('errors') or 0)}</td>"
            f"<td class='nowrap'>{_money(float(row.get('revenue_today') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('revenue_month') or 0))}</td>"
            f"<td>{int(row.get('clicks_today') or 0)}</td>"
            "</tr>"
        )
    site_cards = []
    for row in data.get("projects", []):
        project_id = str(row.get("project_id", "") or "")
        site_cards.append(
            "<a class='metric push-card' href='{url}'>"
            "<div class='label'>Site Push</div>"
            "<div class='value site-card-title'>{project}</div>"
            "<div class='sub'>{scheduled} agendados | {messages} mensagens | hoje {today}</div>"
            "</a>".format(
                url=_push_site_url(project_id),
                project=html.escape(project_id),
                scheduled=int(row.get("scheduled") or 0),
                messages=int(row.get("messages") or 0),
                today=_money(float(row.get("revenue_today") or 0)),
            )
        )

    message_rows = []
    for row in data.get("messages", []):
        message_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('project_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('site', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('utm_campaign', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('message_family', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('message_number', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('send_time', '') or ''))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('revenue_today') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('total_revenue') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('ecpm_today') or 0))}</td>"
            f"<td>{_status_badge(str(row.get('acquisition_link_status', '') or 'unlinked'))}</td>"
            "</tr>"
        )

    link_rows = []
    for row in data.get("links", []):
        link_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('project_id', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('message_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('acquisition_platform', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('acquisition_attr_id', '') or ''))}</td>"
            f"<td>{float(row.get('confidence') or 0):.2f}</td>"
            f"<td>{_status_badge(str(row.get('status', '') or 'candidate'))}</td>"
            "</tr>"
        )

    opt_plan_rows = []
    for row in optimization.get("plans", []):
        payload = {}
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except Exception:
            payload = {}
        slots = ", ".join(payload.get("best_slots") or [])
        opt_plan_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('project_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('source_day', '') or ''))}</td>"
            f"<td>{int(row.get('required_sends') or 0)}</td>"
            f"<td>{int(row.get('eligible_messages') or 0)}</td>"
            f"<td>{int(row.get('pause_count') or 0)}</td>"
            f"<td>{int(row.get('new_content_count') or 0)}</td>"
            f"<td>{html.escape(slots)}</td>"
            f"<td>{_status_badge(str(row.get('status', '') or 'proposed'))}</td>"
            "</tr>"
        )

    opt_action_rows = []
    for row in optimization.get("actions", []):
        opt_action_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('project_id', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('action_type', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('source_utm_campaign', '') or ''))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('revenue') or 0))}</td>"
            f"<td class='nowrap'>{_money(float(row.get('ecpm') or 0))}</td>"
            f"<td>{int(float(row.get('impressions') or 0))}</td>"
            f"<td>{html.escape(str(row.get('target_send_slot', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('target_utm_campaign', '') or ''))}</td>"
            f"<td>{html.escape(str(row.get('reason', '') or ''))}</td>"
            "</tr>"
        )

    install_rows = []
    for row in installation_checks:
        install_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('project_id', '') or ''))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('sw_url', '') or ''))}</td>"
            f"<td>{int(row.get('http_status') or 0)}</td>"
            f"<td>{_status_badge('INSTALADO' if row.get('is_installed') else 'BLOQUEADO')}</td>"
            f"<td>{html.escape(str(row.get('result', '') or ''))}</td>"
            "</tr>"
        )

    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Push Revenue Control</div>
          <h1>Controle de Push</h1>
          <p>Biblioteca viva das UTMs de Push vistas no GAM. Esta tela prepara o sistema para decidir quais mensagens enviar, medir receita por mensagem e associar Push com aquisicao paga.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Biblioteca sincronizada</div>
            <strong>{int(sync.get('messages') or 0)}</strong>
          </div>
          <div>
            <div class='aside-label'>Linhas diarias</div>
            <strong>{int(sync.get('daily_rows') or 0)}</strong>
          </div>
        </div>
      </div>
      <div class='metrics'>
        <div class='metric push-card'>
          <div class='label'>Mensagens Push</div>
          <div class='value'>{int(sync.get('messages') or 0)}</div>
          <div class='sub'>UTMs confirmadas pelo GAM</div>
        </div>
        <div class='metric push-card'>
          <div class='label'>Metricas Diarias</div>
          <div class='value'>{int(sync.get('daily_rows') or 0)}</div>
          <div class='sub'>Base para aprender melhor horario e mensagem</div>
        </div>
        <div class='metric push-card'>
          <div class='label'>Associacoes Candidatas</div>
          <div class='value'>{int(sync.get('candidate_links') or 0)}</div>
          <div class='sub'>Push x aquisicao por UTM/Attr ID</div>
        </div>
      </div>
      <div class='panel'>
        <h2>Sites com controle de Push</h2>
        <p>Cada site tem uma tela propria para acompanhar grade, UTMs, conteudo, risco editorial e resultado GAM.</p>
        <div class='metrics site-card-grid'>{''.join(site_cards) if site_cards else "<div class='muted'>Nenhum site Push catalogado ainda.</div>"}</div>
      </div>
      <div class='panel'>
        <h2>Verificacao de instalacao Push</h2>
        <p>Antes de qualquer envio live, o sistema verifica se o arquivo <span class='mono'>firebase-messaging-sw.js</span> existe e carrega corretamente no projeto.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Service Worker</th><th>HTTP</th><th>Status</th><th>Resultado</th></tr></thead>
          <tbody>{''.join(install_rows) if install_rows else "<tr><td colspan='5'>Nenhum projeto Push para verificar.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Otimizacao diaria dos 8 Pushs</h2>
        <p>Regra: manter 8 envios por projeto/dia, mudar horarios conforme melhores eCPMs, pausar os 3 piores conteudos com pelo menos 100 impressoes no GAM e criar 3 novos conteudos baseados nos vencedores.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Dia analisado</th><th>Envios/dia</th><th>Msgs elegiveis</th><th>Pausar</th><th>Novos</th><th>Horarios sugeridos</th><th>Status</th></tr></thead>
          <tbody>{''.join(opt_plan_rows) if opt_plan_rows else "<tr><td colspan='7'>Ainda nao ha pushs de ontem com base suficiente para otimizar.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Acoes de conteudo Push</h2>
        <p>Fila de decisoes: pausar piores conteudos e criar novas variacoes com teste de titulo, descricao e icone.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Acao</th><th>UTM origem</th><th>Receita</th><th>eCPM</th><th>Impr.</th><th>Horario</th><th>Nova UTM</th><th>Motivo</th></tr></thead>
          <tbody>{''.join(opt_action_rows) if opt_action_rows else "<tr><td colspan='9'>Sem acoes de conteudo propostas.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Resultado por projeto</h2>
        <p>Receita de Push por projeto, puxada do GAM e organizada para tomada de decisao.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Mensagens</th><th>Agendados</th><th>Enviados</th><th>Erros</th><th>Receita Hoje</th><th>Receita Mes</th><th>Cliques Hoje</th></tr></thead>
          <tbody>{''.join(project_rows) if project_rows else "<tr><td colspan='7'>Nenhuma UTM de Push confirmada ainda.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Biblioteca de mensagens</h2>
        <p>O sistema identifica familia, numero da mensagem e horario provavel a partir da UTM.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Site GAM</th><th>UTM Campaign</th><th>Familia</th><th>Msg</th><th>Horario</th><th>Hoje</th><th>Total Mes</th><th>eCPM Hoje</th><th>Aquisicao</th></tr></thead>
          <tbody>{''.join(message_rows) if message_rows else "<tr><td colspan='10'>Nenhuma mensagem catalogada.</td></tr>"}</tbody>
        </table></div>
      </div>
      <div class='panel'>
        <h2>Associacao Push x aquisicao</h2>
        <p>Primeira camada de ligacao: quando o GAM traz Attr ID numerico, o sistema cria uma associacao candidata com a aquisicao paga para validacao e aprendizado.</p>
        <div class='table-scroll'><table>
          <thead><tr><th>Projeto</th><th>Mensagem ID</th><th>Plataforma</th><th>Attr ID</th><th>Confianca</th><th>Status</th></tr></thead>
          <tbody>{''.join(link_rows) if link_rows else "<tr><td colspan='6'>Ainda nao ha associacoes candidatas suficientes.</td></tr>"}</tbody>
        </table></div>
      </div>
    </div>
    """
    return _layout("Controle de Push", content, "push-control")


@app.get("/push-control/site/{project_id}", response_class=HTMLResponse)
def push_control_site_page(project_id: str) -> str:
    runtime = load_runtime(CONFIG_PATH)
    data = load_push_site_dashboard(runtime, project_id, app_today())
    content = _render_push_site_dashboard(project_id, data)
    return _layout(f"Push - {project_id}", content, "push-control")


@app.get("/actions", response_class=HTMLResponse)
def actions_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    rows = load_action_rows(runtime, limit=200)
    lines = []
    for row in rows:
        payload = row.get("payload") or {}
        lines.append(
            "<tr>"
            f"<td>{_status_badge(str(row['status']))}</td>"
            f"<td>{html.escape(str(row['action_type']))}</td>"
            f"<td class='mono'>{html.escape(str(row['entity_id']))}</td>"
            f"<td class='mono'>{html.escape(str(payload.get('current_rule_key', '')))}</td>"
            f"<td class='mono'>{html.escape(str(payload.get('proposed_rule_key', '')))}</td>"
            f"<td class='nowrap'>{_money(float(row['old_value']))}</td>"
            f"<td class='nowrap'>{_money(float(row['new_value']))}</td>"
            f"<td class='nowrap'>{html.escape(str(row['created_at']))}</td>"
            f"<td class='nowrap'>{html.escape(str(row['executed_at']))}</td>"
            "</tr>"
        )
    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Operacao da IA</div>
          <h1>Acoes IA</h1>
          <p>Fila historica de acoes, com regra atual, regra sugerida e status da execucao interna.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Ultima atualizacao</div>
            <strong>{html.escape(load_last_updated(runtime))}</strong>
          </div>
        </div>
      </div>
      <div class='panel'>
        <table>
          <thead>
            <tr>
              <th>Status</th><th>Acao</th><th>Entity</th><th>Rule Atual</th><th>Rule Sugerida</th>
              <th>Piso Atual</th><th>Piso Sugerido</th><th>Criado</th><th>Executado</th>
            </tr>
          </thead>
          <tbody>{''.join(lines)}</tbody>
        </table>
      </div>
    </div>
    """
    return _layout("Acoes IA", content, "actions")


@app.get("/api/google-ads/customers")
def api_google_ads_customers() -> dict[str, object]:
    try:
        runtime = load_runtime(CONFIG_PATH)
        customers = _google_ads_mcc_customers(runtime)
        if customers:
            return {"status": "ok", "source": "google_ads_api", "customers": customers}
        config_customers = google_ads_customers(CONFIG_PATH)
        return {"status": "ok", "source": "config", "warning": "Google Ads API nao retornou contas; usando config.yaml.", "customers": config_customers}
    except Exception as exc:
        try:
            customers = google_ads_customers(CONFIG_PATH)
        except Exception:
            customers = []
        return {"status": "fallback", "source": "config", "error": str(exc)[:500], "customers": customers}


def _config_google_ads_customers_by_id() -> dict[str, dict[str, object]]:
    return {re.sub(r"\D+", "", str(item.get("customer_id") or "")): item for item in google_ads_customers(CONFIG_PATH)}


def _google_ads_mcc_customers(runtime: dict[str, object]) -> list[dict[str, object]]:
    config = runtime.get("config") or {}
    google_cfg = config.get("google_ads") or {}
    by_id = _config_google_ads_customers_by_id()
    client = _google_ads_client_from_runtime(runtime)
    service = client.get_service("GoogleAdsService")
    login_customer_id = re.sub(r"\D+", "", str(google_cfg.get("login_customer_id") or ""))
    customers: list[dict[str, object]] = []

    def append_customer(customer_id: str, name: str = "", currency: str = "", status: str = "", manager: bool = False, hidden: bool = False, source_mcc: str = "") -> None:
        customer_id_clean = re.sub(r"\D+", "", str(customer_id or ""))
        if not customer_id_clean or any(str(item.get("customer_id")) == customer_id_clean for item in customers):
            return
        known = by_id.get(customer_id_clean, {})
        customers.append({
            "customer_id": customer_id_clean,
            "name": str(name or known.get("name") or known.get("site") or "").strip(),
            "site": str(known.get("site") or "").strip(),
            "currency": str(currency or known.get("currency") or ""),
            "status": str(status or ""),
            "manager": bool(manager),
            "hidden": bool(hidden),
            "source_mcc": source_mcc,
        })

    if login_customer_id:
        query = """
            SELECT customer_client.id, customer_client.descriptive_name,
                   customer_client.currency_code, customer_client.status,
                   customer_client.manager, customer_client.hidden
            FROM customer_client
            WHERE customer_client.manager = FALSE
        """
        for row in service.search(customer_id=login_customer_id, query=query):
            cc = row.customer_client
            append_customer(
                str(getattr(cc, "id", "") or ""),
                str(getattr(cc, "descriptive_name", "") or ""),
                str(getattr(cc, "currency_code", "") or ""),
                str(getattr(cc, "status", "") or ""),
                bool(getattr(cc, "manager", False)),
                bool(getattr(cc, "hidden", False)),
                login_customer_id,
            )
    else:
        customer_service = client.get_service("CustomerService")
        accessible = customer_service.list_accessible_customers()
        for resource_name in getattr(accessible, "resource_names", []) or []:
            customer_id = re.sub(r"\D+", "", str(resource_name or ""))
            if not customer_id:
                continue
            try:
                query = """
                    SELECT customer.id, customer.descriptive_name,
                           customer.currency_code, customer.status,
                           customer.manager
                    FROM customer
                    LIMIT 1
                """
                rows = list(service.search(customer_id=customer_id, query=query))
                if rows:
                    c = rows[0].customer
                    append_customer(
                        customer_id,
                        str(getattr(c, "descriptive_name", "") or ""),
                        str(getattr(c, "currency_code", "") or ""),
                        str(getattr(c, "status", "") or ""),
                        bool(getattr(c, "manager", False)),
                        False,
                        "oauth_accessible",
                    )
                else:
                    append_customer(customer_id, source_mcc="oauth_accessible")
            except Exception:
                append_customer(customer_id, source_mcc="oauth_accessible")
    customers = [item for item in customers if not item.get("manager")]
    customers.sort(key=lambda item: (str(item.get("site") or item.get("name") or "").lower(), str(item.get("customer_id") or "")))
    return customers


@app.get("/api/setup-inicial/sites")
def api_setup_initial_sites() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    config = runtime.get("config") or {}
    sites: set[str] = set()
    for key in ("dashboard_projects", "google_ads_projects"):
        for site in config.get(key) or []:
            value = str(site or "").strip()
            if value:
                sites.add(value)
    for item in google_ads_customers(CONFIG_PATH):
        value = str(item.get("site") or "").strip()
        if value and "." in value:
            sites.add(value)
    for value in (runtime.get("site_mapping") or {}).values():
        value = str(value or "").strip()
        if value and "." in value:
            sites.add(value)
    return {"status": "ok", "sites": sorted(sites)}


def _google_http_error_details(exc: Exception) -> dict[str, object]:
    if isinstance(exc, urllib.error.HTTPError):
        raw = exc.read().decode("utf-8", "replace")
        message = raw[:500]
        activation_url = ""
        service = ""
        try:
            payload = json.loads(raw or "{}")
            error = payload.get("error") or {}
            message = str(error.get("message") or message)[:700]
            for detail in error.get("details") or []:
                metadata = detail.get("metadata") or {}
                activation_url = activation_url or str(metadata.get("activationUrl") or "")
                service = service or str(metadata.get("serviceTitle") or metadata.get("service") or "")
        except Exception:
            pass
        return {"http_code": exc.code, "error": message, "activation_url": activation_url, "service": service}
    return {"error": str(exc)[:500]}


def _google_api_probe(runtime: dict[str, object], scopes: list[str], url: str) -> dict[str, object]:
    try:
        credentials = _google_oauth_credentials_from_runtime(runtime, scopes)
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {credentials.token}"})
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read(1)
        return {"ok": True}
    except Exception as exc:
        details = _google_http_error_details(exc)
        details["ok"] = False
        return details


@app.get("/api/setup-inicial/gtm-accounts")
def api_setup_initial_gtm_accounts() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    fallback_accounts = [{"account_id": "6342819886", "name": "LRD + KAIZEN", "path": "accounts/6342819886"}]
    try:
        credentials = _google_oauth_credentials_from_runtime(runtime, ["https://www.googleapis.com/auth/tagmanager.manage.accounts"])
        request = urllib.request.Request(
            "https://tagmanager.googleapis.com/tagmanager/v2/accounts",
            headers={"Authorization": f"Bearer {credentials.token}"},
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
        accounts = []
        seen = set()
        for item in payload.get("account") or []:
            account_id = str(item.get("accountId") or "").strip()
            if not account_id or account_id in seen:
                continue
            seen.add(account_id)
            accounts.append({
                "account_id": account_id,
                "name": str(item.get("name") or ""),
                "path": str(item.get("path") or ""),
            })
        for item in fallback_accounts:
            if item["account_id"] not in seen:
                accounts.insert(0, item)
        accounts.sort(key=lambda item: (0 if item.get("account_id") == "6342819886" else 1, str(item.get("name") or "").lower()))
        return {"status": "ok", "source": "gtm_api_with_fallback", "accounts": accounts}
    except Exception as exc:
        details = _google_http_error_details(exc)
        return {"status": "fallback", "source": "default_lrd_kaizen", "accounts": fallback_accounts, **details}


@app.get("/api/setup-inicial/ga4-accounts")
def api_setup_initial_ga4_accounts() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        credentials = _google_oauth_credentials_from_runtime(runtime, ["https://www.googleapis.com/auth/analytics.edit"])
        headers = {"Authorization": f"Bearer {credentials.token}"}
        accounts_req = urllib.request.Request("https://analyticsadmin.googleapis.com/v1beta/accounts", headers=headers)
        with urllib.request.urlopen(accounts_req, timeout=45) as response:
            accounts_payload = json.loads(response.read().decode("utf-8") or "{}")
        accounts = []
        for account in accounts_payload.get("accounts") or []:
            account_name = str(account.get("name") or "")
            account_id = account_name.split("/")[-1]
            item = {
                "account_id": account_id,
                "name": str(account.get("displayName") or account_id),
                "resource": account_name,
                "properties": [],
            }
            try:
                prop_url = "https://analyticsadmin.googleapis.com/v1beta/properties?" + urlencode({"filter": f"parent:{account_name}"})
                prop_req = urllib.request.Request(prop_url, headers=headers)
                with urllib.request.urlopen(prop_req, timeout=45) as prop_response:
                    prop_payload = json.loads(prop_response.read().decode("utf-8") or "{}")
                for prop in prop_payload.get("properties") or []:
                    prop_name = str(prop.get("name") or "")
                    prop_item = {
                        "property_id": prop_name.split("/")[-1],
                        "name": str(prop.get("displayName") or prop_name),
                        "resource": prop_name,
                        "measurement_id": "",
                        "streams": [],
                    }
                    try:
                        stream_req = urllib.request.Request(f"https://analyticsadmin.googleapis.com/v1beta/{prop_name}/dataStreams", headers=headers)
                        with urllib.request.urlopen(stream_req, timeout=45) as stream_response:
                            stream_payload = json.loads(stream_response.read().decode("utf-8") or "{}")
                        for stream in stream_payload.get("dataStreams") or []:
                            web_data = stream.get("webStreamData") or {}
                            measurement_id = str(web_data.get("measurementId") or "")
                            stream_item = {
                                "name": str(stream.get("displayName") or stream.get("name") or ""),
                                "resource": str(stream.get("name") or ""),
                                "measurement_id": measurement_id,
                            }
                            prop_item["streams"].append(stream_item)
                            if measurement_id and not prop_item["measurement_id"]:
                                prop_item["measurement_id"] = measurement_id
                    except Exception as stream_exc:
                        prop_item["stream_error"] = str(stream_exc)[:220]
                    item["properties"].append(prop_item)
            except Exception as prop_exc:
                item["properties_error"] = str(prop_exc)[:220]
            accounts.append(item)
        accounts.sort(key=lambda item: str(item.get("name") or "").lower())
        return {"status": "ok", "source": "ga4_api", "accounts": accounts}
    except Exception as exc:
        details = _google_http_error_details(exc)
        return {"status": "error", "source": "ga4_api", "accounts": [], **details}


SETUP_OAUTH_SCOPES = [
    "openid",
    "email",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/admanager",
    "https://www.googleapis.com/auth/analytics.edit",
    "https://www.googleapis.com/auth/tagmanager.edit.containers",
    "https://www.googleapis.com/auth/tagmanager.manage.accounts",
    "https://www.googleapis.com/auth/tagmanager.publish",
    "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
    "https://www.googleapis.com/auth/datamanager",
]

def _setup_oauth_status() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    checks = {
        "google_ads": (["https://www.googleapis.com/auth/adwords"], None),
        "ga4": (["https://www.googleapis.com/auth/analytics.edit"], "https://analyticsadmin.googleapis.com/v1beta/accounts"),
        "gtm": (["https://www.googleapis.com/auth/tagmanager.manage.accounts"], "https://tagmanager.googleapis.com/tagmanager/v2/accounts"),
        "data_manager": (["https://www.googleapis.com/auth/datamanager"], None),
    }
    results = {}
    for name, item in checks.items():
        scopes, probe_url = item
        if probe_url:
            results[name] = _google_api_probe(runtime, scopes, probe_url)
            continue
        try:
            _google_oauth_credentials_from_runtime(runtime, scopes)
            results[name] = {"ok": True}
        except Exception as exc:
            results[name] = {"ok": False, "error": str(exc)[:220]}
    ok = all(item.get("ok") for item in results.values())
    return {"status": "ok", "oauth_ok": ok, "checks": results}


def _setup_oauth_url() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    cfg = runtime["config"].get("google_ads") or {}
    client_id = str(cfg.get("client_id") or "").strip()
    if not client_id:
        return {"status": "error", "error": "client_id ausente no config.yaml"}
    redirect_uri = "http://127.0.0.1:8090/"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SETUP_OAUTH_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
    }
    return {"status": "ok", "url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params), "redirect_uri": redirect_uri, "scopes": SETUP_OAUTH_SCOPES}


@app.get("/api/setup-inicial/oauth-status")
def api_setup_initial_oauth_status() -> dict[str, object]:
    return _setup_oauth_status()


@app.get("/api/setup-inicial/oauth-url")
def api_setup_initial_oauth_url() -> dict[str, object]:
    return _setup_oauth_url()


def _setup_oauth_apply_callback(callback_url: str) -> dict[str, object]:
    callback_url = str(callback_url or "").strip()
    code = ""
    if callback_url.startswith("http"):
        code = (parse_qs(urlparse(callback_url).query).get("code") or [""])[0]
    else:
        code = callback_url
    if not code:
        return {"status": "error", "error": "Codigo OAuth ausente. Cole a URL completa de retorno ou o parametro code."}
    runtime = load_runtime(CONFIG_PATH)
    cfg = runtime["config"].get("google_ads") or {}
    client_id = str(cfg.get("client_id") or "").strip()
    client_secret = str(cfg.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        return {"status": "error", "error": "client_id/client_secret ausentes no config.yaml"}
    body = urlencode({"code": code, "client_id": client_id, "client_secret": client_secret, "redirect_uri": "http://127.0.0.1:8090/", "grant_type": "authorization_code"}).encode("utf-8")
    request = urllib.request.Request("https://oauth2.googleapis.com/token", data=body, method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            token_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        message = f"Falha ao trocar OAuth: HTTP {exc.code} {raw[:500]}"
        if "invalid_grant" in raw:
            message = "Codigo OAuth expirado ou ja usado. Gere um novo OAuth completo, autorize novamente e cole a nova URL final."
        return {"status": "error", "error": message}
    except Exception as exc:
        return {"status": "error", "error": "Falha controlada ao salvar OAuth: " + str(exc)[:500]}
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    access_token = str(token_payload.get("access_token") or "").strip()
    if not refresh_token:
        return {"status": "error", "error": "Google nao retornou refresh_token. Gere a URL novamente com prompt=consent e autorize de novo."}
    tokeninfo = {}
    if access_token:
        try:
            info_req = urllib.request.Request("https://oauth2.googleapis.com/tokeninfo?" + urlencode({"access_token": access_token}))
            with urllib.request.urlopen(info_req, timeout=30) as response:
                tokeninfo = json.loads(response.read().decode("utf-8"))
        except Exception:
            tokeninfo = {}
    raw_config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    raw_config.setdefault("google_ads", {})["refresh_token"] = refresh_token
    raw_config.setdefault("gam", {})["refresh_token"] = refresh_token
    backup = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".bak-oauth-" + datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d%H%M%S"))
    backup.write_text(CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    CONFIG_PATH.write_text(yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return {"status": "ok", "message": "OAuth atualizado no config.yaml", "backup": str(backup), "scopes": str(tokeninfo.get("scope") or "").split()}


@app.post("/api/setup-inicial/oauth-callback")
async def api_setup_initial_oauth_callback(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    return _setup_oauth_apply_callback(str(payload.get("callback_url") or payload.get("code") or ""))


SETUP_TRACKING_TEMPLATE = "{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={campaignid}&utm_term={campaignid}_{keyword}&utm_content={campaignid}_{placement}&utm_creative={campaignid}_{creative}"
SETUP_OAUTH_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/admanager",
    "https://www.googleapis.com/auth/analytics.edit",
    "https://www.googleapis.com/auth/tagmanager.edit.containers",
    "https://www.googleapis.com/auth/tagmanager.manage.accounts",
    "https://www.googleapis.com/auth/tagmanager.publish",
    "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
    "https://www.googleapis.com/auth/datamanager",
]


def _setup_oauth_status() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    checks = {
        "google_ads": ["https://www.googleapis.com/auth/adwords"],
        "ga4": ["https://www.googleapis.com/auth/analytics.edit"],
        "gtm": ["https://www.googleapis.com/auth/tagmanager.publish"],
        "data_manager": ["https://www.googleapis.com/auth/datamanager"],
    }
    results = {}
    for name, scopes in checks.items():
        try:
            _google_oauth_credentials_from_runtime(runtime, scopes)
            results[name] = {"ok": True}
        except Exception as exc:
            results[name] = {"ok": False, "error": str(exc)[:220]}
    ok = all(item.get("ok") for item in results.values())
    return {"status": "ok", "oauth_ok": ok, "checks": results}


def _setup_oauth_url() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    cfg = runtime["config"].get("google_ads") or {}
    client_id = str(cfg.get("client_id") or "").strip()
    if not client_id:
        return {"status": "error", "error": "client_id ausente no config.yaml"}
    redirect_uri = "http://127.0.0.1:8090/"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SETUP_OAUTH_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
    }
    return {"status": "ok", "url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params), "redirect_uri": redirect_uri, "scopes": SETUP_OAUTH_SCOPES}

SETUP_EVENTS = ["adView", "adViewInterstitial", "adViewRewarded"]

def _setup_initial_plan(payload: dict[str, object]) -> dict[str, object]:
    domain = str(payload.get("domain") or "").strip().replace("https://", "").replace("http://", "").strip("/")
    customer_id = "".join(ch for ch in str(payload.get("customer_id") or "") if ch.isdigit())
    ga4_measurement_id = str(payload.get("ga4_measurement_id") or "").strip()
    gtm_account = str(payload.get("gtm_account") or "LRD + KAIZEN").strip()
    ga4_account = str(payload.get("ga4_account") or "394842889").strip()
    email = str(payload.get("email") or "").strip()
    clarity = str(payload.get("clarity_project_id") or "").strip()
    clarity_api_token = str(payload.get("clarity_api_token") or "").strip()
    currency = str(payload.get("currency") or "BRL").strip().upper()
    wordpress_url = str(payload.get("wordpress_url") or f"https://{domain}/wp-admin/").strip()
    wordpress_user = str(payload.get("wordpress_user") or "").strip()
    wordpress_password_present = bool(str(payload.get("wordpress_password") or "").strip())
    if not domain:
        return {"status": "error", "error": "Informe o dominio do site."}
    if not customer_id:
        return {"status": "error", "error": "Informe o customer ID do Google Ads."}
    return {
        "status": "ok",
        "domain": domain,
        "email": email,
        "google_ads": {
            "customer_id": customer_id,
            "tracking_template": SETUP_TRACKING_TEMPLATE,
            "conversions": [
                {
                    "name": event,
                    "type": "Visualizacao de pagina",
                    "value_mode": "valores diferentes",
                    "default_value": 1,
                    "currency": currency,
                    "count": "Todas",
                    "click_window_days": 1,
                    "engaged_view_window_days": 1,
                    "attribution": "Ultimo clique",
                    "custom_goal": event,
                }
                for event in SETUP_EVENTS
            ],
            "manual_checks": [],
        },
        "gtm": {
            "account": gtm_account,
            "container_name": domain,
            "wpcode": "Instalar script principal no Header e noscript no Body.",
            "builtin_variables": ["Click Element", "Click Classes", "Click ID", "Click Target", "Click URL", "Click Text"],
            "tags": ["Evento GTM"] + [f"Google Ads {event}" for event in SETUP_EVENTS] + [f"GA4 {event}" for event in SETUP_EVENTS],
            "triggers": ["Initialization - All Pages"] + SETUP_EVENTS,
        },
        "ga4": {
            "account": ga4_account,
            "account_note": "Usar esta conta apenas como conta-mae. Criar uma nova propriedade para este dominio.",
            "property_name": domain,
            "measurement_id": ga4_measurement_id,
            "events": SETUP_EVENTS,
        },
        "clarity": {
            "project_name": domain,
            "project_id": clarity,
            "api_token": clarity_api_token,
            "api_token_received": bool(clarity_api_token),
            "install_mode": "Usar Project ID do Clarity para instalar tag via GTM e token Data Export API para validar leitura de dados.",
            "integrations": ["Google Tag Manager", "Clarity Data Export API", f"Google Ads {customer_id}", ga4_measurement_id or "GA4 measurement ID"],
        },
        "wordpress": {
            "admin_url": wordpress_url,
            "user": wordpress_user,
            "password_received": wordpress_password_present,
            "install_targets": [
                "WPCode Header: codigo principal do Google Tag Manager.",
                "WPCode Body: noscript do Google Tag Manager.",
                "Validar se o GTM aparece no HTML publicado.",
            ],
        },
        "validation": [
            "GTM publicado e instalado no WordPress.",
            "Preview do GTM mostra eventos adView, adViewInterstitial e adViewRewarded.",
            "Tags Google Ads disparam uma vez por evento.",
            "GA4 DebugView recebe os tres eventos.",
            "Clarity recebe sessoes.",
            "Google Ads mostra tag detectada ou aguardando dados.",
            "UTMs aparecem no destino e no GAM por utm_campaign/utm_content.",
        ],
        "automation_status": {
            "google_ads_conversions": "automatizavel via API",
            "tracking_template": "automatizavel via API",
            "gtm_container_tags": "automatizavel via GTM API",
            "ga4_property_stream": "automatizavel via GA Admin API quando OAuth tiver analytics.edit",
            "wpcode_install": "automatizavel por navegador ou WP REST quando disponivel",
            "clarity_project": "Project ID precisa existir no Clarity; criacao de projeto nao e exposta pela API oficial",
            "clarity_data_export_api": "automatizavel por token do projeto",
            "clarity_connections": "tag instalada automaticamente via GTM quando Project ID existe",
            "legal_terms": "nao bloqueia o setup",
        },
    }


@app.get("/api/setup-inicial/oauth-status")
def api_setup_initial_oauth_status() -> dict[str, object]:
    return _setup_oauth_status()


@app.get("/api/setup-inicial/oauth-url")
def api_setup_initial_oauth_url() -> dict[str, object]:
    return _setup_oauth_url()


def _setup_oauth_apply_callback(callback_url: str) -> dict[str, object]:
    callback_url = str(callback_url or "").strip()
    code = ""
    if callback_url.startswith("http"):
        code = (parse_qs(urlparse(callback_url).query).get("code") or [""])[0]
    else:
        code = callback_url
    if not code:
        return {"status": "error", "error": "Codigo OAuth ausente. Cole a URL completa de retorno ou o parametro code."}
    runtime = load_runtime(CONFIG_PATH)
    cfg = runtime["config"].get("google_ads") or {}
    client_id = str(cfg.get("client_id") or "").strip()
    client_secret = str(cfg.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        return {"status": "error", "error": "client_id/client_secret ausentes no config.yaml"}
    body = urlencode({"code": code, "client_id": client_id, "client_secret": client_secret, "redirect_uri": "http://127.0.0.1:8090/", "grant_type": "authorization_code"}).encode("utf-8")
    request = urllib.request.Request("https://oauth2.googleapis.com/token", data=body, method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            token_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        return {"status": "error", "error": f"Falha ao trocar OAuth: HTTP {exc.code} {raw[:500]}"}
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    access_token = str(token_payload.get("access_token") or "").strip()
    if not refresh_token:
        return {"status": "error", "error": "Google nao retornou refresh_token. Gere a URL novamente com prompt=consent e autorize de novo."}
    tokeninfo = {}
    if access_token:
        try:
            info_req = urllib.request.Request("https://oauth2.googleapis.com/tokeninfo?" + urlencode({"access_token": access_token}))
            with urllib.request.urlopen(info_req, timeout=30) as response:
                tokeninfo = json.loads(response.read().decode("utf-8"))
        except Exception:
            tokeninfo = {}
    raw_config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    raw_config.setdefault("google_ads", {})["refresh_token"] = refresh_token
    raw_config.setdefault("gam", {})["refresh_token"] = refresh_token
    backup = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".bak-oauth-" + datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d%H%M%S"))
    backup.write_text(CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    CONFIG_PATH.write_text(yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return {"status": "ok", "message": "OAuth atualizado no config.yaml", "backup": str(backup), "scopes": str(tokeninfo.get("scope") or "").split()}


@app.post("/api/setup-inicial/oauth-callback")
async def api_setup_initial_oauth_callback(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    return _setup_oauth_apply_callback(str(payload.get("callback_url") or payload.get("code") or ""))

@app.post("/api/setup-inicial/plan")
async def api_setup_initial_plan(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    return _setup_initial_plan(payload)


@app.post("/api/setup-inicial/save")
async def api_setup_initial_save(request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", "ignore")
        payload = {key: values[0] for key, values in parse_qs(body).items()}
    plan = _setup_initial_plan(payload)
    if plan.get("status") != "ok":
        return plan
    runtime = load_runtime(CONFIG_PATH)
    saved = save_initial_setup_plan(runtime, payload, plan)
    return {"status": "ok", "setup_id": saved["setup_id"], "setup_status": saved["status"], "created_at": saved["created_at"], "plan": plan}


@app.get("/api/setup-inicial/list")
def api_setup_initial_list() -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    return {"status": "ok", "setups": load_initial_setup_plans(runtime)}


@app.get("/api/setup-inicial/{setup_id}")
def api_setup_initial_detail(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    detail = load_initial_setup_detail(runtime, setup_id)
    if not detail:
        return {"status": "error", "error": "Setup nao encontrado"}
    return {"status": "ok", "setup": detail}


@app.post("/api/setup-inicial/{setup_id}/apply-google-ads")
def api_setup_initial_apply_google_ads(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return apply_initial_setup_google_ads(runtime, setup_id)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/apply-ga4")
def api_setup_initial_apply_ga4(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return apply_initial_setup_ga4(runtime, setup_id)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/apply-gtm")
def api_setup_initial_apply_gtm(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return apply_initial_setup_gtm(runtime, setup_id)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/apply-all")
async def api_setup_initial_apply_all(setup_id: int, request: Request) -> dict[str, object]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    runtime = load_runtime(CONFIG_PATH)
    results: dict[str, object] = {}
    overall = "ok"
    for key, func in (
        ("google_ads", apply_initial_setup_google_ads),
        ("ga4", apply_initial_setup_ga4),
        ("gtm", apply_initial_setup_gtm),
    ):
        try:
            result = func(runtime, setup_id)
        except Exception as exc:
            result = {"status": "error", "error": str(exc)}
        results[key] = result
        if result.get("status") != "ok":
            overall = "partial"
    wp_user = str(payload.get("wordpress_user") or "").strip()
    wp_pass = str(payload.get("wordpress_password") or "").strip()
    if wp_user and wp_pass:
        try:
            results["wordpress_install"] = install_initial_setup_wordpress(runtime, setup_id, payload)
        except Exception as exc:
            results["wordpress_install"] = {"status": "error", "error": str(exc)}
            overall = "partial"
    else:
        results["wordpress_install"] = {"status": "manual", "message": "WordPress sem usuario/senha; instalar WPCode manualmente ou informar credenciais."}
    try:
        results["final_check"] = final_check_initial_setup(load_runtime(CONFIG_PATH), setup_id)
    except Exception as exc:
        results["final_check"] = {"status": "error", "error": str(exc)}
        overall = "partial"
    return {"status": overall, "setup_id": setup_id, "results": results}


@app.post("/api/setup-inicial/{setup_id}/validate-wordpress")
def api_setup_initial_validate_wordpress(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return validate_initial_setup_wordpress(runtime, setup_id)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/install-wordpress")
async def api_setup_initial_install_wordpress(setup_id: int, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        payload = await request.json()
        return install_initial_setup_wordpress(runtime, setup_id, payload)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/validate-clarity-api")
async def api_setup_initial_validate_clarity_api(setup_id: int, request: Request) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        return validate_initial_setup_clarity_api(runtime, setup_id, str(payload.get("clarity_api_token") or ""))
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/final-check")
def api_setup_initial_final_check(setup_id: int) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return final_check_initial_setup(runtime, setup_id)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/setup-inicial/{setup_id}/confirm/{step}")
def api_setup_initial_confirm_step(setup_id: int, step: str) -> dict[str, object]:
    runtime = load_runtime(CONFIG_PATH)
    try:
        return confirm_initial_setup_step(runtime, setup_id, step)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.get("/setup-inicial", response_class=HTMLResponse)
def setup_initial_page() -> str:
    customer_options = ["<option value=''>Carregando contas Google Ads...</option>"]
    runtime = load_runtime(CONFIG_PATH)
    oauth_status = _setup_oauth_status()
    oauth_button_html = "" if oauth_status.get("oauth_ok") else "<button class='action-button secondary' id='setup-oauth' type='button'>Gerar OAuth completo</button>"
    oauth_status_html = "OAuth completo validado." if oauth_status.get("oauth_ok") else "OAuth incompleto: gere OAuth completo somente se GA4/GTM/Google Ads falharem."
    saved_setups = load_initial_setup_plans(runtime, limit=12)
    saved_rows = []
    for item in saved_setups:
        setup_id = html.escape(str(item.get('id', '')))
        saved_rows.append(
            "<tr>"
            f"<td class='mono'>#{setup_id}</td>"
            f"<td>{html.escape(str(item.get('domain', '')))}</td>"
            f"<td class='mono'>{html.escape(str(item.get('customer_id', '')))}</td>"
            f"<td>{html.escape(str(item.get('currency', '')))}</td>"
            f"<td>{_status_badge(str(item.get('status', '')))}</td>"
            f"<td>{html.escape(str(item.get('created_at', '')))}</td>"
            f"<td><button class='action-button secondary setup-view-detail' type='button' data-setup-id='{setup_id}'>Ver detalhes</button></td>"
            f"<td><button class='action-button setup-apply-all' type='button' data-setup-id='{setup_id}'>Aplicar tudo</button> <button class='action-button setup-apply-ads' type='button' data-setup-id='{setup_id}'>Aplicar Google Ads</button> "
            f"<button class='action-button secondary setup-apply-ga4' type='button' data-setup-id='{setup_id}'>Criar GA4</button> "
            f"<button class='action-button secondary setup-apply-gtm' type='button' data-setup-id='{setup_id}'>Criar GTM</button> "
            f"<button class='action-button secondary setup-install-wp' type='button' data-setup-id='{setup_id}'>Instalar WPCode</button> "
            f"<button class='action-button secondary setup-validate-wp' type='button' data-setup-id='{setup_id}'>Validar WP</button> "
            f"<button class='action-button secondary setup-clarity-api' type='button' data-setup-id='{setup_id}'>Validar Clarity API</button> "
            f"<button class='action-button secondary setup-final-check' type='button' data-setup-id='{setup_id}'>Checklist final</button> "
            f"<button class='action-button secondary setup-confirm-step' type='button' data-step='ga4' data-setup-id='{setup_id}'>GA4 OK</button> "
            f"<button class='action-button secondary setup-confirm-step' type='button' data-step='clarity' data-setup-id='{setup_id}'>Clarity OK</button></td>"
            "</tr>"
        )
    content = """
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Implementacao inicial</div>
          <h1>Setup Inicial Julio: Google Ads + GTM + GA4 + Clarity</h1>
          <p>Crie o plano base do projeto Julio antes de anunciar: conversoes, UTMs, GTM, Analytics, Clarity e validacao automatizada do setup.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Modo atual</div>
            <strong>Plano seguro</strong>
          </div>
          <p class='muted'>Esta tela roda na instancia Julio e usa banco/Graphify/config proprios, independente da Julio.</p>
          <p class='muted'>A moeda obedece sempre o valor selecionado manualmente, mesmo que a conta Google Ads use outra moeda.</p>
        </div>
      </div>
      <div class='panel'>
        <div class='setup-grid'>
          <label>Dominio do site<select id='setup-domain-select'><option value=''>Carregando sites...</option></select></label>
          <label>Novo dominio do site<input id='setup-domain-new' value='' placeholder='digite aqui se o site ainda nao existir'></label>
          <label>Google Ads customer ID<select id='setup-customer'>__CUSTOMER_OPTIONS__</select></label>
          <label>Email responsavel<input id='setup-email' value='contato.lucianocokado@gmail.com'></label>
          <label>Conta GTM<select id='setup-gtm'><option value=''>Carregando contas GTM...</option></select></label>
          <label>Conta GA4<select id='setup-ga4-account'><option value=''>Carregando contas GA4...</option></select></label>
          <label>Propriedade GA4 existente<select id='setup-ga4-property'><option value=''>Opcional: criar nova propriedade</option></select></label>
          <label>Measurement ID GA4<input id='setup-ga4' value='' placeholder='o sistema cria/preenche apos aplicar ou selecionar propriedade'></label>
          <label>Novo Clarity Project ID<input id='setup-clarity' placeholder='Project ID do Clarity'></label>
          <label>Clarity Data Export API Token<input id='setup-clarity-token' type='password' placeholder='token gerado em Settings > Data Export'></label>
          <label>Moeda manual<select id='setup-currency'>
            <option value='BRL'>BRL - Real brasileiro</option>
            <option value='USD'>USD - Dolar americano</option>
            <option value='EUR'>EUR - Euro</option>
            <option value='GBP'>GBP - Libra esterlina</option>
            <option value='AUD'>AUD - Dolar australiano</option>
            <option value='CAD'>CAD - Dolar canadense</option>
            <option value='MXN'>MXN - Peso mexicano</option>
            <option value='COP'>COP - Peso colombiano</option>
            <option value='CLP'>CLP - Peso chileno</option>
            <option value='ARS'>ARS - Peso argentino</option>
            <option value='ZAR'>ZAR - Rand sul-africano</option>
            <option value='PHP'>PHP - Peso filipino</option>
            <option value='IDR'>IDR - Rupia indonesia</option>
          </select></label>
          <label>WordPress admin URL<input id='setup-wp-url' value='https://example-nexus-domain.com/wp-admin/' placeholder='https://dominio.com/wp-admin/'></label>
          <label>WordPress usuario<input id='setup-wp-user' placeholder='usuario admin'></label>
          <label>WordPress senha<input id='setup-wp-pass' type='password' placeholder='senha ou application password'></label>
        </div>
        <div class='hero-actions'>
          <button class='action-button' id='setup-generate' type='button'>Gerar, salvar e aplicar setup inicial</button>
          __OAUTH_BUTTON__
        </div>
        <div id='setup-status' class='muted' style='margin-top:12px'>__OAUTH_STATUS__</div>
      </div>
      <div class='panel' id='setup-result' style='display:none'></div>
      <div class='panel'>
        <h2>Setups salvos</h2>
        <p>Planos persistidos para continuar a aplicacao por etapas.</p>
        <table>
          <thead><tr><th>ID</th><th>Dominio</th><th>Google Ads</th><th>Moeda</th><th>Status</th><th>Criado</th><th>Detalhes</th><th>Acao</th></tr></thead>
          <tbody>__SAVED_ROWS__</tbody>
        </table>
      </div>
    </div>
    <style>
      .setup-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }
      .setup-grid label { display:flex; flex-direction:column; gap:7px; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; font-weight:700; }
      .setup-grid input, .setup-grid select { border:1px solid var(--line); border-radius:14px; padding:12px 14px; color:var(--ink); font-size:15px; background:#fff; text-transform:none; letter-spacing:0; font-weight:600; }
      .setup-section { border:1px solid var(--line); border-radius:18px; padding:16px; margin-top:14px; background:#fbfefb; }
      .setup-section h3 { margin:0 0 10px; }
      .setup-list { margin:0; padding-left:18px; }
      .setup-code { white-space:pre-wrap; background:#102018; color:#f7fff8; border-radius:14px; padding:12px; overflow:auto; }
      @media (max-width: 760px) { .setup-grid { grid-template-columns:1fr; } }
    </style>
    <script>
      function setupEscape(value) {
        return String(value || '').replace(/[&<>"']/g, function(ch) {
          return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch];
        });
      }
      function setupList(items) {
        return '<ul class="setup-list">' + (items || []).map(function(item) { return '<li>' + setupEscape(item) + '</li>'; }).join('') + '</ul>';
      }
      function setupStepStatus(label, result, okText) {
        const status = result && result.status ? String(result.status) : 'pendente';
        const ok = status === 'ok' || status === 'success';
        const error = result && result.error ? ' - ' + result.error : '';
        return (ok ? 'OK: ' : (status === 'manual' ? 'MANUAL: ' : 'PENDENTE/ERRO: ')) + label + (okText && ok ? ' - ' + okText : '') + error;
      }
      function setupApplySummary(result) {
        const r = (result && result.results) || {};
        const items = [
          setupStepStatus('Google Ads conversoes e tracking template', r.google_ads, 'aplicado'),
          setupStepStatus('GA4 propriedade e stream', r.ga4, ((r.ga4 || {}).ga4 || {}).measurement_id || ''),
          setupStepStatus('GTM container, tags e publicacao', r.gtm, (((r.gtm || {}).gtm || {}).container_id || '')),
          setupStepStatus('WPCode no WordPress', r.wordpress_install, ''),
          setupStepStatus('Checklist final', r.final_check, '')
        ];
        return '<div class="setup-section"><h3>Resultado da aplicacao automatica</h3><p><strong>Status geral:</strong> ' + setupEscape((result || {}).status || 'pendente') + '</p>' + setupList(items) + '</div>';
      }
      async function refreshOAuthStatus() {
        try {
          const response = await fetch('/api/setup-inicial/oauth-status');
          const data = await response.json();
          const oauthButton = document.getElementById('setup-oauth');
          const status = document.getElementById('setup-status');
          if (data.oauth_ok) {
            if (oauthButton) oauthButton.style.display = 'none';
            if (status && !status.textContent.includes('Contas Google Ads')) status.textContent = 'OAuth completo validado.';
          }
        } catch (error) {}
      }

      async function loadSetupSites() {
        const select = document.getElementById('setup-domain-select');
        try {
          const response = await fetch('/api/setup-inicial/sites', {headers:{'Accept':'application/json'}});
          const data = await response.json();
          const sites = data.sites || [];
          select.innerHTML = '<option value="">Selecione um site existente ou digite novo abaixo</option>' + sites.map(function(site) {
            return '<option value="' + setupEscape(site) + '">' + setupEscape(site) + '</option>';
          }).join('');
        } catch (error) {
          select.innerHTML = '<option value="">Erro ao carregar sites; digite novo abaixo</option>';
        }
      }
      async function loadGtmAccounts() {
        const select = document.getElementById('setup-gtm');
        try {
          const response = await fetch('/api/setup-inicial/gtm-accounts', {headers:{'Accept':'application/json'}});
          const data = await response.json();
          const accounts = data.accounts || [{account_id:'6342819886', name:'LRD + KAIZEN'}];
          select.innerHTML = accounts.map(function(item) {
            const label = (item.name || 'Conta GTM') + ' - ' + (item.account_id || '');
            return '<option value="' + setupEscape(item.account_id || item.name || '') + '" data-account-id="' + setupEscape(item.account_id || '') + '">' + setupEscape(label) + '</option>';
          }).join('');
        } catch (error) {
          select.innerHTML = '<option value="">Erro ao carregar GTMs</option>';
        }
      }


      let setupGa4AccountsCache = [];
      function renderGa4Properties(accountId) {
        const propertySelect = document.getElementById('setup-ga4-property');
        const measurementInput = document.getElementById('setup-ga4');
        const account = setupGa4AccountsCache.find(function(item) { return String(item.account_id || '') === String(accountId || ''); });
        const properties = account ? (account.properties || []) : [];
        propertySelect.innerHTML = '<option value="">Opcional: criar nova propriedade</option>' + properties.map(function(item) {
          const label = (item.name || 'Propriedade GA4') + ' - ' + (item.property_id || '') + (item.measurement_id ? ' | ' + item.measurement_id : '');
          return '<option value="' + setupEscape(item.resource || item.property_id || '') + '" data-measurement-id="' + setupEscape(item.measurement_id || '') + '">' + setupEscape(label) + '</option>';
        }).join('');
        if (measurementInput && !propertySelect.value) measurementInput.value = '';
      }
      async function loadGa4Accounts() {
        const select = document.getElementById('setup-ga4-account');
        try {
          const response = await fetch('/api/setup-inicial/ga4-accounts', {headers:{'Accept':'application/json'}});
          const data = await response.json();
          setupGa4AccountsCache = data.accounts || [];
          if (!setupGa4AccountsCache.length) {
            select.innerHTML = '<option value="">OAuth GA4 necessario para listar contas</option>';
            renderGa4Properties('');
            return;
          }
          select.innerHTML = setupGa4AccountsCache.map(function(item) {
            const label = (item.name || 'Conta GA4') + ' - ' + (item.account_id || '');
            return '<option value="' + setupEscape(item.account_id || '') + '">' + setupEscape(label) + '</option>';
          }).join('');
          renderGa4Properties(select.value);
        } catch (error) {
          select.innerHTML = '<option value="">Erro ao carregar contas GA4</option>';
          renderGa4Properties('');
        }
      }

      async function loadGoogleAdsCustomers() {
        const select = document.getElementById('setup-customer');
        try {
          const response = await fetch('/api/google-ads/customers', {headers:{'Accept':'application/json'}});
          const data = await response.json();
          const customers = data.customers || [];
          if (!customers.length) {
            select.innerHTML = '<option value="">Nenhuma conta encontrada</option>';
            return;
          }
          select.innerHTML = customers.map(function(item) {
            const id = String(item.customer_id || '');
            const label = (item.name ? item.name + ' - ' : '') + id + (item.site ? ' | ' + item.site : '') + (item.currency ? ' | ' + item.currency : '');
            return '<option value="' + setupEscape(id) + '">' + setupEscape(label) + '</option>';
          }).join('');
          const status = document.getElementById('setup-status');
          status.textContent = data.source === 'google_ads_api' ? 'Contas Google Ads carregadas pelo email autorizado.' : ('Contas Google Ads carregadas do config. ' + (data.error || data.warning || 'API nao retornou contas.'));
        } catch (error) {
          select.innerHTML = '<option value="">Erro ao carregar contas</option>';
        }
      }
      const setupOauthButton = document.getElementById('setup-oauth');
      if (setupOauthButton) setupOauthButton.addEventListener('click', async function() {
        const status = document.getElementById('setup-status');
        status.textContent = 'Gerando URL OAuth completa...';
        const response = await fetch('/api/setup-inicial/oauth-url');
        const data = await response.json();
        if (data.status !== 'ok') {
          status.textContent = data.error || 'Falha ao gerar OAuth.';
          return;
        }
        status.innerHTML = '<strong>OAuth completo:</strong> abra o link abaixo e autorize Google Ads + GAM + GA4 + GTM.<br><a href="' + setupEscape(data.url) + '" target="_blank" rel="noopener">Abrir autorizacao Google</a><br><small>Depois cole abaixo a URL final que apareceu no navegador.</small><br><textarea id="setup-oauth-callback" style="width:100%;min-height:74px;margin-top:10px;border:1px solid var(--line);border-radius:12px;padding:10px" placeholder="Cole aqui a URL http://127.0.0.1:8090/?code=..."></textarea><br><button class="action-button" id="setup-oauth-apply" type="button" style="margin-top:8px">Salvar OAuth na VPS</button>';
      });

      document.addEventListener('click', async function(event) {
        if (!event.target.closest('#setup-oauth-apply')) return;
        const status = document.getElementById('setup-status');
        const input = document.getElementById('setup-oauth-callback');
        status.appendChild(document.createElement('br'));
        const line = document.createElement('span');
        line.textContent = 'Validando e salvando OAuth...';
        status.appendChild(line);
        const response = await fetch('/api/setup-inicial/oauth-callback', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({callback_url: input ? input.value : ''})});
        const data = await response.json();
        if (data.status === 'ok') {
          line.textContent = 'OAuth salvo e validado. Atualizando listas de Google Ads, GTM e GA4...';
          await refreshOAuthStatus();
          await loadGoogleAdsCustomers();
          await loadGtmAccounts();
          await loadGa4Accounts();
          line.textContent = 'OAuth salvo. Listas atualizadas pelo email autorizado.';
        } else {
          line.textContent = data.error || 'Falha ao salvar OAuth.';
        }
      });

      document.getElementById('setup-generate').addEventListener('click', async function() {
        const status = document.getElementById('setup-status');
        const resultBox = document.getElementById('setup-result');
        status.textContent = 'Gerando e salvando plano...';
        resultBox.style.display = 'none';
        const payload = {
          domain: (document.getElementById('setup-domain-new').value || document.getElementById('setup-domain-select').value),
          customer_id: document.getElementById('setup-customer').value,
          email: document.getElementById('setup-email').value,
          gtm_account: document.getElementById('setup-gtm').value,
          ga4_account: document.getElementById('setup-ga4-account').value,
          ga4_property_resource: document.getElementById('setup-ga4-property').value,
          ga4_measurement_id: document.getElementById('setup-ga4').value,
          clarity_project_id: document.getElementById('setup-clarity').value,
          clarity_api_token: document.getElementById('setup-clarity-token').value,
          currency: document.getElementById('setup-currency').value,
          wordpress_url: document.getElementById('setup-wp-url').value,
          wordpress_user: document.getElementById('setup-wp-user').value,
          wordpress_password: document.getElementById('setup-wp-pass').value
        };
        const response = await fetch('/api/setup-inicial/save', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
        const data = await response.json();
        if (data.status !== 'ok') {
          status.textContent = data.error || 'Erro ao gerar plano.';
          return;
        }
        status.textContent = 'Plano salvo: #' + data.setup_id + '. Aplicando Google Ads, GA4 e GTM...';
        let applyAllResult = null;
        try {
          const applyResponse = await fetch('/api/setup-inicial/' + encodeURIComponent(data.setup_id) + '/apply-all', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify(payload)
          });
          applyAllResult = await applyResponse.json();
          status.textContent = applyAllResult.status === 'ok'
            ? 'Setup aplicado automaticamente: Google Ads, GA4 e GTM executados.'
            : 'Setup salvo e aplicado parcialmente. Veja detalhes abaixo.';
        } catch (error) {
          applyAllResult = {status:'error', error:String(error && error.message ? error.message : error)};
          status.textContent = 'Plano salvo, mas a aplicacao automatica falhou. Veja detalhes abaixo.';
        }
        data.plan.setup_id = data.setup_id;
        data.plan.setup_status = data.setup_status;
        const plan = data.plan;
        resultBox.innerHTML = `
          <div class='setup-section'><h3>Plano salvo</h3><p><strong>ID:</strong> #${setupEscape(data.setup_id)} | <strong>Status:</strong> ${setupEscape(applyAllResult && applyAllResult.status ? applyAllResult.status : data.setup_status)}</p></div>
          ${setupApplySummary(applyAllResult)}
          <div class='setup-section'><h3>Google Ads</h3>
            <p><strong>Conta:</strong> ${setupEscape(plan.google_ads.customer_id)}</p>
            <p><strong>Tracking template</strong></p>
            <pre class='setup-code'>${setupEscape(plan.google_ads.tracking_template)}</pre>
            ${setupList(plan.google_ads.conversions.map(function(item) { return item.name + ' | ' + item.type + ' | valor ' + item.default_value + ' ' + item.currency + ' | ' + item.attribution; }))}
          </div>
          <div class='setup-section'><h3>Pendencias manuais Google Ads</h3>${setupList(plan.google_ads.manual_checks)}</div>
          <div class='setup-section'><h3>Google Tag Manager</h3>
            <p><strong>Conta:</strong> ${setupEscape(plan.gtm.account)} | <strong>Container:</strong> ${setupEscape(plan.gtm.container_name)}</p>
            ${setupList(plan.gtm.tags)}
          </div>
          <div class='setup-section'><h3>GA4</h3>
            <p><strong>Conta mae:</strong> ${setupEscape(plan.ga4.account)} | <strong>Nova propriedade:</strong> ${setupEscape(plan.ga4.property_name)} | <strong>ID:</strong> ${setupEscape(plan.ga4.measurement_id || 'pendente')}</p>
            <p>${setupEscape(plan.ga4.account_note)}</p>
            ${setupList(plan.ga4.events)}
          </div>
          <div class='setup-section'><h3>Clarity</h3><p>${setupEscape(plan.clarity.install_mode)}</p>${setupList(plan.clarity.integrations)}</div>
          <div class='setup-section'><h3>WordPress / WPCode</h3>
            <p><strong>Admin:</strong> ${setupEscape(plan.wordpress.admin_url)} | <strong>Usuario:</strong> ${setupEscape(plan.wordpress.user || 'pendente')}</p>
            <p><strong>Senha recebida:</strong> ${plan.wordpress.password_received ? 'sim, apenas para esta operacao' : 'nao'}</p>
            ${setupList(plan.wordpress.install_targets)}
          </div>
          <div class='setup-section'><h3>Validacao obrigatoria</h3>${setupList(plan.validation)}</div>
          <div class='setup-section'><h3>Status da automacao</h3><pre class='setup-code'>${setupEscape(JSON.stringify(plan.automation_status, null, 2))}</pre></div>
        `;
        resultBox.style.display = 'block';
      });
      document.addEventListener('click', async function(event) {
        const detailButton = event.target.closest('.setup-view-detail');
        if (!detailButton) return;
        const setupId = detailButton.dataset.setupId;
        const status = document.getElementById('setup-status');
        const resultBox = document.getElementById('setup-result');
        status.textContent = 'Carregando setup #' + setupId + '...';
        const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId), {headers:{'Accept':'application/json'}});
        const data = await response.json();
        if (data.status !== 'ok') {
          status.textContent = data.error || 'Setup nao encontrado.';
          return;
        }
        const setup = data.setup;
        const plan = setup.plan || {};
        const gtm = plan.gtm_apply || {};
        const snippets = gtm.snippets || {};
        const ads = plan.google_ads_apply || {};
        const wpv = plan.wordpress_validation || {};
        const wpi = plan.wordpress_install || {};
        const finalCheck = plan.final_check || {};
        status.textContent = 'Setup #' + setupId + ' carregado.';
        resultBox.innerHTML = `
          <div class='setup-section'><h3>Setup #${setupEscape(setup.id)}</h3>
            <p><strong>Dominio:</strong> ${setupEscape(setup.domain)} | <strong>Status:</strong> ${setupEscape(setup.status)} | <strong>Moeda:</strong> ${setupEscape(setup.currency)}</p>
          </div>
          <div class='setup-section'><h3>Google Ads</h3>
            <p><strong>Status:</strong> ${setupEscape(ads.status || 'pendente')} | <strong>Conta:</strong> ${setupEscape(setup.customer_id)}</p>
            ${setupList((ads.conversions || []).map(function(item) { return item.name + ' | AW-' + (item.tag_id || '-') + ' | label ' + (item.label || '-'); }))}
          </div>
          <div class='setup-section'><h3>GTM</h3>
            <p><strong>Status:</strong> ${setupEscape(gtm.status || 'pendente')} | <strong>Container:</strong> ${setupEscape(gtm.container_id || 'pendente')}</p>
            <p><strong>Publicacao:</strong> ${setupEscape((gtm.publish || {}).status || 'pendente')}</p>
          </div>
          <div class='setup-section'><h3>WPCode Header</h3>
            <pre class='setup-code'>${setupEscape(snippets.head || 'Crie o GTM primeiro para gerar o codigo Header.')}</pre>
          </div>
          <div class='setup-section'><h3>WPCode Body</h3>
            <pre class='setup-code'>${setupEscape(snippets.body || 'Crie o GTM primeiro para gerar o codigo Body.')}</pre>
          </div>
          <div class='setup-section'><h3>Validacao WordPress</h3>
            <p><strong>Status:</strong> ${setupEscape(wpv.status || 'pendente')} | <strong>URL:</strong> ${setupEscape(wpv.url || '-')}</p>
            <p><strong>Instalacao WPCode:</strong> ${setupEscape(wpi.status || 'pendente')}</p>
            <pre class='setup-code'>${setupEscape(JSON.stringify(wpv.checks || {}, null, 2))}</pre>
          </div>
          <div class='setup-section'><h3>Checklist final</h3>
            <p><strong>Status:</strong> ${setupEscape(finalCheck.status || 'pendente')}</p>
            <pre class='setup-code'>${setupEscape(JSON.stringify(finalCheck.checks || [], null, 2))}</pre>
          </div>
        `;
        resultBox.style.display = 'block';
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-apply-all');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Aplicando...';
        const status = document.getElementById('setup-status');
        const resultBox = document.getElementById('setup-result');
        status.textContent = 'Aplicando setup completo #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/apply-all', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
          const data = await response.json();
          status.textContent = data.status === 'ok' ? 'Setup completo aplicado.' : 'Setup aplicado parcialmente. Veja detalhes.';
          resultBox.style.display = 'block';
          resultBox.innerHTML = setupApplySummary(data);
        } catch (error) {
          status.textContent = 'Erro ao aplicar setup completo: ' + (error.message || error);
        }
        button.disabled = false;
        button.textContent = original;
      });

      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-apply-ads');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Aplicando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Aplicando Google Ads no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/apply-google-ads', {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao aplicar Google Ads: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          status.textContent = 'Google Ads aplicado no setup #' + setupId + '. Atualize a pagina para ver o status.';
          button.textContent = 'Aplicado';
        } catch (error) {
          status.textContent = 'Erro ao aplicar Google Ads: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-apply-ga4');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Criando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Criando GA4 no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/apply-ga4', {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao criar GA4: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          status.textContent = 'GA4 criado no setup #' + setupId + ': ' + ((data.ga4 || {}).measurement_id || 'measurement pendente');
          button.textContent = 'GA4 criado';
        } catch (error) {
          status.textContent = 'Erro ao criar GA4: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-apply-gtm');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Criando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Criando GTM no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/apply-gtm', {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao criar GTM: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          status.textContent = 'GTM criado e publicado no setup #' + setupId + ': ' + (data.gtm.container_id || '');
          button.textContent = 'GTM criado';
        } catch (error) {
          status.textContent = 'Erro ao criar GTM: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-install-wp');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Instalando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Instalando WPCode no setup #' + setupId + '...';
        try {
          const payload = {
            wordpress_url: document.getElementById('setup-wp-url').value,
            wordpress_user: document.getElementById('setup-wp-user').value,
            wordpress_password: document.getElementById('setup-wp-pass').value
          };
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/install-wordpress', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify(payload)
          });
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao instalar WPCode: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          status.textContent = data.validation && data.validation.status === 'installed'
            ? 'WPCode instalado e validado no site.'
            : 'WPCode enviado, mas GTM ainda nao apareceu no HTML publico.';
          button.disabled = false;
          button.textContent = original;
        } catch (error) {
          status.textContent = 'Erro ao instalar WPCode: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-validate-wp');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Validando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Validando WordPress no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/validate-wordpress', {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao validar WordPress: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          status.textContent = data.wordpress.status === 'installed'
            ? 'WordPress validado: GTM encontrado no site.'
            : 'GTM ainda nao encontrado no HTML publico do site.';
          button.disabled = false;
          button.textContent = original;
        } catch (error) {
          status.textContent = 'Erro ao validar WordPress: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-confirm-step');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const step = button.dataset.step;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Confirmando...';
        const status = document.getElementById('setup-status');
        const resultBox = document.getElementById('setup-result');
        status.textContent = 'Confirmando etapa manual no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/confirm/' + encodeURIComponent(step), {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro ao confirmar etapa: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          const finalCheck = data.final_check || {};
          status.textContent = finalCheck.status === 'released_for_traffic'
            ? 'Setup liberado para trafego.'
            : 'Confirmacao registrada. Ainda existem etapas pendentes.';
          resultBox.innerHTML = `
            <div class='setup-section'><h3>Checklist final #${setupEscape(setupId)}</h3>
              <p><strong>Status:</strong> ${setupEscape(finalCheck.status || '-')}</p>
              <p><strong>Bloqueios:</strong> ${(finalCheck.blockers || []).length} | <strong>Manuais:</strong> ${(finalCheck.manual_required || []).length}</p>
              <pre class='setup-code'>${setupEscape(JSON.stringify(finalCheck.checks || [], null, 2))}</pre>
            </div>
          `;
          resultBox.style.display = 'block';
          button.disabled = false;
          button.textContent = original;
        } catch (error) {
          status.textContent = 'Erro ao confirmar etapa: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-clarity-api');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const token = prompt('Cole o token Data Export API do Clarity deste projeto:');
        if (!token) return;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Validando...';
        const status = document.getElementById('setup-status');
        status.textContent = 'Validando Clarity API no setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/validate-clarity-api', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({clarity_api_token: token})});
          const data = await response.json();
          status.textContent = data.status === 'ok' ? 'Clarity API validada. Linhas lidas: ' + ((data.validation || {}).rows || 0) : 'Erro Clarity API: ' + (data.error || 'falha desconhecida');
        } catch (error) {
          status.textContent = 'Erro Clarity API: ' + (error.message || error);
        }
        button.disabled = false;
        button.textContent = original;
      });

      document.addEventListener('click', async function(event) {
        const button = event.target.closest('.setup-final-check');
        if (!button) return;
        const setupId = button.dataset.setupId;
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Checando...';
        const status = document.getElementById('setup-status');
        const resultBox = document.getElementById('setup-result');
        status.textContent = 'Rodando checklist final do setup #' + setupId + '...';
        try {
          const response = await fetch('/api/setup-inicial/' + encodeURIComponent(setupId) + '/final-check', {method:'POST'});
          const data = await response.json();
          if (data.status !== 'ok') {
            status.textContent = 'Erro no checklist final: ' + (data.error || 'falha desconhecida');
            button.disabled = false;
            button.textContent = original;
            return;
          }
          const finalCheck = data.final_check || {};
          status.textContent = finalCheck.status === 'blocked'
            ? 'Checklist final bloqueado: ainda existem pendencias tecnicas.'
            : 'Checklist final pronto para validacao manual.';
          resultBox.innerHTML = `
            <div class='setup-section'><h3>Checklist final #${setupEscape(setupId)}</h3>
              <p><strong>Status:</strong> ${setupEscape(finalCheck.status || '-')}</p>
              <p><strong>Bloqueios:</strong> ${(finalCheck.blockers || []).length} | <strong>Manuais:</strong> ${(finalCheck.manual_required || []).length}</p>
              <pre class='setup-code'>${setupEscape(JSON.stringify(finalCheck.checks || [], null, 2))}</pre>
            </div>
          `;
          resultBox.style.display = 'block';
          button.disabled = false;
          button.textContent = original;
        } catch (error) {
          status.textContent = 'Erro no checklist final: ' + (error.message || error);
          button.disabled = false;
          button.textContent = original;
        }
      });
      function normalizeSetupDomain(value) {
        return String(value || '').replace(/^https?:\/\//i, '').replace(/\/.*$/, '').trim();
      }
      function syncWordPressAdminUrl() {
        const domain = normalizeSetupDomain(document.getElementById('setup-domain-new').value || document.getElementById('setup-domain-select').value);
        const wpUrl = document.getElementById('setup-wp-url');
        if (!domain) return;
        const expected = 'https://' + domain + '/wp-admin/';
        if (!wpUrl.value || wpUrl.dataset.manualEdited !== '1') {
          wpUrl.value = expected;
          wpUrl.dataset.autofilled = '1';
        }
      }
      document.getElementById('setup-domain-new').addEventListener('input', syncWordPressAdminUrl);
      document.getElementById('setup-domain-select').addEventListener('change', syncWordPressAdminUrl);
      document.getElementById('setup-wp-url').addEventListener('input', function() {
        this.dataset.manualEdited = '1';
      });
      document.getElementById('setup-ga4-account').addEventListener('change', function(event) { renderGa4Properties(event.target.value); });
      document.getElementById('setup-ga4-property').addEventListener('change', function(event) {
        const option = event.target.options[event.target.selectedIndex];
        const measurementId = option ? option.getAttribute('data-measurement-id') : '';
        if (measurementId) document.getElementById('setup-ga4').value = measurementId;
      });
      syncWordPressAdminUrl();
      loadSetupSites();
      loadGoogleAdsCustomers();
      loadGtmAccounts();
      loadGa4Accounts();
      refreshOAuthStatus();
    </script>
    """
    content = content.replace("__CUSTOMER_OPTIONS__", "".join(customer_options))
    content = content.replace("__SAVED_ROWS__", "".join(saved_rows) if saved_rows else "<tr><td colspan='8'>Nenhum setup salvo ainda.</td></tr>").replace("__OAUTH_BUTTON__", oauth_button_html).replace("__OAUTH_STATUS__", oauth_status_html)
    return _layout("Setup Inicial", content, "setup-inicial")





@app.get("/campaign-builder", response_class=HTMLResponse)
def campaign_builder_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    customers = google_ads_customers(CONFIG_PATH)
    customer_options = "".join(
        f"<option value='{html.escape(customer['customer_id'])}'>{html.escape(customer.get('name') or customer['customer_id'])} - {html.escape(customer['customer_id'])}</option>"
        for customer in customers
    )
    country_options = "<option value='WORLD'>Mundo - detectar idioma da URL</option>" + "".join(
        f"<option value='{html.escape(country['code'])}'>{html.escape(country['name'])} - {html.escape(country['language_name'])}</option>"
        for country in country_targets()
    )
    size_rows = "".join(
        f"<label style='display:flex;align-items:center;gap:8px;padding:10px;border:1px solid #d8e8dc;border-radius:12px;background:#fff'><input class='cb-size' type='checkbox' value='{html.escape(item['size'])}' {'checked' if item['size'] == '300x250' else ''}> <span style='flex:1'>{html.escape(item['label'])}</span><input class='cb-size-qty' data-size='{html.escape(item['size'])}' type='number' min='1' max='20' value='1' style='width:64px;padding:6px;border:1px solid #cbded1;border-radius:8px'></label>"
        for item in display_upload_sizes()
    )
    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Google Ads</div>
          <h1>Criador de Campanha</h1>
          <p>Gera rascunho de campanha a partir da URL de destino, imagens encontradas na pagina e, se informado, uma campanha modelo da conta.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Modo atual</div>
            <strong>Rascunho seguro</strong>
            <p class='muted'>Ainda nao aplica no Google Ads sem revisao.</p>
          </div>
        </div>
      </div>
      <div class='panel'>
        <h2>Dados da campanha</h2>
        <div class='grid'>
          <div style='display:grid;gap:12px'>
            <label>Conta destino Google Ads<br><select id='cb-customer' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'>{customer_options}</select></label>
            <label>Tipo de campanha<br><select id='cb-type' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='DEMAND_GEN'>Geracao de demanda</option><option value='DISPLAY'>Display</option><option value='PERFORMANCE_MAX'>Performance Max</option><option value='SEARCH'>Pesquisa</option></select></label>
            <label>Pais alvo<br><select id='cb-country' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'>{country_options}</select></label>
            <label>Or?amento diario<br><input id='cb-budget' type='number' step='0.01' min='0' placeholder='Ex: 50.00' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
            <input id='cb-site' type='hidden'>
            <label>Objetivo<br><select id='cb-traffic-goal' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='trafego'>trafego</option><option value='lead'>captacao de lead</option></select></label>
            <label>Nicho<br><input id='cb-niche' value='loan' placeholder='Ex: loan, mcmv, veraz' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
          </div>
          <div style='display:grid;gap:12px;align-content:start'>
            <label>Conta do modelo<br><select id='cb-model-customer' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value=''>Opcional</option>{customer_options}</select></label>
            <div id='cb-model-fields' style='display:none;gap:12px'>
              <label>Campanha modelo ID<br><input id='cb-model' placeholder='Opcional' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
              <label>Grupo modelo ID<br><input id='cb-model-group' placeholder='Opcional' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
            </div>
            <label>Variacao<br><input id='cb-variation' value='v1' placeholder='Ex: v2, test-196, draft-185418' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
            <label class='cb-media-field'>Midia criativa<br><select id='cb-media-type' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='imagem'>imagem</option><option value='video'>video</option><option value='misto'>misto</option></select></label>
            <label id='cb-channel-field'>Canal Demand Gen<br><select id='cb-channel' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='discover'>Discover</option><option value='youtube-instream'>In-stream no YouTube</option><option value='youtube-infeed'>In-feed no YouTube</option><option value='youtube-shorts'>YouTube Shorts</option><option value='gmail'>Gmail</option><option value='display'>Rede de Display do Google</option></select></label>
          </div>
        </div>
        <label>URL de destino<br><input id='cb-url' placeholder='https://...' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
        <label id='cb-files-field'>Imagens modelo adicionais<br><input id='cb-files' type='file' accept='image/*' multiple style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
        <div id='cb-display-section' class='panel-soft' style='display:none;margin:14px 0;padding:14px;border:1px solid #d8e8dc;border-radius:16px;background:#f7fbf8'>
          <h3 style='margin:0 0 8px'>Display: tipo de anuncio</h3>
          <p class='muted' style='margin-top:0'>Se houver campanha modelo, o sistema tenta identificar se ela usa anuncio responsivo de display ou upload de anuncios de display.</p>
          <label>Formato do anuncio Display<br><select id='cb-display-ad-method' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'>
            <option value='upload_display'>Fazer upload de anuncios de display</option>
            <option value='responsive_display'>Anuncio responsivo de display</option>
          </select></label>
          <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px;margin-top:12px'>{size_rows}</div>
        </div>
        <div id='cb-search-section' class='panel-soft' style='display:none;margin:14px 0;padding:14px;border:1px solid #d8e8dc;border-radius:16px;background:#f7fbf8'>
          <h3 style='margin:0 0 8px'>Pesquisa: palavras-chave e assets</h3>
          <p class='muted' style='margin-top:0'>Com URL + palavras-chave, o sistema gera 3 RSAs, 20 frases de destaque e 20 sitelinks.</p>
          <label>Modo de publicacao Search<br><select id='cb-publish-status' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='ENABLED'>Publicar ativa</option><option value='PAUSED'>Criar pausada para revisao</option></select></label>
          <label>Palavras-chave<br><textarea id='cb-keywords' rows='5' placeholder='palavra principal
segunda palavra
terceira palavra' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></textarea></label>
          <label>Palavras-chave negativas<br><textarea id='cb-negative-keywords' rows='4' placeholder='palavra negativa 1
palavra negativa 2' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></textarea></label>
        </div>
        <div id='cb-demand-section' class='panel-soft' style='display:none;margin:14px 0;padding:14px;border:1px solid #d8e8dc;border-radius:16px;background:#f7fbf8'>
          <h3 style='margin:0 0 8px'>Geracao de demanda: canais, publico e criativos</h3>
          <p class='muted' style='margin-top:0'>Escolha canais e informe sinais de publico. As imagens/videos sao orientados pelo tema da URL.</p>
          <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;margin-bottom:12px'>
            <label><input class='cb-dg-channel' type='checkbox' value='display' checked> Rede de Display do Google</label>
            <label><input class='cb-dg-channel' type='checkbox' value='discover'> Discover</label>
            <label><input class='cb-dg-channel' type='checkbox' value='youtube-instream'> In-stream no YouTube</label>
            <label><input class='cb-dg-channel' type='checkbox' value='youtube-infeed'> In-feed no YouTube</label>
            <label><input class='cb-dg-channel' type='checkbox' value='youtube-shorts'> YouTube Shorts</label>
            <label><input class='cb-dg-channel' type='checkbox' value='gmail'> Gmail</label>
          </div>
          <label>Publico / sinais de publico<br><textarea id='cb-audience-signals' rows='4' placeholder='Ex: adViewInterstitial
Interesse em credito
Visitantes do site
Remarketing' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></textarea></label>
        </div>
        <div id='cb-pmax-section' class='panel-soft' style='display:none;margin:14px 0;padding:14px;border:1px solid #d8e8dc;border-radius:16px;background:#f7fbf8'>
          <h3 style='margin:0 0 8px'>Performance Max: asset group e sinais</h3>
          <p class='muted' style='margin-top:0'>Informe o grupo de assets, expansao de URL e sinais de publico para o plano PMAX.</p>
          <label>Nome do asset group<br><input id='cb-asset-group-name' placeholder='Asset group 1' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'></label>
          <label>Expansao de URL final<br><select id='cb-final-url-expansion' style='width:100%;padding:12px;border-radius:12px;border:1px solid #cbded1'><option value='off'>Desativada</option><option value='on'>Ativada</option></select></label>
        </div>
        <p class='muted'>Se informar campanha ou grupo modelo, escolha tambem a conta onde esse ID existe. O sistema analisa tipo, orcamento, lances, anuncios, URLs, keywords e placements e adapta o rascunho para o pais/idioma alvo. O botao Publicar campanha aplica no Google Ads quando a pre-validacao estiver aprovada.</p>
        <button class='action-button' id='cb-generate' type='button'>Gerar rascunho</button> <button class='action-button secondary' id='cb-apply' type='button'>Aplicar campanha pausada</button>
      </div>
      <div class='panel'>
        <h2>Placar dos agentes</h2>
        <div id='cb-agent-scoreboard' class='muted'>Carregando aprendizado dos agentes...</div>
      </div>
      <div class='panel'>
        <h2>Rascunho gerado</h2>
        <div id='cb-status' class='muted'>Preencha os dados e clique em gerar.</div>
        <div id='cb-preview' style='display:none;margin-top:16px'></div>
        <details style='margin-top:18px'>
          <summary class='muted' style='cursor:pointer'>Ver JSON tecnico</summary>
          <pre id='cb-output' class='mono' style='white-space:pre-wrap;background:#f7fbf8;border:1px solid #d8e8dc;border-radius:16px;padding:16px;overflow:auto'></pre>
        </details>
      </div>
    </div>
    <script>
      async function loadAgentScoreboard() {{
        const box = document.getElementById('cb-agent-scoreboard');
        if (!box) return;
        try {{
          const response = await fetch('/api/campaign-builder/agent-scoreboard');
          const data = await response.json();
          const agents = data.agents || [];
          const tasks = data.study_tasks || [];
          if (!agents.length && !tasks.length) {{
            box.textContent = 'Ainda sem eventos de aprendizado no Campaign Builder.';
            return;
          }}
          const scoreHtml = agents.length ? `<div class='ad-meta-grid'>${{agents.slice(0, 8).map(item => `
            <div class='ad-pill'><span>${{escapeHtml(item.agent)}} ? ${{escapeHtml(item.role)}}</span><strong>${{item.score}} pts</strong><small>${{item.events}} eventos</small></div>
          `).join('')}}</div>` : '';
          const taskHtml = tasks.length ? `<h4 style='margin:14px 0 8px'>Estudo obrigatorio aberto</h4><div>${{tasks.slice(0, 6).map(item => `
            <div class='agent-warning'><strong>${{escapeHtml(item.agent)}}</strong>: ${{escapeHtml(item.title)}}<br><span class='muted'>${{escapeHtml(item.missing_evidence)}} | Regra: ${{escapeHtml(item.new_rule_needed)}}</span></div>
          `).join('')}}</div>` : '';
          box.innerHTML = scoreHtml + taskHtml;
        }} catch (error) {{
          box.textContent = 'Nao foi possivel carregar o placar dos agentes.';
        }}
      }}
      async function readFiles() {{
        const files = Array.from(document.getElementById('cb-files').files || []);
        const result = [];
        for (const file of files.slice(0, 4)) {{
          const dataUrl = await new Promise((resolve, reject) => {{
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result);
            reader.onerror = reject;
            reader.readAsDataURL(file);
          }});
          result.push({{name: file.name, type: file.type, size: file.size, data_url: dataUrl}});
        }}
        return result;
      }}
      function escapeHtml(value) {{
        return String(value || '').replace(/[&<>"']/g, (ch) => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
      }}
      function money(value) {{
        const number = Number(value || 0);
        return number.toLocaleString('pt-BR', {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
      }}
      async function loadCampaignBuilderCustomers() {{
        const selects = [document.getElementById('cb-customer'), document.getElementById('cb-model-customer')].filter(Boolean);
        if (!selects.length) return;
        const previous = new Map(selects.map(select => [select.id, select.value]));
        try {{
          const response = await fetch('/api/google-ads/customers', {{headers:{{'Accept':'application/json'}}}});
          const data = await response.json();
          const customers = data.customers || [];
          if (!customers.length) return;
          const options = customers.map(item => {{
            const id = String(item.customer_id || '');
            const label = (item.name ? item.name + ' - ' : '') + id + (item.site ? ' | ' + item.site : '') + (item.currency ? ' | ' + item.currency : '');
            return `<option value='${{escapeHtml(id)}}'>${{escapeHtml(label)}}</option>`;
          }}).join('');
          selects.forEach(select => {{
            select.innerHTML = (select.id === 'cb-model-customer' ? `<option value=''>Opcional</option>` : '') + options;
            const oldValue = previous.get(select.id);
            if (oldValue && Array.from(select.options).some(option => option.value === oldValue)) select.value = oldValue;
            if (select.id === 'cb-model-customer' && !oldValue) select.value = '';
          }});
          syncModelFields();
          const status = document.getElementById('cb-status');
          if (status) status.textContent = data.source === 'mcc_nexus' ? `Contas carregadas da MCC Julio: ${{customers.length}}.` : `Contas carregadas do config: ${{customers.length}}.`;
        }} catch (error) {{
          const status = document.getElementById('cb-status');
          if (status) status.textContent = 'Nao foi possivel atualizar contas da MCC; usando lista local.';
        }}
      }}
      function usdMoney(value) {{
        const number = Number(value || 0);
        return `US$ ${{number.toLocaleString('pt-BR', {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}}`;
      }}
      function blockEcpm(block) {{
        return block.ecpm_currency === 'USD' ? usdMoney(block.ecpm) : money(block.ecpm);
      }}
      function selectedDisplaySizes() {{
        return Array.from(document.querySelectorAll('.cb-size:checked')).map(input => {{
          const qty = document.querySelector(`.cb-size-qty[data-size="${{input.value}}"]`);
          return {{size: input.value, quantity: Number(qty?.value || 1)}};
        }});
      }}
      function fieldLines(id) {{
        const el = document.getElementById(id);
        return (el?.value || '').split('\\n').map(v => v.trim()).filter(Boolean);
      }}
      function selectedDemandGenChannels() {{
        return Array.from(document.querySelectorAll('.cb-dg-channel:checked')).map(input => input.value);
      }}
      function syncCampaignTypeSections() {{
        const type = document.getElementById('cb-type').value;
        const isSearch = type === 'SEARCH';
        const isDemand = type === 'DEMAND_GEN';
        const isDisplay = type === 'DISPLAY';
        const isPmax = type === 'PERFORMANCE_MAX';
        document.getElementById('cb-display-section').style.display = isDisplay ? 'block' : 'none';
        document.getElementById('cb-search-section').style.display = isSearch ? 'block' : 'none';
        document.getElementById('cb-demand-section').style.display = isDemand ? 'block' : 'none';
        document.getElementById('cb-pmax-section').style.display = isPmax ? 'block' : 'none';
        const channelField = document.getElementById('cb-channel-field');
        if (channelField) channelField.style.display = isDemand ? 'block' : 'none';
        document.querySelectorAll('.cb-media-field').forEach(el => el.style.display = isSearch ? 'none' : 'block');
        const filesField = document.getElementById('cb-files-field');
        if (filesField) filesField.style.display = isSearch ? 'none' : 'block';
        syncModelFields();
      }}
      function syncModelFields() {{
        const modelCustomer = document.getElementById('cb-model-customer');
        const box = document.getElementById('cb-model-fields');
        if (!box || !modelCustomer) return;
        const show = Boolean(modelCustomer.value);
        box.style.display = show ? 'grid' : 'none';
        if (!show) {{
          document.getElementById('cb-model').value = '';
          document.getElementById('cb-model-group').value = '';
        }}
      }}
      function renderList(items) {{
        return (items || []).map(item => `<li>${{escapeHtml(item)}}</li>`).join('');
      }}
      function renderPreview(data) {{
        const preview = document.getElementById('cb-preview');
        const copy = data.copy || {{}};
        const model = data.model_analysis || {{}};
        const campaign = model.campaign || {{}};
        const headlines = copy.headlines || [];
        const descriptions = copy.descriptions || [];
        const modelImages = data.model_images || [];
        const adaptedImages = data.adapted_images?.length ? data.adapted_images : (data.uploaded_model_images?.length ? data.uploaded_model_images : []);
        const uploadDisplayOnly = data.creative_preview_mode === 'upload_display_image';
        const images = adaptedImages.length ? adaptedImages : (uploadDisplayOnly ? [] : (modelImages.length ? modelImages : (data.image_suggestions || [])));
        const image = images[0]?.data_url || images[0]?.url || '';
        const displayMethod = data.display_rules?.campaign_constraints?.ad_creation_method || '';
        const displayMethodLabel = displayMethod === 'RESPONSIVE_DISPLAY_AD' ? 'Anuncio responsivo de display' : (displayMethod ? 'Fazer upload de anuncios de display' : 'Padrao Google Ads');
        const imageGallery = images.slice(0, 6).map((item) => item.data_url || item.url).filter(Boolean);
        const modelGallery = modelImages.slice(0, 6).map((item) => item.data_url || item.url).filter(Boolean);
        const title = headlines[0] || campaign.name || 'Campaign preview';
        const title2 = headlines[1] || '';
        const title3 = headlines[2] || '';
        const description = descriptions[0] || 'Ad description preview.';
        const domain = (() => {{ try {{ return new URL(data.final_url).hostname; }} catch(e) {{ return data.final_url || ''; }} }})();
        const agents = data.campaign_agents || [];
        const agentOwner = String(data.campaign_type || '').toUpperCase() === 'SEARCH' ? 'agente-search' : (String(data.campaign_type || '').toUpperCase() === 'DEMAND_GEN' ? 'agente-demandgen' : (String(data.campaign_type || '').toUpperCase() === 'PERFORMANCE_MAX' ? 'agente-pmax' : 'agente-display'));
        const agentDecision = String(data.campaign_type || '').toUpperCase() === 'SEARCH' ? 'Pode aplicar campanha pausada se checklist estiver ok' : 'Pacote validado; aplicar real somente apos assets/configuracoes obrigatorias';
        const approval = data.approval_score || {{}};
        const approvalLabel = approval.decision === 'approved' ? 'Aprovado' : (approval.decision === 'observe' ? 'Observacao' : (approval.decision === 'blocked' ? 'Bloqueado' : 'Reprovado'));
        const approvalClass = approval.decision === 'approved' ? 'ok' : (approval.decision === 'observe' ? 'warn' : 'bad');
        const agentRows = agents.map(a => `<div class='agent-row'><div><strong>${{escapeHtml(a.agent || '')}}</strong><span>${{escapeHtml(a.responsibility || '')}}</span></div><em>validado</em></div>`).join('');
        const blockerRows = (approval.blockers || []).map(x => `<span class='score-chip bad'>${{escapeHtml(x)}}</span>`).join('');
        const warningRows = (approval.warnings || []).map(x => `<span class='score-chip warn'>${{escapeHtml(x)}}</span>`).join('');
        const preflight = data.preflight || {{}};
        const assetPlan = data.asset_plan || {{}};
        const preflightClass = preflight.status === 'can_create' ? 'ok' : (preflight.status === 'review_recommended' ? 'warn' : 'bad');
        const preflightChecks = (preflight.checks || []).map(c => `<div class='agent-row'><div><strong>${{escapeHtml(c.name || '')}}</strong><span>${{escapeHtml(c.detail || '')}}</span></div><em class='${{c.status === 'ok' ? 'ok' : 'bad'}}'>${{c.status === 'ok' ? 'ok' : 'falha'}}</em></div>`).join('');
        const requiredAssets = (assetPlan.required || []).map(x => `<span class='badge'>${{escapeHtml(x)}}</span>`).join(' ');
        const autoApplyAssets = (assetPlan.auto_apply || []).map(x => `<span class='score-chip ok'>${{escapeHtml(x)}}</span>`).join(' ');
        const pendingAssets = (assetPlan.pending || []).map(x => `<span class='score-chip warn'>${{escapeHtml(x)}}</span>`).join(' ');
        const adHtml = `
          <style>
            .ad-preview-wrap {{display:grid;grid-template-columns:minmax(280px,420px) 1fr;gap:18px;align-items:start}}
            .ad-card {{background:#fff;border:1px solid #d8e8dc;border-radius:22px;box-shadow:0 14px 40px rgba(10,45,25,.08);overflow:hidden;max-width:420px}}
            .ad-image {{height:210px;background:linear-gradient(135deg,#eaf7ef,#f9f2db);display:flex;align-items:center;justify-content:center;color:#52715f;font-weight:800;letter-spacing:.08em;text-transform:uppercase}}
            .ad-image img {{width:100%;height:100%;object-fit:contain;display:block;background:#fff}}
            .ad-gallery {{display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));gap:8px;margin-top:10px}}
            .ad-gallery img {{width:100%;height:150px;object-fit:contain;border-radius:12px;border:1px solid #d8e8dc;background:#fff}}
            .ad-body {{padding:18px}}
            .ad-label {{font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:#5f7d69;margin-bottom:6px}}
            .ad-title {{font-size:22px;line-height:1.1;font-weight:900;color:#062115;margin-bottom:8px}}
            .ad-desc {{font-size:14px;line-height:1.45;color:#315342;margin-bottom:14px}}
            .ad-url {{font-size:12px;color:#167044;word-break:break-all;margin-bottom:14px}}
            .ad-cta {{display:inline-block;background:#0b6b3d;color:#fff;border-radius:999px;padding:10px 15px;font-weight:800;font-size:13px}}
            .ad-meta {{background:#f7fbf8;border:1px solid #d8e8dc;border-radius:18px;padding:16px}}
            .ad-meta-grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:10px}}
            .ad-pill {{background:#fff;border:1px solid #d8e8dc;border-radius:14px;padding:10px}}
            .ad-pill span {{display:block;font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#6c8473}}
            .ad-pill strong {{font-size:15px;color:#082417}}
            .agent-panel {{margin-top:14px;background:#fff;border:1px solid #cfe6d7;border-radius:18px;padding:14px}}
            .agent-row {{display:flex;justify-content:space-between;gap:10px;border-top:1px solid #edf5ef;padding:9px 0;align-items:flex-start}}
            .agent-row:first-of-type {{border-top:0}}
            .agent-row strong {{display:block;color:#062115;font-size:13px}}
            .agent-row span {{display:block;color:#536b5c;font-size:12px;line-height:1.35}}
            .agent-row em {{font-style:normal;background:#e7f6ed;color:#086238;border:1px solid #bfe4cb;border-radius:999px;padding:4px 8px;font-size:11px;font-weight:800;white-space:nowrap}}
            .agent-row em.bad {{background:#fde7e4;color:#9c1d13;border-color:#f2b8b0}}
            .agent-row em.ok {{background:#e7f6ed;color:#086238;border-color:#bfe4cb}}
            .agent-warning {{background:#fff7dd;border:1px solid #efd48b;color:#5b4100;border-radius:12px;padding:9px;margin-top:10px;font-size:12px}}
            .preflight-panel {{margin-top:14px;background:#fff;border:1px solid #d8e8dc;border-radius:18px;padding:14px}}
            .score-box {{display:grid;grid-template-columns:110px 1fr;gap:12px;align-items:center;background:#f7fbf8;border:1px solid #d8e8dc;border-radius:16px;padding:12px;margin:10px 0}}
            .score-number {{font-size:32px;font-weight:900;color:#062115;text-align:center}}
            .score-label {{display:inline-block;border-radius:999px;padding:5px 10px;font-size:12px;font-weight:900;text-transform:uppercase}}
            .score-label.ok,.score-chip.ok {{background:#e4f7ec;color:#066238;border:1px solid #bce4c9}}
            .score-label.warn,.score-chip.warn {{background:#fff6d8;color:#6a4700;border:1px solid #ead28a}}
            .score-label.bad,.score-chip.bad {{background:#fde7e4;color:#9c1d13;border:1px solid #f2b8b0}}
            .score-chip {{display:inline-block;border-radius:999px;padding:5px 8px;font-size:11px;font-weight:800;margin:3px}}
            @media(max-width:800px) {{.ad-preview-wrap {{grid-template-columns:1fr}}}}
          </style>
          <div class='ad-preview-wrap'>
            <div class='ad-card'>
              <div class='ad-image'>${{image ? `<img src='${{escapeHtml(image)}}' alt='preview'>` : 'Imagem do anuncio'}}</div>
              <div class='ad-body'>
                <div class='ad-label'>${{escapeHtml(data.campaign_type || 'DISPLAY')}} | ${{escapeHtml(data.target_country?.name || '')}}</div>
                ${{uploadDisplayOnly ? `<div class='ad-desc'>Modelo de upload de imagem. A pe?a original fica como refer?ncia visual; a nova pe?a deve manter esse layout e trocar os textos para o idioma selecionado antes da aplica??o.</div>` : `
                  <div class='ad-title'>${{escapeHtml(title)}}${{title2 ? ' | ' + escapeHtml(title2) : ''}}${{title3 ? ' | ' + escapeHtml(title3) : ''}}</div>
                  <div class='ad-desc'>${{escapeHtml(description)}}</div>
                  <span class='ad-cta'>Learn more</span>
                `}}
                <div class='ad-url'>${{escapeHtml(domain)}}</div>
              </div>
            </div>
            <div class='ad-meta'>
              <h3 style='margin:0 0 6px'>Como ficara configurado</h3>
              <p class='muted' style='margin:0'>Previa visual do rascunho. A criacao real ainda exigira etapa de aplicacao.</p>
              <div class='ad-meta-grid'>
                <div class='ad-pill'><span>Conta destino</span><strong>${{escapeHtml(data.customer_id)}}</strong></div>
                <div class='ad-pill'><span>Conta modelo</span><strong>${{escapeHtml(data.model_customer_id)}}</strong></div>
                <div class='ad-pill'><span>Pais/idioma</span><strong>${{escapeHtml(data.target_country?.name)}} / ${{escapeHtml(data.copy?.language)}}</strong></div>
                <div class='ad-pill'><span>Or?amento</span><strong>${{money(data.budget)}}</strong></div>
                <div class='ad-pill'><span>Nome campanha</span><strong>${{escapeHtml(data.campaign_naming?.name || '')}}</strong></div>
                <div class='ad-pill'><span>Nome grupo</span><strong>${{escapeHtml(data.ad_group_naming?.name || '')}}</strong></div>
                <div class='ad-pill'><span>UTM nivel</span><strong>${{escapeHtml(data.tracking?.level || '')}}</strong></div>
                <div class='ad-pill'><span>Modelo</span><strong>${{escapeHtml(campaign.name || 'Sem modelo')}}</strong></div>
                <div class='ad-pill'><span>URL final</span><strong>${{escapeHtml(domain)}}</strong></div>
              </div>
              <h4 style='margin:18px 0 8px'>Tracking template</h4>
              <pre class='mono' style='white-space:pre-wrap;background:#f7fbf8;border:1px solid #d8e8dc;border-radius:14px;padding:10px;overflow:auto'>${{escapeHtml(data.tracking?.template || '')}}</pre>
              <div class='preflight-panel'>
                <h4 style='margin:0 0 8px'>Pre-validacao antes de aplicar</h4>
                <span class='score-label ${{preflightClass}}'>${{escapeHtml(preflight.status || 'sem validacao')}}</span>
                <div class='ad-meta-grid' style='margin:10px 0'>
                  <div class='ad-pill'><span>Falhas criticas</span><strong>${{preflight.critical_failures ?? 0}}</strong></div>
                  <div class='ad-pill'><span>Avisos</span><strong>${{preflight.warnings ?? 0}}</strong></div>
                  <div class='ad-pill'><span>Uploads</span><strong>${{assetPlan.uploaded_assets ?? 0}}</strong></div>
                  <div class='ad-pill'><span>Imagens landing</span><strong>${{assetPlan.landing_images ?? 0}}</strong></div>
                </div>
                ${{preflightChecks || '<p class="muted">Sem checklist retornado.</p>'}}
                <h4 style='margin:14px 0 6px'>Plano de assets</h4>
                <div>${{requiredAssets || '<span class="muted">Sem requisitos.</span>'}}</div>
                <div style='margin-top:8px'>Auto aplica: ${{autoApplyAssets || '<span class="muted">nenhum</span>'}}</div>
                <div style='margin-top:8px'>Pendente: ${{pendingAssets || '<span class="muted">nada pendente</span>'}}</div>
              </div>
              <div class='agent-panel'>
                <h4 style='margin:0 0 8px'>Revisao dos agentes</h4>
                <div class='score-box'>
                  <div class='score-number'>${{approval.score_total ?? 0}}</div>
                  <div>
                    <span class='score-label ${{approvalClass}}'>${{approvalLabel}}</span>
                    <p class='muted' style='margin:6px 0 0'>${{escapeHtml(approval.rule || '')}}</p>
                  </div>
                </div>
                <div class='ad-meta-grid' style='margin-bottom:8px'>
                  <div class='ad-pill'><span>Financeiro</span><strong>${{approval.score_financial_impact ?? 0}}/40</strong></div>
                  <div class='ad-pill'><span>Evidencia</span><strong>${{approval.score_evidence ?? 0}}/25</strong></div>
                  <div class='ad-pill'><span>Risco</span><strong>${{approval.score_risk ?? 0}}/20</strong></div>
                  <div class='ad-pill'><span>Clareza</span><strong>${{approval.score_clarity ?? 0}}/15</strong></div>
                  <div class='ad-pill'><span>Responsavel final</span><strong>${{escapeHtml(agentOwner)}}</strong></div>
                  <div class='ad-pill'><span>Decisao</span><strong>${{escapeHtml(agentDecision)}}</strong></div>
                  <div class='ad-pill'><span>Agentes envolvidos</span><strong>${{agents.length}}</strong></div>
                </div>
                ${{blockerRows ? `<h4 style='margin:12px 0 5px'>Bloqueios</h4><div>${{blockerRows}}</div>` : ''}}
                ${{warningRows ? `<h4 style='margin:12px 0 5px'>Avisos</h4><div>${{warningRows}}</div>` : ''}}
                ${{agentRows || '<p class="muted">Nenhum agente retornado para este rascunho.</p>'}}
                <div class='agent-warning'>Se algum item obrigatorio faltar, a ferramenta deve manter como pacote validado e nao aplicar campanha ativa.</div>
              </div>
              ${{uploadDisplayOnly ? `
                <h4 style='margin:18px 0 8px'>Imagem original do modelo</h4>
                <p class='muted' style='margin:0 0 8px'>Modelo base visivel. Use este layout como referencia e substitua os textos pelo idioma do pais alvo.</p>
                <div class='ad-gallery'>${{modelGallery.map(src => `<img src='${{escapeHtml(src)}}' alt='modelo original'>`).join('') || '<span class="muted">Sem imagem original retornada pela API.</span>'}}</div>
                ${{adaptedImages.length ? `
                  <h4 style='margin:18px 0 8px'>Imagem nova gerada</h4>
                  <div class='ad-gallery'>${{imageGallery.map(src => `<img src='${{escapeHtml(src)}}' alt='imagem nova gerada'>`).join('')}}</div>
                ` : `
                  <h4 style='margin:18px 0 8px'>Imagem nova gerada</h4>
                  <p class='muted'>Ainda nao foi gerada. Status: ${{escapeHtml(data.image_generation?.status || 'pendente')}} ${{data.image_generation?.message ? '- ' + escapeHtml(data.image_generation.message) : ''}}</p>
                `}}
              ` : `
                <h4 style='margin:18px 0 8px'>Headlines</h4>
                <div>${{headlines.slice(0,8).map(h => `<span class='badge'>${{escapeHtml(h)}}</span>`).join(' ')}}</div>
                <h4 style='margin:18px 0 8px'>Descricoes</h4>
                <ul style='margin:0;padding-left:18px'>${{descriptions.slice(0,4).map(d => `<li>${{escapeHtml(d)}}</li>`).join('')}}</ul>
              `}}
              ${{data.search_rules ? `
                <h4 style='margin:18px 0 8px'>Regras Pesquisa</h4>
                <div class='ad-meta-grid'>
                  <div class='ad-pill'><span>Keywords</span><strong>${{data.search_rules.keywords.length}}</strong></div>
                  <div class='ad-pill'><span>Negativas</span><strong>${{data.search_rules.negative_keywords.length}}</strong></div>
                  <div class='ad-pill'><span>Anuncios</span><strong>${{data.search_asset_plan?.ads?.length || 0}}</strong></div>
                  <div class='ad-pill'><span>Frases destaque</span><strong>${{data.search_asset_plan?.callouts?.length || 0}}</strong></div>
                  <div class='ad-pill'><span>Sitelinks</span><strong>${{data.search_asset_plan?.sitelinks?.length || 0}}</strong></div>
                </div>
                <h4 style='margin:18px 0 8px'>3 anuncios responsivos</h4>
                <div>${{(data.search_asset_plan?.ads || []).map(ad => `<span class='badge'>${{escapeHtml(ad.name)}}: ${{escapeHtml((ad.headlines || [])[0] || '')}}</span>`).join(' ')}}</div>
                <h4 style='margin:18px 0 8px'>20 frases de destaque</h4>
                <div>${{(data.search_asset_plan?.callouts || []).map(p => `<span class='badge'>${{escapeHtml(p)}}</span>`).join(' ')}}</div>
                <h4 style='margin:18px 0 8px'>20 sitelinks</h4>
                <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px'>${{(data.search_asset_plan?.sitelinks || []).map(p => `<div class='ad-pill'><strong>${{escapeHtml(p.text || '')}}</strong><span>${{escapeHtml(p.description_1 || p.description1 || '')}}</span><span>${{escapeHtml(p.description_2 || p.description2 || '')}}</span></div>`).join('')}}</div>
                <h4 style='margin:18px 0 8px'>Palavras-chave</h4>
                <div>${{data.search_rules.keywords.map(p => `<span class='badge'>${{escapeHtml(p)}}</span>`).join(' ')}}</div>
                <h4 style='margin:18px 0 8px'>Negativas</h4>
                <div>${{data.search_rules.negative_keywords.map(p => `<span class='badge'>${{escapeHtml(p)}}</span>`).join(' ')}}</div>
              ` : ''}}
              ${{data.demand_gen_plan ? `
                <h4 style='margin:18px 0 8px'>Plano Demand Gen</h4>
                <div class='ad-meta-grid'>
                  <div class='ad-pill'><span>Canais</span><strong>${{(data.demand_gen_plan.channels || []).join(', ')}}</strong></div>
                  <div class='ad-pill'><span>Publicos</span><strong>${{data.demand_gen_plan.audience?.signals?.length || 0}}</strong></div>
                  <div class='ad-pill'><span>Midia</span><strong>${{escapeHtml(data.demand_gen_plan.creative_brief?.media_type || '')}}</strong></div>
                </div>
                <h4 style='margin:18px 0 8px'>Brief imagem/video</h4>
                <pre class='mono' style='white-space:pre-wrap;background:#f7fbf8;border:1px solid #d8e8dc;border-radius:14px;padding:10px'>${{escapeHtml(data.demand_gen_plan.creative_brief?.image_prompt || '')}}

${{escapeHtml(data.demand_gen_plan.creative_brief?.video_prompt || '')}}</pre>
              ` : ''}}
              ${{data.pmax_plan ? `
                <h4 style='margin:18px 0 8px'>Plano PMAX</h4>
                <div class='ad-meta-grid'>
                  <div class='ad-pill'><span>Asset group</span><strong>${{escapeHtml(data.pmax_plan.asset_group_name || '')}}</strong></div>
                  <div class='ad-pill'><span>Expansao URL</span><strong>${{escapeHtml(data.pmax_plan.final_url_expansion || '')}}</strong></div>
                  <div class='ad-pill'><span>Sinais</span><strong>${{data.pmax_plan.audience_signals?.length || 0}}</strong></div>
                </div>
                <h4 style='margin:18px 0 8px'>Brief PMAX</h4>
                <pre class='mono' style='white-space:pre-wrap;background:#f7fbf8;border:1px solid #d8e8dc;border-radius:14px;padding:10px'>${{escapeHtml(data.pmax_plan.creative_brief?.image_prompt || '')}}

${{escapeHtml(data.pmax_plan.creative_brief?.video_prompt || '')}}</pre>
              ` : ''}}
              ${{data.display_rules ? `
                <h4 style='margin:18px 0 8px'>Regras Display obrigatorias</h4>
                <div class='ad-meta-grid'>
                  <div class='ad-pill'><span>Grupo por campanha</span><strong>${{data.display_rules.campaign_constraints.ad_group_count}}</strong></div>
                  <div class='ad-pill'><span>Metodo</span><strong>${{escapeHtml(displayMethodLabel)}}</strong></div>
                  <div class='ad-pill'><span>Segmentacao otimizada</span><strong>Desativada</strong></div>
                  <div class='ad-pill'><span>Keyword unica</span><strong>${{escapeHtml(data.display_rules.targeting.keyword.keyword)}}</strong></div>
                </div>
                <h4 style='margin:18px 0 8px'>Tamanhos selecionados</h4>
                <div>${{data.display_rules.upload_plan.selected_sizes.map(s => `<span class='badge'>${{escapeHtml(s.size)}} x ${{escapeHtml(s.quantity)}}</span>`).join(' ')}}</div>
                <h4 style='margin:18px 0 8px'>Adequacao de conteudo</h4>
                <ul style='margin:0;padding-left:18px'>${{renderList(data.display_rules.content_suitability.sensitive_content_excluded)}}</ul>
                <h4 style='margin:18px 0 8px'>Canais excluidos proprios</h4>
                <div>${{data.display_rules.excluded_placements.map(p => `<span class='badge'>${{escapeHtml(p)}}</span>`).join(' ')}}</div>
              ` : ''}}
            </div>
          </div>`;
        preview.innerHTML = adHtml;
        preview.style.display = 'block';
      }}

      function setSelectValue(id, value) {{
        const el = document.getElementById(id);
        if (!el || !value) return;
        const normalized = String(value).toUpperCase();
        const option = Array.from(el.options || []).find((item) => String(item.value).toUpperCase() === normalized || String(item.textContent).toUpperCase() === normalized);
        if (option) el.value = option.value;
      }}
      function setInputValue(id, value) {{
        const el = document.getElementById(id);
        if (el && value) el.value = value;
      }}
      async function loadModelPrefill() {{
        const campaignId = document.getElementById('cb-model').value.trim();
        const groupId = document.getElementById('cb-model-group').value.trim();
        if (!campaignId && !groupId) return;
        const status = document.getElementById('cb-status');
        status.textContent = 'Analisando campanha modelo...';
        try {{
          const response = await fetch('/api/campaign-builder/model-prefill', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{
            customer_id: document.getElementById('cb-customer').value,
            model_customer_id: document.getElementById('cb-model-customer').value,
            model_campaign_id: campaignId,
            model_ad_group_id: groupId
          }})}});
          const data = await response.json();
          if (data.status !== 'ok') {{
            status.textContent = data.error ? `Modelo nao analisado: ${{data.error}}` : 'Modelo nao encontrado.';
            return;
          }}
          const prefill = data.prefill || {{}};
          setInputValue('cb-site', prefill.site);
          setSelectValue('cb-type', prefill.campaign_type);
          setSelectValue('cb-traffic-goal', prefill.traffic_goal);
          setInputValue('cb-niche', prefill.niche);
          setInputValue('cb-variation', prefill.variation || 'v1');
          setSelectValue('cb-media-type', prefill.media_type);
          setSelectValue('cb-channel', prefill.channel);
          setSelectValue('cb-display-ad-method', prefill.display_ad_method);
          syncCampaignTypeSections();
          status.textContent = `Modelo identificado e ajustado ao padrao: ${{data.model?.campaign_name || campaignId}}`;
        }} catch (error) {{
          status.textContent = `Falha ao analisar modelo: ${{error.message || error}}`;
        }}
      }}
      document.getElementById('cb-model').addEventListener('change', loadModelPrefill);
      document.getElementById('cb-model').addEventListener('blur', loadModelPrefill);
      document.getElementById('cb-model-group').addEventListener('change', loadModelPrefill);
      document.getElementById('cb-model-customer').addEventListener('change', loadModelPrefill);

      document.getElementById('cb-type').addEventListener('change', syncCampaignTypeSections);
      document.getElementById('cb-model-customer').addEventListener('change', syncModelFields);
      syncCampaignTypeSections();
      loadCampaignBuilderCustomers();
      loadAgentScoreboard();
      async function campaignBuilderPayload() {{
        return {{
          customer_id: document.getElementById('cb-customer').value,
          model_customer_id: document.getElementById('cb-model-customer').value,
          target_country: document.getElementById('cb-country').value,
          campaign_type: document.getElementById('cb-type').value,
          budget: document.getElementById('cb-budget').value,
          final_url: document.getElementById('cb-url').value,
          model_campaign_id: document.getElementById('cb-model').value,
          model_ad_group_id: document.getElementById('cb-model-group').value,
          site: document.getElementById('cb-site').value,
          traffic_goal: document.getElementById('cb-traffic-goal').value,
          niche: document.getElementById('cb-niche').value,
          variation: document.getElementById('cb-variation').value,
          media_type: document.getElementById('cb-media-type').value,
          channel: document.getElementById('cb-channel').value,
          uploaded_images: await readFiles(),
          display_ad_method: document.getElementById('cb-display-ad-method').value,
          display_sizes: selectedDisplaySizes(),
          keywords: fieldLines('cb-keywords'),
          negative_keywords: fieldLines('cb-negative-keywords'),
          demand_gen_channels: selectedDemandGenChannels(),
          audience_signals: fieldLines('cb-audience-signals'),
          asset_group_name: document.getElementById('cb-asset-group-name').value,
          final_url_expansion: document.getElementById('cb-final-url-expansion').value,
          publish_status: document.getElementById('cb-publish-status').value
        }};
      }}
      document.getElementById('cb-generate').addEventListener('click', async () => {{
        const status = document.getElementById('cb-status');
        const output = document.getElementById('cb-output');
        const preview = document.getElementById('cb-preview');
        status.textContent = 'Preparando pacote de publicacao...';
        output.textContent = '';
        preview.style.display = 'none';
        preview.innerHTML = '';
        const payload = await campaignBuilderPayload();
        const response = await fetch('/api/campaign-builder/draft', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(payload)}});
        const data = await response.json();
        status.textContent = data.status === 'error' ? 'Erro ao preparar pacote' : 'Pacote visual pronto para publicar';
        output.textContent = JSON.stringify(data, null, 2);
        if (data.status !== 'error') renderPreview(data);
      }});

      document.getElementById('cb-apply').addEventListener('click', async () => {{
        const status = document.getElementById('cb-status');
        const output = document.getElementById('cb-output');
        const preview = document.getElementById('cb-preview');
        const payload = await campaignBuilderPayload();
        const type = String(payload.campaign_type || '').toUpperCase();
        status.textContent = 'Validando e publicando campanha pausada no Google Ads...';
        const response = await fetch('/api/campaign-builder/apply', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(payload)}});
        const data = await response.json();
        if (data.status === 'applied_paused' || data.status === 'applied_published') {{
          const id = data.result && data.result.campaign_id ? ' | ID: ' + data.result.campaign_id : '';
          status.textContent = (data.message || 'Campanha criada no Google Ads') + id;
        }} else if (data.status === 'already_exists') {{
          const id = data.result && data.result.campaign_id ? ' | ID existente: ' + data.result.campaign_id : '';
          status.textContent = (data.message || 'Campanha ja existe no Google Ads') + id;
        }} else if (data.status === 'draft_only') {{
          status.textContent = 'Pacote validado pela ferramenta. Campanha aplicada pausada quando o Google Ads aceitar o pacote.';
        }} else if (data.status === 'blocked') {{
          status.textContent = 'Publicacao bloqueada pela pre-validacao. Corrija os campos criticos e tente novamente.';
        }} else if (data.status === 'error') {{
          status.textContent = 'Erro ao publicar: ' + (data.error || data.message || 'sem detalhe retornado');
        }} else {{
          status.textContent = data.error || data.message || 'Aplicacao nao concluida';
        }}
        output.textContent = JSON.stringify(data, null, 2);
        if (data.draft && data.draft.status !== 'error') renderPreview(data.draft);
      }});
    </script>
    """
    return _layout("Criador de Campanha", content, "campaign-builder")


@app.get("/gam-sync", response_class=HTMLResponse)
def gam_sync_page() -> str:
    runtime = load_runtime(CONFIG_PATH)
    rows = load_gam_sync_rows(runtime, limit=200)
    lines = []
    for row in rows:
        lines.append(
            "<tr>"
            f"<td>{_status_badge(str(row['status']))}</td>"
            f"<td>{html.escape(str(row['site_name']))}</td>"
            f"<td>{html.escape(str(row['block_name']))}</td>"
            f"<td class='mono'>{html.escape(str(row['attr_id']))}</td>"
            f"<td class='mono'>{html.escape(str(row['current_rule_key']))}</td>"
            f"<td class='mono'>{html.escape(str(row['target_rule_key']))}</td>"
            f"<td class='nowrap'>{_money(float(row['current_floor_brl']))}</td>"
            f"<td class='nowrap'>{_money(float(row['target_floor_brl']))}</td>"
            f"<td class='nowrap'>{html.escape(str(row['created_at']))}</td>"
            f"<td class='nowrap'>{html.escape(str(row['synced_at']))}</td>"
            "</tr>"
        )
    content = f"""
    <div class='shell'>
      <div class='masthead'>
        <div class='hero'>
          <div class='aside-label'>Fila do GAM</div>
          <h1>Execucao GAM</h1>
          <p>Jobs prontos para aplicador externo, com piso atual, piso alvo e status de sincronizacao.</p>
          [[NAV]]
        </div>
        <div class='aside'>
          <div>
            <div class='aside-label'>Ultima atualizacao</div>
            <strong>{html.escape(load_last_updated(runtime))}</strong>
          </div>
        </div>
      </div>
      <div class='panel'>
        <table>
          <thead>
            <tr>
              <th>Status</th><th>Site</th><th>Bloco</th><th>Attr ID</th><th>Rule Atual</th>
              <th>Rule Alvo</th><th>Piso Atual</th><th>Piso Alvo</th><th>Criado</th><th>Sincronizado</th>
            </tr>
          </thead>
          <tbody>{''.join(lines)}</tbody>
        </table>
      </div>
    </div>
    """
    return _layout("Execucao GAM", content, "sync")
