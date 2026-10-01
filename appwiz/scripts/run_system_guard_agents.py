#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import webapp_service
from quality_error_registry import register_quality_error, register_predictive_risk
try:
    from hermes_bridge import queue_operational_event
except ModuleNotFoundError:
    def queue_operational_event(*args, **kwargs):
        return None

APP_TZ = ZoneInfo("America/Sao_Paulo")
FINAL_STATUSES = {"applied", "success", "dry_run", "cancelled", "canceled", "error", "superseded", "ignored", "resolved"}


def now_sp() -> datetime:
    return datetime.now(APP_TZ)


def as_float(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def queue_learning(conn, *, now: str, owner: str, reviewer: str, entity_key: str, lesson_type: str, lesson_text: str, evidence: dict) -> None:
    exists = conn.execute(
        """
        SELECT 1 FROM nexus_agent_learning_queue
        WHERE owner_agent_key=? AND entity_key=? AND lesson_type=? AND lower(status) IN ('open','pending')
        LIMIT 1
        """,
        (owner, entity_key, lesson_type),
    ).fetchone()
    if exists:
        return
    conn.execute(
        """
        INSERT INTO nexus_agent_learning_queue
            (created_at, status, owner_agent_key, reviewer_agent_key, entity_key, lesson_type, lesson_text, evidence_json)
        VALUES (?, 'pending', ?, ?, ?, ?, ?, ?)
        """,
        (now, owner, reviewer, entity_key, lesson_type, lesson_text, json.dumps(evidence, ensure_ascii=False, sort_keys=True)),
    )


def ensure_guard_event_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nexus_agent_events (
            event_id TEXT PRIMARY KEY,
            agent_slug TEXT NOT NULL DEFAULT '',
            event_type TEXT NOT NULL DEFAULT '',
            priority TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            entity_type TEXT NOT NULL DEFAULT '',
            entity_id TEXT NOT NULL DEFAULT '',
            project_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operational_health_events (
            event_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT '',
            area TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nexus_agent_learning_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            owner_agent_key TEXT NOT NULL DEFAULT '',
            reviewer_agent_key TEXT NOT NULL DEFAULT '',
            entity_key TEXT NOT NULL DEFAULT '',
            lesson_type TEXT NOT NULL DEFAULT '',
            lesson_text TEXT NOT NULL DEFAULT '',
            evidence_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nexus_agent_learnings (
            learning_key TEXT PRIMARY KEY,
            agent_slug TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            rule TEXT NOT NULL DEFAULT '',
            learned_at TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )


def event(conn, *, now: str, agent: str, priority: str, title: str, message: str, entity_type: str = "system", entity_id: str = "", project_id: str = "", metadata: dict | None = None, impact_score: int = 50, next_action: str = "Revisar evidencia e executar correcao segura.", auto_fix: str = "none") -> None:
    ensure_guard_event_tables(conn)
    register_quality_error(
        agent_slug=agent,
        scope=project_id or entity_type or "system",
        severity=priority,
        error_type="system_guard",
        message=title or message,
        phase="guard_event",
        source="wiz-system-guard-agents",
        evidence={"message": message, "entity_type": entity_type, "entity_id": entity_id, "project_id": project_id, "metadata": metadata or {}},
        learned_rule="Guardas do sistema devem registrar eventos no banco maximo de erros.",
    )
    register_predictive_risk(
        agent_slug=agent,
        scope=project_id or entity_type or "system",
        severity="INFO" if priority == "INFO" else "HIGH",
        message=f"Risco preditivo avaliado antes/depois do evento: {title}",
        phase="predictive_guard",
        source="wiz-system-guard-agents",
        evidence={"auto_fix": auto_fix, "next_action": next_action},
        learned_rule="Todo evento operacional gera memoria preditiva para evitar repeticao.",
    )
    if impact_score == 50:
        impact_score = {"CRITICAL": 100, "HIGH": 80, "INFO": 10}.get(priority, 50)
    if next_action == "Revisar evidencia e executar correcao segura.":
        if priority in {"HIGH", "CRITICAL"}:
            next_action = "Responsavel deve corrigir a causa raiz antes de permitir decisao automatica baseada nesses dados."
        else:
            next_action = "Manter monitoramento horario."
    payload_data = dict(metadata or {})
    payload_data.update({"impact_score": impact_score, "next_action": next_action, "auto_fix": auto_fix, "timezone": "America/Sao_Paulo"})
    payload = json.dumps(payload_data, ensure_ascii=False, sort_keys=True)
    key_base = f"{agent}:{title}:{entity_type}:{entity_id or project_id or 'global'}:{now[:13]}"
    event_id = "guard::" + str(abs(hash(key_base)))
    conn.execute(
        """
        INSERT OR REPLACE INTO nexus_agent_events
            (event_id, agent_slug, event_type, priority, title, message, entity_type, entity_id, project_id, created_at, metadata_json)
        VALUES (?, ?, 'system_guard', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (event_id, agent, priority, title, message, entity_type, entity_id, project_id, now, payload),
    )
    payload_data["hermes_dispatch"] = "deferred_by_guard_no_reentrant_sqlite_lock"

    if priority in {"HIGH", "CRITICAL"}:
        conn.execute(
            """
            INSERT OR REPLACE INTO operational_health_events
                (event_key, created_at, severity, area, message, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, now, priority, agent, message, payload),
        )
        if agent not in {"orquestrador", "aprendizado"}:
            queue_learning(conn, now=now, owner=agent, reviewer="orquestrador", entity_key=entity_id or project_id or title, lesson_type=title.lower().replace(" ", "_"), lesson_text=next_action, evidence=payload_data)


def valid_host(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", str(value or "").strip().lower()))

def check_web_health(conn, now: str) -> int:
    try:
        with urlopen("http://127.0.0.1:8010/health", timeout=8) as response:
            body = response.read().decode("utf-8", "ignore")[:200]
            if response.status == 200:
                event(conn, now=now, agent="saude-web", priority="INFO", title="Web OK", message="Healthcheck local respondeu 200.", metadata={"body": body})
                return 0
            raise RuntimeError(f"HTTP {response.status}: {body}")
    except Exception as exc:
        restarted = False
        try:
            subprocess.run(["systemctl", "restart", "wiz-roas-web.service"], check=True, timeout=20)
            restarted = True
        except Exception:
            restarted = False
        event(conn, now=now, agent="saude-web", priority="CRITICAL", title="Web falhou", message=f"Healthcheck local falhou: {exc}. Restart automatico: {'sim' if restarted else 'nao'}.", metadata={"error": str(exc), "restarted": restarted}, impact_score=100, next_action="Investigar logs do servico e corrigir rota/timeout para evitar 502/504 recorrente.", auto_fix="restart_wiz_roas_web" if restarted else "failed_restart")
        return 1


def check_ads_script(conn, now_dt: datetime, now: str) -> int:
    cutoff = (now_dt - timedelta(minutes=90)).isoformat(timespec="seconds")
    recent = conn.execute("SELECT COUNT(*) c FROM google_ads_script_runs WHERE created_at >= ? AND status IN ('success','preview')", (cutoff,)).fetchone()["c"]
    errors = conn.execute("SELECT COUNT(*) c FROM google_ads_script_runs WHERE created_at >= ? AND status='error'", (cutoff,)).fetchone()["c"]
    if errors:
        event(conn, now=now, agent="google-ads-script", priority="HIGH", title="Apps Script com erro", message=f"Apps Script registrou {errors} erro(s) nos ultimos 90 minutos.", metadata={"cutoff": cutoff})
        return 1
    if not recent:
        event(conn, now=now, agent="google-ads-script", priority="HIGH", title="Apps Script atrasado", message="Nenhum snapshot recente do Apps Script nos ultimos 90 minutos.", metadata={"cutoff": cutoff})
        return 1
    event(conn, now=now, agent="google-ads-script", priority="INFO", title="Apps Script OK", message="Snapshots recentes do Apps Script encontrados.", metadata={"recent_runs": recent})
    return 0


def check_pending_actions(conn, now_dt: datetime, now: str) -> int:
    problems = 0
    for table, label, agent in [
        ("campaign_budget_manual_actions", "orcamento", "orcamento-cpa"),
        ("campaign_bid_manual_actions", "cpa/cpc", "orcamento-cpa"),
        ("campaign_status_manual_actions", "status", "orquestrador"),
    ]:
        cutoff = (now_dt - timedelta(hours=2)).isoformat(timespec="seconds")
        rows = conn.execute(
            f"SELECT project, attr_id, campaign, status, created_at FROM {table} WHERE created_at < ? AND lower(status) NOT IN ('applied','success','dry_run','cancelled','canceled','error','superseded','ignored','resolved') LIMIT 20",
            (cutoff,),
        ).fetchall()
        for row in rows:
            problems += 1
            event(
                conn,
                now=now,
                agent=agent,
                priority="HIGH",
                title=f"Acao de {label} pendente",
                message=f"Acao pendente ha mais de 2h: {row['campaign']} | status {row['status']}.",
                entity_type="campaign",
                entity_id=str(row["attr_id"] or ""),
                project_id=str(row["project"] or ""),
                metadata=dict(row),
            )
    return problems


def check_data_consistency(conn, today: str, now: str) -> int:
    problems = 0
    rows = conn.execute(
        """
        SELECT a.site, a.attr_id, MAX(a.campaign) campaign, SUM(a.cost) cost, COALESCE(g.revenue,0) revenue
        FROM ads_daily a
        LEFT JOIN (
            SELECT site, attr_id, SUM(revenue) revenue FROM gam_attr_daily WHERE day=? GROUP BY site, attr_id
        ) g ON g.site=a.site AND g.attr_id=a.attr_id
        WHERE a.day=? AND a.cost > 0
        GROUP BY a.site, a.attr_id
        HAVING cost >= 5 AND revenue <= 0
        LIMIT 50
        """,
        (today, today),
    ).fetchall()
    for row in rows:
        problems += 1
        event(conn, now=now, agent="consistencia-dados", priority="HIGH", title="Gasto sem receita atribuida", message=f"{row['site']} attr {row['attr_id']} tem gasto R${row['cost']:.2f} e receita GAM zero.", entity_type="attr_id", entity_id=str(row["attr_id"]), project_id=str(row["site"]), metadata=dict(row))

    rows = conn.execute(
        """
        SELECT a.site, a.attr_id, MAX(a.campaign) campaign, SUM(a.cost) cost,
               MAX(COALESCE(NULLIF(a.daily_budget,0), NULLIF(s.daily_budget,0), 0)) budget
        FROM ads_daily a
        LEFT JOIN google_ads_campaign_settings_daily s
          ON s.day=a.day AND s.customer_id=a.customer_id AND s.campaign_id=a.campaign_id
        WHERE a.day=? AND a.cost > 0
        GROUP BY a.site, a.attr_id
        HAVING budget <= 0
        LIMIT 50
        """,
        (today,),
    ).fetchall()
    for row in rows:
        problems += 1
        event(conn, now=now, agent="google-ads-script", priority="HIGH", title="Orcamento ausente com gasto", message=f"{row['site']} attr {row['attr_id']} tem gasto mas sem orcamento exato no snapshot.", entity_type="attr_id", entity_id=str(row["attr_id"]), project_id=str(row["site"]), metadata=dict(row))
    return problems


def check_gam_consistency(conn, today: str, now: str) -> int:
    problems = 0
    rows = conn.execute(
        """
        SELECT site, attr_id, SUM(revenue) revenue, SUM(impressions) imps, SUM(unfilled_impressions) unfilled,
               SUM(total_requests) requests
        FROM gam_attr_daily
        WHERE day=?
        GROUP BY site, attr_id
        HAVING revenue > 0 AND imps <= 0
        LIMIT 50
        """,
        (today,),
    ).fetchall()
    for row in rows:
        problems += 1
        event(conn, now=now, agent="gam", priority="HIGH", title="GAM receita sem impressoes", message=f"{row['site']} attr {row['attr_id']} tem receita GAM mas sem impressoes atribu?das.", entity_type="attr_id", entity_id=str(row["attr_id"]), project_id=str(row["site"]), metadata=dict(row))

    rows = conn.execute(
        """
        SELECT site, attr_id, COUNT(DISTINCT ROUND(match_rate,4)) pmr_values, SUM(revenue) revenue, SUM(impressions) imps
        FROM gam_attr_daily
        WHERE day=?
        GROUP BY site, attr_id
        HAVING imps > 0 AND pmr_values = 1 AND COUNT(*) >= 3
        LIMIT 50
        """,
        (today,),
    ).fetchall()
    for row in rows:
        event(conn, now=now, agent="gam", priority="INFO", title="PMR uniforme para revisar", message=f"{row['site']} attr {row['attr_id']} tem PMR igual em varios blocos; revisar se veio agregado do GAM.", entity_type="attr_id", entity_id=str(row["attr_id"]), project_id=str(row["site"]), metadata=dict(row))
    return problems


def check_utm_attrid(conn, today: str, now: str) -> int:
    problems = 0
    rows = conn.execute(
        """
        SELECT site, attr_id, campaign, campaign_id, ad_group_id, channel_type, attr_level, cost
        FROM ads_daily
        WHERE day=? AND cost > 0 AND upper(campaign) LIKE '%DEMAND_GEN%' AND COALESCE(ad_group_id,'') != '' AND attr_id != ad_group_id
        LIMIT 50
        """,
        (today,),
    ).fetchall()
    for row in rows:
        problems += 1
        event(conn, now=now, agent="utm-attrid", priority="HIGH", title="Demand Gen fora do adgroupId", message=f"Demand Gen deve usar adgroupId como attrId: {row['campaign']}", entity_type="campaign", entity_id=str(row["campaign_id"]), project_id=str(row["site"]), metadata=dict(row))
    return problems


def check_new_campaigns(conn, today: str, now: str) -> int:
    problems = 0
    rows = conn.execute(
        """
        SELECT a.site, a.customer_id, a.campaign_id, MAX(a.campaign) campaign, SUM(a.cost) cost
        FROM ads_daily a
        LEFT JOIN projects p ON p.project_id=a.site OR p.project_name=a.site
        WHERE a.day=? AND a.cost > 0 AND COALESCE(a.site,'') != '' AND p.project_id IS NULL
        GROUP BY a.site, a.customer_id, a.campaign_id
        LIMIT 50
        """,
        (today,),
    ).fetchall()
    for row in rows:
        problems += 1
        auto_fix = "none"
        if valid_host(row["site"]):
            conn.execute(
                """
                INSERT OR IGNORE INTO projects (project_id, project_name, country, niche, currency, status, metadata_json)
                VALUES (?, ?, '', '', 'BRL', 'active', ?)
                """,
                (row["site"], row["site"], json.dumps({"created_by": "campanhas-novas", "reason": "ads_cost_detected", "campaign_id": row["campaign_id"], "created_at": now}, ensure_ascii=False)),
            )
            auto_fix = "project_inserted_if_missing"
        event(conn, now=now, agent="campanhas-novas", priority="HIGH", title="Projeto com gasto nao cadastrado", message=f"{row['site']} tem campanha com gasto e nao estava em projects.", entity_type="campaign", entity_id=str(row["campaign_id"]), project_id=str(row["site"]), metadata=dict(row), impact_score=80, next_action="Validar mapeamento GAM/Ads do novo projeto e liberar exibicao no painel.", auto_fix=auto_fix)
    return problems


def ensure_agent_entity_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nexus_agent_entity_state (
            agent_slug TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'active',
            score INTEGER NOT NULL DEFAULT 0,
            mode TEXT NOT NULL DEFAULT 'operational',
            last_seen_at TEXT NOT NULL DEFAULT '',
            last_improved_at TEXT NOT NULL DEFAULT '',
            repeated_errors INTEGER NOT NULL DEFAULT 0,
            learned_rules INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    for agent in [
        "saude-web", "consistencia-dados", "google-ads-script", "gam",
        "orcamento-cpa", "campanhas-novas", "utm-attrid", "aprendizado",
        "orquestrador", "floor-verification",
    ]:
        conn.execute(
            """
            INSERT OR IGNORE INTO nexus_agent_entity_state
                (agent_slug, status, score, mode, last_seen_at, metadata_json)
            VALUES (?, 'active', 0, 'operational', '', '{}')
            """,
            (agent,),
        )


def _learning_rule_text(owner: str, lesson_type: str, repeats: int) -> str:
    templates = {
        "orcamento_ausente_com_gasto": "Antes de recomendar aumento de or?amento, validar se o snapshot trouxe daily_budget real; se houver gasto e or?amento ausente, bloquear decis?o e corrigir Apps Script.",
        "gasto_sem_receita_atribuida": "Quando houver gasto sem receita por attrId, n?o usar ROI por attrId para decis?o; validar UTM/GAM/MGID/Push e manter receita apenas no topo se n?o for atribu?vel.",
        "demand_gen_fora_do_adgroupid": "Demand Gen deve usar adgroupId como attrId; qualquer campanha com campaignId no attrId deve ser marcada como tracking inv?lido antes de otimizar.",
        "gam_receita_sem_impressoes": "Receita GAM sem impress?es invalida PMR/eCPM; exigir relat?rio GAM por chave-valor/bloco antes de mexer em floor.",
        "apps_script_atrasado": "Se Apps Script n?o rodar no intervalo esperado, bloquear decis?es baseadas em Ads at? novo snapshot confi?vel.",
    }
    base = templates.get(lesson_type, "Transformar erro repetido em bloqueio operacional: exigir evid?ncia, causa raiz e pr?xima a??o antes de repetir recomenda??o.")
    return f"{base} Erro repetido {repeats} vezes pelo agente {owner}."


def check_learning(conn, now: str) -> int:
    ensure_guard_event_tables(conn)
    ensure_agent_entity_tables(conn)
    rows = conn.execute(
        """
        SELECT owner_agent_key, lesson_type, COUNT(*) repeats,
               GROUP_CONCAT(DISTINCT entity_key) entities,
               MAX(created_at) last_error_at
        FROM nexus_agent_learning_queue
        WHERE lower(status) IN ('open','pending')
        GROUP BY owner_agent_key, lesson_type
        HAVING repeats >= 3
        LIMIT 50
        """
    ).fetchall()
    improved = 0
    for row in rows:
        owner = str(row["owner_agent_key"] or "")
        lesson_type = str(row["lesson_type"] or "")
        repeats = int(row["repeats"] or 0)
        rule = _learning_rule_text(owner, lesson_type, repeats)
        learning_key = f"auto::{owner}::{lesson_type}"
        payload = {
            "owner_agent": owner,
            "lesson_type": lesson_type,
            "repeats": repeats,
            "entities": str(row["entities"] or ""),
            "last_error_at": row["last_error_at"],
            "created_by": "aprendizado",
            "auto_improvement": True,
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO nexus_agent_learnings
                (learning_key, agent_slug, scope, rule, learned_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (learning_key, owner, lesson_type, rule, now, json.dumps(payload, ensure_ascii=False, sort_keys=True)),
        )
        conn.execute(
            """
            UPDATE nexus_agent_learning_queue
            SET status='learned'
            WHERE owner_agent_key=? AND lesson_type=? AND lower(status) IN ('open','pending')
            """,
            (owner, lesson_type),
        )
        mode = "study" if repeats >= 3 else "operational"
        score_delta = -min(10, repeats)
        conn.execute(
            """
            INSERT INTO nexus_agent_entity_state
                (agent_slug, status, score, mode, last_seen_at, last_improved_at, repeated_errors, learned_rules, metadata_json)
            VALUES (?, 'active', ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(agent_slug) DO UPDATE SET
                score = score + excluded.score,
                mode = excluded.mode,
                last_seen_at = excluded.last_seen_at,
                last_improved_at = excluded.last_improved_at,
                repeated_errors = repeated_errors + excluded.repeated_errors,
                learned_rules = learned_rules + 1,
                metadata_json = excluded.metadata_json
            """,
            (owner, score_delta, mode, now, now, repeats, json.dumps(payload, ensure_ascii=False, sort_keys=True)),
        )
        improved += 1
        event(
            conn,
            now=now,
            agent="aprendizado",
            priority="HIGH",
            title="Agente se autoaperfeicoou",
            message=f"{owner} aprendeu nova regra para {lesson_type} ap?s {repeats} repeti??es.",
            entity_type="agent",
            entity_id=owner,
            metadata={**payload, "learned_rule": rule},
            impact_score=75,
            next_action="Aplicar a regra aprendida nas pr?ximas decis?es e n?o repetir recomenda??o sem evid?ncia nova.",
            auto_fix="learning_rule_created",
        )
    for row in conn.execute("SELECT agent_slug FROM nexus_agent_entity_state"):
        conn.execute(
            "UPDATE nexus_agent_entity_state SET last_seen_at=? WHERE agent_slug=?",
            (now, row["agent_slug"]),
        )
    return improved


def main() -> None:
    runtime = webapp_service.load_runtime("config.yaml")
    db_path = runtime["db_path"]
    dt = now_sp()
    now = dt.isoformat(timespec="seconds")
    today = dt.date().isoformat()
    total = 0

    pending_result = None
    pending_error = None
    gam_phase1 = None
    gam_phase1_error = None
    try:
        pending_result = subprocess.run([str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "scripts" / "apply_pending_google_ads_actions.py")], capture_output=True, text=True, timeout=45)
    except Exception as exc:
        pending_error = exc
    try:
        gam_phase1 = subprocess.run([str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "scripts" / "verify_gam_phase1_readiness.py")], capture_output=True, text=True, timeout=45)
    except Exception as exc:
        gam_phase1_error = exc

    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        total += check_web_health(conn, now)
        total += check_ads_script(conn, dt, now)
        event(conn, now=now, agent="consistencia-dados", priority="INFO", title="Validador Ads roda separado", message="Conferencia Google Ads x plataforma executa fora do agente principal com flock para evitar lock SQLite.", metadata={"job":"run_google_ads_spend_reconciliation.sh"}, impact_score=10)
        if pending_error is not None:
            event(conn, now=now, agent="orcamento-cpa", priority="HIGH", title="Aplicador CPA falhou", message=f"Erro ao executar aplicador de CPA pendente: {pending_error}", metadata={"error": str(pending_error)}, auto_fix="failed_pending_cpa_agent")
            total += 1
        elif pending_result is not None and pending_result.returncode != 0:
            event(conn, now=now, agent="orcamento-cpa", priority="HIGH", title="Aplicador CPA falhou", message=pending_result.stderr[-500:] or pending_result.stdout[-500:] or "Falha sem saida.", metadata={"stdout": pending_result.stdout[-1000:], "stderr": pending_result.stderr[-1000:]}, auto_fix="failed_pending_cpa_agent")
            total += 1
        total += check_pending_actions(conn, dt, now)
        total += check_data_consistency(conn, today, now)
        total += check_gam_consistency(conn, today, now)
        if gam_phase1_error is not None:
            event(conn, now=now, agent="gam", priority="HIGH", title="Fase 1 GAM falhou", message=f"Erro ao validar key-values GAM da Fase 1: {gam_phase1_error}", metadata={"error": str(gam_phase1_error)}, auto_fix="failed_gam_phase1_readiness")
            total += 1
        elif gam_phase1 is not None and gam_phase1.returncode != 0:
            total += 1
        total += check_utm_attrid(conn, today, now)
        total += check_new_campaigns(conn, today, now)
        total += check_learning(conn, now)
        event(conn, now=now, agent="orquestrador", priority="INFO" if total == 0 else "HIGH", title="Agentes de sustentacao executados", message=f"Ciclo proativo concluido com {total} problema(s) critico(s).", metadata={"problems": total, "date": today, "timezone": "America/Sao_Paulo"})
        conn.commit()

    try:
        dispatch = subprocess.run([str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "scripts" / "dispatch_hermes_outbox.py")], capture_output=True, text=True, timeout=30)
        if dispatch.returncode != 0:
            with sqlite3.connect(db_path, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA busy_timeout=5000")
                event(conn, now=now, agent="orquestrador", priority="HIGH", title="Hermes dispatcher falhou", message=dispatch.stderr[-500:] or dispatch.stdout[-500:] or "Falha sem saida.", metadata={"stdout": dispatch.stdout[-1000:], "stderr": dispatch.stderr[-1000:]}, auto_fix="failed_hermes_dispatch")
                conn.commit()
    except Exception as exc:
        with sqlite3.connect(db_path, timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            event(conn, now=now, agent="orquestrador", priority="HIGH", title="Hermes dispatcher falhou", message=f"Erro ao despachar fila Hermes: {exc}", metadata={"error": str(exc)}, auto_fix="failed_hermes_dispatch")
            conn.commit()
    print(json.dumps({"ok": True, "problems": total, "created_at": now}, ensure_ascii=False))

if __name__ == "__main__":
    main()
