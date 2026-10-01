from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


QUALITY_AGENTS = [
    {"slug": "pesquisa-referencias", "target_score": 95, "area": "research"},
    {"slug": "copy-conversao", "target_score": 95, "area": "copy"},
    {"slug": "design-ux", "target_score": 95, "area": "ux"},
    {"slug": "seo-avancado", "target_score": 95, "area": "seo"},
    {"slug": "performance-core-web-vitals", "target_score": 95, "area": "performance"},
    {"slug": "juridico-regulatorio", "target_score": 98, "area": "legal"},
    {"slug": "seguranca-dados", "target_score": 98, "area": "security"},
    {"slug": "analytics-dados", "target_score": 98, "area": "data"},
    {"slug": "qa-visual", "target_score": 95, "area": "visual_qa"},
    {"slug": "qa-final", "target_score": 98, "area": "final_qa"},
    {"slug": "orquestrador", "target_score": 98, "area": "orchestration"},
]

GOOGLE_ADS_AGENTS = [
    "classificador-modelo-negocio",
    "google-ads-praticas-inaceitaveis",
    "identidade-transparencia-anunciante",
    "consistencia-anuncio-destino",
    "destination-requirements",
    "copy-google-ads",
    "tracking-google-ads",
    "privacidade-coleta-dados-google-ads",
    "monitor-politicas-google",
    "qa-visual-google-ads",
    "orquestrador-google-ads",
]

MONETIZATION_AGENTS = [
    "adsense-arquitetura-inventario",
    "adsense-politica-experiencia",
    "adsense-viewability-performance",
    "adsense-yield-experimentos",
    "adsense-qa-blocos",
    "agente-gam",
    "agente-consistencia-dados",
]

PUBLICATION_GATE = [
    "todos os agentes aplicaveis concluiram",
    "todas as notas atingiram a meta",
    "nao existem erros altos ou criticos abertos",
    "riscos altos e criticos estao controlados",
    "identidade publica esta consistente",
    "anuncio e destino sao coerentes",
    "QA desktop e mobile foi realizado",
    "URLs, analytics, consentimento e performance foram validados",
    "evidencias pertencem a versao atual",
    "orquestrador aprovou o conjunto completo",
]

MAXIMUM_ERROR_LEARNING_RULES = [
    "todo erro real deve ser registrado no banco de erros antes de encerrar a execucao",
    "todo risco previsivel deve ser registrado antes da acao do agente",
    "todo agente deve ler erros e riscos abertos antes de executar qualquer acao",
    "erro recorrente deve virar regra de aprendizado para todos os agentes aplicaveis",
    "construtor so entrega depois de auditor independente aprovar com nota minima 95",
    "se auditor reprovar, construtor refaz ate atingir a pontuacao minima",
]

SERVICE_AGENTS = [
    {"slug": "wiz-roas-fast-sync", "name": "Wiz ROAS Fast Sync", "area": "data_sync", "source": "systemd"},
    {"slug": "wiz-roas-full-sync", "name": "Wiz ROAS Full Sync", "area": "data_sync", "source": "systemd"},
    {"slug": "wiz-roas-clarity-daily", "name": "Wiz Clarity Daily Sync", "area": "analytics", "source": "systemd"},
    {"slug": "wiz-roas-oauth-health", "name": "Wiz OAuth Health Guard", "area": "auth_health", "source": "systemd"},
    {"slug": "wiz-campaign-status-applier", "name": "Wiz Campaign Status Applier", "area": "campaign_ops", "source": "systemd"},
    {"slug": "wiz-floor-bucket", "name": "Wiz Floor Bucket Agent", "area": "monetization", "source": "systemd"},
    {"slug": "wiz-gam-countries", "name": "Wiz GAM Countries Sync", "area": "gam", "source": "systemd"},
    {"slug": "wiz-gam-floor-auto", "name": "Wiz GAM Floor Auto", "area": "gam", "source": "systemd"},
    {"slug": "wiz-gam-floor-midnight-reset", "name": "Wiz GAM Floor Midnight Reset", "area": "gam", "source": "systemd"},
    {"slug": "wiz-agent-cycle", "name": "Wiz Agent Cycle", "area": "orchestration", "source": "systemd"},
    {"slug": "wiz-push-reconcile", "name": "Wiz Push GAM Reconciliation", "area": "push_reconciliation", "source": "systemd"},
    {"slug": "wiz-agent-authority", "name": "Wiz Agent Authority", "area": "agent_governance", "source": "systemd"},
    {"slug": "wiz-system-guard-agents", "name": "Wiz System Guard Agents", "area": "system_guard", "source": "systemd"},
    {"slug": "wiz-db-maintenance", "name": "Wiz DB Runtime Maintenance", "area": "maintenance", "source": "systemd"},
    {"slug": "wiz-google-ads-spend-audit", "name": "Wiz Google Ads Spend Audit", "area": "google_ads_audit", "source": "systemd"},
    {"slug": "wiz-google-ads-tracking-template-audit", "name": "Wiz Google Ads Tracking Template Audit", "area": "google_ads_audit", "source": "systemd"},
    {"slug": "wiz-quality-os-rules", "name": "Wiz Quality OS Rules Validator", "area": "quality_governance", "source": "systemd"},
    {"slug": "wiz-quality-error-registry", "name": "Wiz Quality Error Registry", "area": "quality_governance", "source": "systemd"},
]


def _write_json_if_missing(path: Path, payload: Any) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text_if_missing(path: Path, text: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _target_score_for_existing(agent: dict[str, Any]) -> int:
    text = " ".join(str(agent.get(key, "")) for key in ("slug", "area", "type", "epic", "name")).lower()
    authority = int(agent.get("authority") or 0)
    if authority >= 5 or any(term in text for term in ("critic", "critico", "guard", "juridico", "compliance", "seguranca", "dados", "tracking")):
        return 98
    return 95


def _load_existing_operational_agents() -> list[dict[str, Any]]:
    agents: dict[str, dict[str, Any]] = {}
    try:
        from agent_backlog import AGENT_BACKLOG

        for item in AGENT_BACKLOG:
            slug = str(item.get("slug") or "").strip()
            if not slug:
                continue
            agents[slug] = {
                "slug": slug,
                "name": item.get("name") or slug,
                "area": item.get("epic") or "legacy_backlog",
                "source": "agent_backlog",
                "status": "backlog_integrated",
                "mission": item.get("mission") or "",
            }
    except Exception:
        pass

    try:
        from campaign_agent_governance import CAMPAIGN_AGENT_PROFILES

        for slug, profile in CAMPAIGN_AGENT_PROFILES.items():
            current = agents.get(slug, {})
            current.update(
                {
                    "slug": slug,
                    "name": current.get("name") or slug,
                    "area": profile.get("type") or current.get("area") or "campaign_governance",
                    "source": "campaign_agent_governance",
                    "status": "operational_profile",
                    "authority": profile.get("authority") or 1,
                    "decider": profile.get("decider") or "orquestrador",
                    "role": profile.get("role") or "",
                    "blocks": profile.get("blocks") or [],
                }
            )
            agents[slug] = current
    except Exception:
        pass

    for item in SERVICE_AGENTS:
        agents.setdefault(
            item["slug"],
            {
                **item,
                "status": "service_or_timer",
            },
        )

    output = []
    for item in sorted(agents.values(), key=lambda row: str(row.get("slug") or "")):
        item["target_score"] = _target_score_for_existing(item)
        item["integrated_into_quality_os"] = True
        output.append(item)
    return output


def _merge_operational_agents_table(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "rows": []}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"version": 1, "rows": []}
    existing = {
        str(item.get("slug") or item.get("agent_slug") or ""): item
        for item in payload.get("rows", [])
        if isinstance(item, dict)
    }
    for row in rows:
        slug = str(row.get("slug") or "")
        existing[slug] = {**existing.get(slug, {}), **row}
    payload["rows"] = [existing[key] for key in sorted(existing) if key]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def ensure_quality_operating_system(app_root: str | Path) -> dict[str, Any]:
    root = Path(app_root)
    now = datetime.now().isoformat(timespec="seconds")
    quality_runs = root / "quality-runs"
    quality_agents = root / "quality-agents"
    tables = root / "operational-db" / "tables"
    existing_agents = _load_existing_operational_agents()

    _write_json_if_missing(quality_runs / "error-database.json", {"version": 1, "errors": []})
    _write_json_if_missing(quality_runs / "risk-database.json", {"version": 1, "risks": []})
    quality_agents.mkdir(parents=True, exist_ok=True)
    (quality_agents / "agent-scorecards.json").write_text(
        json.dumps(
            {
                "version": 1,
                "targets": {
                    "copy_design_ux_seo_performance": 95,
                    "legal_security_data_monetization": 98,
                    "google_ads_financial": 98,
                    "google_ads_hard_blockers": 100,
                },
                "agents": QUALITY_AGENTS,
                "google_ads_agents": GOOGLE_ADS_AGENTS,
                "monetization_agents": MONETIZATION_AGENTS,
                "existing_operational_agents": existing_agents,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text_if_missing(
        quality_agents / "mandatory-agent-header.md",
        """
# Mandatory Agent Header

Before acting, read the error database, risk database, operating system, scorecards and this header.

Every run must share the same run_id, context_hash, evidence_version, pages, error database and risk set.
Executors produce and correct. Critics score with evidence and may block. The orchestrator releases only after all gates pass.
Maximum rule: every real error and every predictive risk must be registered. Every agent must read open errors and risks before acting. Builders only deliver after an independent auditor scores at least 95; below target means mandatory rework until approved.
""",
    )
    _write_text_if_missing(
        quality_agents / "agent-operating-system.md",
        """
# Portable Agent Operating System

The system is not independent agents. It is a quality operating system where memory, evidence, agents, critics and gates work in the same run.

Core loop: understand the problem, read existing errors and risks, run a pre-mortem, select only applicable agents, execute synchronized work, score each area with an independent critic, correct anything below target, block publication while high or critical risk remains, register decisions/tests/publication, and convert user corrections into permanent rules.

Maximum error-learning rule: no agent may act without checking the error/risk database first. Any failure, near miss, blocked decision, bad assumption, stale data, broken credential, route error or user correction must be registered as learning memory for the responsible agent and as shared memory for all agents.

Funnel stages must follow decision complexity: P1 orientation, P2 diagnosis/result, P3 decision, then action only when the user has enough information. Do not create pages only for pageviews.

Commercial signals must be editorial, proportional and truthful. Never invent commercial intent, approval, partnership or official status.

Publication gate requires all applicable agents complete, scores at target, high/critical issues controlled, consistent public identity, ad/destination coherence, desktop/mobile QA, validated URLs/analytics/consent/performance, current-version evidence and final orchestrator approval.
""",
    )

    for name in [
        "projects",
        "pages",
        "agents",
        "scorecards",
        "qa_runs",
        "publications",
        "errors",
        "risks",
        "agent_sync_runs",
    ]:
        _write_json_if_missing(tables / f"{name}.json", {"version": 1, "rows": []})
    _merge_operational_agents_table(tables / "agents.json", existing_agents)

    manifest = {
        "version": 1,
        "updated_at": now,
        "quality_runs": str(quality_runs),
        "quality_agents": str(quality_agents),
        "operational_tables": str(tables),
        "required_memory": [
            "quality-runs/error-database.json",
            "quality-runs/risk-database.json",
            "quality-agents/agent-operating-system.md",
            "quality-agents/agent-scorecards.json",
            "quality-agents/mandatory-agent-header.md",
        ],
        "publication_gate": PUBLICATION_GATE,
        "maximum_error_learning_rules": MAXIMUM_ERROR_LEARNING_RULES,
        "agents": QUALITY_AGENTS,
        "google_ads_agents": GOOGLE_ADS_AGENTS,
        "monetization_agents": MONETIZATION_AGENTS,
        "existing_operational_agents": existing_agents,
        "integration_policy": [
            "agentes ja existentes continuam como perfis operacionais ou backlog integrado",
            "o Quality OS nao substitui memoria existente; ele passa a exigir gates, score e evidencia comum",
            "servicos e timers do app.wiz entram como agentes de execucao ou guardas operacionais",
            "novos agentes devem declarar fonte, decisor, score alvo, riscos e evidencias antes de operar",
        ],
    }
    (root / "quality-operating-system.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return load_quality_operating_system_status(root)


def load_quality_operating_system_status(app_root: str | Path) -> dict[str, Any]:
    root = Path(app_root)
    manifest_path = root / "quality-operating-system.json"
    required = [
        root / "quality-runs" / "error-database.json",
        root / "quality-runs" / "risk-database.json",
        root / "quality-agents" / "agent-operating-system.md",
        root / "quality-agents" / "agent-scorecards.json",
        root / "quality-agents" / "mandatory-agent-header.md",
        root / "quality-operating-system.json",
    ]
    tables = list((root / "operational-db" / "tables").glob("*.json")) if (root / "operational-db" / "tables").exists() else []
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    existing_agents: list[dict[str, Any]] = []
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            existing_agents = manifest.get("existing_operational_agents") or []
        except json.JSONDecodeError:
            existing_agents = []
    error_count = risk_count = audit_count = open_error_count = 0
    db_path = root / "roas_cache.sqlite3"
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as conn:
                tables_db = {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}
                if "quality_error_events" in tables_db:
                    error_count = int(conn.execute("select count(*) from quality_error_events").fetchone()[0])
                    open_error_count = int(conn.execute("select count(*) from quality_error_events where status='open'").fetchone()[0])
                    risk_count = int(conn.execute("select count(*) from quality_error_events where predictive=1").fetchone()[0])
                if "quality_audit_events" in tables_db:
                    audit_count = int(conn.execute("select count(*) from quality_audit_events").fetchone()[0])
        except Exception:
            pass
    return {
        "ok": not missing and len(tables) >= 9,
        "missing": missing,
        "required_files": [str(path.relative_to(root)) for path in required],
        "operational_tables": [str(path.relative_to(root)) for path in sorted(tables)],
        "publication_gate": PUBLICATION_GATE,
        "agent_count": len(QUALITY_AGENTS),
        "google_ads_agent_count": len(GOOGLE_ADS_AGENTS),
        "monetization_agent_count": len(MONETIZATION_AGENTS),
        "existing_operational_agent_count": len(existing_agents),
        "total_integrated_agent_count": len(QUALITY_AGENTS) + len(GOOGLE_ADS_AGENTS) + len(MONETIZATION_AGENTS) + len(existing_agents),
        "existing_operational_agents": existing_agents,
        "error_count": error_count,
        "open_error_count": open_error_count,
        "risk_count": risk_count,
        "audit_count": audit_count,
        "maximum_error_learning_rules": MAXIMUM_ERROR_LEARNING_RULES,
    }
