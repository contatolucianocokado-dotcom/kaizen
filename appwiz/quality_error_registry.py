from __future__ import annotations

import hashlib
import json
import sqlite3
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any


APP_ROOT = Path(__file__).resolve().parent
DB_PATH = APP_ROOT / "roas_cache.sqlite3"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _key(prefix: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return f"{prefix}::{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def ensure_quality_error_store(app_root: str | Path = APP_ROOT, db_path: str | Path = DB_PATH) -> None:
    root = Path(app_root)
    for rel, payload in {
        "quality-runs/error-database.json": {"version": 1, "errors": []},
        "quality-runs/risk-database.json": {"version": 1, "risks": []},
        "operational-db/tables/errors.json": {"version": 1, "rows": []},
        "operational-db/tables/risks.json": {"version": 1, "rows": []},
        "operational-db/tables/qa_runs.json": {"version": 1, "rows": []},
    }.items():
        path = root / rel
        if not path.exists():
            _write_json(path, payload)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_error_events (
                error_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                agent_slug TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT '',
                severity TEXT NOT NULL DEFAULT '',
                error_type TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                phase TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                predictive INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                context_hash TEXT NOT NULL DEFAULT '',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                learned_rule TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_audit_events (
                audit_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                agent_slug TEXT NOT NULL DEFAULT '',
                auditor_slug TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT '',
                score INTEGER NOT NULL DEFAULT 0,
                target_score INTEGER NOT NULL DEFAULT 95,
                status TEXT NOT NULL DEFAULT '',
                criteria_json TEXT NOT NULL DEFAULT '{}',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                required_rework TEXT NOT NULL DEFAULT ''
            )
            """
        )


def register_quality_error(
    *,
    agent_slug: str,
    message: str,
    severity: str = "HIGH",
    error_type: str = "runtime_error",
    scope: str = "general",
    phase: str = "execution",
    predictive: bool = False,
    source: str = "quality_error_registry",
    evidence: dict[str, Any] | None = None,
    learned_rule: str = "",
    app_root: str | Path = APP_ROOT,
    db_path: str | Path = DB_PATH,
) -> dict[str, Any]:
    ensure_quality_error_store(app_root, db_path)
    evidence = dict(evidence or {})
    row = {
        "created_at": _now(),
        "agent_slug": str(agent_slug or "unknown"),
        "scope": str(scope or "general"),
        "severity": str(severity or "HIGH").upper(),
        "error_type": str(error_type or "runtime_error"),
        "message": str(message or "")[:2000],
        "phase": str(phase or "execution"),
        "status": "open",
        "predictive": bool(predictive),
        "source": str(source or "quality_error_registry"),
        "evidence": evidence,
        "learned_rule": str(learned_rule or ""),
    }
    row["context_hash"] = hashlib.sha256(json.dumps(row["evidence"], ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    row["error_key"] = _key("quality-error", row)
    root = Path(app_root)
    errors_path = root / "quality-runs" / "error-database.json"
    table_path = root / "operational-db" / "tables" / "errors.json"
    for path, key in ((errors_path, "errors"), (table_path, "rows")):
        payload = _load_json(path, {"version": 1, key: []})
        rows = payload.setdefault(key, [])
        if not any(item.get("error_key") == row["error_key"] for item in rows if isinstance(item, dict)):
            rows.append(row)
        _write_json(path, payload)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            INSERT OR IGNORE INTO quality_error_events
                (error_key, created_at, agent_slug, scope, severity, error_type, message, phase, status, predictive, source, context_hash, evidence_json, learned_rule)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["error_key"], row["created_at"], row["agent_slug"], row["scope"], row["severity"], row["error_type"],
                row["message"], row["phase"], row["status"], 1 if row["predictive"] else 0, row["source"], row["context_hash"],
                json.dumps(row["evidence"], ensure_ascii=False, sort_keys=True, default=str), row["learned_rule"],
            ),
        )
    return row


def register_predictive_risk(**kwargs: Any) -> dict[str, Any]:
    kwargs["predictive"] = True
    kwargs.setdefault("error_type", "predictive_risk")
    row = register_quality_error(**kwargs)
    root = Path(kwargs.get("app_root") or APP_ROOT)
    risks_path = root / "quality-runs" / "risk-database.json"
    table_path = root / "operational-db" / "tables" / "risks.json"
    risk = {**row, "risk_key": row["error_key"].replace("quality-error::", "quality-risk::", 1)}
    for path, key in ((risks_path, "risks"), (table_path, "rows")):
        payload = _load_json(path, {"version": 1, key: []})
        rows = payload.setdefault(key, [])
        if not any(item.get("risk_key") == risk["risk_key"] for item in rows if isinstance(item, dict)):
            rows.append(risk)
        _write_json(path, payload)
    return risk


def get_open_quality_errors(
    *,
    agent_slug: str = "",
    scope: str = "",
    app_root: str | Path = APP_ROOT,
) -> list[dict[str, Any]]:
    ensure_quality_error_store(app_root)
    payload = _load_json(Path(app_root) / "quality-runs" / "error-database.json", {"version": 1, "errors": []})
    rows = [row for row in payload.get("errors", []) if isinstance(row, dict) and row.get("status") == "open"]
    if agent_slug:
        rows = [row for row in rows if row.get("agent_slug") in {agent_slug, "general", "orquestrador"} or row.get("scope") == "global"]
    if scope:
        rows = [row for row in rows if row.get("scope") in {scope, "global", "general"} or row.get("agent_slug") == agent_slug]
    return rows


def preflight_agent_action(agent_slug: str, scope: str = "general", evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    open_errors = get_open_quality_errors(agent_slug=agent_slug, scope=scope)
    predictive = register_predictive_risk(
        agent_slug=agent_slug,
        scope=scope,
        severity="INFO" if not open_errors else "HIGH",
        message="Preflight obrigatorio executado antes da acao do agente.",
        phase="preflight",
        source="preflight_agent_action",
        evidence={"open_errors": len(open_errors), **(evidence or {})},
        learned_rule="Agente deve ler erros e riscos antes de agir para nao repetir falhas.",
    )
    return {"allowed": not open_errors, "open_errors": open_errors, "predictive_risk": predictive}


def register_audit_score(
    *,
    agent_slug: str,
    auditor_slug: str,
    scope: str,
    score: int,
    target_score: int = 95,
    criteria: dict[str, Any] | None = None,
    evidence: dict[str, Any] | None = None,
    required_rework: str = "",
    app_root: str | Path = APP_ROOT,
    db_path: str | Path = DB_PATH,
) -> dict[str, Any]:
    ensure_quality_error_store(app_root, db_path)
    status = "approved" if int(score) >= int(target_score) else "rework_required"
    row = {
        "audit_key": _key("quality-audit", {"agent_slug": agent_slug, "auditor_slug": auditor_slug, "scope": scope, "score": score, "evidence": evidence or {}}),
        "created_at": _now(),
        "agent_slug": agent_slug,
        "auditor_slug": auditor_slug,
        "scope": scope,
        "score": int(score),
        "target_score": int(target_score),
        "status": status,
        "criteria": criteria or {},
        "evidence": evidence or {},
        "required_rework": required_rework,
    }
    if status != "approved":
        register_quality_error(
            agent_slug=auditor_slug,
            scope=scope,
            severity="HIGH",
            error_type="audit_score_below_target",
            message=f"Construtor {agent_slug} ficou com nota {score}; minimo {target_score}. Refazer ate passar.",
            phase="audit",
            source="register_audit_score",
            evidence=row,
            learned_rule="Entrega bloqueada ate auditor independente aprovar com nota minima.",
            app_root=app_root,
            db_path=db_path,
        )
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO quality_audit_events
                (audit_key, created_at, agent_slug, auditor_slug, scope, score, target_score, status, criteria_json, evidence_json, required_rework)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["audit_key"], row["created_at"], row["agent_slug"], row["auditor_slug"], row["scope"], row["score"],
                row["target_score"], row["status"], json.dumps(row["criteria"], ensure_ascii=False, sort_keys=True),
                json.dumps(row["evidence"], ensure_ascii=False, sort_keys=True, default=str), row["required_rework"],
            ),
        )
    payload = _load_json(Path(app_root) / "operational-db" / "tables" / "qa_runs.json", {"version": 1, "rows": []})
    payload.setdefault("rows", []).append(row)
    _write_json(Path(app_root) / "operational-db" / "tables" / "qa_runs.json", payload)
    return row


def register_exception(agent_slug: str, exc: BaseException, *, scope: str = "runtime", source: str = "exception", evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return register_quality_error(
        agent_slug=agent_slug,
        scope=scope,
        severity="CRITICAL",
        error_type=exc.__class__.__name__,
        message=str(exc),
        phase="exception",
        source=source,
        evidence={**(evidence or {}), "traceback": traceback.format_exc(limit=12)},
        learned_rule="Toda excecao deve virar aprendizado e bloquear repeticao sem causa raiz.",
    )


def mirror_legacy_events(limit: int = 500) -> dict[str, int]:
    ensure_quality_error_store()
    created = 0
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        tables = {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}
        if "nexus_agent_events" not in tables:
            return {"created": 0}
        rows = conn.execute(
            """
            SELECT *
            FROM nexus_agent_events
            WHERE upper(priority) IN ('HIGH', 'CRITICAL')
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    for raw in rows:
        row = dict(raw)
        registered = register_quality_error(
            agent_slug=str(row.get("agent_slug") or "legacy-agent"),
            scope=str(row.get("project_id") or row.get("entity_type") or "legacy"),
            severity=str(row.get("priority") or "HIGH"),
            error_type=str(row.get("event_type") or "legacy_event"),
            message=str(row.get("title") or row.get("message") or ""),
            phase="legacy_mirror",
            source="nexus_agent_events",
            evidence=row,
            learned_rule="Evento legado espelhado para impedir repeticao no Quality OS.",
        )
        if registered:
            created += 1
    return {"created": created}
