#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = [
    "quality_error_registry.py",
    "quality-operating-system.json",
    "quality-agents/agent-operating-system.md",
    "quality-agents/agent-scorecards.json",
    "quality-agents/mandatory-agent-header.md",
    "quality-runs/error-database.json",
    "quality-runs/risk-database.json",
    "operational-db/tables/agents.json",
    "operational-db/tables/qa_runs.json",
    "operational-db/tables/risks.json",
]
REQUIRED_GATE_TERMS = [
    "todos os agentes aplicaveis concluiram",
    "todas as notas atingiram a meta",
    "nao existem erros altos ou criticos abertos",
    "orquestrador aprovou o conjunto completo",
]


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> None:
    errors: list[str] = []
    for item in REQUIRED_FILES:
        path = ROOT / item
        if not path.exists() or path.stat().st_size == 0:
            errors.append(f"arquivo obrigatorio ausente ou vazio: {item}")

    manifest = load_json(ROOT / "quality-operating-system.json")
    gate = [str(item) for item in manifest.get("publication_gate") or []]
    for term in REQUIRED_GATE_TERMS:
        if term not in gate:
            errors.append(f"gate obrigatorio ausente: {term}")

    scorecards = load_json(ROOT / "quality-agents/agent-scorecards.json")
    if len(scorecards.get("existing_operational_agents") or []) < 50:
        errors.append("inventario de agentes existentes abaixo do esperado")

    agent_table = load_json(ROOT / "operational-db/tables/agents.json")
    rows = agent_table.get("rows") or []
    if len(rows) < 50:
        errors.append("operational-db/tables/agents.json sem inventario completo")
    missing_scores = [row.get("slug") for row in rows if not row.get("target_score")]
    if missing_scores:
        errors.append(f"agentes sem target_score: {', '.join(map(str, missing_scores[:10]))}")

    service_files = [
        "/etc/systemd/system/wiz-quality-error-registry.service",
        "/etc/systemd/system/wiz-quality-error-registry.timer",
    ]
    for service_file in service_files:
        if not Path(service_file).exists():
            errors.append(f"servico obrigatorio ausente: {service_file}")

    if errors:
        print("QUALITY_OS_RULES_INVALID")
        for error in errors:
            print("-", error)
        sys.exit(1)
    print("QUALITY_OS_RULES_OK")


if __name__ == "__main__":
    main()
