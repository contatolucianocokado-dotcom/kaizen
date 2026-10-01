#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

APP_TZ = ZoneInfo("America/Sao_Paulo")
ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "roas_cache.sqlite3"

AGENT_AUTHORITY = {
    "orquestrador": {"level": 5, "final_decider": "luciano", "can_execute": ["approve_safe_fix", "block_automation", "assign_owner", "escalate_to_luciano"], "requires_luciano": ["financial_strategy_change", "new_automation_logic"]},
    "saude-web": {"level": 4, "final_decider": "orquestrador", "can_execute": ["restart_web", "mark_stale_data", "open_incident"], "requires_luciano": []},
    "utm-attrid": {"level": 4, "final_decider": "orquestrador", "can_execute": ["fix_tracking_template", "normalize_attr_id", "block_invalid_attr"], "requires_luciano": ["change_attr_rule"]},
    "consistencia-dados": {"level": 3, "final_decider": "orquestrador", "can_execute": ["block_bad_roi_decision", "open_data_incident"], "requires_luciano": ["ignore_data_divergence"]},
    "google-ads-script": {"level": 3, "final_decider": "orquestrador", "can_execute": ["request_resync", "mark_ads_stale", "repair_ads_daily"], "requires_luciano": ["change_tracking_logic"]},
    "gam": {"level": 3, "final_decider": "orquestrador", "can_execute": ["request_gam_resync", "mark_gam_stale", "block_floor_decision"], "requires_luciano": ["enable_auto_floor"]},
    "orcamento-cpa": {"level": 2, "final_decider": "luciano", "can_execute": ["apply_preapproved_budget", "apply_preapproved_cpa", "apply_preapproved_status"], "requires_luciano": ["increase_budget", "pause_campaign", "decrease_cpa", "increase_cpa"]},
    "campanhas-novas": {"level": 2, "final_decider": "orquestrador", "can_execute": ["create_project_from_spend", "flag_missing_mapping"], "requires_luciano": ["launch_campaign"]},
    "aprendizado": {"level": 2, "final_decider": "orquestrador", "can_execute": ["create_learning_rule", "move_agent_to_study"], "requires_luciano": ["promote_rule_to_automation"]},
    "floor-verification": {"level": 1, "final_decider": "luciano", "can_execute": ["audit_floor", "report_floor_mismatch"], "requires_luciano": ["enable_floor", "change_floor", "disable_floor_exception"]},
}

def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
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
    """)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(nexus_agent_entity_state)")}
    if "authority_level" not in columns:
        conn.execute("ALTER TABLE nexus_agent_entity_state ADD COLUMN authority_level INTEGER NOT NULL DEFAULT 1")
    if "final_decider" not in columns:
        conn.execute("ALTER TABLE nexus_agent_entity_state ADD COLUMN final_decider TEXT NOT NULL DEFAULT 'orquestrador'")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nexus_agent_authority_events (
            event_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            agent_slug TEXT NOT NULL,
            authority_level INTEGER NOT NULL,
            final_decider TEXT NOT NULL,
            policy_json TEXT NOT NULL
        )
    """)

def main() -> None:
    now = datetime.now(APP_TZ).isoformat(timespec="seconds")
    with sqlite3.connect(DB, timeout=15) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        ensure_tables(conn)
        for agent, policy in AGENT_AUTHORITY.items():
            payload = json.dumps(policy, ensure_ascii=False, sort_keys=True)
            conn.execute("""
                INSERT INTO nexus_agent_entity_state
                    (agent_slug, status, score, mode, authority_level, final_decider, last_seen_at, metadata_json)
                VALUES (?, 'active', 0, 'operational', ?, ?, ?, ?)
                ON CONFLICT(agent_slug) DO UPDATE SET
                    status='active',
                    authority_level=excluded.authority_level,
                    final_decider=excluded.final_decider,
                    last_seen_at=excluded.last_seen_at,
                    metadata_json=excluded.metadata_json
            """, (agent, int(policy["level"]), str(policy["final_decider"]), now, payload))
            conn.execute("""
                INSERT OR REPLACE INTO nexus_agent_authority_events
                    (event_key, created_at, agent_slug, authority_level, final_decider, policy_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (f"authority::{agent}", now, agent, int(policy["level"]), str(policy["final_decider"]), payload))
        conn.execute("""
            INSERT OR REPLACE INTO nexus_agent_events
                (event_id, agent_slug, event_type, priority, title, message, entity_type, entity_id, project_id, created_at, metadata_json)
            VALUES (?, 'orquestrador', 'authority_policy', 'INFO', 'Autoridade dos agentes validada', ?, 'system', 'agent_authority', '', ?, ?)
        """, (f"authority-policy::{now[:13]}", "Niveis de autoridade, decisores finais e limites de execucao foram validados.", now, json.dumps({"agents": AGENT_AUTHORITY}, ensure_ascii=False, sort_keys=True)))
        conn.commit()
    print(json.dumps({"ok": True, "agents": len(AGENT_AUTHORITY), "created_at": now}, ensure_ascii=False))

if __name__ == "__main__":
    main()
