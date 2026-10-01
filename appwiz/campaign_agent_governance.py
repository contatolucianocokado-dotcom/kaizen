from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

APP_TZ = ZoneInfo("America/Sao_Paulo")

CAMPAIGN_AGENT_PROFILES: dict[str, dict[str, Any]] = {
    "agente-idioma-url": {"type": "core", "authority": 4, "decider": "orquestrador", "role": "Detecta idioma, pais, tema e compatibilidade da URL final.", "blocks": ["url_sem_conteudo", "idioma_incompativel"]},
    "agente-tracking-utm": {"type": "core", "authority": 5, "decider": "orquestrador", "role": "Valida tracking por campaignId/adgroupId e impede atribuicao errada.", "blocks": ["demand_gen_sem_adgroupid", "account_template_invalido"]},
    "agente-compliance": {"type": "core", "authority": 4, "decider": "orquestrador", "role": "Revisa copy, promessas, riscos de policy e coerencia com a pagina.", "blocks": ["promessa_financeira_sem_base", "texto_incompativel_com_url"]},
    "agente-search-keywords": {"type": "search", "authority": 3, "decider": "agente-search", "role": "Valida keywords, negativas e intencao de busca.", "blocks": ["search_sem_keywords", "keywords_fora_do_tema"]},
    "agente-search-lances": {"type": "search", "authority": 3, "decider": "agente-search", "role": "Define lance inicial, budget seguro e stop-loss para Search.", "blocks": ["budget_invalido", "sem_regra_stop_loss"]},
    "agente-demand-canais": {"type": "demand_gen", "authority": 3, "decider": "agente-demandgen", "role": "Valida canais Demand Gen e separacao por grupo.", "blocks": ["canal_nao_definido"]},
    "agente-demand-publicos": {"type": "demand_gen", "authority": 3, "decider": "agente-demandgen", "role": "Valida sinais de publico e padroes vencedores por audiencia.", "blocks": ["publico_vazio", "publico_incompativel"]},
    "agente-demand-assets": {"type": "demand_gen", "authority": 3, "decider": "agente-demandgen", "role": "Valida imagens/videos antes de publicar Demand Gen.", "blocks": ["asset_obrigatorio_ausente"]},
    "agente-pmax-assets": {"type": "pmax", "authority": 3, "decider": "agente-pmax", "role": "Valida asset group, imagens, videos e textos PMAX.", "blocks": ["asset_group_incompleto"]},
    "agente-pmax-url": {"type": "pmax", "authority": 3, "decider": "agente-pmax", "role": "Valida expansao de URL, exclusoes e pagina destino PMAX.", "blocks": ["url_expansion_risco"]},
    "agente-display-assets": {"type": "display", "authority": 3, "decider": "agente-display", "role": "Valida criativos enviados e dimensoes Display.", "blocks": ["imagem_fora_do_padrao"]},
    "agente-display-inventario": {"type": "display", "authority": 3, "decider": "agente-display", "role": "Valida inventario, placements e risco de trafego ruim.", "blocks": ["inventario_nao_validado"]},
    "agente-search": {"type": "owner", "authority": 4, "decider": "orquestrador", "role": "Responsavel final por Search e aprendizado de keywords/copy.", "blocks": []},
    "agente-demandgen": {"type": "owner", "authority": 4, "decider": "orquestrador", "role": "Responsavel final por Demand Gen e aprendizado de canais/publicos/assets.", "blocks": []},
    "agente-pmax": {"type": "owner", "authority": 4, "decider": "orquestrador", "role": "Responsavel final por PMAX e aprendizado de asset groups/sinais.", "blocks": []},
    "agente-display": {"type": "owner", "authority": 4, "decider": "orquestrador", "role": "Responsavel final por Display e aprendizado de criativo/inventario.", "blocks": []},
    "agente-descoberta-campanhas": {"type": "operations", "authority": 4, "decider": "orquestrador", "role": "Detecta campanhas novas em snapshots Google Ads e garante inclusao no projeto mesmo sem gasto.", "blocks": ["campanha_nova_ausente_no_painel"]},
    "agente-reconciliacao-campanhas": {"type": "operations", "authority": 5, "decider": "orquestrador", "role": "Reconcilia nome, status, budget, CPA, meta e URL apos cada sincronizacao.", "blocks": ["alteracao_ads_nao_refletida", "snapshot_ads_desatualizado"]},
    "agente-identidade-google-ads": {"type": "operations", "authority": 5, "decider": "orquestrador", "role": "Garante que campaign_id, customer_id e Attr ID nao sejam confundidos.", "blocks": ["campaign_id_invalido", "customer_id_ausente", "attr_id_usado_como_campaign_id"]},
    "agente-links-google-ads": {"type": "operations", "authority": 5, "decider": "orquestrador", "role": "Valida que cada botao Google Ads abra diretamente a campanha correta na conta correspondente.", "blocks": ["link_google_ads_invalido", "link_abre_seletor_de_contas", "link_aponta_campanha_errada"]},
    "agente-sinais-comerciais-editorial": {"type": "content", "authority": 3, "decider": "agente-critico-sinais-comerciais", "role": "Mapeia intencao, entidades, servicos e taxonomia IAB que pertencem naturalmente ao conteudo, sem aproximar anuncios de controles interativos.", "blocks": ["sinal_comercial_artificial", "contexto_comercial_sem_relacao_com_a_dor", "anuncio_proximo_controle"]},
    "agente-critico-sinais-comerciais": {"type": "critic", "authority": 5, "decider": "orquestrador", "role": "Reprova keyword stuffing, promessa financeira, sinal artificial, conflito cognitivo, anuncio a menos de 180px de controles e risco de politica.", "blocks": ["keyword_stuffing", "promessa_sem_base", "sinal_comercial_artificial", "conteudo_raso", "anuncio_proximo_controle", "risco_clique_acidental"]},
    "agente-conteudo-alto-valor": {"type": "content", "authority": 4, "decider": "agente-critico-conteudo", "role": "Transforma cada etapa do funil em solucao executavel com passo a passo, documentos, excecoes, fontes e proximo passo.", "blocks": ["sem_passo_a_passo", "sem_solucao_para_excecao", "sem_fonte_oficial"]},
    "agente-critico-conteudo": {"type": "critic", "authority": 5, "decider": "orquestrador", "role": "Avalia profundidade, utilidade, momento cognitivo, verificabilidade, qualidade visual e espacamento antes da publicacao.", "blocks": ["nota_conteudo_abaixo_95", "conteudo_generico", "solucao_incompleta", "espaco_vazio_excessivo"]},
}

OWNER_BY_TYPE = {
    "SEARCH": "agente-search",
    "DEMAND_GEN": "agente-demandgen",
    "PERFORMANCE_MAX": "agente-pmax",
    "DISPLAY": "agente-display",
}


def evaluate_editorial_content_quality(payload: dict[str, Any]) -> dict[str, Any]:
    required = {
        "intent": 20,
        "direct_answer": 15,
        "step_by_step": 20,
        "documents": 10,
        "exceptions": 10,
        "official_sources": 10,
        "next_action": 5,
        "policy_safe": 5,
        "commercial_context_natural": 5,
        "commercial_signal_score": 0,
        "iab_context": 0,
        "decision_vocabulary": 0,
        "cognitive_moment": 0,
        "ad_clearance_valid": 0,
        "visual_spacing_valid": 0,
        "decision_comparison": 0,
        "interactive_decision_tool": 0,
        "real_financial_consequence": 0,
        "family_profile_examples": 0,
    }
    failures: list[str] = []
    score = 0
    for field, points in required.items():
        if payload.get(field):
            score += points
        else:
            failures.append(field)
    mandatory_commercial = {
        "decision_comparison",
        "interactive_decision_tool",
        "real_financial_consequence",
        "family_profile_examples",
    }
    if mandatory_commercial.intersection(failures):
        score = min(score, 79)
    if payload.get("keyword_stuffing") or payload.get("unfounded_promise"):
        score = min(score, 49)
        failures.append("commercial_signal_policy_risk")
    commercial_score = int(payload.get("commercial_signal_score_value") or 0)
    if commercial_score and commercial_score < 95:
        score = min(score, commercial_score)
        failures.append("commercial_signal_score_baixo")
    if not payload.get("iab_context") or not payload.get("decision_vocabulary") or not payload.get("cognitive_moment"):
        score = min(score, 79)
        failures.append("sinais_comerciais_incompletos")
    distance = payload.get("minimum_ad_control_distance_px")
    if not payload.get("ad_clearance_valid") or distance is None or int(distance) < 180:
        score = min(score, 49)
        failures.append("anuncio_proximo_controle")
    empty_gap = payload.get("maximum_unexplained_vertical_gap_px")
    if not payload.get("visual_spacing_valid") or empty_gap is None or int(empty_gap) > 120:
        score = min(score, 49)
        failures.append("espaco_vazio_excessivo")
    status = "APROVADO" if score >= 95 and not failures else "REPROVADO"
    actions = [f"corrigir:{item}" for item in failures]
    return {
        "status": status,
        "nota": score,
        "principal_problema": failures[0] if failures else "nenhum",
        "falhas": failures,
        "acoes_obrigatorias": actions,
        "instrucao_para_refatorador": "; ".join(actions) if actions else "nenhuma refacao obrigatoria",
        "pode_entregar_ao_usuario": status == "APROVADO",
    }


def _now() -> str:
    return datetime.now(APP_TZ).isoformat(timespec="seconds")


def _db_path(config_path: str | Path) -> Path:
    cfg = yaml.safe_load(Path(config_path).read_text()) or {}
    database = cfg.get("database") or {}
    return Path(database.get("path") or Path(config_path).with_name("roas_cache.sqlite3"))


def _agent_db_path(config_path: str | Path) -> Path:
    return _db_path(config_path).with_name("agent_memory.sqlite3")


def _key(prefix: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return f"{prefix}::{hashlib.sha1(raw.encode('utf-8')).hexdigest()}"



def _fallback_log(config_path: str | Path, event_type: str, payload: dict[str, Any]) -> None:
    path = Path(config_path).with_name("logs") / "campaign_agent_governance_fallback.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"created_at": _now(), "event_type": event_type, "payload": payload}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")

def ensure_campaign_agent_governance(config_path: str | Path) -> Path:
    db_path = _agent_db_path(config_path)
    now = _now()
    with sqlite3.connect(db_path, timeout=60) as conn:
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_agent_profiles (
                agent_slug TEXT PRIMARY KEY,
                campaign_type TEXT NOT NULL DEFAULT '',
                authority_level INTEGER NOT NULL DEFAULT 1,
                final_decider TEXT NOT NULL DEFAULT 'orquestrador',
                role TEXT NOT NULL DEFAULT '',
                policy_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_agent_decisions (
                decision_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                campaign_type TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                owner_agent TEXT NOT NULL DEFAULT '',
                customer_id TEXT NOT NULL DEFAULT '',
                site TEXT NOT NULL DEFAULT '',
                final_url TEXT NOT NULL DEFAULT '',
                mode TEXT NOT NULL DEFAULT '',
                decision TEXT NOT NULL DEFAULT 'draft_validated',
                hypothesis TEXT NOT NULL DEFAULT '',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_agent_patterns (
                pattern_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                campaign_type TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                title TEXT NOT NULL,
                rule TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                score INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_score_events (
                event_key TEXT PRIMARY KEY,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                suggestion_key TEXT NOT NULL DEFAULT '',
                points INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                score_role TEXT NOT NULL DEFAULT 'author',
                decision_type TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_study_tasks (
                task_key TEXT PRIMARY KEY,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                title TEXT NOT NULL,
                error_committed TEXT NOT NULL DEFAULT '',
                missing_evidence TEXT NOT NULL DEFAULT '',
                new_rule_needed TEXT NOT NULL DEFAULT '',
                apply_example TEXT NOT NULL DEFAULT '',
                avoid_example TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        for slug, profile in CAMPAIGN_AGENT_PROFILES.items():
            conn.execute("""
                INSERT INTO campaign_agent_profiles
                    (agent_slug, campaign_type, authority_level, final_decider, role, policy_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_slug) DO UPDATE SET
                    campaign_type=excluded.campaign_type,
                    authority_level=excluded.authority_level,
                    final_decider=excluded.final_decider,
                    role=excluded.role,
                    policy_json=excluded.policy_json,
                    updated_at=excluded.updated_at
            """, (slug, str(profile.get("type", "")), int(profile.get("authority", 1)), str(profile.get("decider", "orquestrador")), str(profile.get("role", "")), json.dumps(profile, ensure_ascii=False, sort_keys=True), now, now))
        conn.commit()
    return db_path


def _score_event_points(mode: str, approval_decision: str, role: str) -> int:
    if mode == "applied_paused" and approval_decision == "approved":
        return 2 if role == "author" else 1
    if mode == "validated_package" and approval_decision == "approved":
        return 1
    if mode == "blocked" and approval_decision != "approved":
        return -1 if role == "author" else 1
    return 0


def _study_task_payload(agent_slug: str, campaign_type: str, mode: str, approval: dict[str, Any], evidence: dict[str, Any]) -> dict[str, str]:
    blockers = ", ".join(str(item) for item in approval.get("blockers") or []) or "sem score aprovado"
    warnings = ", ".join(str(item) for item in approval.get("warnings") or []) or "validacao insuficiente"
    return {
        "title": f"Estudar melhoria para {campaign_type} apos bloqueio/observacao",
        "error_committed": f"O agente {agent_slug} participou de uma sugestao {mode} sem aprovacao plena.",
        "missing_evidence": f"Faltou evidencia suficiente: {blockers}. Avisos: {warnings}.",
        "new_rule_needed": "Definir quais dados minimos tornam a campanha aplicavel sem aumentar risco operacional.",
        "apply_example": "Aplicar quando URL, idioma, budget, tracking e assets/keywords estiverem completos e score >= 70.",
        "avoid_example": "Nao aplicar quando faltar orcamento, assets, publico, keyword ou evidencia de compatibilidade com a URL final.",
    }


def _score_reason(mode: str, approval_decision: str, role: str) -> str:
    if mode == "blocked" and approval_decision != "approved":
        if role == "responsible":
            return "Responsavel bloqueou aplicacao sem score aprovado."
        return "Sugestao nao atingiu score aprovado para aplicacao."
    if approval_decision == "approved":
        return "Sugestao passou pelo score dos agentes."
    return "Evento registrado para aprendizado sem pontuacao."


def record_campaign_builder_decision(config_path: str | Path, draft: dict[str, Any], mode: str, result: dict[str, Any] | None = None) -> None:
    now = _now()
    campaign_type = str(draft.get("campaign_type") or "").upper()
    owner = OWNER_BY_TYPE.get(campaign_type, "orquestrador")
    agents = draft.get("campaign_agents") or []
    approval = draft.get("approval_score") or {}
    approval_decision = str(approval.get("decision") or "").lower()
    evidence = {
        "tracking": draft.get("tracking"),
        "localization": draft.get("localization"),
        "campaign_naming": draft.get("campaign_naming"),
        "ad_group_naming": draft.get("ad_group_naming"),
        "budget": draft.get("budget"),
        "status": draft.get("status"),
        "approval_score": approval,
    }
    try:
        db_path = ensure_campaign_agent_governance(config_path)
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                with sqlite3.connect(db_path, timeout=60) as conn:
                    conn.execute("PRAGMA busy_timeout=60000")
                    conn.execute("PRAGMA journal_mode=WAL")
                    suggestion_key = _key("campaign_builder", {"type": campaign_type, "url": draft.get("final_url"), "customer_id": draft.get("customer_id"), "mode": mode})
                    recorded_agents: set[str] = set()
                    for item in agents:
                        slug = str(item.get("agent") or "").strip()
                        if not slug:
                            continue
                        recorded_agents.add(slug)
                        payload = {"agent": slug, "type": campaign_type, "url": draft.get("final_url"), "mode": mode, "created_at": now}
                        decision_value = "applied_paused" if mode == "applied_paused" else ("blocked" if mode == "blocked" else "validated_package")
                        conn.execute("""
                            INSERT OR REPLACE INTO campaign_agent_decisions
                                (decision_key, created_at, campaign_type, agent_slug, owner_agent, customer_id, site, final_url, mode, decision, hypothesis, evidence_json, result_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            _key("campaign_decision", payload), now, campaign_type, slug, owner,
                            str(draft.get("customer_id") or ""), str((draft.get("naming_inputs") or {}).get("site") or ""), str(draft.get("final_url") or ""),
                            mode, decision_value,
                            f"Criar {campaign_type} com base na URL e nos padroes aprendidos sem perder tracking/idioma.",
                            json.dumps(evidence, ensure_ascii=False, sort_keys=True), json.dumps(result or {}, ensure_ascii=False, sort_keys=True),
                        ))
                        points = _score_event_points(mode, approval_decision, "author")
                        conn.execute("""
                            INSERT OR REPLACE INTO agent_score_events
                                (event_key, instance, agent_slug, suggestion_key, points, reason, created_at, payload_json, score_role, decision_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            _key("campaign_score", {"agent": slug, "role": "author", "suggestion": suggestion_key, "mode": mode, "created_at": now}),
                            "campaign_builder", slug, suggestion_key, points, _score_reason(mode, approval_decision, "author"), now,
                            json.dumps(evidence, ensure_ascii=False, sort_keys=True), "author", approval_decision or mode,
                        ))
                        if points < 0:
                            study = _study_task_payload(slug, campaign_type, mode, approval, evidence)
                            conn.execute("""
                                INSERT OR IGNORE INTO agent_study_tasks
                                    (task_key, instance, agent_slug, status, title, error_committed, missing_evidence, new_rule_needed, apply_example, avoid_example, created_at, payload_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                _key("campaign_study", {"agent": slug, "type": campaign_type, "decision": approval_decision, "url": draft.get("final_url")}),
                                "campaign_builder", slug, "OPEN", study["title"], study["error_committed"], study["missing_evidence"], study["new_rule_needed"], study["apply_example"], study["avoid_example"], now,
                                json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                            ))
                    if owner and owner not in recorded_agents:
                        points = _score_event_points(mode, approval_decision, "responsible")
                        conn.execute("""
                            INSERT OR REPLACE INTO agent_score_events
                                (event_key, instance, agent_slug, suggestion_key, points, reason, created_at, payload_json, score_role, decision_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            _key("campaign_score", {"agent": owner, "role": "responsible", "suggestion": suggestion_key, "mode": mode, "created_at": now}),
                            "campaign_builder", owner, suggestion_key, points, _score_reason(mode, approval_decision, "responsible"), now,
                            json.dumps(evidence, ensure_ascii=False, sort_keys=True), "responsible", approval_decision or mode,
                        ))
                    conn.commit()
                    return
            except sqlite3.OperationalError as exc:
                last_error = exc
                if "locked" not in str(exc).lower() or attempt == 4:
                    raise
                time.sleep(2 + (attempt * 2))
        if last_error:
            raise last_error
    except Exception as exc:
        _fallback_log(config_path, "campaign_builder_decision", {"error": str(exc), "mode": mode, "campaign_type": campaign_type, "owner": owner, "agents": agents, "evidence": evidence, "result": result or {}})
