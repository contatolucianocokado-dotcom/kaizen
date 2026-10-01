from __future__ import annotations
import base64
import hashlib
import logging

import sqlite3
import time
from collections import defaultdict
import json
import os
import re
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlparse
import http.cookiejar
import urllib.error
import urllib.request
import uuid
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from dimension_normalizer import dimension_override_map
from dimension_normalizer import enrich_dimensions
from data_quality_guard import build_data_guard_reconciliation_candidate
from data_quality_guard import evaluate_project_decision_guard
from floor_bucket_service import ensure_floor_bucket_store
from pull_roas import (
    _ensure_google_ads_campaign_settings_table,
    ads_rows_from_store,
    database_path,
    find_site,
    gam_rows_from_store,
    load_config,
    ptax_usd_brl_rate,
    recent_30_day_range,
)
try:
    from roas_store import ensure_store, read_ads_rows, read_ads_rows_operational, read_ads_rows_reporting, read_gam_rows, read_mgid_rows, read_table, upsert_ads_rows, upsert_ads_hourly_rows, upsert_ads_placement_rows
    from roas_store import insert_actions, insert_decisions, insert_nexus_agent_events, upsert_nexus_telegram_channels
except ImportError:
    from roas_store import ensure_store, read_ads_rows, read_ads_rows_operational, read_ads_rows_reporting, read_gam_rows, read_table, upsert_ads_rows, upsert_ads_hourly_rows, upsert_ads_placement_rows
    from roas_store import insert_actions, insert_decisions, insert_nexus_agent_events, upsert_nexus_telegram_channels

    def read_mgid_rows(*args: Any, **kwargs: Any) -> list[Any]:
        return []
from roi_aggregator import (
    _build_attribution_detail_rows,
    build_fact_rows,
    build_project_kpi_summary,
    project_campaign_analysis_rows,
    _resolve_request_metrics,
)
from telegram_notifier import seed_default_telegram_channels
from telegram_notifier import queue_war_room_events
from telegram_notifier import get_telegram_webhook_info
from telegram_notifier import get_telegram_bot_info
from quality_os import ensure_quality_operating_system, load_quality_operating_system_status


LOGGER = logging.getLogger(__name__)


def _app_tz() -> object:
    try:
        return ZoneInfo("America/Sao_Paulo")
    except ZoneInfoNotFoundError:
        from datetime import timezone, timedelta
        return timezone(timedelta(hours=-3), name="America/Sao_Paulo")


APP_TZ = _app_tz()
PAGE_INTELLIGENCE_SEED: tuple[dict[str, Any], ...] = (
    {"project": "us.foconadica.com", "url_path": "/unclaimed-money-america-government-holding-your-cash-b/", "funnel_stage": "P1", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/government-benefits/unclaimed-money", "iab_content_label": "Personal Finance / Government Benefits / Unclaimed Money", "commercial_signal_type": "country_selection_and_problem_discovery", "commercial_intent_score": 82, "audience_intent_label": "User wants to identify where unclaimed funds may be held", "recommended_next_page": "/unclaimed-money-diagnosis/"},
    {"project": "us.foconadica.com", "url_path": "/unclaimed-money-diagnosis/", "funnel_stage": "P2", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/eligibility/documents", "iab_content_label": "Personal Finance / Eligibility / Documents", "commercial_signal_type": "diagnosis_by_country_money_type_and_evidence", "commercial_intent_score": 88, "audience_intent_label": "User needs the safest search route before using official databases", "recommended_next_page": "/unclaimed-money-personalized-search-path/"},
    {"project": "us.foconadica.com", "url_path": "/unclaimed-money-personalized-search-path/", "funnel_stage": "P3", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/search-path/official-databases", "iab_content_label": "Personal Finance / Official Databases / Search Path", "commercial_signal_type": "database_selection_and_next_step", "commercial_intent_score": 90, "audience_intent_label": "User is ready to choose the official search database", "recommended_next_page": "/unclaimed-money-claim-document-checklist/"},
    {"project": "us.foconadica.com", "url_path": "/how-to-find-unclaimed-money-us-missingmoney-state-portals/", "funnel_stage": "P3", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/search-path/state-portals", "iab_content_label": "Personal Finance / State Portals / Official Search", "commercial_signal_type": "official_database_search_instructions", "commercial_intent_score": 86, "audience_intent_label": "User wants to search MissingMoney, Unclaimed.org or state portals", "recommended_next_page": "/unclaimed-money-claim-document-checklist/"},
    {"project": "us.foconadica.com", "url_path": "/unclaimed-money-by-state-search-database-us/", "funnel_stage": "P3", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/search-path/state-database", "iab_content_label": "Personal Finance / State Database / Official Search", "commercial_signal_type": "state_database_selection", "commercial_intent_score": 84, "audience_intent_label": "User needs the correct state-level search database", "recommended_next_page": "/unclaimed-money-claim-document-checklist/"},
    {"project": "us.foconadica.com", "url_path": "/unclaimed-money-claim-document-checklist/", "funnel_stage": "P4", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/documents/claim-preparation", "iab_content_label": "Personal Finance / Documents / Claim Preparation", "commercial_signal_type": "documents_deadlines_and_claim_readiness", "commercial_intent_score": 92, "audience_intent_label": "User found a possible match and needs documents before claiming", "recommended_next_page": "/claim-unclaimed-money-us-documents-deadlines-payment/"},
    {"project": "us.foconadica.com", "url_path": "/claim-unclaimed-money-us-documents-deadlines-payment/", "funnel_stage": "P5", "iab_content_taxonomy_version": "IAB Content Taxonomy", "iab_content_id": "personal-finance/claim-process/official-action", "iab_content_label": "Personal Finance / Claim Process / Official Action", "commercial_signal_type": "claim_submission_and_official_action", "commercial_intent_score": 94, "audience_intent_label": "User is preparing to submit or verify an official claim", "recommended_next_page": ""},
)
PUSH_CONFIRMED_SOURCES = {"quali", "por", "push"}
PUSH_CONFIRMED_MEDIUMS = {"atc", "auto", "broadcast"}
PUSH_CAMPAIGN_PREFIXES = ("mcmv_", "veraz_", "dinheiro_esquecido", "bc_")
def _is_high_risk_campaign_type(channel_type: Any, campaign_name: Any = "") -> bool:
    text = f"{channel_type or ''} {campaign_name or ''}".strip().lower()
    return "pmax" in text or "performance max" in text or "demand gen" in text or "demandgen" in text or "geracao de demanda" in text or "geração de demanda" in text


def _build_lossy_campaign_pause_candidate(
    project_id: str,
    row: dict[str, Any],
    current_day: date,
) -> dict[str, Any] | None:
    if not _is_high_risk_campaign_type(row.get("channel_type"), row.get("campaign")):
        return None

    spend_today = _to_float(row.get("investimento_hoje"))
    spend_yesterday = _to_float(row.get("investimento_ontem"))
    spend_7d = _to_float(row.get("investimento_7d"))
    roi_today = _to_float(row.get("roi_hoje"))
    roi_yesterday = _to_float(row.get("roi_ontem"))
    roi_7d = _to_float(row.get("roi_7d"))

    two_day_total = spend_today + spend_yesterday
    if spend_today >= 5 and spend_yesterday >= 5 and two_day_total >= 10 and roi_today <= -0.5 and roi_yesterday <= -0.5:
        return {
            "project_id": project_id,
            "entity_type": "campaign",
            "entity_id": str(row.get("campaign_id") or row.get("campaign") or ""),
            "campaign_id": str(row.get("campaign_id") or ""),
            "campaign_name": str(row.get("campaign") or ""),
            "channel_type": str(row.get("channel_type") or ""),
            "action_type": "pause_lossy_campaign",
            "priority": "HIGH",
            "expected_reward": abs(min(roi_today, roi_yesterday)),
            "probability_success": 0.9,
            "reason": (
                f"Campanha {str(row.get('campaign') or row.get('campaign_id') or '')} "
                f"em {project_id} com gasto de {two_day_total:.2f} em 2 dias e ROI "
                f"hoje {roi_today*100:.2f}% / ontem {roi_yesterday*100:.2f}%."
            ),
            "message": (
                f"Pausar campanha {str(row.get('campaign') or row.get('campaign_id') or '')} "
                f"em {project_id} no nivel de campanha. "
                f"ROI hoje {roi_today*100:.2f}% e ontem {roi_yesterday*100:.2f}% com gasto de {two_day_total:.2f}."
            ),
        }

    if spend_7d >= 10 and roi_7d <= -0.5 and (spend_today > 0 or spend_yesterday > 0):
        return {
            "project_id": project_id,
            "entity_type": "campaign",
            "entity_id": str(row.get("campaign_id") or row.get("campaign") or ""),
            "campaign_id": str(row.get("campaign_id") or ""),
            "campaign_name": str(row.get("campaign") or ""),
            "channel_type": str(row.get("channel_type") or ""),
            "action_type": "pause_lossy_campaign_7d",
            "priority": "HIGH",
            "expected_reward": abs(roi_7d),
            "probability_success": 0.84,
            "reason": (
                f"Campanha {str(row.get('campaign') or row.get('campaign_id') or '')} "
                f"em {project_id} com gasto de {spend_7d:.2f} nos ultimos 7 dias e ROI {roi_7d*100:.2f}%."
            ),
            "message": (
                f"Pausar campanha {str(row.get('campaign') or row.get('campaign_id') or '')} "
                f"em {project_id} no nivel de campanha. "
                f"ROI 7 dias {roi_7d*100:.2f}% com gasto de {spend_7d:.2f}."
            ),
        }
    return None


def ensure_veraz_audio_store(runtime: dict[str, Any]) -> None:
    db_path = runtime["db_path"]
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS veraz_audio_leads (
                lead_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                page_slug TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                landing_url TEXT NOT NULL DEFAULT '',
                utm_source TEXT NOT NULL DEFAULT '',
                utm_medium TEXT NOT NULL DEFAULT '',
                utm_campaign TEXT NOT NULL DEFAULT '',
                utm_term TEXT NOT NULL DEFAULT '',
                utm_content TEXT NOT NULL DEFAULT '',
                gclid TEXT NOT NULL DEFAULT '',
                consent INTEGER NOT NULL DEFAULT 0,
                audio_path TEXT NOT NULL DEFAULT '',
                audio_mime TEXT NOT NULL DEFAULT '',
                audio_bytes INTEGER NOT NULL DEFAULT 0,
                transcript_text TEXT NOT NULL DEFAULT '',
                detected_intent TEXT NOT NULL DEFAULT '',
                response_text TEXT NOT NULL DEFAULT '',
                response_audio_path TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                user_agent TEXT NOT NULL DEFAULT '',
                ip_hash TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_veraz_audio_created ON veraz_audio_leads(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_veraz_audio_intent ON veraz_audio_leads(detected_intent, status)")
        existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(veraz_audio_leads)").fetchall()}
        migrations = {
            'typed_text': "ALTER TABLE veraz_audio_leads ADD COLUMN typed_text TEXT NOT NULL DEFAULT ''",
            'recommended_path': "ALTER TABLE veraz_audio_leads ADD COLUMN recommended_path TEXT NOT NULL DEFAULT ''",
            'cta_label': "ALTER TABLE veraz_audio_leads ADD COLUMN cta_label TEXT NOT NULL DEFAULT ''",
            'cta_url': "ALTER TABLE veraz_audio_leads ADD COLUMN cta_url TEXT NOT NULL DEFAULT ''",
            'confidence': "ALTER TABLE veraz_audio_leads ADD COLUMN confidence REAL NOT NULL DEFAULT 0",
            'risk_flags_json': "ALTER TABLE veraz_audio_leads ADD COLUMN risk_flags_json TEXT NOT NULL DEFAULT '[]'",
            'agent_review_status': "ALTER TABLE veraz_audio_leads ADD COLUMN agent_review_status TEXT NOT NULL DEFAULT 'pending'",
            'selected_category': "ALTER TABLE veraz_audio_leads ADD COLUMN selected_category TEXT NOT NULL DEFAULT ''",
            'audio_duration_seconds': "ALTER TABLE veraz_audio_leads ADD COLUMN audio_duration_seconds REAL NOT NULL DEFAULT 0",
            'transcript_was_edited': "ALTER TABLE veraz_audio_leads ADD COLUMN transcript_was_edited INTEGER NOT NULL DEFAULT 0",
            'cta_clicked_at': "ALTER TABLE veraz_audio_leads ADD COLUMN cta_clicked_at TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                conn.execute(statement)
        conn.commit()


def _hash_ip(value: str) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    return hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]


def _clean_audio_b64(value: str) -> str:
    text = str(value or '').strip()
    if ',' in text and text.lower().startswith('data:'):
        text = text.split(',', 1)[1]
    return text


def _strip_accents_text(value: str) -> str:
    return str(value or '').translate(str.maketrans('????????????', 'aeiouAEIOUnN'))


def _veraz_detect_intent(transcript: str) -> str:
    text = _strip_accents_text(transcript).lower()
    if any(word in text for word in ['rechaz', 'no me aprobaron', 'deneg']):
        return 'credito_rechazado'
    if any(word in text for word in ['pague', 'pagada', 'saldada', 'salir', 'limpiar', 'borrar']):
        return 'deuda_pagada_o_limpiar'
    if any(word in text for word in ['score', 'puntaje', 'historial']):
        return 'score_crediticio'
    if any(word in text for word in ['prestamo', 'credito', 'tarjeta', 'financ']):
        return 'pedir_credito'
    if any(word in text for word in ['veraz', 'dni', 'cuil', 'deuda', 'moroso']):
        return 'consultar_veraz'
    return 'orientacion_general'


def _veraz_path_for_intent(intent: str) -> dict[str, Any]:
    paths = {
        'credito_rechazado': {'path': 'revisar_rechazo', 'cta_label': 'Ver que revisar antes de pedir otra vez', 'cta_url': 'https://esp-g.quali.sonora123.com/que-puntaje-necesitas-para-que-te-aprueben-un-prestamo-y-por-que-te-rechazan-igual/', 'confidence': 0.82},
        'deuda_pagada_o_limpiar': {'path': 'limpiar_registro', 'cta_label': 'Ver pasos para ordenar una deuda pagada', 'cta_url': 'https://esp-g.quali.sonora123.com/como-limpiar-tu-nombre-y-salir-del-veraz-cuando-la-deuda-ya-esta-paga-paso-a-paso/', 'confidence': 0.80},
        'score_crediticio': {'path': 'entender_score', 'cta_label': 'Entender score y senales de rechazo', 'cta_url': 'https://esp-g.quali.sonora123.com/que-puntaje-necesitas-para-que-te-aprueben-un-prestamo-y-por-que-te-rechazan-igual/', 'confidence': 0.76},
        'pedir_credito': {'path': 'preparar_credito', 'cta_label': 'Ver orden correcto antes de pedir credito', 'cta_url': 'https://esp-g.quali.sonora123.com/orden-correcto-para-buscar-prestamo/', 'confidence': 0.74},
        'consultar_veraz': {'path': 'consultar_veraz', 'cta_label': 'Ver como consultar Veraz con CUIL', 'cta_url': 'https://esp-g.quali.sonora123.com/como-consultar-veraz-con-cuil-sin-complicaciones/', 'confidence': 0.78},
        'orientacion_general': {'path': 'revision_preventiva', 'cta_label': 'Ver guia para revisar antes de pedir credito', 'cta_url': 'https://esp-g.quali.sonora123.com/como-saber-si-estas-en-el-veraz-usando-tu-dni-o-cuil/', 'confidence': 0.55},
    }
    return paths.get(intent, paths['orientacion_general'])


def _veraz_risk_flags(text: str) -> list[str]:
    normalized = _strip_accents_text(text).lower()
    flags: list[str] = []
    if re.search(r'\b\d{7,11}\b', normalized):
        flags.append('possible_dni_cuil_or_phone')
    if any(word in normalized for word in ['banco', 'cuenta', 'tarjeta', 'cbu', 'alias']):
        flags.append('possible_financial_data')
    if any(word in normalized for word in ['urgente', 'hoy', 'desesperado', 'embargo']):
        flags.append('high_urgency')
    return flags


def _veraz_response_for_intent(intent: str, has_transcript: bool = True) -> str:
    if not has_transcript:
        return 'Recibimos tu audio, pero falta una transcripcion clara para orientar mejor. Escribi brevemente tu caso y toca el boton de abajo para seguir con el proximo paso recomendado. No informes DNI, CUIL ni datos bancarios.'
    responses = {
        'credito_rechazado': 'Por lo que contas, el punto principal parece ser un rechazo de credito. Antes de volver a pedir, conviene revisar si hay deuda activa, registro viejo, score bajo o datos inconsistentes. El camino mas seguro es confirmar tu situacion en Veraz y recien despues elegir una opcion de credito compatible.',
        'deuda_pagada_o_limpiar': 'Tu caso parece relacionado con una deuda ya pagada o con limpiar un registro. El primer paso es verificar si la deuda sigue apareciendo, si figura como cancelada y desde cuando. No conviene pedir credito hasta confirmar que el informe este actualizado.',
        'score_crediticio': 'Lo que mencionas apunta al score o historial crediticio. El score puede verse afectado por pagos atrasados, consultas recientes, deuda activa o registros viejos. El camino recomendado es revisar el informe y entender que senal puede estar frenando la aprobacion.',
        'pedir_credito': 'Si queres pedir credito, lo mas importante es no hacerlo a ciegas. Primero revisa tu estado en Veraz, posibles deudas y senales de riesgo. Asi evitas rechazos innecesarios y podes elegir mejor el proximo paso.',
        'consultar_veraz': 'Tu duda principal parece ser saber si apareces en Veraz. La orientacion inicial es consultar tu estado antes de pedir credito, sin entregar datos bancarios y sin avanzar en solicitudes hasta entender tu situacion.',
        'orientacion_general': 'Con lo que contaste, te recomiendo empezar por una revision preventiva: confirmar si hay senales en Veraz, deuda activa, registro antiguo o inconsistencias. Despues de eso se puede definir si conviene pedir credito, ordenar una deuda o revisar el score.',
    }
    closing = ' Para avanzar con mas seguridad, toca el boton de abajo. Ese es el proximo paso recomendado segun lo que contaste y te ayuda a evitar pedir credito a ciegas o seguir un camino que no corresponde a tu situacion.'
    return responses.get(intent, responses['orientacion_general']) + closing + ' Esta orientacion es inicial y no reemplaza una consulta oficial ni asesoramiento financiero o legal.'


def record_veraz_audio_lead(runtime: dict[str, Any], payload: dict[str, Any], request_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    ensure_veraz_audio_store(runtime)
    if not bool(payload.get('consent')):
        raise ValueError('consent_required')
    browser_transcript = str(payload.get('transcript_text') or '').strip()
    typed_text = str(payload.get('typed_text') or '').strip()
    transcript = (typed_text or browser_transcript)[:5000]
    audio_b64 = _clean_audio_b64(str(payload.get('audio_base64') or ''))
    if not audio_b64 and not transcript:
        raise ValueError('audio_or_text_required')
    audio_bytes = b''
    if audio_b64:
        try:
            audio_bytes = base64.b64decode(audio_b64, validate=False)
        except Exception as exc:
            raise ValueError('invalid_audio') from exc
    if len(audio_bytes) > 8 * 1024 * 1024:
        raise ValueError('audio_too_large')
    mime = str(payload.get('audio_mime') or '').lower()[:80]
    ext = 'webm' if 'webm' in mime else ('mp4' if 'mp4' in mime else 'ogg' if 'ogg' in mime else 'bin')
    lead_id = uuid.uuid4().hex
    now_text = datetime.now(APP_TZ).isoformat(timespec='seconds')
    audio_path = ''
    if audio_bytes:
        storage_dir = Path(runtime['db_path']).with_name('veraz_audio_uploads') / now_text[:10]
        storage_dir.mkdir(parents=True, exist_ok=True)
        audio_file = storage_dir / f'{lead_id}.{ext}'
        audio_file.write_bytes(audio_bytes)
        audio_path = str(audio_file)
    selected_category = str(payload.get('selected_category') or '').strip()[:120]
    category_intent = {
        'consultar_veraz': 'consultar_veraz',
        'deuda_pagada': 'deuda_pagada_o_limpiar',
        'credito_rechazado': 'credito_rechazado',
        'pedir_credito': 'pedir_credito',
        'score': 'score_crediticio',
    }.get(selected_category)
    intent = category_intent or _veraz_detect_intent(transcript)
    path = _veraz_path_for_intent(intent)
    risk_flags = _veraz_risk_flags(transcript)
    response_text = _veraz_response_for_intent(intent, bool(transcript))
    request_meta = request_meta or {}
    row = {
        'lead_id': lead_id,
        'created_at': now_text,
        'page_slug': str(payload.get('page_slug') or 'veraz-audio')[:120],
        'source_url': str(payload.get('source_url') or '')[:500],
        'landing_url': str(payload.get('landing_url') or '')[:500],
        'utm_source': str(payload.get('utm_source') or '')[:180],
        'utm_medium': str(payload.get('utm_medium') or '')[:180],
        'utm_campaign': str(payload.get('utm_campaign') or '')[:220],
        'utm_term': str(payload.get('utm_term') or '')[:220],
        'utm_content': str(payload.get('utm_content') or '')[:220],
        'gclid': str(payload.get('gclid') or '')[:220],
        'selected_category': selected_category,
        'audio_duration_seconds': _to_float(payload.get('audio_duration_seconds')),
        'transcript_was_edited': 1 if bool(payload.get('transcript_was_edited')) else 0,
        'consent': 1,
        'audio_path': audio_path,
        'audio_mime': mime,
        'audio_bytes': len(audio_bytes),
        'transcript_text': transcript,
        'typed_text': typed_text[:5000],
        'detected_intent': intent,
        'response_text': response_text,
        'recommended_path': str(path.get('path') or ''),
        'cta_label': str(path.get('cta_label') or ''),
        'cta_url': str(path.get('cta_url') or ''),
        'confidence': float(path.get('confidence') or 0),
        'risk_flags_json': json.dumps(risk_flags, ensure_ascii=False),
        'response_audio_path': '',
        'status': 'answered_rule_based' if transcript else 'received_pending_transcription',
        'agent_review_status': 'needs_review' if risk_flags or not transcript else 'pending',
        'user_agent': str(request_meta.get('user_agent') or '')[:500],
        'ip_hash': _hash_ip(str(request_meta.get('ip') or '')),
        'metadata_json': json.dumps({'client_meta': payload.get('client_meta') or {}, 'browser_transcript': browser_transcript, 'needs_agent_review': bool(risk_flags) or not bool(transcript)}, ensure_ascii=False, sort_keys=True),
    }
    with sqlite3.connect(runtime['db_path'], timeout=30) as conn:
        conn.execute('PRAGMA busy_timeout=30000')
        conn.execute(
            """
            INSERT INTO veraz_audio_leads (
                lead_id, created_at, page_slug, source_url, landing_url, utm_source, utm_medium, utm_campaign,
                utm_term, utm_content, gclid, selected_category, audio_duration_seconds, transcript_was_edited, consent, audio_path, audio_mime, audio_bytes, transcript_text, typed_text,
                detected_intent, response_text, response_audio_path, recommended_path, cta_label, cta_url, confidence, risk_flags_json, status, agent_review_status, user_agent, ip_hash, metadata_json
            ) VALUES (
                :lead_id, :created_at, :page_slug, :source_url, :landing_url, :utm_source, :utm_medium, :utm_campaign,
                :utm_term, :utm_content, :gclid, :selected_category, :audio_duration_seconds, :transcript_was_edited, :consent, :audio_path, :audio_mime, :audio_bytes, :transcript_text, :typed_text,
                :detected_intent, :response_text, :response_audio_path, :recommended_path, :cta_label, :cta_url, :confidence, :risk_flags_json, :status, :agent_review_status, :user_agent, :ip_hash, :metadata_json
            )
            """,
            row,
        )
        conn.commit()
    return {'ok': True, 'lead_id': lead_id, 'intent': intent, 'recommended_path': row['recommended_path'], 'cta_label': row['cta_label'], 'cta_url': row['cta_url'], 'confidence': row['confidence'], 'risk_flags': risk_flags, 'response_text': response_text, 'status': row['status']}


def mark_veraz_audio_cta_click(runtime: dict[str, Any], lead_id: str) -> dict[str, Any]:
    ensure_veraz_audio_store(runtime)
    now_text = datetime.now(APP_TZ).isoformat(timespec='seconds')
    with sqlite3.connect(runtime['db_path'], timeout=30) as conn:
        conn.execute('PRAGMA busy_timeout=30000')
        row = conn.execute('SELECT lead_id FROM veraz_audio_leads WHERE lead_id=?', (str(lead_id or ''),)).fetchone()
        if not row:
            return {'ok': False, 'reason': 'not_found'}
        conn.execute('UPDATE veraz_audio_leads SET cta_clicked_at=? WHERE lead_id=?', (now_text, str(lead_id or '')))
        conn.commit()
    return {'ok': True, 'lead_id': lead_id, 'cta_clicked_at': now_text}


def load_veraz_audio_leads(runtime: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    ensure_veraz_audio_store(runtime)
    with sqlite3.connect(runtime['db_path'], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute('SELECT * FROM veraz_audio_leads ORDER BY datetime(created_at) DESC LIMIT ?', (limit,)).fetchall()]


def app_today() -> date:
    return datetime.now(APP_TZ).date()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _floor_payload_value(value: Any) -> float:
    text = str(value or "").strip().lower()
    bucket_match = re.match(r"^fl[_-]?(\d+)$", text)
    if bucket_match:
        return int(bucket_match.group(1)) / 100.0
    text = re.sub(r"^(r\$|us\$|\$)\s*", "", text).strip()
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    return _to_float(text)


GAM_NET_REVENUE_FACTOR = 1.0


def _net_gam_revenue(value: Any) -> float:
    return round(_to_float(value) * GAM_NET_REVENUE_FACTOR, 6)


def _project_uses_usd_block_ecpm(project: str) -> bool:
    normalized = str(project or "").strip().lower()
    return (
        "institutodeidentificacao.com.br" in normalized
        or "wizseoservicesaustralia.com" in normalized
        or "foconadica.com" in normalized
    )


def _gam_brl_to_usd(value: Any, day_value: Any) -> float:
    amount = _to_float(value)
    if amount <= 0:
        return 0.0
    try:
        rate = float(ptax_usd_brl_rate(str(day_value or app_today().isoformat())))
    except Exception:
        rate = 0.0
    if rate <= 0:
        return 0.0
    return amount / rate


def _project_kpi_dicts(values: list[list[Any]]) -> list[dict[str, Any]]:
    if not values:
        return []
    header = values[0]
    return [dict(zip(header, row)) for row in values[1:] if row]



def ensure_campaign_bid_manual_action_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_bid_manual_actions (
                action_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT '',
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                campaign_id TEXT NOT NULL DEFAULT '',
                campaign TEXT NOT NULL DEFAULT '',
                conversion_goal TEXT NOT NULL DEFAULT '',
                action_type TEXT NOT NULL DEFAULT 'target_cpa_change',
                old_value REAL NOT NULL DEFAULT 0,
                new_value REAL NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending_apply',
                source TEXT NOT NULL DEFAULT 'manual_ui',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(campaign_bid_manual_actions)").fetchall()}
        migrations = {
            "applied_at": "ALTER TABLE campaign_bid_manual_actions ADD COLUMN applied_at TEXT NOT NULL DEFAULT ''",
            "message": "ALTER TABLE campaign_bid_manual_actions ADD COLUMN message TEXT NOT NULL DEFAULT ''",
            "verified_at": "ALTER TABLE campaign_bid_manual_actions ADD COLUMN verified_at TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in migrations.items():
            if column not in columns:
                conn.execute(statement)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_bid_manual_attr ON campaign_bid_manual_actions(project, attr_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_bid_manual_status ON campaign_bid_manual_actions(status, created_at)")
        conn.commit()


def ensure_campaign_budget_manual_action_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("create table if not exists campaign_budget_manual_actions (action_key text primary key, created_at text not null default '', applied_at text not null default '', project text not null default '', attr_id text not null default '', customer_id text not null default '', campaign_id text not null default '', campaign text not null default '', old_budget real not null default 0, new_budget real not null default 0, reason text not null default '', status text not null default 'pending_apply', source text not null default 'manual_ui', message text not null default '', metadata_json text not null default '{}')")
        columns = {row[1] for row in conn.execute("pragma table_info(campaign_budget_manual_actions)").fetchall()}
        migrations = {
            "applied_at": "alter table campaign_budget_manual_actions add column applied_at text not null default ''",
            "customer_id": "alter table campaign_budget_manual_actions add column customer_id text not null default ''",
            "message": "alter table campaign_budget_manual_actions add column message text not null default ''",
            "verified_at": "alter table campaign_budget_manual_actions add column verified_at text not null default ''",
        }
        for column, statement in migrations.items():
            if column not in columns:
                conn.execute(statement)
        conn.execute("create index if not exists idx_campaign_budget_manual_attr on campaign_budget_manual_actions(project, attr_id, created_at)")
        conn.execute("create index if not exists idx_campaign_budget_manual_status on campaign_budget_manual_actions(status, created_at)")
        conn.commit()


def ensure_campaign_status_manual_action_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_status_manual_actions (
                action_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT '',
                applied_at TEXT NOT NULL DEFAULT '',
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                customer_id TEXT NOT NULL DEFAULT '',
                campaign_id TEXT NOT NULL DEFAULT '',
                campaign TEXT NOT NULL DEFAULT '',
                old_status TEXT NOT NULL DEFAULT '',
                new_status TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending_apply',
                source TEXT NOT NULL DEFAULT 'manual_ui',
                message TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_status_manual_attr ON campaign_status_manual_actions(project, attr_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_status_manual_status ON campaign_status_manual_actions(status, created_at)")
        conn.commit()


def record_campaign_status_manual_action(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_campaign_status_manual_action_store(runtime["db_path"])
    created_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    project = str(payload.get("project") or "").strip()
    attr_id = str(payload.get("attr_id") or "").strip()
    customer_id = re.sub(r"\D+", "", str(payload.get("customer_id") or ""))
    campaign_id = re.sub(r"\D+", "", str(payload.get("campaign_id") or ""))
    campaign = str(payload.get("campaign") or "").strip()
    old_status = str(payload.get("old_status") or "").strip()
    new_status = str(payload.get("new_status") or "").strip().upper()
    reason = str(payload.get("reason") or "").strip()
    if new_status in {"PAUSE", "PAUSED"}:
        new_status = "PAUSED"
    elif new_status in {"ENABLE", "ENABLED"}:
        new_status = "ENABLED"
    if not project or not attr_id or not campaign_id:
        raise ValueError("project, attr_id e campaign_id sao obrigatorios")
    if new_status not in {"PAUSED", "ENABLED"}:
        raise ValueError("Status invalido. Use PAUSED ou ENABLED")
    if not customer_id:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT customer_id
                FROM google_ads_campaign_settings_daily
                WHERE (campaign_id=? OR attr_id=?) AND (site=? OR ?='')
                  AND COALESCE(customer_id,'') <> ''
                ORDER BY day DESC, updated_at DESC
                LIMIT 1
                """,
                (campaign_id, attr_id, project, project),
            ).fetchone()
            if row:
                customer_id = re.sub(r"\D+", "", str(row["customer_id"] or ""))
            if not customer_id:
                row = conn.execute(
                    """
                    SELECT customer_id
                    FROM ads_daily
                    WHERE (campaign_id=? OR attr_id=?) AND (site=? OR ?='')
                      AND COALESCE(customer_id,'') <> ''
                    ORDER BY day DESC
                    LIMIT 1
                    """,
                    (campaign_id, attr_id, project, project),
                ).fetchone()
                if row:
                    customer_id = re.sub(r"\D+", "", str(row["customer_id"] or ""))
    if not customer_id:
        raise ValueError("customer_id ausente; rode o Apps Script/snapshot antes de alterar status")
    action_key = hashlib.sha256(f"{created_at}|campaign_status|{project}|{attr_id}|{campaign_id}|{new_status}".encode()).hexdigest()
    metadata = {"requested_by": "manual_ui"}
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        existing = conn.execute(
            """
            SELECT action_key, status
            FROM campaign_status_manual_actions
            WHERE campaign_id=? AND new_status=? AND status IN ('pending_apply','sent')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (campaign_id, new_status),
        ).fetchone()
        if existing:
            return {"ok": True, "action_key": str(existing[0]), "created_at": created_at, "status": str(existing[1]), "new_status": new_status, "deduplicated": True}
        conn.execute(
            """
            INSERT INTO campaign_status_manual_actions (
                action_key, created_at, project, attr_id, customer_id, campaign_id,
                campaign, old_status, new_status, reason, status, source, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (action_key, created_at, project, attr_id, customer_id, campaign_id, campaign, old_status, new_status, reason, "pending_apply", "manual_ui", json.dumps(metadata, ensure_ascii=False)),
        )
        conn.commit()
    return {"ok": True, "action_key": action_key, "created_at": created_at, "status": "pending_apply", "new_status": new_status}


def record_campaign_budget_manual_action(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_campaign_budget_manual_action_store(runtime["db_path"])
    created_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    project = str(payload.get("project") or "").strip()
    attr_id = str(payload.get("attr_id") or "").strip()
    customer_id = str(payload.get("customer_id") or "").strip()
    campaign_id = str(payload.get("campaign_id") or "").strip()
    campaign = str(payload.get("campaign") or "").strip()
    old_budget = _to_float(payload.get("old_budget"))
    new_budget = _to_float(payload.get("new_budget"))
    reason = str(payload.get("reason") or "").strip()
    if not project or not attr_id or not campaign_id:
        raise ValueError("project, attr_id e campaign_id sao obrigatorios")
    if old_budget <= 0:
        raise ValueError("Orcamento atual indisponivel")
    if new_budget <= 0:
        raise ValueError("Informe o novo orcamento")
    if new_budget <= old_budget:
        raise ValueError("O novo orcamento deve ser maior que o atual")
    if not customer_id:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT customer_id
                FROM google_ads_campaign_settings_daily
                WHERE (campaign_id=? OR attr_id=?) AND (site=? OR ?='')
                  AND COALESCE(customer_id,'') <> ''
                ORDER BY day DESC, updated_at DESC
                LIMIT 1
                """,
                (campaign_id, attr_id, project, project),
            ).fetchone()
            if row:
                customer_id = str(row["customer_id"] or "").strip()
            if not customer_id:
                row = conn.execute(
                    """
                    SELECT customer_id
                    FROM ads_daily
                    WHERE (campaign_id=? OR attr_id=?) AND (site=? OR ?='')
                      AND COALESCE(customer_id,'') <> ''
                    ORDER BY day DESC
                    LIMIT 1
                    """,
                    (campaign_id, attr_id, project, project),
                ).fetchone()
                if row:
                    customer_id = str(row["customer_id"] or "").strip()
    metadata = {"roi_today": payload.get("roi_today"), "roi_7d": payload.get("roi_7d"), "pmr_today": payload.get("pmr_today"), "pmr_7d": payload.get("pmr_7d")}
    action_key = hashlib.sha256(f"{created_at}|manual_budget|{project}|{attr_id}|{campaign_id}|{old_budget}|{new_budget}".encode()).hexdigest()
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        existing = conn.execute(
            """
            SELECT action_key, status
            FROM campaign_budget_manual_actions
            WHERE campaign_id=? AND new_budget=? AND status IN ('pending_apply','sent','verified')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (campaign_id, new_budget),
        ).fetchone()
        if existing:
            return {"ok": True, "action_key": str(existing[0]), "created_at": created_at, "status": str(existing[1]), "old_budget": old_budget, "new_budget": new_budget, "deduplicated": True}
        conn.execute("insert into campaign_budget_manual_actions (action_key, created_at, project, attr_id, customer_id, campaign_id, campaign, old_budget, new_budget, reason, status, source, metadata_json) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (action_key, created_at, project, attr_id, customer_id, campaign_id, campaign, old_budget, new_budget, reason, "pending_apply", "manual_ui", json.dumps(metadata, ensure_ascii=False)))
        conn.commit()
    return {"ok": True, "action_key": action_key, "created_at": created_at, "status": "pending_apply", "old_budget": old_budget, "new_budget": new_budget}


def record_campaign_bid_manual_action(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_campaign_bid_manual_action_store(runtime["db_path"])
    created_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    project = str(payload.get("project") or "").strip()
    attr_id = str(payload.get("attr_id") or "").strip()
    campaign_id = str(payload.get("campaign_id") or "").strip()
    campaign = str(payload.get("campaign") or "").strip()
    conversion_goal = str(payload.get("conversion_goal") or "").strip()
    action_type = str(payload.get("action_type") or "target_cpa_change").strip() or "target_cpa_change"
    old_value = _to_float(payload.get("old_value"))
    new_value = _to_float(payload.get("new_value"))
    reason = str(payload.get("reason") or "").strip()
    if not project or not attr_id or not campaign_id:
        raise ValueError("project, attr_id e campaign_id sao obrigatorios")
    if new_value <= 0:
        raise ValueError("Informe o novo CPA desejado")
    if len(reason) < 8:
        raise ValueError("Informe um motivo objetivo para o ajuste")
    status = "pending_apply"
    metadata = {
        "roi_today": payload.get("roi_today"),
        "roi_7d": payload.get("roi_7d"),
        "pmr_today": payload.get("pmr_today"),
        "pmr_7d": payload.get("pmr_7d"),
        "avg_cpc": payload.get("avg_cpc"),
    }
    action_key = hashlib.sha256(f"{created_at}|{project}|{attr_id}|{campaign_id}|{old_value}|{new_value}".encode()).hexdigest()
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        existing = conn.execute(
            """
            SELECT action_key, status
            FROM campaign_bid_manual_actions
            WHERE attr_id=? AND campaign_id=? AND new_value=? AND status IN ('pending_apply','sent','verified')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (attr_id, campaign_id, new_value),
        ).fetchone()
        if existing:
            return {"ok": True, "action_key": str(existing[0]), "created_at": created_at, "status": str(existing[1]), "old_value": old_value, "new_value": new_value, "deduplicated": True}
        conn.execute(
            """
            INSERT INTO campaign_bid_manual_actions (
                action_key, created_at, project, attr_id, campaign_id, campaign,
                conversion_goal, action_type, old_value, new_value, reason, status,
                source, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action_key,
                created_at,
                project,
                attr_id,
                campaign_id,
                campaign,
                conversion_goal,
                action_type,
                old_value,
                new_value,
                reason,
                status,
                "manual_ui",
                json.dumps(metadata, ensure_ascii=False),
            ),
        )
        conn.commit()
    return {
        "ok": True,
        "action_key": action_key,
        "created_at": created_at,
        "status": status,
        "old_value": old_value,
        "new_value": new_value,
    }



def ensure_gam_floor_manual_action_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gam_floor_price_manual_actions (
                action_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT '',
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                campaign TEXT NOT NULL DEFAULT '',
                block_name TEXT NOT NULL DEFAULT '',
                period TEXT NOT NULL DEFAULT '',
                current_floor REAL NOT NULL DEFAULT 0,
                new_floor REAL NOT NULL DEFAULT 0,
                change_pct REAL,
                currency TEXT NOT NULL DEFAULT 'USD',
                status TEXT NOT NULL DEFAULT 'registered',
                source TEXT NOT NULL DEFAULT 'manual_ui',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(gam_floor_price_manual_actions)")}
        if "pmr_at_registration" not in existing_cols:
            conn.execute("ALTER TABLE gam_floor_price_manual_actions ADD COLUMN pmr_at_registration REAL")
        if "placement" not in existing_cols:
            conn.execute("ALTER TABLE gam_floor_price_manual_actions ADD COLUMN placement TEXT NOT NULL DEFAULT ''")
        if "scope" not in existing_cols:
            conn.execute("ALTER TABLE gam_floor_price_manual_actions ADD COLUMN scope TEXT NOT NULL DEFAULT 'legacy'")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gam_floor_manual_attr ON gam_floor_price_manual_actions(attr_id, block_name, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gam_floor_manual_placement ON gam_floor_price_manual_actions(project, attr_id, placement, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gam_floor_manual_placement_block ON gam_floor_price_manual_actions(project, attr_id, placement, block_name, created_at)")
        conn.commit()


def _floor_registration_context(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    placement: str,
    block_name: str,
) -> dict[str, Any]:
    target = str(block_name or "").strip()
    if not target:
        return {}
    current_day = app_today()
    try:
        rows = load_project_campaign_placements(runtime, project, attr_id, "today", current_day)
        for row in rows:
            row_placement = str(row.get("placement") or row.get("display_name") or "").strip().lower()
            if row_placement != str(placement or "").strip().lower():
                continue
            for block in row.get("ad_blocks") or []:
                if str(block.get("block_name") or "").strip() == target:
                    return {
                        "block_name": target,
                        "placement": row_placement,
                        "impressions_at_registration": _to_float(block.get("impressions")),
                        "ecpm_at_registration": _to_float(block.get("ecpm")),
                        "pmr_at_registration": _to_float(block.get("pmr")),
                        "source": "placement_block",
                    }
    except Exception:
        pass
    return {}


JULIO_SETUP_SUFFIXES = (".wizseoservicesaustralia.com", ".foconadica.com")
JULIO_SETUP_DOMAINS = ("lordkuma.com",)


def normalize_initial_setup_domain(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^https?://", "", text)
    text = text.split("/", 1)[0].strip()
    return text


def is_julio_setup_domain(domain: str) -> bool:
    clean = normalize_initial_setup_domain(domain)
    return bool(clean) and (clean.endswith(JULIO_SETUP_SUFFIXES) or clean in JULIO_SETUP_DOMAINS)


def ensure_initial_setup_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS initial_setups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                customer_id TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                currency TEXT NOT NULL DEFAULT 'BRL',
                status TEXT NOT NULL DEFAULT 'rascunho',
                plan_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_initial_setups_domain ON initial_setups(domain)")


def save_initial_setup_plan(runtime: dict[str, Any], payload: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    ensure_initial_setup_store(runtime["db_path"])
    now_text = datetime.now(APP_TZ).isoformat(timespec="seconds")
    domain = normalize_initial_setup_domain(plan.get("domain") or payload.get("domain"))
    if not is_julio_setup_domain(domain):
        raise ValueError("Dominio fora da instancia Julio")
    safe_payload = dict(payload)
    if "wordpress_password" in safe_payload:
        safe_payload["wordpress_password"] = "***received***" if str(safe_payload.get("wordpress_password") or "").strip() else ""
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        cursor = conn.execute(
            """
            INSERT INTO initial_setups(domain, customer_id, email, currency, status, plan_json, payload_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                domain,
                str(plan.get("google_ads", {}).get("customer_id") or payload.get("customer_id") or "").strip(),
                str(payload.get("email") or "").strip(),
                str(payload.get("currency") or "BRL").strip() or "BRL",
                "rascunho",
                json.dumps(plan, ensure_ascii=False),
                json.dumps(safe_payload, ensure_ascii=False),
                now_text,
                now_text,
            ),
        )
        setup_id = int(cursor.lastrowid)
    return {"setup_id": setup_id, "status": "rascunho", "created_at": now_text}


def load_initial_setup_plans(runtime: dict[str, Any], limit: int = 20) -> list[dict[str, Any]]:
    ensure_initial_setup_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, domain, customer_id, email, currency, status, created_at, updated_at
            FROM initial_setups
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit or 20),),
        ).fetchall()
    return [dict(row) for row in rows]


def load_initial_setup_plan(runtime: dict[str, Any], setup_id: int) -> dict[str, Any] | None:
    ensure_initial_setup_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM initial_setups WHERE id = ?", (int(setup_id),)).fetchone()
    return dict(row) if row else None


def load_initial_setup_detail(runtime: dict[str, Any], setup_id: int) -> dict[str, Any] | None:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return None
    detail = dict(row)
    try:
        detail["plan"] = json.loads(row.get("plan_json") or "{}")
    except Exception:
        detail["plan"] = {}
    try:
        detail["payload"] = json.loads(row.get("payload_json") or "{}")
    except Exception:
        detail["payload"] = {}
    return detail


def update_initial_setup_plan(runtime: dict[str, Any], setup_id: int, status: str, plan: dict[str, Any]) -> None:
    ensure_initial_setup_store(runtime["db_path"])
    now_text = datetime.now(APP_TZ).isoformat(timespec="seconds")
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.execute(
            "UPDATE initial_setups SET status = ?, plan_json = ?, updated_at = ? WHERE id = ?",
            (status, json.dumps(plan, ensure_ascii=False), now_text, int(setup_id)),
        )


def _initial_setup_wp_url(setup: dict[str, Any]) -> str:
    plan = setup.get("plan") or {}
    payload = setup.get("payload") or {}
    url = str(payload.get("wordpress_url") or plan.get("wordpress", {}).get("admin_url") or "").strip()
    if url:
        return url
    domain = normalize_initial_setup_domain(setup.get("domain"))
    return f"https://{domain}/wp-admin/"


def _initial_setup_event_gtm_html() -> str:
    return """<script>
window.dataLayer = window.dataLayer || [];
window.googletag = window.googletag || {};
googletag.cmd = googletag.cmd || [];
var adSlots = {};
function generateTransactionId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") return window.crypto.randomUUID();
  return "tx_" + new Date().getTime() + "_" + Math.random().toString(36).substring(2, 10);
}
function pushEvent(eventName, data) {
  var eventData = {event: eventName, transaction_id: generateTransactionId()};
  for (var key in data) if (Object.prototype.hasOwnProperty.call(data, key)) eventData[key] = data[key];
  window.dataLayer.push(eventData);
}
function getAdEventName(slotId) {
  if (slotId.indexOf("interstitial") !== -1) return "adViewInterstitial";
  if (slotId.indexOf("rewarded") !== -1 || slotId.indexOf("rewards") !== -1) return "adViewRewarded";
  return "adView";
}
function hasClass(element, className) {
  return !!(element && element.className && (" " + element.className + " ").indexOf(" " + className + " ") !== -1);
}
function getClosestButton(element) {
  while (element && element !== document) {
    if (hasClass(element, "wp-block-button__link") || hasClass(element, "card-button") || hasClass(element, "btn-ios")) return element;
    element = element.parentNode;
  }
  return null;
}
function getButtonUrl(button) {
  var element = button;
  while (element && element !== document) {
    if (element.tagName && element.tagName.toLowerCase() === "a") return element.getAttribute("href") || "";
    element = element.parentNode;
  }
  return button.getAttribute("href") || "";
}
googletag.cmd.push(function () {
  googletag.pubads().addEventListener("slotRenderEnded", function (event) {
    var slotId = String(event.slot.getSlotId()).toLowerCase();
    adSlots[slotId] = {adUnit: event.slot.getAdUnitPath(), slotId: slotId, size: event.size || null, isEmpty: event.isEmpty};
  });
  googletag.pubads().addEventListener("impressionViewable", function (event) {
    var slotId = String(event.slot.getSlotId()).toLowerCase();
    var adData = adSlots[slotId] || {adUnit: event.slot.getAdUnitPath(), slotId: slotId, size: null, isEmpty: null};
    pushEvent(getAdEventName(slotId), adData);
  });
});
document.addEventListener("click", function (event) {
  var button = getClosestButton(event.target || event.srcElement);
  if (!button) return;
  var pagePath = window.location.pathname || "/";
  var pageSlug = pagePath.replace(/^\/+|\/+$/g, "") || "home";
  var buttonText = (button.textContent || button.innerText || "").replace(/\s+/g, " ").replace(/^\s+|\s+$/g, "");
  pushEvent("buttonClick", {page_path: pagePath, page_slug: pageSlug, button_text: buttonText, button_url: getButtonUrl(button), button_classes: button.className || ""});
}, true);
</script>"""


def _gtm_param(param_type: str, key: str, value: Any) -> dict[str, str]:
    return {"type": param_type, "key": key, "value": str(value)}


def _initial_setup_extract_ads_conversions(plan: dict[str, Any], currency: str) -> list[dict[str, str]]:
    conversions: list[dict[str, str]] = []
    for action in (plan.get("google_ads_conversions_apply") or {}).get("actions") or []:
        name = str(action.get("name") or "").strip()
        for snippet in action.get("snippets") or []:
            event_snippet = str(snippet.get("event_snippet") or "")
            match = re.search(r"AW-(\d+)/([A-Za-z0-9_-]+)", event_snippet)
            if name and match:
                conversions.append({"name": name, "conversion_id": match.group(1), "label": match.group(2), "currency": currency})
                break
    return conversions


def _initial_setup_create_gtm_tags(plan: dict[str, Any], gtm_account_name: str, gtm_id: str, currency: str) -> dict[str, Any]:
    if not gtm_id or "CRIAR" in gtm_id:
        return {"status": "manual_required", "message": "Container GTM ausente."}
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        account_id = {"LRD + KAIZEN": "6342819886"}.get(gtm_account_name, "")
        if not account_id:
            raise ValueError(f"Conta GTM nao mapeada: {gtm_account_name}")
        credentials_path = Path("credentials/gtm_service_account.json")
        scopes = [
            "https://www.googleapis.com/auth/tagmanager.edit.containers",
            "https://www.googleapis.com/auth/tagmanager.readonly",
            "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
            "https://www.googleapis.com/auth/tagmanager.publish",
        ]
        credentials = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
        service = build("tagmanager", "v2", credentials=credentials, cache_discovery=False)
        containers = service.accounts().containers().list(parent=f"accounts/{account_id}").execute().get("container", [])
        container = next((item for item in containers if str(item.get("publicId") or "") == gtm_id), None)
        if not container:
            raise ValueError(f"Container {gtm_id} nao encontrado")
        container_path = container["path"]
        workspaces_api = service.accounts().containers().workspaces()
        workspaces = workspaces_api.list(parent=container_path).execute().get("workspace", [])
        workspace = next((item for item in workspaces if str(item.get("name") or "") == "Kaizen Google Ads Tags"), None)
        if not workspace:
            workspace = workspaces_api.create(parent=container_path, body={"name": "Kaizen Google Ads Tags", "description": "Tags de conversao criadas pelo Setup Inicial"}).execute()
        workspace_path = workspace["path"]
        tags_api = service.accounts().containers().workspaces().tags()
        triggers_api = service.accounts().containers().workspaces().triggers()
        existing_tags = {str(item.get("name") or ""): item for item in tags_api.list(parent=workspace_path).execute().get("tag", [])}
        existing_triggers = {str(item.get("name") or ""): item for item in triggers_api.list(parent=workspace_path).execute().get("trigger", [])}

        def ensure_trigger(event_name: str) -> str:
            name = f"Acionador {event_name}"
            if name in existing_triggers:
                return str(existing_triggers[name]["triggerId"])
            created = triggers_api.create(
                parent=workspace_path,
                body={
                    "name": name,
                    "type": "customEvent",
                    "customEventFilter": [{
                        "type": "equals",
                        "parameter": [_gtm_param("template", "arg0", "{{_event}}"), _gtm_param("template", "arg1", event_name)],
                    }],
                },
            ).execute()
            existing_triggers[name] = created
            return str(created["triggerId"])

        def ensure_tag(name: str, body: dict[str, Any]) -> dict[str, Any]:
            if name in existing_tags:
                return {"name": name, "status": "exists", "tagId": existing_tags[name].get("tagId")}
            created = tags_api.create(parent=workspace_path, body=body).execute()
            existing_tags[name] = created
            return {"name": name, "status": "created", "tagId": created.get("tagId")}

        tags = [
            ensure_tag("Evento GTM", {
                "name": "Evento GTM",
                "type": "html",
                "parameter": [_gtm_param("template", "html", _initial_setup_event_gtm_html()), _gtm_param("boolean", "supportDocumentWrite", "false")],
                "firingTriggerId": ["2147479553"],
            }),
            ensure_tag("Conversion Linker", {
                "name": "Conversion Linker",
                "type": "gclidw",
                "parameter": [_gtm_param("boolean", "enableCrossDomain", "false")],
                "firingTriggerId": ["2147479553"],
            }),
        ]
        conversions = _initial_setup_extract_ads_conversions(plan, currency)
        if conversions:
            conversion_id = conversions[0]["conversion_id"]
            tags.append(ensure_tag(f"Tag do Google AW-{conversion_id}", {
                "name": f"Tag do Google AW-{conversion_id}",
                "type": "googtag",
                "parameter": [_gtm_param("template", "tagId", f"AW-{conversion_id}")],
                "firingTriggerId": ["2147479553"],
            }))
        for conversion in conversions:
            trigger_id = ensure_trigger(conversion["name"])
            tags.append(ensure_tag(f"Google Ads {conversion['name']}", {
                "name": f"Google Ads {conversion['name']}",
                "type": "awct",
                "parameter": [
                    _gtm_param("template", "conversionValue", "1"),
                    _gtm_param("template", "orderId", "{{transaction_id}}"),
                    _gtm_param("template", "conversionId", conversion["conversion_id"]),
                    _gtm_param("template", "currencyCode", conversion.get("currency") or currency),
                    _gtm_param("template", "conversionLabel", conversion["label"]),
                ],
                "firingTriggerId": [trigger_id],
            }))
        missing = []
        if not conversions:
            missing.append("labels das conversoes do Google Ads")
        publish_status = "manual_required"
        publish_message = ""
        try:
            version = workspaces_api.create_version(
                path=workspace_path,
                body={"name": f"Kaizen Setup Inicial {datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M')}", "notes": "Tags criadas pelo Setup Inicial Kaizen."},
            ).execute()
            container_version = version.get("containerVersion") or {}
            version_path = str(container_version.get("path") or "")
            if version_path:
                fingerprint = str(container_version.get("fingerprint") or "")
                if fingerprint:
                    service.accounts().containers().versions().publish(path=version_path, fingerprint=fingerprint).execute()
                else:
                    service.accounts().containers().versions().publish(path=version_path).execute()
                publish_status = "published"
                publish_message = version_path
            else:
                publish_message = "Versao criada sem path retornado pela API."
        except Exception as exc:
            publish_message = str(exc)
        return {
            "status": "tags_ready" if conversions else "partial_tags_ready",
            "workspace": "Kaizen Google Ads Tags",
            "workspace_path": workspace_path,
            "container_id": gtm_id,
            "tags": tags,
            "missing": missing,
            "publish_status": publish_status,
            "publish_message": publish_message,
            "manual_next": [] if publish_status == "published" else ["Publicar o workspace no GTM apos validar as tags."],
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def validate_initial_setup_wordpress(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    domain = normalize_initial_setup_domain(setup.get("domain"))
    gtm_id = str(plan.get("gtm_apply", {}).get("container_id") or "").strip()
    public_url = f"https://{domain}/"
    checks = {"public_url": public_url, "gtm_id": gtm_id, "gtm_found": False, "http_status": 0, "error": ""}
    try:
        request = urllib.request.Request(public_url, headers={"User-Agent": "Mozilla/5.0 KaizenSetupValidator"})
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read(250000).decode("utf-8", "replace")
            checks["http_status"] = int(response.status)
            checks["gtm_found"] = bool(gtm_id and gtm_id in body)
    except Exception as exc:
        checks["error"] = str(exc)
    installed = bool(checks["gtm_found"])
    plan["wordpress_validation"] = {"status": "installed" if installed else "pending", "url": public_url, "checks": checks}
    update_initial_setup_plan(runtime, setup_id, "wordpress_validado" if installed else str(setup.get("status") or "gtm_codigo_gerado"), plan)
    return {"status": "ok", "wordpress": plan["wordpress_validation"]}


def prepare_initial_setup_ga4(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    payload = setup.get("payload") or {}
    domain = normalize_initial_setup_domain(setup.get("domain"))
    measurement_id = str(payload.get("ga4_measurement_id") or plan.get("ga4", {}).get("measurement_id") or "").strip()
    ga4_plan = {
        "status": "manual_required" if not measurement_id else "measurement_id_registered",
        "account": str(payload.get("ga4_account") or plan.get("ga4", {}).get("account") or "").strip(),
        "property_name": domain,
        "measurement_id": measurement_id,
        "events": ["adView", "adViewInterstitial", "adViewRewarded", "buttonClick"],
        "manual_checks": [
            "Criar nova propriedade GA4 para o dominio.",
            "Criar fluxo Web do dominio.",
            "Colar o Measurement ID neste setup.",
            "Criar tags GA4 no GTM para adView, adViewInterstitial, adViewRewarded e buttonClick.",
        ],
    }
    plan["ga4_apply"] = ga4_plan
    update_initial_setup_plan(runtime, setup_id, "ga4_preparado", plan)
    return {"status": "ok", "ga4": ga4_plan}


def prepare_initial_setup_clarity(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    payload = setup.get("payload") or {}
    domain = normalize_initial_setup_domain(setup.get("domain"))
    clarity_id = str(payload.get("clarity_project_id") or plan.get("clarity", {}).get("project_id") or "").strip()
    clarity_plan = {
        "status": "manual_required" if not clarity_id else "project_id_registered",
        "project_name": domain,
        "project_id": clarity_id,
        "install_mode": "Criar novo projeto dentro da conta Clarity existente.",
        "integrations": ["Google Tag Manager", "Google Ads", "GA4"],
        "manual_checks": [
            "Criar novo projeto Clarity com o dominio.",
            "Instalar pelo GTM.",
            "Conectar Google Ads da conta selecionada.",
            "Conectar a nova propriedade GA4.",
        ],
    }
    plan["clarity_apply"] = clarity_plan
    update_initial_setup_plan(runtime, setup_id, "clarity_preparado", plan)
    return {"status": "ok", "clarity": clarity_plan}


def install_initial_setup_wordpress(runtime: dict[str, Any], setup_id: int, credentials: dict[str, Any]) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    wp_url = str(credentials.get("wordpress_url") or _initial_setup_wp_url(setup)).strip()
    user = str(credentials.get("wordpress_user") or "").strip()
    password = str(credentials.get("wordpress_password") or "").strip()
    install = {"status": "pending", "wordpress_url": wp_url, "user_received": bool(user), "password_received": bool(password), "message": ""}
    if not user or not password:
        install["status"] = "manual_required"
        install["message"] = "Informe usuario e senha/application password para instalar automaticamente."
    elif not plan.get("gtm_apply", {}).get("snippets"):
        install["status"] = "manual_required"
        install["message"] = "Crie o GTM primeiro para gerar o codigo Header/Body."
    else:
        try:
            snippets = plan.get("gtm_apply", {}).get("snippets", {})
            result = _install_wpcode_header_footer(wp_url, user, password, str(snippets.get("head") or ""), str(snippets.get("body") or ""))
            install.update(result)
        except Exception as exc:
            install["status"] = "error"
            install["message"] = str(exc)
    plan["wordpress_install"] = install
    update_initial_setup_plan(runtime, setup_id, "wordpress_instalacao_preparada", plan)
    validation = validate_initial_setup_wordpress(runtime, setup_id)
    return {"status": "ok", "wordpress_install": install, "validation": validation.get("wordpress", {})}


def _install_wpcode_header_footer(wp_url: str, user: str, password: str, head: str, body: str) -> dict[str, Any]:
    if not head or not body:
        raise ValueError("Snippets GTM ausentes")
    parsed = urlparse(wp_url if "://" in wp_url else f"https://{wp_url}")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    admin_url = wp_url if wp_url.endswith("/") else wp_url + "/"
    wpcode_url = urljoin(origin, "/wp-admin/admin.php?page=wpcode-headers-footers")
    login_candidates = [admin_url, urljoin(origin, "/wp-login.php")]
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    headers = {"User-Agent": "Mozilla/5.0 KaizenSetupInstaller"}

    login_page = ""
    login_url = login_candidates[0]
    for candidate in login_candidates:
        try:
            request = urllib.request.Request(candidate, headers=headers)
            with opener.open(request, timeout=25) as response:
                login_page = response.read(300000).decode("utf-8", "replace")
                login_url = response.geturl()
            if "name=\"log\"" in login_page or "wp-submit" in login_page or "loginform" in login_page:
                break
        except Exception:
            continue

    redirect_to = wpcode_url
    post_data = {
        "log": user,
        "pwd": password,
        "wp-submit": "Log In",
        "redirect_to": redirect_to,
        "testcookie": "1",
    }
    action_match = re.search(r"<form[^>]+id=[\"']loginform[\"'][^>]*action=[\"']([^\"']+)", login_page, re.I)
    post_url = action_match.group(1).replace("&amp;", "&") if action_match else urljoin(origin, "/wp-login.php")
    request = urllib.request.Request(post_url, data=urlencode(post_data).encode(), headers={**headers, "Content-Type": "application/x-www-form-urlencoded"})
    with opener.open(request, timeout=30) as response:
        response.read(100000)

    request = urllib.request.Request(wpcode_url, headers=headers)
    with opener.open(request, timeout=30) as response:
        page = response.read(500000).decode("utf-8", "replace")
        final_url = response.geturl()
    if "wp-login.php" in final_url or "name=\"log\"" in page:
        raise ValueError("Login WordPress nao confirmado. Verifique usuario/senha ou permissao de administrador.")
    if "wpcode" not in page.lower() and "ihaf_insert_header" not in page:
        raise ValueError("Pagina WPCode Header/Footer nao encontrada. Confirme se o WPCode esta instalado.")

    nonce_match = re.search(r"name=[\"'](?:_wpnonce|insert-headers-and-footers_nonce)[\"']\s+value=[\"']([^\"']+)", page)
    nonce_name_match = re.search(r"name=[\"'](_wpnonce|insert-headers-and-footers_nonce)[\"']\s+value=[\"'][^\"']+", page)
    option_page_match = re.search(r"name=[\"']option_page[\"']\s+value=[\"']([^\"']+)", page)
    action_match = re.search(r"name=[\"']action[\"']\s+value=[\"']([^\"']+)", page)
    if not nonce_match:
        raise ValueError("Nonce do WPCode nao encontrado.")
    payload = {
        "option_page": option_page_match.group(1) if option_page_match else "insert-headers-and-footers",
        "action": action_match.group(1) if action_match else "update",
        "_wp_http_referer": "/wp-admin/admin.php?page=wpcode-headers-footers",
        "ihaf_insert_header": head,
        "ihaf_insert_body": body,
        "ihaf_insert_footer": "",
    }
    payload[nonce_name_match.group(1) if nonce_name_match else "_wpnonce"] = nonce_match.group(1)
    request = urllib.request.Request(wpcode_url, data=urlencode(payload).encode(), headers={**headers, "Content-Type": "application/x-www-form-urlencoded", "Referer": wpcode_url})
    with opener.open(request, timeout=30) as response:
        response.read(100000)
    return {"status": "installed", "message": "GTM instalado no WPCode Header/Footer.", "wpcode_url": wpcode_url}


def final_check_initial_setup(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    manual = plan.get("manual_confirmations", {})
    def step_applied(section: str) -> bool:
        data = plan.get(section) or {}
        status = str(data.get("status") or data.get("api_status") or "").strip().lower()
        return bool(status and status not in {"manual_required", "pending", "error"})
    checks = [
        {"key": "google_ads", "label": "Google Ads configurado", "ok": bool(step_applied("google_ads_apply") or manual.get("ads_account"))},
        {"key": "gtm", "label": "GTM criado/publicado", "ok": bool(step_applied("gtm_apply") or manual.get("gtm_preview"))},
        {"key": "wordpress", "label": "GTM instalado no WordPress", "ok": bool(plan.get("wordpress_validation", {}).get("status") == "installed" or manual.get("gtm_preview"))},
        {"key": "ga4", "label": "GA4 validado", "ok": bool(step_applied("ga4_apply") or manual.get("ga4"))},
        {"key": "clarity", "label": "Clarity validado", "ok": bool(step_applied("clarity_apply") or manual.get("clarity"))},
    ]
    blockers = [item for item in checks if not item["ok"]]
    status_name = "released_for_traffic" if not blockers else "blocked"
    plan["final_check"] = {"status": status_name, "checks": checks, "blockers": blockers, "manual_required": blockers}
    update_initial_setup_plan(runtime, setup_id, status_name, plan)
    return {"status": "ok", "final_check": plan["final_check"]}


def confirm_initial_setup_step(runtime: dict[str, Any], setup_id: int, step: str) -> dict[str, Any]:
    setup = load_initial_setup_detail(runtime, setup_id)
    if not setup:
        raise ValueError("Setup nao encontrado")
    plan = dict(setup.get("plan") or {})
    allowed = {"ads_account", "gtm_preview", "ga4", "clarity"}
    if step not in allowed:
        raise ValueError("Etapa invalida")
    manual = dict(plan.get("manual_confirmations") or {})
    manual[step] = datetime.now(APP_TZ).isoformat(timespec="seconds")
    plan["manual_confirmations"] = manual
    update_initial_setup_plan(runtime, setup_id, str(setup.get("status") or "rascunho"), plan)
    return final_check_initial_setup(runtime, setup_id)


def record_gam_floor_manual_action(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_gam_floor_manual_action_store(runtime["db_path"])
    created_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    project = str(payload.get("project") or "").strip()
    attr_id = str(payload.get("attr_id") or "").strip()
    campaign = str(payload.get("campaign") or "").strip()
    placement = str(payload.get("placement") or "").strip().lower()
    scope = str(payload.get("scope") or "").strip().lower()
    block_name = str(payload.get("block_name") or "").strip()
    period = str(payload.get("period") or "").strip() or "today"
    currency = str(payload.get("currency") or "USD").strip() or "USD"
    if "sonora123.com" in project.lower():
        currency = "BRL"
    action_status = str(payload.get("status") or "registered").strip() or "registered"
    if action_status not in {"registered", "disabled"}:
        action_status = "registered"
    payload_source = str(payload.get("source") or "manual_ui").strip() or "manual_ui"
    current_floor = _floor_payload_value(payload.get("current_floor_bucket") or payload.get("current_floor"))
    new_floor = _floor_payload_value(payload.get("new_floor_bucket") or payload.get("new_floor"))
    if scope not in {"campaign_block", "placement_block"}:
        raise ValueError("Escopo de floor invalido")
    if not project or not attr_id or not block_name or (scope == "placement_block" and not placement):
        raise ValueError("A regra de floor da instancia Julio exige project, attr_id, block_name e placement quando for por placement")
    if current_floor < 0 or new_floor < 0:
        raise ValueError("floor nao pode ser negativo")
    pmr_at_registration = _to_float(payload.get("pmr"))
    if pmr_at_registration <= 0:
        try:
            for block in load_project_campaign_blocks(runtime, project, attr_id, "today"):
                if str(block.get("block_name") or "") == block_name:
                    pmr_at_registration = _to_float(block.get("pmr"))
                    break
        except Exception:
            pmr_at_registration = 0.0
    registration_context = _floor_registration_context(runtime, project, attr_id, placement, block_name)
    guard_expected_floor = _floor_from_ecpm(
        _to_float(registration_context.get("ecpm_at_registration")),
        _to_float(registration_context.get("impressions_at_registration")),
    )
    guard_corrected_floor = False
    if action_status == "registered" and current_floor <= 0 and guard_expected_floor and guard_expected_floor > new_floor:
        new_floor = float(guard_expected_floor)
        guard_corrected_floor = True
    change_pct = ((new_floor - current_floor) / current_floor) if current_floor > 0 else None
    action_key = hashlib.sha256(f"{created_at}|{project}|{attr_id}|{scope}|{placement}|{block_name}|{current_floor}|{new_floor}".encode()).hexdigest()
    metadata = {
        "campaign": campaign,
        "placement": placement,
        "scope": scope,
        "period": period,
        "direction": "increase" if new_floor > current_floor else "decrease" if new_floor < current_floor else "same",
        "impressions_at_registration": _to_float(registration_context.get("impressions_at_registration")),
        "ecpm_at_registration": _to_float(registration_context.get("ecpm_at_registration")),
        "registered_source": str(registration_context.get("source") or ""),
        "registered_day": app_today().isoformat(),
        "floor_guard_corrected": guard_corrected_floor,
        "floor_guard_expected_floor": _to_float(guard_expected_floor),
    }
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.execute(
            """
            INSERT INTO gam_floor_price_manual_actions (
                action_key, created_at, project, attr_id, campaign, block_name, period,
                current_floor, new_floor, change_pct, currency, status, source, metadata_json,
                pmr_at_registration, placement, scope
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action_key, created_at, project, attr_id, campaign, block_name, period,
                current_floor, new_floor, change_pct, currency, action_status,
                payload_source, json.dumps(metadata, ensure_ascii=False),
                pmr_at_registration, placement, scope,
            ),
        )
        if guard_corrected_floor:
            learning_key = _agent_key("julio", "floor-price", "Floor inicial nunca pode ficar abaixo do eCPM registrado", project, attr_id, block_name)
            evidence = {
                "project": project,
                "attr_id": attr_id,
                "block_name": block_name,
                "ecpm_at_registration": _to_float(registration_context.get("ecpm_at_registration")),
                "impressions_at_registration": _to_float(registration_context.get("impressions_at_registration")),
                "corrected_floor": new_floor,
                "rule": "Na primeira analise do dia, se impressoes >= 2 e PMR >= 80%, o bucket inicial deve ser o proximo bucket acima do eCPM; nunca limitar pelo teto antigo.",
                "responsible_agent": "floor-price",
                "review_agent": "monetizacao",
                "audit_agent": "governanca-dados",
                "approver": "orquestrador",
            }
            agent_db_path = ensure_agent_memory_store(runtime)
            with sqlite3.connect(agent_db_path, timeout=30) as agent_conn:
                agent_conn.execute(
                    """
                    INSERT INTO agent_learnings (learning_key, instance, agent_slug, title, evidence_json, confidence, created_at, updated_at)
                    VALUES (?, 'julio', 'floor-price', 'Floor inicial nunca pode ficar abaixo do eCPM registrado', ?, 0.95, ?, ?)
                    ON CONFLICT(learning_key) DO UPDATE SET
                        evidence_json = excluded.evidence_json,
                        confidence = excluded.confidence,
                        updated_at = excluded.updated_at
                    """,
                    (learning_key, json.dumps(evidence, ensure_ascii=False, sort_keys=True), created_at, created_at),
                )
                agent_conn.commit()
        conn.commit()
    return {
        "status": "ok",
        "action_key": action_key,
        "created_at": created_at,
        "project": project,
        "attr_id": attr_id,
        "block_name": block_name,
        "placement": placement,
        "scope": scope,
        "current_floor": current_floor,
        "new_floor": new_floor,
        "change_pct": change_pct,
        "pmr_at_registration": pmr_at_registration,
        "currency": currency,
        "record_status": action_status,
    }

def ensure_google_ads_script_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS google_ads_script_runs (run_id TEXT PRIMARY KEY, created_at TEXT NOT NULL DEFAULT '', customer_id TEXT NOT NULL DEFAULT '', account TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '', spend_rows INTEGER NOT NULL DEFAULT 0, settings_rows INTEGER NOT NULL DEFAULT 0, placement_rows INTEGER NOT NULL DEFAULT 0, actions_count INTEGER NOT NULL DEFAULT 0, message TEXT NOT NULL DEFAULT '')
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS google_ads_script_action_results (action_key TEXT PRIMARY KEY, action_type TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '', old_value REAL NOT NULL DEFAULT 0, new_value REAL NOT NULL DEFAULT 0, applied_at TEXT NOT NULL DEFAULT '', message TEXT NOT NULL DEFAULT '', received_at TEXT NOT NULL DEFAULT '')
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS google_ads_placement_exclusion_actions (action_key TEXT PRIMARY KEY, created_at TEXT NOT NULL DEFAULT '', applied_at TEXT NOT NULL DEFAULT '', project TEXT NOT NULL DEFAULT '', attr_id TEXT NOT NULL DEFAULT '', customer_id TEXT NOT NULL DEFAULT '', campaign_id TEXT NOT NULL DEFAULT '', placement TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending_apply', source TEXT NOT NULL DEFAULT 'manual_ui', message TEXT NOT NULL DEFAULT '')
        """)
        placement_cols = {row[1] for row in conn.execute("PRAGMA table_info(google_ads_placement_exclusion_actions)").fetchall()}
        if "verified_at" not in placement_cols:
            conn.execute("ALTER TABLE google_ads_placement_exclusion_actions ADD COLUMN verified_at TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_google_ads_script_placement_status ON google_ads_placement_exclusion_actions(status, created_at)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ads_daily_operational ON ads_daily(day, customer_id, campaign_id, ad_group_id, channel_type)")
        conn.execute("""
            DELETE FROM floor_bucket_state
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM floor_bucket_state
                GROUP BY site, attr_id, slot
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_floor_bucket_state_operational ON floor_bucket_state(site, attr_id, slot)")
        conn.commit()
    ensure_campaign_bid_manual_action_store(db_path)
    ensure_campaign_budget_manual_action_store(db_path)


def ensure_google_ads_settings_operational_store(db_path: str | Path) -> None:
    _ensure_google_ads_campaign_settings_table(db_path)
    expected_pk = ["day", "customer_id", "campaign_id", "ad_group_id"]
    columns_sql = """
        day TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        campaign_id TEXT NOT NULL,
        campaign TEXT NOT NULL DEFAULT '',
        ad_group_id TEXT NOT NULL DEFAULT '',
        ad_group_name TEXT NOT NULL DEFAULT '',
        attr_id TEXT NOT NULL DEFAULT '',
        site TEXT NOT NULL DEFAULT '',
        channel_type TEXT NOT NULL DEFAULT '',
        start_date TEXT NOT NULL DEFAULT '',
        bidding_strategy_type TEXT NOT NULL DEFAULT '',
        target_cpa REAL NOT NULL DEFAULT 0,
        campaign_target_cpa REAL NOT NULL DEFAULT 0,
        ad_group_target_cpa REAL NOT NULL DEFAULT 0,
        avg_cpc REAL NOT NULL DEFAULT 0,
        cost REAL NOT NULL DEFAULT 0,
        conversions REAL NOT NULL DEFAULT 0,
        daily_budget REAL NOT NULL DEFAULT 0,
        configured_conversion_actions_json TEXT NOT NULL DEFAULT '[]',
        observed_conversion_actions_json TEXT NOT NULL DEFAULT '[]',
        primary_conversion_action TEXT NOT NULL DEFAULT '',
        landing_url TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (day, customer_id, campaign_id, ad_group_id)
    """
    copy_columns = [
        "day", "customer_id", "campaign_id", "campaign", "ad_group_id", "ad_group_name",
        "attr_id", "site", "channel_type", "start_date", "bidding_strategy_type",
        "target_cpa", "campaign_target_cpa", "ad_group_target_cpa", "avg_cpc", "cost",
        "conversions", "daily_budget", "configured_conversion_actions_json",
        "observed_conversion_actions_json", "primary_conversion_action", "landing_url", "updated_at",
    ]
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        info = conn.execute("PRAGMA table_info(google_ads_campaign_settings_daily)").fetchall()
        pk = [row[1] for row in sorted([row for row in info if row[5]], key=lambda item: item[5])]
        if pk != expected_pk:
            backup = "google_ads_campaign_settings_daily_legacy_" + datetime.now(APP_TZ).strftime("%Y%m%d%H%M%S")
            conn.execute(f"ALTER TABLE google_ads_campaign_settings_daily RENAME TO {backup}")
            conn.execute(f"CREATE TABLE google_ads_campaign_settings_daily ({columns_sql})")
            old_cols = {row[1] for row in conn.execute(f"PRAGMA table_info({backup})").fetchall()}
            select_expr = []
            for col in copy_columns:
                if col in old_cols:
                    select_expr.append(col)
                elif col in {"target_cpa", "campaign_target_cpa", "ad_group_target_cpa", "avg_cpc", "cost", "conversions", "daily_budget"}:
                    select_expr.append(f"0 AS {col}")
                elif col in {"configured_conversion_actions_json", "observed_conversion_actions_json"}:
                    select_expr.append(f"'[]' AS {col}")
                else:
                    select_expr.append(f"'' AS {col}")
            conn.execute(
                f"""
                INSERT OR REPLACE INTO google_ads_campaign_settings_daily ({",".join(copy_columns)})
                SELECT {",".join(select_expr)}
                FROM {backup}
                """
            )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(google_ads_campaign_settings_daily)").fetchall()}
        if "landing_url" not in columns:
            conn.execute("ALTER TABLE google_ads_campaign_settings_daily ADD COLUMN landing_url TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_google_ads_settings_operational ON google_ads_campaign_settings_daily(day, customer_id, campaign_id, ad_group_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_google_ads_settings_attr ON google_ads_campaign_settings_daily(attr_id, day)")
        conn.commit()


def verify_google_ads_script_token(runtime: dict[str, Any], token: str) -> None:
    expected = str(((runtime.get("config") or {}).get("google_ads_script") or {}).get("token") or "").strip()
    if not expected or str(token or "").strip() != expected:
        raise ValueError("token invalido")


def _google_ads_campaign_ui_url(runtime: dict[str, Any], customer_id: Any, campaign_id: Any) -> str:
    campaign = re.sub(r"\D+", "", str(campaign_id or ""))
    if not campaign:
        return ""
    customer = re.sub(r"\D+", "", str(customer_id or ""))
    ui_links = ((runtime.get("config") or {}).get("google_ads") or {}).get("ui_links") or {}
    params = ui_links.get(customer) or {}
    if params:
        query = [
            f"campaignId={campaign}",
            f"ocid={params.get('ocid')}",
            f"workspaceId={params.get('workspaceId')}",
            f"euid={params.get('euid')}",
            f"__u={params.get('__u')}",
            f"uscid={params.get('uscid') or params.get('ocid')}",
            f"__c={params.get('__c')}",
            "authuser=0",
        ]
        return "https://ads.google.com/aw/campaigns?" + "&".join(part for part in query if not part.endswith("=None") and not part.endswith("="))
    if customer:
        return "https://ads.google.com/aw/campaigns?" + urlencode(
            {"campaignId": campaign, "customerId": customer, "authuser": "0"}
        )
    return "https://ads.google.com/aw/campaigns?" + urlencode(
        {"campaignId": campaign, "authuser": "0"}
    )


def _fallback_campaign_landing_url(project: Any, campaign: Any = "") -> str:
    domain = str(project or "").strip().lower()
    if not domain or "/" in domain or not (domain.endswith(".foconadica.com") or domain.endswith(".wizseoservicesaustralia.com")):
        return ""
    campaign_text = str(campaign or "").lower()
    if domain == "uk.foconadica.com":
        suffix = "" if "url 2" in campaign_text else "-b"
        return f"https://uk.foconadica.com/unclaimed-money-uk-government-holding-your-cash{suffix}/"
    if domain == "us.foconadica.com":
        suffix = "" if "url 2" in campaign_text or "p1b" in campaign_text else "-b"
        return f"https://us.foconadica.com/unclaimed-money-america-government-holding-your-cash{suffix}/"
    if domain == "ca.foconadica.com":
        return "https://ca.foconadica.com/unclaimed-money-canada-government-holding-your-cash"
    return ""


def _script_customer_site(runtime: dict[str, Any], customer_id: str, account: str) -> str:
    customer_id = re.sub(r"\D+", "", str(customer_id or ""))
    for item in ((runtime.get("config") or {}).get("google_ads") or {}).get("customers") or []:
        if re.sub(r"\D+", "", str(item.get("customer_id") or "")) == customer_id:
            return str(item.get("site") or item.get("name") or account or "").strip()
    return str(account or "").strip()


def _script_row_site(runtime: dict[str, Any], row: dict[str, Any]) -> str:
    explicit = str(row.get("site") or "").strip()
    if explicit:
        return explicit
    mapped = find_site(str(row.get("campaign") or ""), runtime.get("site_mapping") or {})
    if mapped != "SEM_MAPEAMENTO":
        return mapped
    return _script_customer_site(runtime, str(row.get("customer_id") or ""), str(row.get("account") or ""))


def ensure_google_ads_conversion_goals_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS google_ads_conversion_goals_daily (
                day TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign TEXT NOT NULL DEFAULT '',
                goal_config_level TEXT NOT NULL DEFAULT '',
                custom_conversion_goal_resource TEXT NOT NULL DEFAULT '',
                conversion_goal_name TEXT NOT NULL DEFAULT '',
                conversion_actions_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (day, customer_id, campaign_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_google_ads_conversion_goals_campaign ON google_ads_conversion_goals_daily(campaign_id, day)")
        conn.commit()


def sync_google_ads_conversion_goals(runtime: dict[str, Any], customer_ids: list[str] | None = None) -> dict[str, Any]:
    ensure_google_ads_conversion_goals_store(runtime["db_path"])
    client = _google_ads_client_from_runtime(runtime)
    if client is None:
        return {"ok": False, "rows": 0, "message": "google_ads_client_unavailable"}
    ads_cfg = (runtime.get("config") or {}).get("google_ads") or {}
    configured = []
    for item in ads_cfg.get("customers") or []:
        cid = re.sub(r"\D+", "", str(item.get("customer_id") or ""))
        if cid:
            configured.append(cid)
    wanted = [re.sub(r"\D+", "", str(cid or "")) for cid in (customer_ids or configured)]
    wanted = [cid for cid in dict.fromkeys(wanted) if cid]
    if not wanted:
        return {"ok": True, "rows": 0, "message": "no_customers"}
    query = """
        SELECT
          campaign.id,
          campaign.name,
          conversion_goal_campaign_config.goal_config_level,
          conversion_goal_campaign_config.custom_conversion_goal,
          custom_conversion_goal.name,
          custom_conversion_goal.conversion_actions
        FROM conversion_goal_campaign_config
        WHERE campaign.status != REMOVED
    """
    service = client.get_service("GoogleAdsService")
    day = app_today().isoformat()
    updated_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    rows: list[tuple[Any, ...]] = []
    errors: list[str] = []
    for customer_id in wanted:
        try:
            stream = service.search_stream(customer_id=customer_id, query=query)
            for batch in stream:
                for item in batch.results:
                    campaign = item.campaign
                    cfg = item.conversion_goal_campaign_config
                    custom_goal = item.custom_conversion_goal
                    goal_name = str(getattr(custom_goal, "name", "") or "").strip()
                    if not goal_name:
                        continue
                    actions = [str(action) for action in list(getattr(custom_goal, "conversion_actions", []) or [])]
                    rows.append((
                        day,
                        customer_id,
                        str(campaign.id),
                        str(campaign.name or ""),
                        str(cfg.goal_config_level.name if hasattr(cfg.goal_config_level, "name") else cfg.goal_config_level),
                        str(cfg.custom_conversion_goal or ""),
                        goal_name,
                        json.dumps(actions, ensure_ascii=False),
                        updated_at,
                    ))
        except Exception as exc:
            errors.append(f"{customer_id}: {exc}")
    if rows:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.execute("PRAGMA busy_timeout=20000")
            conn.executemany("""
                INSERT OR REPLACE INTO google_ads_conversion_goals_daily
                (day, customer_id, campaign_id, campaign, goal_config_level, custom_conversion_goal_resource, conversion_goal_name, conversion_actions_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
    return {"ok": not errors, "rows": len(rows), "errors": errors}


def _exact_conversion_goals_by_campaign(runtime: dict[str, Any], current_day: date) -> dict[str, dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    try:
        ensure_google_ads_conversion_goals_store(db_path)
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT *
                FROM google_ads_conversion_goals_daily
                WHERE day <= ? AND conversion_goal_name != ''
                ORDER BY day DESC, updated_at DESC
            """, (current_day.isoformat(),)).fetchall()
    except Exception:
        return {}
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        data = dict(row)
        campaign_id = str(data.get("campaign_id") or "")
        if campaign_id and campaign_id not in output:
            output[campaign_id] = data
    return output


def ensure_google_ads_campaign_budgets_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS google_ads_campaign_budgets_daily (
                day TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign TEXT NOT NULL DEFAULT '',
                daily_budget REAL NOT NULL DEFAULT 0,
                channel_type TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (day, customer_id, campaign_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_google_ads_campaign_budgets_campaign ON google_ads_campaign_budgets_daily(campaign_id, day)")
        conn.commit()


def sync_google_ads_campaign_budgets(runtime: dict[str, Any], customer_ids: list[str] | None = None) -> dict[str, Any]:
    ensure_google_ads_campaign_budgets_store(runtime["db_path"])
    client = _google_ads_client_from_runtime(runtime)
    if client is None:
        return {"ok": False, "rows": 0, "message": "google_ads_client_unavailable"}
    ads_cfg = (runtime.get("config") or {}).get("google_ads") or {}
    configured = []
    for item in ads_cfg.get("customers") or []:
        cid = re.sub(r"\D+", "", str(item.get("customer_id") or ""))
        if cid:
            configured.append(cid)
    wanted = [re.sub(r"\D+", "", str(cid or "")) for cid in (customer_ids or configured)]
    wanted = [cid for cid in dict.fromkeys(wanted) if cid]
    if not wanted:
        return {"ok": True, "rows": 0, "message": "no_customers"}
    query = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign_budget.amount_micros
        FROM campaign
        WHERE campaign.status != REMOVED
    """
    service = client.get_service("GoogleAdsService")
    day = app_today().isoformat()
    updated_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    rows: list[tuple[Any, ...]] = []
    errors: list[str] = []
    for customer_id in wanted:
        try:
            stream = service.search_stream(customer_id=customer_id, query=query)
            for batch in stream:
                for item in batch.results:
                    amount = _to_float(getattr(item.campaign_budget, "amount_micros", 0)) / 1000000.0
                    if amount <= 0:
                        continue
                    channel = item.campaign.advertising_channel_type
                    rows.append((
                        day,
                        customer_id,
                        str(item.campaign.id),
                        str(item.campaign.name or ""),
                        amount,
                        str(channel.name if hasattr(channel, "name") else channel),
                        updated_at,
                    ))
        except Exception as exc:
            errors.append(f"{customer_id}: {exc}")
    if rows:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.execute("PRAGMA busy_timeout=20000")
            conn.executemany("""
                INSERT OR REPLACE INTO google_ads_campaign_budgets_daily
                (day, customer_id, campaign_id, campaign, daily_budget, channel_type, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
    return {"ok": not errors, "rows": len(rows), "errors": errors}


def _script_float(value: Any) -> float:
    return _to_float(value)


def _is_runtime_excluded_project(runtime: dict[str, Any], *values: Any) -> bool:
    excluded = [str(value or "").strip().lower() for value in (runtime.get("excluded_projects") or []) if str(value or "").strip()]
    if not excluded:
        return False
    excluded_compact = {re.sub(r"[^a-z0-9]+", "", value) for value in excluded}
    for value in values:
        raw = str(value or "").strip().lower()
        if not raw:
            continue
        compact = re.sub(r"[^a-z0-9]+", "", raw)
        for item in excluded:
            if raw == item or item in raw or raw in item:
                return True
        if compact and any(item and (compact == item or item in compact or compact in item) for item in excluded_compact):
            return True
    return False


def ingest_google_ads_script_spend(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    rows = []
    skipped_excluded = 0
    for raw in list((payload or {}).get("rows") or []):
        row = dict(raw or {})
        site = _script_row_site(runtime, row)
        if _is_runtime_excluded_project(runtime, site, row.get("account"), row.get("campaign"), row.get("ad_group_name")):
            skipped_excluded += 1
            continue
        rows.append({"day": str(row.get("day") or app_today().isoformat()), "account": str(row.get("account") or ""), "customer_id": re.sub(r"\D+", "", str(row.get("customer_id") or "")), "campaign_id": re.sub(r"\D+", "", str(row.get("campaign_id") or "")), "campaign": str(row.get("campaign") or ""), "ad_group_id": re.sub(r"\D+", "", str(row.get("ad_group_id") or "")), "ad_group_name": str(row.get("ad_group_name") or ""), "channel_type": str(row.get("channel_type") or ""), "attr_level": str(row.get("attr_level") or ("ad_group" if row.get("ad_group_id") else "campaign")), "attr_id": str(row.get("attr_id") or row.get("ad_group_id") or row.get("campaign_id") or ""), "site": site, "cost": _script_float(row.get("cost")), "clicks": int(_script_float(row.get("clicks"))), "impressions": int(_script_float(row.get("impressions"))), "conversions": _script_float(row.get("conversions")), "daily_budget": _script_float(row.get("daily_budget")), "campaign_status": str(row.get("campaign_status") or ""), "ad_group_status": str(row.get("ad_group_status") or "")})
    if rows:
        days = sorted({str(row.get("day") or "") for row in rows if str(row.get("day") or "")})
        customers = sorted({str(row.get("customer_id") or "") for row in rows if str(row.get("customer_id") or "")})
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.execute("PRAGMA busy_timeout=20000")
            for day in days:
                for customer_id in customers:
                    conn.execute("DELETE FROM ads_daily WHERE day=? AND customer_id=?", (day, customer_id))
                    conn.execute("DELETE FROM ads_hourly WHERE day=? AND customer_id=?", (day, customer_id))
            conn.commit()
    upsert_ads_rows(runtime["db_path"], rows)
    verified = verify_google_ads_sent_actions(runtime)
    return {"ok": True, "rows": len(rows), "skipped_excluded": skipped_excluded, "verified_actions": verified}


def ingest_google_ads_script_placements(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    rows = []
    skipped_excluded = 0
    for raw in list((payload or {}).get("rows") or []):
        row = dict(raw or {})
        site = _script_row_site(runtime, row)
        if _is_runtime_excluded_project(runtime, site, row.get("account"), row.get("campaign"), row.get("ad_group_name")):
            skipped_excluded += 1
            continue
        rows.append({"day": str(row.get("day") or app_today().isoformat()), "account": str(row.get("account") or ""), "customer_id": re.sub(r"\D+", "", str(row.get("customer_id") or "")), "campaign_id": re.sub(r"\D+", "", str(row.get("campaign_id") or "")), "campaign": str(row.get("campaign") or ""), "ad_group_id": re.sub(r"\D+", "", str(row.get("ad_group_id") or "")), "ad_group_name": str(row.get("ad_group_name") or ""), "channel_type": str(row.get("channel_type") or ""), "attr_id": str(row.get("attr_id") or row.get("campaign_id") or ""), "placement": str(row.get("placement") or ""), "placement_display_name": str(row.get("placement_display_name") or row.get("placement") or ""), "placement_type": str(row.get("placement_type") or ""), "target_url": str(row.get("target_url") or ""), "site": site, "cost": _script_float(row.get("cost")), "clicks": int(_script_float(row.get("clicks"))), "impressions": int(_script_float(row.get("impressions"))), "conversions": _script_float(row.get("conversions")), "campaign_status": str(row.get("campaign_status") or ""), "ad_group_status": str(row.get("ad_group_status") or "")})
    upsert_ads_placement_rows(runtime["db_path"], rows)
    return {"ok": True, "rows": len(rows), "skipped_excluded": skipped_excluded}


def ingest_google_ads_script_settings(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    ensure_google_ads_settings_operational_store(runtime["db_path"])
    updated_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    rows = []
    skipped_excluded = 0
    for raw in list((payload or {}).get("rows") or []):
        row = dict(raw or {})
        customer_id = re.sub(r"\D+", "", str(row.get("customer_id") or ""))
        campaign_id = re.sub(r"\D+", "", str(row.get("campaign_id") or ""))
        ad_group_id = re.sub(r"\D+", "", str(row.get("ad_group_id") or ""))
        campaign_target = _script_float(row.get("campaign_target_cpa") if row.get("campaign_target_cpa") is not None else row.get("target_cpa"))
        ad_group_target = _script_float(row.get("ad_group_target_cpa"))
        target_cpa = ad_group_target if ad_group_target > 0 else campaign_target
        primary_conversion_action = str(row.get("conversion_goal_name") or row.get("custom_conversion_goal_name") or row.get("primary_conversion_action") or "").strip()
        configured_actions = row.get("configured_conversion_actions") or ([] if not primary_conversion_action else [primary_conversion_action])
        site = _script_row_site(runtime, row)
        if _is_runtime_excluded_project(runtime, site, row.get("account"), row.get("campaign"), row.get("ad_group_name")):
            skipped_excluded += 1
            continue
        landing_url = str(row.get("landing_url") or row.get("final_url") or row.get("article_url") or "").strip()
        rows.append((str(row.get("day") or app_today().isoformat()), customer_id, campaign_id, str(row.get("campaign") or ""), ad_group_id, str(row.get("ad_group_name") or ""), str(row.get("attr_id") or ad_group_id or campaign_id), site, str(row.get("channel_type") or ""), str(row.get("start_date") or ""), str(row.get("bidding_strategy_type") or ""), target_cpa, campaign_target, ad_group_target, _script_float(row.get("avg_cpc")), _script_float(row.get("cost")), _script_float(row.get("conversions")), _script_float(row.get("daily_budget")), json.dumps(configured_actions, ensure_ascii=False), json.dumps(row.get("observed_conversion_actions") or [], ensure_ascii=False), primary_conversion_action, landing_url, updated_at))
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.executemany("INSERT OR REPLACE INTO google_ads_campaign_settings_daily (day, customer_id, campaign_id, campaign, ad_group_id, ad_group_name, attr_id, site, channel_type, start_date, bidding_strategy_type, target_cpa, campaign_target_cpa, ad_group_target_cpa, avg_cpc, cost, conversions, daily_budget, configured_conversion_actions_json, observed_conversion_actions_json, primary_conversion_action, landing_url, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    verified = verify_google_ads_sent_actions(runtime)
    return {"ok": True, "rows": len(rows), "skipped_excluded": skipped_excluded, "verified_actions": verified}


def google_ads_script_pending_actions(runtime: dict[str, Any], customer_id: str = "") -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    ensure_campaign_bid_manual_action_store(runtime["db_path"])
    ensure_campaign_budget_manual_action_store(runtime["db_path"])
    ensure_campaign_status_manual_action_store(runtime["db_path"])
    actions: list[dict[str, Any]] = []
    cutoff = str(((runtime.get("config") or {}).get("google_ads_script") or {}).get("enabled_from") or "").strip()
    customer_filter = re.sub(r"\D+", "", str(customer_id or ""))
    if not customer_filter:
        return {"actions": [], "message": "customer_id obrigatorio para evitar aplicar acao em conta errada"}
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT * FROM campaign_bid_manual_actions WHERE status='pending_apply' ORDER BY created_at LIMIT 100"):
            lookup = conn.execute("SELECT customer_id, campaign_id, ad_group_id FROM google_ads_campaign_settings_daily WHERE customer_id=? AND (campaign_id=? OR ad_group_id=? OR attr_id=?) ORDER BY day DESC LIMIT 1", (customer_filter, str(row["campaign_id"]), str(row["attr_id"]), str(row["attr_id"]))).fetchone()
            if not lookup:
                continue
            actions.append({"action_key": str(row["action_key"]), "type": "set_ad_group_cpa", "customer_id": str(lookup["customer_id"]), "campaign_id": str(row["campaign_id"]), "ad_group_id": str(lookup["ad_group_id"] if lookup["ad_group_id"] else ""), "old_value": _to_float(row["old_value"]), "new_value": _to_float(row["new_value"])})
        for row in conn.execute("SELECT * FROM campaign_budget_manual_actions WHERE status='pending_apply' AND customer_id=? ORDER BY created_at LIMIT 100", (customer_filter,)):
            actions.append({"action_key": str(row["action_key"]), "type": "set_campaign_budget", "customer_id": str(row["customer_id"]), "campaign_id": str(row["campaign_id"]), "old_value": _to_float(row["old_budget"]), "new_value": _to_float(row["new_budget"])})
        for row in conn.execute("SELECT * FROM campaign_status_manual_actions WHERE status='pending_apply' AND customer_id=? ORDER BY created_at LIMIT 100", (customer_filter,)):
            actions.append({"action_key": str(row["action_key"]), "type": "set_campaign_status", "customer_id": str(row["customer_id"]), "campaign_id": str(row["campaign_id"]), "old_value": 0, "new_value": 0, "new_status": str(row["new_status"])})
        for row in conn.execute("SELECT * FROM google_ads_placement_exclusion_actions WHERE status='pending_apply' AND customer_id=? ORDER BY created_at LIMIT 100", (customer_filter,)):
            actions.append({"action_key": str(row["action_key"]), "type": "exclude_campaign_placement", "customer_id": str(row["customer_id"]), "campaign_id": str(row["campaign_id"]), "placement": str(row["placement"]), "old_value": 0, "new_value": 0})
    return {"actions": actions}


def verify_google_ads_sent_actions(runtime: dict[str, Any]) -> dict[str, int]:
    ensure_google_ads_script_store(runtime["db_path"])
    ensure_google_ads_settings_operational_store(runtime["db_path"])
    now = datetime.now(APP_TZ).isoformat(timespec="seconds")
    checked = 0
    applied = 0
    errors = 0
    tolerance = 0.01
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        budget_rows = conn.execute(
            """
            SELECT *
            FROM campaign_budget_manual_actions
            WHERE status='sent'
            ORDER BY applied_at, created_at
            LIMIT 200
            """
        ).fetchall()
        for row in budget_rows:
            checked += 1
            real = conn.execute(
                """
                SELECT daily_budget, updated_at
                FROM google_ads_campaign_settings_daily
                WHERE customer_id=? AND campaign_id=? AND daily_budget > 0
                ORDER BY day DESC, updated_at DESC
                LIMIT 1
                """,
                (str(row["customer_id"]), str(row["campaign_id"])),
            ).fetchone()
            real_value = _to_float(real["daily_budget"] if real else 0)
            expected = _to_float(row["new_budget"])
            if real_value > 0 and abs(real_value - expected) <= tolerance:
                conn.execute("UPDATE campaign_budget_manual_actions SET status='applied', verified_at=?, message=? WHERE action_key=?", (now, f"Verificado no snapshot: orcamento real {real_value:.2f}", row["action_key"]))
                applied += 1
            elif real and str(real["updated_at"] or "") >= str(row["applied_at"] or ""):
                conn.execute("UPDATE campaign_budget_manual_actions SET status='error', verified_at=?, message=? WHERE action_key=?", (now, f"Acao enviada, mas snapshot retornou orcamento {real_value:.2f}; esperado {expected:.2f}", row["action_key"]))
                errors += 1
        bid_rows = conn.execute(
            """
            SELECT *
            FROM campaign_bid_manual_actions
            WHERE status='sent'
            ORDER BY applied_at, created_at
            LIMIT 200
            """
        ).fetchall()
        for row in bid_rows:
            checked += 1
            real = conn.execute(
                """
                SELECT target_cpa, campaign_target_cpa, ad_group_target_cpa, updated_at
                FROM google_ads_campaign_settings_daily
                WHERE campaign_id=? AND (attr_id=? OR ad_group_id=? OR campaign_id=?)
                ORDER BY day DESC, updated_at DESC
                LIMIT 1
                """,
                (str(row["campaign_id"]), str(row["attr_id"]), str(row["attr_id"]), str(row["campaign_id"])),
            ).fetchone()
            expected = _to_float(row["new_value"])
            values = []
            if real:
                values = [_to_float(real["target_cpa"]), _to_float(real["campaign_target_cpa"]), _to_float(real["ad_group_target_cpa"])]
            if any(value > 0 and abs(value - expected) <= tolerance for value in values):
                conn.execute("UPDATE campaign_bid_manual_actions SET status='applied', verified_at=?, message=? WHERE action_key=?", (now, f"Verificado no snapshot: CPA real {expected:.2f}", row["action_key"]))
                applied += 1
            elif real and str(real["updated_at"] or "") >= str(row["applied_at"] or ""):
                seen = max(values) if values else 0
                conn.execute("UPDATE campaign_bid_manual_actions SET status='error', verified_at=?, message=? WHERE action_key=?", (now, f"Acao enviada, mas snapshot retornou CPA {seen:.2f}; esperado {expected:.2f}", row["action_key"]))
                errors += 1
        conn.commit()
    return {"checked": checked, "applied": applied, "errors": errors}


def record_google_ads_script_result(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    action_key = str((payload or {}).get("action_key") or "").strip()
    if not action_key:
        raise ValueError("action_key ausente")
    status = str(payload.get("status") or "").strip() or "unknown"
    applied_at = str(payload.get("applied_at") or datetime.now(APP_TZ).isoformat(timespec="seconds"))
    message = str(payload.get("message") or "")
    action_type = str(payload.get("type") or "")
    old_value = _script_float(payload.get("old_value"))
    new_value = _script_float(payload.get("new_value"))
    received_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    success_status = status in {"success", "dry_run"}
    final_status = "sent" if success_status else "error"
    placement_status = "applied" if success_status else "error"
    verified_at = received_at if success_status else ""
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("INSERT OR REPLACE INTO google_ads_script_action_results (action_key, action_type, status, old_value, new_value, applied_at, message, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (action_key, action_type, status, old_value, new_value, applied_at, message, received_at))
        conn.execute("UPDATE campaign_bid_manual_actions SET status=?, applied_at=?, message=?, metadata_json=json_set(COALESCE(NULLIF(metadata_json,''),'{}'), '$.script_message', ?) WHERE action_key=?", (final_status, applied_at, message, message, action_key))
        conn.execute("UPDATE campaign_budget_manual_actions SET status=?, applied_at=?, message=? WHERE action_key=?", (final_status, applied_at, message, action_key))
        conn.execute("UPDATE campaign_status_manual_actions SET status=?, applied_at=?, message=? WHERE action_key=?", (placement_status, applied_at, message, action_key))
        conn.execute("UPDATE google_ads_placement_exclusion_actions SET status=?, applied_at=?, verified_at=?, message=? WHERE action_key=?", (placement_status, applied_at, verified_at, message, action_key))
        conn.commit()
    if final_status == "sent":
        verify_google_ads_sent_actions(runtime)
    return {"ok": True, "status": final_status}


def record_google_ads_script_run(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    if _is_runtime_excluded_project(runtime, (payload or {}).get("account"), (payload or {}).get("site")):
        return {"ok": True, "run_id": "", "skipped_excluded": 1}
    run_id = str((payload or {}).get("run_id") or hashlib.sha256(str(payload).encode()).hexdigest())
    with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("INSERT OR REPLACE INTO google_ads_script_runs (run_id, created_at, customer_id, account, status, spend_rows, settings_rows, placement_rows, actions_count, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (run_id, str(payload.get("created_at") or datetime.now(APP_TZ).isoformat(timespec="seconds")), re.sub(r"\D+", "", str(payload.get("customer_id") or "")), str(payload.get("account") or ""), str(payload.get("status") or ""), int(_script_float(payload.get("spend_rows"))), int(_script_float(payload.get("settings_rows"))), int(_script_float(payload.get("placement_rows"))), int(_script_float(payload.get("actions_count"))), str(payload.get("message") or "")))
        conn.commit()
    return {"ok": True, "run_id": run_id}


def load_operational_health(runtime: dict[str, Any]) -> dict[str, Any]:
    ensure_google_ads_script_store(runtime["db_path"])
    ensure_google_ads_settings_operational_store(runtime["db_path"])
    now = datetime.now(APP_TZ)
    alerts: list[dict[str, Any]] = []
    checks: dict[str, Any] = {}
    ads_cfg = (runtime.get("config") or {}).get("google_ads") or {}
    active_customers: dict[str, dict[str, Any]] = {}
    for item in ads_cfg.get("customers") or []:
        customer_id = re.sub(r"\D+", "", str(item.get("customer_id") or ""))
        if not customer_id:
            continue
        if str(item.get("status") or "").lower() == "suspended" or bool(item.get("ads_actions_blocked")):
            continue
        active_customers[customer_id] = dict(item)
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        run_rows = {str(row["customer_id"]): dict(row) for row in conn.execute("""
            SELECT customer_id, account, MAX(created_at) AS last_run,
                   SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
            FROM google_ads_script_runs
            WHERE customer_id != ''
            GROUP BY customer_id
            ORDER BY last_run DESC
        """).fetchall()}
        settings_rows = {str(row["customer_id"]): dict(row) for row in conn.execute("""
            SELECT customer_id, MAX(updated_at) AS last_settings_at, MAX(day) AS last_day, COUNT(*) AS rows_today
            FROM google_ads_campaign_settings_daily
            WHERE customer_id != '' AND day=?
            GROUP BY customer_id
        """, (app_today().isoformat(),)).fetchall()}
        ads_runs: list[dict[str, Any]] = []
        stale_ads = []
        for customer_id, item in active_customers.items():
            run_row = run_rows.get(customer_id) or {}
            settings_row = settings_rows.get(customer_id) or {}
            account = str(run_row.get("account") or item.get("site") or item.get("name") or customer_id)
            last_candidates = [str(run_row.get("last_run") or ""), str(settings_row.get("last_settings_at") or "")]
            last_seen = max([value for value in last_candidates if value] or [""])
            row = {
                "customer_id": customer_id,
                "account": account,
                "last_run": str(run_row.get("last_run") or ""),
                "last_settings_at": str(settings_row.get("last_settings_at") or ""),
                "last_seen": last_seen,
                "rows_today": int(settings_row.get("rows_today") or 0),
                "errors": int(run_row.get("errors") or 0),
            }
            try:
                age = int((now - datetime.fromisoformat(last_seen).astimezone(APP_TZ)).total_seconds() // 60)
            except Exception:
                age = 999999
            row["age_minutes"] = age
            ads_runs.append(row)
            if row["rows_today"] <= 0:
                stale_ads.append(row)
                alerts.append({"level": "error", "source": "google_ads", "message": f"Sem snapshot Google Ads de hoje em {account}"})
            elif age > 180:
                stale_ads.append(row)
                alerts.append({"level": "error", "source": "google_ads", "message": f"Ads desatualizado em {account}: {age} min"})
        checks["google_ads"] = {"ok": not stale_ads, "runs": ads_runs}
        pending = {}
        for table in ("campaign_bid_manual_actions", "campaign_budget_manual_actions", "google_ads_placement_exclusion_actions"):
            rows = conn.execute(f"SELECT status, COUNT(*) qty FROM {table} GROUP BY status").fetchall()
            pending[table] = {str(row["status"]): int(row["qty"]) for row in rows}
            old = conn.execute(f"SELECT COUNT(*) qty FROM {table} WHERE status IN ('pending_apply','sent','verified') AND created_at < ?", ((now - timedelta(hours=2)).isoformat(timespec="seconds"),)).fetchone()
            if old and int(old["qty"]) > 0:
                alerts.append({"level": "error", "source": table, "message": f"{int(old['qty'])} acao(oes) pendentes antigas"})
        checks["actions"] = pending
        for table, label in (("gam_attr_daily", "GAM attr"), ("gam_site_daily", "GAM site"), ("mgid_widget_attr_map", "MGID")):
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if not exists:
                checks[label] = {"ok": False, "message": "tabela ausente"}
                alerts.append({"level": "warn", "source": label, "message": "tabela ausente"})
                continue
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            day_col = "day" if "day" in cols else ""
            if not day_col:
                checks[label] = {"ok": True, "message": "sem coluna day"}
                continue
            row = conn.execute(f"SELECT MAX(day) last_day FROM {table}").fetchone()
            last_day = str(row["last_day"] or "") if row else ""
            ok = last_day >= app_today().isoformat()
            checks[label] = {"ok": ok, "last_day": last_day}
            if not ok:
                alerts.append({"level": "warn", "source": label, "message": f"ultimo dia {last_day or 'vazio'}"})
        active_ids = list(active_customers.keys())
        placeholders = ",".join("?" for _ in active_ids) or "''"
        suspicious = conn.execute(f"""
            SELECT s.site, s.customer_id, s.campaign_id, s.campaign, s.ad_group_id, s.cost,
                   MAX(COALESCE(s.daily_budget, 0), COALESCE(a.real_budget, 0)) AS daily_budget,
                   s.target_cpa, s.campaign_target_cpa, s.ad_group_target_cpa, s.bidding_strategy_type
            FROM google_ads_campaign_settings_daily s
            LEFT JOIN (
                SELECT day, customer_id, campaign_id, MAX(daily_budget) AS real_budget
                FROM ads_daily
                WHERE day=?
                GROUP BY day, customer_id, campaign_id
            ) a ON a.day=s.day AND a.customer_id=s.customer_id AND a.campaign_id=s.campaign_id
            WHERE s.day=? AND s.customer_id IN ({placeholders}) AND s.cost > 0
              AND (
                MAX(COALESCE(s.daily_budget, 0), COALESCE(a.real_budget, 0)) <= 0
                OR (
                  s.target_cpa <= 0
                  AND UPPER(COALESCE(s.bidding_strategy_type,'')) NOT IN ('MAXIMIZE_CONVERSIONS')
                )
              )
            LIMIT 50
        """, [app_today().isoformat(), app_today().isoformat(), *active_ids]).fetchall()
        if suspicious:
            alerts.append({"level": "warn", "source": "data_validation", "message": f"{len(suspicious)} campanha(s) ativa(s) com gasto e budget/CPA suspeito no snapshot"})
        checks["data_validation"] = {"ok": not suspicious, "suspicious_rows": [dict(row) for row in suspicious]}
    return {"ok": not any(item["level"] == "error" for item in alerts), "generated_at": now.isoformat(timespec="seconds"), "alerts": alerts, "checks": checks}


def load_runtime(config_path: str | Path) -> dict[str, Any]:
    config_file = Path(config_path)
    config = load_config(config_file)
    db_path = ensure_store(database_path(config, config_file))
    ensure_runtime_schema(db_path)
    ensure_attr_alias_store(db_path)
    ensure_gam_floor_manual_action_store(db_path)
    ensure_push_control_store(db_path)
    seed_default_telegram_channels(db_path, config)
    runtime = {
        "config": config,
        "config_path": config_file,
        "db_path": db_path,
        "site_mapping": config.get("site_mapping", {}),
        "dimension_overrides": dimension_override_map(config),
        "excluded_projects": {str(value).strip() for value in (config.get("excluded_projects") or []) if str(value).strip()},
        "_request_cache": {},
    }
    runtime["quality_os"] = ensure_quality_operating_system(config_file.parent)
    return runtime


def load_quality_os_dashboard(runtime: dict[str, Any]) -> dict[str, Any]:
    return load_quality_operating_system_status(Path(runtime["config_path"]).parent)


def ensure_runtime_schema(db_path: str | Path) -> None:
    ensure_google_ads_settings_operational_store(db_path)
    ensure_floor_bucket_store(db_path)
    ensure_page_intelligence_store(db_path)


def ensure_page_intelligence_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS page_intelligence (
                project TEXT NOT NULL DEFAULT '',
                url_path TEXT NOT NULL DEFAULT '',
                funnel_stage TEXT NOT NULL DEFAULT '',
                iab_content_taxonomy_version TEXT NOT NULL DEFAULT '',
                iab_content_id TEXT NOT NULL DEFAULT '',
                iab_content_label TEXT NOT NULL DEFAULT '',
                commercial_signal_type TEXT NOT NULL DEFAULT '',
                commercial_intent_score REAL NOT NULL DEFAULT 0,
                audience_intent_label TEXT NOT NULL DEFAULT '',
                recommended_next_page TEXT NOT NULL DEFAULT '',
                content_source TEXT NOT NULL DEFAULT 'manual',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (project, url_path)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_page_intelligence_stage ON page_intelligence(project, funnel_stage)")
        now = datetime.now(APP_TZ).isoformat(timespec="seconds")
        for item in PAGE_INTELLIGENCE_SEED:
            conn.execute(
                """
                INSERT INTO page_intelligence (
                    project, url_path, funnel_stage, iab_content_taxonomy_version,
                    iab_content_id, iab_content_label, commercial_signal_type,
                    commercial_intent_score, audience_intent_label, recommended_next_page,
                    content_source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual_seed', ?)
                ON CONFLICT(project, url_path) DO NOTHING
                """,
                (
                    str(item.get("project") or ""),
                    str(item.get("url_path") or ""),
                    str(item.get("funnel_stage") or ""),
                    str(item.get("iab_content_taxonomy_version") or ""),
                    str(item.get("iab_content_id") or ""),
                    str(item.get("iab_content_label") or ""),
                    str(item.get("commercial_signal_type") or ""),
                    _to_float(item.get("commercial_intent_score")),
                    str(item.get("audience_intent_label") or ""),
                    str(item.get("recommended_next_page") or ""),
                    now,
                ),
            )
        conn.commit()


def load_page_intelligence_map(runtime: dict[str, Any], projects: list[str] | set[str]) -> dict[str, dict[str, Any]]:
    ensure_page_intelligence_store(runtime["db_path"])
    project_values = sorted({str(project or "").strip() for project in projects if str(project or "").strip()})
    if not project_values:
        return {}
    placeholders = ",".join("?" for _ in project_values)
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT project, url_path, funnel_stage, iab_content_taxonomy_version,
                   iab_content_id, iab_content_label, commercial_signal_type,
                   commercial_intent_score, audience_intent_label, recommended_next_page,
                   content_source, updated_at
            FROM page_intelligence
            WHERE project IN ({placeholders})
            """,
            project_values,
        ).fetchall()
    return {str(row["url_path"] or ""): dict(row) for row in rows if str(row["url_path"] or "")}


def _runtime_cache(runtime: dict[str, Any]) -> dict[str, Any]:
    cache = runtime.get("_request_cache")
    if not isinstance(cache, dict):
        cache = {}
        runtime["_request_cache"] = cache
    return cache


def _runtime_cache_get(runtime: dict[str, Any], key: tuple[Any, ...], factory: Callable[[], Any]) -> Any:
    cache = _runtime_cache(runtime)
    if key not in cache:
        cache[key] = factory()
    return cache[key]


def _wordpress_post_ids_by_path(runtime: dict[str, Any], project: str) -> dict[str, str]:
    def fetch() -> dict[str, str]:
        result: dict[str, str] = {}
        for post_type in ("pages", "posts"):
            endpoint = (
                f"https://{project}/wp-json/wp/v2/{post_type}"
                "?per_page=100&status=publish&_fields=id,link"
            )
            try:
                request = urllib.request.Request(endpoint, headers={"User-Agent": "ROAS-Control/1.0"})
                with urllib.request.urlopen(request, timeout=8) as response:
                    rows = json.loads(response.read().decode("utf-8"))
            except Exception:
                continue
            for row in rows if isinstance(rows, list) else []:
                parsed = urlparse(str(row.get("link") or ""))
                if str(parsed.hostname or "").lower() != project.lower():
                    continue
                path = parsed.path or "/"
                result[path] = str(row.get("id") or "")
        return result

    return _runtime_cache_get(runtime, ("wordpress-post-ids", project), fetch)


def _google_ads_landing_url(runtime: dict[str, Any], project: str, campaign_id: str, attr_id: str) -> str:
    def fetch() -> str:
        try:
            from google.ads.googleads.client import GoogleAdsClient

            ads_cfg = (runtime.get("config") or {}).get("google_ads") or {}
            client_cfg = {
                "developer_token": ads_cfg["developer_token"],
                "client_id": ads_cfg["client_id"],
                "client_secret": ads_cfg["client_secret"],
                "refresh_token": ads_cfg["refresh_token"],
                "use_proto_plus": True,
            }
            if ads_cfg.get("login_customer_id"):
                client_cfg["login_customer_id"] = re.sub(r"\D+", "", str(ads_cfg["login_customer_id"]))
            client = GoogleAdsClient.load_from_dict(client_cfg, version=ads_cfg.get("api_version", "v23"))
            service = client.get_service("GoogleAdsService")
            customers = [
                re.sub(r"\D+", "", str(item.get("customer_id") or ""))
                for item in ads_cfg.get("customers") or []
                if str(item.get("site") or "").strip().lower() == project.lower()
            ]
            campaign_value = re.sub(r"\D+", "", campaign_id)
            attr_value = re.sub(r"\D+", "", attr_id)
            where = [f"campaign.id={campaign_value}", "ad_group_ad.status != REMOVED"]
            if attr_value and attr_value != campaign_value:
                where.append(f"ad_group.id={attr_value}")
            query = "SELECT ad_group_ad.ad.final_urls FROM ad_group_ad WHERE " + " AND ".join(where)
            urls: set[str] = set()
            for customer_id in customers:
                for batch in service.search_stream(customer_id=customer_id, query=query):
                    for row in batch.results:
                        urls.update(str(url).strip() for url in row.ad_group_ad.ad.final_urls if str(url).strip())
            return next(iter(urls)) if len(urls) == 1 else ""
        except Exception:
            return ""

    return _runtime_cache_get(runtime, ("google-ads-landing-url", project, campaign_id, attr_id), fetch)


def ensure_attr_alias_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attr_aliases (
                site TEXT NOT NULL,
                source_attr_id TEXT NOT NULL,
                target_attr_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (site, source_attr_id, target_attr_id)
            )
            """
        )


def ensure_push_control_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_message_library (
                message_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                site TEXT NOT NULL,
                source_system TEXT NOT NULL DEFAULT 'gam',
                utm_campaign TEXT NOT NULL,
                utm_source TEXT NOT NULL DEFAULT '',
                utm_medium TEXT NOT NULL DEFAULT '',
                message_family TEXT NOT NULL DEFAULT '',
                message_number TEXT NOT NULL DEFAULT '',
                schedule_hint TEXT NOT NULL DEFAULT '',
                send_time TEXT NOT NULL DEFAULT '',
                acquisition_link_status TEXT NOT NULL DEFAULT 'unlinked',
                status TEXT NOT NULL DEFAULT 'observed',
                first_seen_day TEXT NOT NULL DEFAULT '',
                last_seen_day TEXT NOT NULL DEFAULT '',
                first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                total_revenue REAL NOT NULL DEFAULT 0,
                total_impressions REAL NOT NULL DEFAULT 0,
                total_clicks REAL NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
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
            CREATE TABLE IF NOT EXISTS push_acquisition_links (
                link_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                acquisition_platform TEXT NOT NULL DEFAULT '',
                acquisition_campaign_id TEXT NOT NULL DEFAULT '',
                acquisition_adgroup_id TEXT NOT NULL DEFAULT '',
                acquisition_attr_id TEXT NOT NULL DEFAULT '',
                link_method TEXT NOT NULL DEFAULT 'project_day_window',
                confidence REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'candidate',
                first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                metadata_json TEXT NOT NULL DEFAULT '{}'
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_content_actions (
                action_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                source_message_id TEXT NOT NULL DEFAULT '',
                source_utm_campaign TEXT NOT NULL DEFAULT '',
                action_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'proposed',
                reason TEXT NOT NULL DEFAULT '',
                score REAL NOT NULL DEFAULT 0,
                ecpm REAL NOT NULL DEFAULT 0,
                revenue REAL NOT NULL DEFAULT 0,
                impressions REAL NOT NULL DEFAULT 0,
                clicks REAL NOT NULL DEFAULT 0,
                title_test TEXT NOT NULL DEFAULT '',
                description_test TEXT NOT NULL DEFAULT '',
                icon_test TEXT NOT NULL DEFAULT '',
                target_send_slot TEXT NOT NULL DEFAULT '',
                target_utm_campaign TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_send_queue (
                send_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                source_action_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                scheduled_at TEXT NOT NULL DEFAULT '',
                aiwebpush_campaign_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                body TEXT NOT NULL DEFAULT '',
                icon TEXT NOT NULL DEFAULT '',
                image TEXT NOT NULL DEFAULT '',
                link TEXT NOT NULL DEFAULT '',
                topics_json TEXT NOT NULL DEFAULT '[]',
                utm_source TEXT NOT NULL DEFAULT '',
                utm_medium TEXT NOT NULL DEFAULT 'push',
                utm_campaign TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sent_at TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_send_logs (
                log_id TEXT PRIMARY KEY,
                send_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                status TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'aiwebpush',
                provider_response_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_site_installation_checks (
                project_id TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                sw_url TEXT NOT NULL,
                http_status INTEGER NOT NULL DEFAULT 0,
                is_installed INTEGER NOT NULL DEFAULT 0,
                result TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (project_id, checked_at)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nexus_agent_learnings (
                learning_key TEXT PRIMARY KEY,
                agent_slug TEXT NOT NULL,
                scope TEXT NOT NULL,
                rule TEXT NOT NULL,
                learned_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        existing = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(push_send_queue)").fetchall()
        }
        for column, ddl in {
            "message_id": "ALTER TABLE push_send_queue ADD COLUMN message_id TEXT NOT NULL DEFAULT ''",
            "updated_at": "ALTER TABLE push_send_queue ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        }.items():
            if column not in existing:
                conn.execute(ddl)


def _row_payload(row: Any) -> dict[str, Any]:
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if hasattr(row, "__dict__"):
        return dict(row.__dict__)
    return {
        key: getattr(row, key)
        for key in dir(row)
        if not key.startswith("_") and not callable(getattr(row, key, None))
    }


def ensure_initial_setup_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS initial_setups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                email TEXT DEFAULT '',
                currency TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'rascunho',
                plan_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_initial_setups_domain ON initial_setups(domain)")


def save_initial_setup_plan(runtime: dict[str, Any], payload: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    ensure_initial_setup_store(runtime["db_path"])
    now_text = datetime.now(APP_TZ).isoformat(timespec="seconds")
    domain = str(plan.get("domain") or payload.get("domain") or "").strip()
    customer_id = str((plan.get("google_ads") or {}).get("customer_id") or payload.get("customer_id") or "").strip()
    email = str(plan.get("email") or payload.get("email") or "").strip()
    currency = str(payload.get("currency") or "").strip().upper()
    safe_payload = dict(payload)
    if "wordpress_password" in safe_payload:
        safe_payload["wordpress_password"] = "***received***" if str(safe_payload.get("wordpress_password") or "").strip() else ""
    if "clarity_api_token" in safe_payload:
        safe_payload["clarity_api_token"] = "***received***" if str(safe_payload.get("clarity_api_token") or "").strip() else ""
    with sqlite3.connect(runtime["db_path"]) as conn:
        cursor = conn.execute(
            """
            INSERT INTO initial_setups(domain, customer_id, email, currency, status, plan_json, payload_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                domain,
                customer_id,
                email,
                currency,
                "rascunho",
                json.dumps(plan, ensure_ascii=False),
                json.dumps(safe_payload, ensure_ascii=False),
                now_text,
                now_text,
            ),
        )
        setup_id = int(cursor.lastrowid)
    return {"setup_id": setup_id, "status": "rascunho", "created_at": now_text}


def load_initial_setup_plans(runtime: dict[str, Any], limit: int = 20) -> list[dict[str, Any]]:
    ensure_initial_setup_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, domain, customer_id, email, currency, status, created_at, updated_at
            FROM initial_setups
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def load_initial_setup_plan(runtime: dict[str, Any], setup_id: int) -> dict[str, Any] | None:
    ensure_initial_setup_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM initial_setups WHERE id = ?", (setup_id,)).fetchone()
    return dict(row) if row else None


def load_initial_setup_detail(runtime: dict[str, Any], setup_id: int) -> dict[str, Any] | None:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return None
    try:
        row["plan"] = json.loads(row.get("plan_json") or "{}")
    except Exception:
        row["plan"] = {}
    try:
        row["payload"] = json.loads(row.get("payload_json") or "{}")
    except Exception:
        row["payload"] = {}
    clarity = row.get("plan", {}).get("clarity") if isinstance(row.get("plan"), dict) else None
    if isinstance(clarity, dict) and str(clarity.get("api_token") or "").strip():
        clarity["api_token"] = "***received***"
    row.pop("plan_json", None)
    row.pop("payload_json", None)
    return row


def update_initial_setup_plan(runtime: dict[str, Any], setup_id: int, status: str, plan: dict[str, Any]) -> None:
    ensure_initial_setup_store(runtime["db_path"])
    now_text = datetime.now(APP_TZ).isoformat(timespec="seconds")
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.execute(
            """
            UPDATE initial_setups
            SET status = ?, plan_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, json.dumps(plan, ensure_ascii=False), now_text, setup_id),
        )


def validate_initial_setup_clarity_api(runtime: dict[str, Any], setup_id: int, token: str | None = None) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    clarity = plan.setdefault("clarity", {})
    api_token = str(token or clarity.get("api_token") or "").strip()
    if not api_token:
        return {"status": "error", "error": "Informe o token da Clarity Data Export API."}
    url = "https://www.clarity.ms/export-data/api/v1/project-live-insights?" + urlencode({"numOfDays": "1", "dimension1": "URL"})
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + api_token, "Content-Type": "application/json"})
    checked_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        result = {"status": "error", "checked_at": checked_at, "http_status": exc.code, "message": raw[:500]}
        clarity["api_validation"] = result
        if token:
            clarity["api_token"] = api_token
            clarity["api_token_received"] = True
        update_initial_setup_plan(runtime, setup_id, str(row.get("status") or "rascunho"), plan)
        return {"status": "error", "error": f"Clarity API HTTP {exc.code}", "validation": result}
    except Exception as exc:
        result = {"status": "error", "checked_at": checked_at, "message": str(exc)}
        clarity["api_validation"] = result
        if token:
            clarity["api_token"] = api_token
            clarity["api_token_received"] = True
        update_initial_setup_plan(runtime, setup_id, str(row.get("status") or "rascunho"), plan)
        return {"status": "error", "error": str(exc), "validation": result}
    row_count = sum(len(item.get("information") or []) for item in payload if isinstance(item, dict)) if isinstance(payload, list) else 0
    result = {"status": "ok", "checked_at": checked_at, "endpoint": "project-live-insights", "num_of_days": 1, "dimension1": "URL", "rows": row_count}
    clarity["api_token"] = api_token
    clarity["api_token_received"] = True
    clarity["api_validation"] = result
    update_initial_setup_plan(runtime, setup_id, str(row.get("status") or "rascunho"), plan)
    return {"status": "ok", "setup_id": setup_id, "validation": result}


def _google_ads_client_from_runtime(runtime: dict[str, Any]):
    from google.ads.googleads.client import GoogleAdsClient

    cfg = runtime["config"].get("google_ads", {})
    client_cfg = {
        "developer_token": cfg.get("developer_token"),
        "client_id": cfg.get("client_id"),
        "client_secret": cfg.get("client_secret"),
        "refresh_token": cfg.get("refresh_token"),
        "use_proto_plus": True,
    }
    if cfg.get("login_customer_id"):
        client_cfg["login_customer_id"] = re.sub(r"\D+", "", str(cfg["login_customer_id"]))
    return GoogleAdsClient.load_from_dict(client_cfg)


def _ads_customer_id(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _safe_search(service: Any, customer_id: str, query: str) -> tuple[list[Any], str | None]:
    try:
        return list(service.search(customer_id=customer_id, query=query)), None
    except Exception as exc:
        return [], str(exc)


def _conversion_label_from_snippets(snippets: Any) -> str:
    text = " ".join(str(snippet) for snippet in snippets)
    match = re.search(r"AW-\d+/([^'\"\\s]+)", text)
    if match:
        return match.group(1)
    match = re.search(r"label=([^'\"&\\s]+)", text)
    return match.group(1) if match else ""


def _conversion_tag_id_from_snippets(snippets: Any) -> str:
    text = " ".join(str(snippet) for snippet in snippets)
    match = re.search(r"AW-(\d+)", text)
    return match.group(1) if match else ""


def _find_conversion_action(client: Any, customer_id: str, name: str) -> dict[str, Any] | None:
    service = client.get_service("GoogleAdsService")
    safe_name = name.replace("'", "\\'")
    query = f"""
        SELECT conversion_action.id, conversion_action.name, conversion_action.tag_snippets
        FROM conversion_action
        WHERE conversion_action.name = '{safe_name}'
          AND conversion_action.status != REMOVED
        LIMIT 1
    """
    rows, error = _safe_search(service, customer_id, query)
    if error or not rows:
        return None
    action = rows[0].conversion_action
    return {
        "name": str(action.name or name),
        "id": str(action.id),
        "tag_id": _conversion_tag_id_from_snippets(action.tag_snippets),
        "label": _conversion_label_from_snippets(action.tag_snippets),
    }


def _create_or_get_conversion_action(client: Any, customer_id: str, item: dict[str, Any]) -> dict[str, Any]:
    name = str(item.get("name") or "").strip()
    if not name:
        raise ValueError("Nome da conversao ausente")
    existing = _find_conversion_action(client, customer_id, name)
    if existing:
        return {"status": "exists", **existing}

    service = client.get_service("ConversionActionService")
    operation = client.get_type("ConversionActionOperation")
    conversion = operation.create
    conversion.name = name
    conversion.type_ = client.enums.ConversionActionTypeEnum.WEBPAGE
    conversion.category = client.enums.ConversionActionCategoryEnum.PAGE_VIEW
    conversion.status = client.enums.ConversionActionStatusEnum.ENABLED
    conversion.include_in_conversions_metric = True
    conversion.counting_type = client.enums.ConversionActionCountingTypeEnum.EVERY
    conversion.value_settings.default_value = float(item.get("default_value") or 1)
    conversion.value_settings.default_currency_code = str(item.get("currency") or "BRL")
    conversion.value_settings.always_use_default_value = False
    conversion.click_through_lookback_window_days = int(item.get("click_window_days") or 1)
    conversion.view_through_lookback_window_days = int(item.get("engaged_view_window_days") or 1)
    try:
        conversion.attribution_model_settings.attribution_model = client.enums.AttributionModelEnum.LAST_CLICK
    except Exception:
        pass
    response = service.mutate_conversion_actions(customer_id=customer_id, operations=[operation])
    resource_name = response.results[0].resource_name
    created_id = resource_name.split("/")[-1]
    created = _find_conversion_action(client, customer_id, name) or {"name": name, "id": created_id, "label": ""}
    return {"status": "created", **created}


def _apply_customer_tracking_template(client: Any, customer_id: str, template: str) -> dict[str, Any]:
    from google.protobuf.field_mask_pb2 import FieldMask

    service = client.get_service("CustomerService")
    operation = client.get_type("CustomerOperation")
    customer = operation.update
    customer.resource_name = service.customer_path(customer_id)
    customer.tracking_url_template = template
    operation.update_mask.CopyFrom(FieldMask(paths=["tracking_url_template"]))
    service.mutate_customer(customer_id=customer_id, operation=operation)
    return {"status": "updated", "tracking_template": template}


def _google_oauth_credentials_from_runtime(runtime: dict[str, Any], scopes: list[str]):
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    cfg = runtime["config"].get("google_ads") or {}
    client_id = str(cfg.get("client_id") or "").strip()
    client_secret = str(cfg.get("client_secret") or "").strip()
    refresh_token = str(cfg.get("refresh_token") or "").strip()
    if not client_id or not client_secret or not refresh_token:
        raise ValueError("OAuth Google ausente no config.yaml")
    credentials = Credentials(token=None, refresh_token=refresh_token, token_uri="https://oauth2.googleapis.com/token", client_id=client_id, client_secret=client_secret, scopes=scopes)
    credentials.refresh(Request())
    return credentials


def _ga4_admin_request(runtime: dict[str, Any], method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    credentials = _google_oauth_credentials_from_runtime(runtime, ["https://www.googleapis.com/auth/analytics.edit"])
    token = credentials.token
    if not token:
        raise ValueError("Token OAuth GA4 ausente apos refresh")
    url = "https://analyticsadmin.googleapis.com/v1beta" + path
    body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=body, method=method.upper())
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1200]
        if exc.code in (401, 403):
            raise PermissionError("OAuth sem permissao GA4 Admin. Renove OAuth incluindo https://www.googleapis.com/auth/analytics.edit. " + detail)
        raise RuntimeError(f"GA4 Admin HTTP {exc.code}: {detail}")


def apply_initial_setup_ga4(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    domain = str(plan.get("domain") or row.get("domain") or "").strip()
    ga4 = plan.get("ga4") or {}
    existing_measurement = str(ga4.get("measurement_id") or "").strip()
    if existing_measurement.startswith("G-"):
        plan["ga4_apply"] = {"status": "exists", "applied_at": datetime.now(APP_TZ).isoformat(timespec="seconds"), "measurement_id": existing_measurement, "message": "Measurement ID ja informado no plano."}
        update_initial_setup_plan(runtime, setup_id, "ga4_aplicado", plan)
        return {"status": "ok", "setup_id": setup_id, "ga4": plan["ga4_apply"]}
    account_id = str(ga4.get("account") or "").strip()
    if not account_id:
        return {"status": "error", "error": "Conta GA4 mae ausente"}
    if not domain:
        return {"status": "error", "error": "Dominio ausente"}
    currency = str(row.get("currency") or (plan.get("google_ads", {}).get("conversions") or [{}])[0].get("currency") or "BRL").upper()
    created_property = _ga4_admin_request(runtime, "POST", "/properties", {"parent": f"accounts/{account_id}", "displayName": domain, "timeZone": "America/Sao_Paulo", "currencyCode": currency})
    property_name = str(created_property.get("name") or "")
    if not property_name:
        raise RuntimeError("GA4 criou resposta sem property name")
    stream = _ga4_admin_request(runtime, "POST", f"/{property_name}/dataStreams", {"type": "WEB_DATA_STREAM", "displayName": domain, "webStreamData": {"defaultUri": f"https://{domain}"}})
    measurement_id = str((stream.get("webStreamData") or {}).get("measurementId") or "").strip()
    ga4["property_resource"] = property_name
    ga4["property_id"] = property_name.split("/")[-1] if "/" in property_name else property_name
    ga4["stream_resource"] = str(stream.get("name") or "")
    ga4["measurement_id"] = measurement_id
    plan["ga4"] = ga4
    plan["ga4_apply"] = {"status": "success", "applied_at": datetime.now(APP_TZ).isoformat(timespec="seconds"), "property": created_property, "stream": stream, "measurement_id": measurement_id}
    update_initial_setup_plan(runtime, setup_id, "ga4_aplicado", plan)
    return {"status": "ok", "setup_id": setup_id, "ga4": plan["ga4_apply"]}


def apply_initial_setup_google_ads(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    google_ads = plan.get("google_ads") or {}
    customer_id = _ads_customer_id(google_ads.get("customer_id") or row.get("customer_id"))
    if not customer_id:
        return {"status": "error", "error": "Customer ID ausente"}

    client = _google_ads_client_from_runtime(runtime)
    conversions = []
    for item in google_ads.get("conversions") or []:
        conversions.append(_create_or_get_conversion_action(client, customer_id, item))
    tracking = _apply_customer_tracking_template(client, customer_id, str(google_ads.get("tracking_template") or ""))

    plan["google_ads_apply"] = {
        "status": "success",
        "applied_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "customer_id": customer_id,
        "conversions": conversions,
        "tracking": tracking,
    }
    update_initial_setup_plan(runtime, setup_id, "google_ads_aplicado", plan)
    return {"status": "ok", "setup_id": setup_id, "google_ads": plan["google_ads_apply"]}


def _gtm_account_id(runtime: dict[str, Any], plan: dict[str, Any]) -> str:
    plan_value = str((plan.get("gtm") or {}).get("account") or "").strip()
    if re.fullmatch(r"\d{6,}", plan_value):
        return plan_value
    account_path_match = re.fullmatch(r"accounts/(\d+)", plan_value)
    if account_path_match:
        return account_path_match.group(1)
    if plan_value:
        try:
            service = _GtmRestService(runtime)
            accounts = service.request("GET", "/accounts").get("account") or []
            normalized = plan_value.strip().lower()
            for account in accounts:
                account_id = str(account.get("accountId") or "").strip()
                account_name = str(account.get("name") or "").strip()
                account_path = str(account.get("path") or "").strip()
                if normalized in {account_name.lower(), account_id.lower(), account_path.lower()}:
                    return account_id
        except Exception:
            pass
    cfg = runtime["config"].get("gtm") or runtime["config"].get("google_tag_manager") or {}
    value = str(cfg.get("account_id") or os.getenv("GTM_ACCOUNT_ID") or "").strip()
    if value:
        return value
    if "LRD" in plan_value.upper() or "KAIZEN" in plan_value.upper():
        return "6342819886"
    return value


def _gtm_credentials_path(runtime: dict[str, Any]) -> Path:
    cfg = runtime["config"].get("gtm") or runtime["config"].get("google_tag_manager") or {}
    configured = str(cfg.get("credentials_file") or os.getenv("GTM_CREDENTIALS_FILE") or "credentials/gtm_service_account.json")
    path = Path(configured)
    if not path.is_absolute():
        path = Path(runtime.get("config_path") or ".").parent / path
    return path


class _RestExecute:
    def __init__(self, caller: Callable[[], dict[str, Any]]):
        self._caller = caller

    def execute(self) -> dict[str, Any]:
        return self._caller()


class _GtmRestService:
    base_url = "https://tagmanager.googleapis.com/tagmanager/v2"

    def __init__(self, runtime: dict[str, Any]):
        scopes = [
            "https://www.googleapis.com/auth/tagmanager.edit.containers",
            "https://www.googleapis.com/auth/tagmanager.manage.accounts",
            "https://www.googleapis.com/auth/tagmanager.publish",
            "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
        ]
        self._credentials = _google_oauth_credentials_from_runtime(runtime, scopes)

    def accounts(self) -> "_GtmAccountsResource":
        return _GtmAccountsResource(self)

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            method=method.upper(),
            headers={
                "Authorization": f"Bearer {self._credentials.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            if exc.code in (401, 403):
                raise PermissionError("OAuth sem permissao GTM. Renove OAuth incluindo escopos do Tag Manager.") from exc
            raise RuntimeError(f"GTM API HTTP {exc.code}: {raw[:1000]}") from exc


class _GtmAccountsResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def containers(self) -> "_GtmContainersResource":
        return _GtmContainersResource(self._service)


class _GtmContainersResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def list(self, parent: str) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("GET", f"/{parent}/containers"))

    def create(self, parent: str, body: dict[str, Any]) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{parent}/containers", body))

    def workspaces(self) -> "_GtmWorkspacesResource":
        return _GtmWorkspacesResource(self._service)

    def versions(self) -> "_GtmVersionsResource":
        return _GtmVersionsResource(self._service)


class _GtmWorkspacesResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def list(self, parent: str) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("GET", f"/{parent}/workspaces"))

    def create(self, parent: str, body: dict[str, Any]) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{parent}/workspaces", body))

    def triggers(self) -> "_GtmTriggersResource":
        return _GtmTriggersResource(self._service)

    def tags(self) -> "_GtmTagsResource":
        return _GtmTagsResource(self._service)

    def create_version(self, path: str, body: dict[str, Any]) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{path}:create_version", body))


class _GtmTriggersResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def list(self, parent: str) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("GET", f"/{parent}/triggers"))

    def create(self, parent: str, body: dict[str, Any]) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{parent}/triggers", body))


class _GtmTagsResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def list(self, parent: str) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("GET", f"/{parent}/tags"))

    def create(self, parent: str, body: dict[str, Any]) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{parent}/tags", body))


class _GtmVersionsResource:
    def __init__(self, service: _GtmRestService):
        self._service = service

    def publish(self, path: str) -> _RestExecute:
        return _RestExecute(lambda: self._service.request("POST", f"/{path}:publish"))


def _gtm_service(runtime: dict[str, Any]):
    scopes = [
        "https://www.googleapis.com/auth/tagmanager.edit.containers",
        "https://www.googleapis.com/auth/tagmanager.manage.accounts",
        "https://www.googleapis.com/auth/tagmanager.publish",
        "https://www.googleapis.com/auth/tagmanager.edit.containerversions",
    ]
    credentials_path = _gtm_credentials_path(runtime)
    if credentials_path.exists():
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            creds = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
            return build("tagmanager", "v2", credentials=creds)
        except ModuleNotFoundError:
            pass
    return _GtmRestService(runtime)


def _gtm_find_or_create_container(service: Any, account_id: str, domain: str) -> dict[str, Any]:
    account_path = f"accounts/{account_id}"
    containers = service.accounts().containers().list(parent=account_path).execute().get("container", [])
    for container in containers:
        names = [str(container.get("name") or "").lower()]
        names.extend(str(item or "").lower() for item in container.get("domainName", []))
        if domain.lower() in names:
            return {"status": "exists", **container}
    container = service.accounts().containers().create(
        parent=account_path,
        body={"name": domain, "usageContext": ["web"], "domainName": [domain]},
    ).execute()
    return {"status": "created", **container}


def _gtm_workspace(service: Any, container_path: str) -> str:
    workspaces = service.accounts().containers().workspaces().list(parent=container_path).execute().get("workspace", [])
    for workspace in workspaces:
        if workspace.get("name") == "Kaizen Setup":
            return workspace["path"]
    try:
        workspace = service.accounts().containers().workspaces().create(
            parent=container_path,
            body={"name": "Kaizen Setup", "description": "Setup inicial automatizado"},
        ).execute()
        return workspace["path"]
    except Exception:
        if workspaces:
            return workspaces[0]["path"]
        raise


def _gtm_upsert_trigger(service: Any, workspace: str, name: str, event_name: str) -> dict[str, Any]:
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().triggers().list(parent=workspace).execute().get("trigger", [])}
    if name in existing:
        return {"status": "exists", **existing[name]}
    trigger = service.accounts().containers().workspaces().triggers().create(
        parent=workspace,
        body={
            "name": name,
            "type": "CUSTOM_EVENT",
            "customEventFilter": [{
                "type": "EQUALS",
                "parameter": [
                    {"type": "template", "key": "arg0", "value": "{{_event}}"},
                    {"type": "template", "key": "arg1", "value": event_name},
                ],
            }],
        },
    ).execute()
    return {"status": "created", **trigger}


def _gtm_upsert_all_pages_trigger(service: Any, workspace: str) -> dict[str, Any]:
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().triggers().list(parent=workspace).execute().get("trigger", [])}
    if "Initialization - All Pages" in existing:
        return {"status": "exists", **existing["Initialization - All Pages"]}
    trigger = service.accounts().containers().workspaces().triggers().create(
        parent=workspace,
        body={"name": "Initialization - All Pages", "type": "INIT"},
    ).execute()
    return {"status": "created", **trigger}


def _setup_gtm_event_html() -> str:
    return """<script>
window.dataLayer=window.dataLayer||[];
window.googletag=window.googletag||{};googletag.cmd=googletag.cmd||[];
var kaizenAdSlots={};
function kaizenTx(){return window.crypto&&crypto.randomUUID?crypto.randomUUID():"tx_"+Date.now()+"_"+Math.random().toString(36).slice(2);}
function kaizenPush(eventName,data){data=data||{};data.event=eventName;data.transaction_id=kaizenTx();window.dataLayer.push(data);}
function kaizenSlotText(slot){var p=[];try{p.push(slot.getSlotElementId()||"")}catch(e){}try{p.push(slot.getAdUnitPath()||"")}catch(e){}try{p.push(String(slot.getSlotId()||""))}catch(e){}return p.join(" ").toLowerCase();}
function kaizenEvent(slot){var s=kaizenSlotText(slot);if(s.indexOf("interstitial")>-1)return"adViewInterstitial";if(s.indexOf("rewarded")>-1||s.indexOf("rewards")>-1)return"adViewRewarded";return"adView";}
googletag.cmd.push(function(){
  googletag.pubads().addEventListener("slotRenderEnded",function(e){var id=kaizenSlotText(e.slot);kaizenAdSlots[id]={adUnit:e.slot.getAdUnitPath(),slotId:id,size:e.size||null,isEmpty:e.isEmpty};});
  googletag.pubads().addEventListener("impressionViewable",function(e){var id=kaizenSlotText(e.slot);kaizenPush(kaizenEvent(e.slot),kaizenAdSlots[id]||{adUnit:e.slot.getAdUnitPath(),slotId:id});});
});
document.addEventListener("click",function(e){var el=e.target;while(el&&el!==document){if((" "+(el.className||"")+" ").indexOf(" wp-block-button__link ")>-1||(" "+(el.className||"")+" ").indexOf(" card-button ")>-1||(" "+(el.className||"")+" ").indexOf(" btn-ios ")>-1){kaizenPush("buttonClick",{page_path:location.pathname||"/",button_text:(el.textContent||"").replace(/\\s+/g," ").trim(),button_classes:el.className||""});break;}el=el.parentNode;}},true);
</script>"""


def _gtm_upsert_html_tag(service: Any, workspace: str, trigger_id: str) -> dict[str, Any]:
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().tags().list(parent=workspace).execute().get("tag", [])}
    name = "Evento GTM"
    if name in existing:
        return {"status": "exists", **existing[name]}
    tag = service.accounts().containers().workspaces().tags().create(
        parent=workspace,
        body={
            "name": name,
            "type": "html",
            "parameter": [
                {"type": "template", "key": "html", "value": _setup_gtm_event_html()},
                {"type": "boolean", "key": "supportDocumentWrite", "value": "false"},
            ],
            "firingTriggerId": [str(trigger_id)],
        },
    ).execute()
    return {"status": "created", **tag}


def _gtm_upsert_clarity_tag(service: Any, workspace: str, trigger_id: str, project_id: str) -> dict[str, Any] | None:
    project_id = str(project_id or "").strip()
    if not project_id:
        return None
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().tags().list(parent=workspace).execute().get("tag", [])}
    name = "Microsoft Clarity"
    if name in existing:
        return {"status": "exists", **existing[name]}
    html_code = (
        '<script type="text/javascript">\n'
        '(function(c,l,a,r,i,t,y){\n'
        ' c[a]=c[a]||function(){(c[a].q=c[a].q||[]).push(arguments)};\n'
        ' t=l.createElement(r);t.async=1;t.src="https://www.clarity.ms/tag/"+i;\n'
        ' y=l.getElementsByTagName(r)[0];y.parentNode.insertBefore(t,y);\n'
        '})(window, document, "clarity", "script", "' + project_id + '");\n'
        '</script>'
    )
    tag = service.accounts().containers().workspaces().tags().create(
        parent=workspace,
        body={
            "name": name,
            "type": "html",
            "parameter": [
                {"type": "template", "key": "html", "value": html_code},
                {"type": "boolean", "key": "supportDocumentWrite", "value": "false"},
            ],
            "firingTriggerId": [str(trigger_id)],
        },
    ).execute()
    return {"status": "created", **tag}


def _gtm_upsert_ads_tag(service: Any, workspace: str, event_name: str, trigger_id: str, conversion: dict[str, Any], currency: str) -> dict[str, Any]:
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().tags().list(parent=workspace).execute().get("tag", [])}
    name = f"Google Ads {event_name}"
    if name in existing:
        return {"status": "exists", **existing[name]}
    conversion_id = str(conversion.get("tag_id") or "")
    conversion_label = str(conversion.get("label") or "")
    if not conversion_id or not conversion_label:
        raise ValueError(f"Conversion ID/label ausente para {event_name}. Aplique Google Ads primeiro.")
    tag = service.accounts().containers().workspaces().tags().create(
        parent=workspace,
        body={
            "name": name,
            "type": "awct",
            "parameter": [
                {"type": "template", "key": "conversionId", "value": conversion_id},
                {"type": "template", "key": "conversionLabel", "value": conversion_label},
                {"type": "template", "key": "currencyCode", "value": currency},
                {"type": "boolean", "key": "enableRemarketing", "value": "true"},
                {"type": "boolean", "key": "enableDynamicRemarketing", "value": "false"},
            ],
            "firingTriggerId": [str(trigger_id)],
        },
    ).execute()
    return {"status": "created", **tag}


def _gtm_upsert_ga4_tag(service: Any, workspace: str, event_name: str, trigger_id: str, measurement_id: str) -> dict[str, Any] | None:
    if not measurement_id:
        return None
    existing = {item["name"]: item for item in service.accounts().containers().workspaces().tags().list(parent=workspace).execute().get("tag", [])}
    name = f"GA4 {event_name}"
    if name in existing:
        return {"status": "exists", **existing[name]}
    tag = service.accounts().containers().workspaces().tags().create(
        parent=workspace,
        body={
            "name": name,
            "type": "gaawe",
            "parameter": [
                {"type": "template", "key": "eventName", "value": event_name},
                {"type": "template", "key": "measurementIdOverride", "value": measurement_id},
            ],
            "firingTriggerId": [str(trigger_id)],
        },
    ).execute()
    return {"status": "created", **tag}


def _gtm_snippets(container_id: str) -> dict[str, str]:
    head = f"""<!-- Google Tag Manager -->
<script>(function(w,d,s,l,i){{w[l]=w[l]||[];w[l].push({{'gtm.start':
new Date().getTime(),event:'gtm.js'}});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
}})(window,document,'script','dataLayer','{container_id}');</script>
<!-- End Google Tag Manager -->"""
    body = f"""<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id={container_id}"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->"""
    return {"head": head, "body": body}


def _gtm_publish_workspace(service: Any, workspace: str, domain: str) -> dict[str, Any]:
    version = service.accounts().containers().workspaces().create_version(
        path=workspace,
        body={
            "name": f"Kaizen Setup - {domain}",
            "notes": "Setup inicial Google Ads + GA4 gerado pelo ROAS Control Center",
        },
    ).execute()
    version_path = version.get("containerVersion", {}).get("path")
    if not version_path:
        return {"status": "version_created_without_path", "response": version}
    publish = service.accounts().containers().versions().publish(path=version_path).execute()
    return {"status": "published", "version_path": version_path, "response": publish}


def apply_initial_setup_gtm(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    domain = str(plan.get("domain") or row.get("domain") or "").strip()
    if not domain:
        return {"status": "error", "error": "Dominio ausente"}
    google_ads_apply = plan.get("google_ads_apply") or {}
    if google_ads_apply.get("status") != "success":
        return {"status": "error", "error": "Aplique Google Ads antes de criar o GTM."}
    account_id = _gtm_account_id(runtime, plan)
    if not account_id:
        return {"status": "error", "error": "GTM account ID ausente"}

    service = _gtm_service(runtime)
    try:
        container = _gtm_find_or_create_container(service, account_id, domain)
    except Exception as exc:
        plan["gtm_apply"] = {
            "status": "permission_required",
            "applied_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
            "account_id": account_id,
            "container_id": "",
            "message": "Nao foi possivel criar container nesta conta GTM. Confirme permissao de Publicar/Editar/Criar container ou escolha outra conta GTM.",
            "permission_error": str(exc),
        }
        update_initial_setup_plan(runtime, setup_id, "gtm_permissao_pendente", plan)
        return {"status": "error", "setup_id": setup_id, "error": plan["gtm_apply"]["message"], "gtm": plan["gtm_apply"]}
    snippets = _gtm_snippets(str(container.get("publicId") or ""))
    currency = str(row.get("currency") or plan.get("google_ads", {}).get("conversions", [{}])[0].get("currency") or "BRL")
    triggers: list[dict[str, Any]] = []
    ads_tags: list[dict[str, Any]] = []
    ga4_tags: list[dict[str, Any]] = []
    clarity_tag: dict[str, Any] | None = None
    event_tag: dict[str, Any] = {}
    publish: dict[str, Any] = {}
    workspace = ""
    status = "success"
    permission_error = ""
    try:
        workspace = _gtm_workspace(service, container["path"])
        init_trigger = _gtm_upsert_all_pages_trigger(service, workspace)
        event_tag = _gtm_upsert_html_tag(service, workspace, str(init_trigger["triggerId"]))
        clarity_project_id = str((plan.get("clarity") or {}).get("project_id") or "").strip()
        clarity_tag = _gtm_upsert_clarity_tag(service, workspace, str(init_trigger["triggerId"]), clarity_project_id)
        conversions_by_name = {str(item.get("name")): item for item in google_ads_apply.get("conversions") or []}
        measurement_id = str((plan.get("ga4") or {}).get("measurement_id") or "")
        for event_name in ["adView", "adViewInterstitial", "adViewRewarded"]:
            trigger = _gtm_upsert_trigger(service, workspace, event_name, event_name)
            triggers.append({"name": event_name, "triggerId": trigger.get("triggerId"), "status": trigger.get("status")})
            ads_tag = _gtm_upsert_ads_tag(service, workspace, event_name, str(trigger["triggerId"]), conversions_by_name.get(event_name, {}), currency)
            ads_tags.append({"name": event_name, "tagId": ads_tag.get("tagId"), "status": ads_tag.get("status")})
            ga4_tag = _gtm_upsert_ga4_tag(service, workspace, event_name, str(trigger["triggerId"]), measurement_id)
            if ga4_tag:
                ga4_tags.append({"name": event_name, "tagId": ga4_tag.get("tagId"), "status": ga4_tag.get("status")})
        publish = _gtm_publish_workspace(service, workspace, domain)
    except Exception as exc:
        status = "container_ready_tags_pending"
        permission_error = str(exc)
    plan["gtm_apply"] = {
        "status": status,
        "applied_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "account_id": account_id,
        "container_id": container.get("publicId"),
        "container_numeric_id": container.get("containerId"),
        "container_status": container.get("status"),
        "workspace": workspace,
        "event_tag": {"tagId": event_tag.get("tagId"), "status": event_tag.get("status")},
        "triggers": triggers,
        "google_ads_tags": ads_tags,
        "ga4_tags": ga4_tags,
        "clarity_tag": ({"tagId": clarity_tag.get("tagId"), "status": clarity_tag.get("status")} if clarity_tag else {"status": "manual", "message": "Informe o Clarity Project ID para criar a tag automaticamente."}),
        "publish": publish,
        "snippets": snippets,
        "permission_error": permission_error,
    }
    update_initial_setup_plan(runtime, setup_id, "gtm_aplicado" if status == "success" else "gtm_codigo_gerado", plan)
    return {"status": "ok", "setup_id": setup_id, "gtm": plan["gtm_apply"]}


def validate_initial_setup_wordpress(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    domain = str(plan.get("domain") or row.get("domain") or "").strip()
    container_id = str((plan.get("gtm_apply") or {}).get("container_id") or "").strip()
    if not domain:
        return {"status": "error", "error": "Dominio ausente"}
    if not container_id:
        return {"status": "error", "error": "Container GTM ausente. Crie o GTM primeiro."}

    url = f"https://{domain}/"
    checks = {
        "container_id": False,
        "gtm_js": False,
        "gtm_noscript": False,
    }
    error = ""
    html_text = ""
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "ROAS-Control-Center/1.0"})
        with urllib.request.urlopen(request, timeout=20) as response:
            html_text = response.read().decode("utf-8", "ignore")
        checks["container_id"] = container_id in html_text
        checks["gtm_js"] = "googletagmanager.com/gtm.js" in html_text and container_id in html_text
        checks["gtm_noscript"] = "googletagmanager.com/ns.html" in html_text and container_id in html_text
    except Exception as exc:
        error = str(exc)

    installed = bool(checks["container_id"] and checks["gtm_js"])
    plan["wordpress_validation"] = {
        "status": "installed" if installed else "not_found",
        "checked_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "url": url,
        "container_id": container_id,
        "checks": checks,
        "error": error,
    }
    update_initial_setup_plan(runtime, setup_id, "wordpress_validado" if installed else str(row.get("status") or "gtm_aplicado"), plan)
    return {"status": "ok", "setup_id": setup_id, "wordpress": plan["wordpress_validation"]}


def _wp_absolute_url(admin_url: str, path: str) -> str:
    parsed = urlparse(admin_url if admin_url.startswith(("http://", "https://")) else f"https://{admin_url}")
    base = f"{parsed.scheme}://{parsed.netloc}"
    return base + path


def _wp_request(opener: urllib.request.OpenerDirector, url: str, data: dict[str, Any] | None = None) -> tuple[int, str]:
    body = None
    headers = {"User-Agent": "ROAS-Control-Center/1.0"}
    if data is not None:
        body = urlencode(data).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(url, data=body, headers=headers)
    with opener.open(request, timeout=30) as response:
        return int(response.getcode() or 0), response.read().decode("utf-8", "ignore")


def _wp_hidden_inputs(html_text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in re.finditer(r"<input\b[^>]*>", html_text, re.I):
        tag = match.group(0)
        type_match = re.search(r"\btype=['\"]?([^'\"\s>]+)", tag, re.I)
        if type_match and type_match.group(1).lower() not in {"hidden", "submit"}:
            continue
        name_match = re.search(r"\bname=['\"]([^'\"]+)['\"]", tag, re.I)
        if not name_match:
            continue
        value_match = re.search(r"\bvalue=['\"]([^'\"]*)['\"]", tag, re.I)
        values[name_match.group(1)] = html.unescape(value_match.group(1) if value_match else "")
    return values


def _wp_textarea_names(html_text: str) -> list[str]:
    names: list[str] = []
    for match in re.finditer(r"<textarea\b[^>]*\bname=['\"]([^'\"]+)['\"]", html_text, re.I):
        names.append(html.unescape(match.group(1)))
    return names


def _wpcode_payload(html_text: str, head: str, body: str) -> dict[str, str]:
    payload = _wp_hidden_inputs(html_text)
    textarea_names = _wp_textarea_names(html_text)
    for name in textarea_names:
        lowered = name.lower()
        if "header" in lowered or "head" in lowered:
            payload[name] = head
        elif "body" in lowered:
            payload[name] = body
        elif "footer" in lowered:
            payload[name] = ""
    for name in ("ihaf_insert_header", "wpcode_header", "wpcode_headers", "header"):
        payload.setdefault(name, head)
    for name in ("ihaf_insert_body", "wpcode_body", "body"):
        payload.setdefault(name, body)
    for name in ("ihaf_insert_footer", "wpcode_footer", "wpcode_footers", "footer"):
        payload.setdefault(name, "")
    payload.setdefault("submit", "Save Changes")
    return payload


def install_initial_setup_wordpress(runtime: dict[str, Any], setup_id: int, credentials: dict[str, Any]) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    snippets = ((plan.get("gtm_apply") or {}).get("snippets") or {})
    head = str(snippets.get("head") or "").strip()
    body = str(snippets.get("body") or "").strip()
    if not head:
        return {"status": "error", "error": "Crie o GTM primeiro para gerar o codigo Header."}

    wp = plan.get("wordpress") or {}
    admin_url = str(wp.get("admin_url") or credentials.get("wordpress_url") or "").strip()
    username = str(wp.get("user") or credentials.get("wordpress_user") or "").strip()
    password = str(credentials.get("wordpress_password") or "").strip()
    if not admin_url or not username or not password:
        return {"status": "error", "error": "Informe URL, usuario e senha do WordPress para instalar."}

    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    login_url = _wp_absolute_url(admin_url, "/wp-login.php")
    redirect_url = _wp_absolute_url(admin_url, "/wp-admin/")
    _wp_request(opener, login_url, {
        "log": username,
        "pwd": password,
        "wp-submit": "Log In",
        "redirect_to": redirect_url,
        "testcookie": "1",
    })

    attempts: list[dict[str, Any]] = []
    pages = [
        "/wp-admin/admin.php?page=wpcode-headers-footers",
        "/wp-admin/admin.php?page=insert-headers-and-footers",
    ]
    for page in pages:
        page_url = _wp_absolute_url(admin_url, page)
        try:
            code, form_html = _wp_request(opener, page_url)
            if code >= 400 or ("wp-login.php" in form_html and "loginform" in form_html):
                attempts.append({"page": page, "status": "login_failed_or_no_access"})
                continue
            payload = _wpcode_payload(form_html, head, body)
            post_code, post_html = _wp_request(opener, page_url, payload)
            attempts.append({"page": page, "status": "posted", "http_code": post_code})
            if post_code < 400 and ("Settings saved" in post_html or "updated" in post_html or "wpcode" in post_html.lower()):
                break
        except Exception as exc:
            attempts.append({"page": page, "status": "error", "error": str(exc)})

    validation = validate_initial_setup_wordpress(runtime, setup_id)
    plan = json.loads((load_initial_setup_plan(runtime, setup_id) or {}).get("plan_json") or "{}")
    plan["wordpress_install"] = {
        "status": "installed" if validation.get("wordpress", {}).get("status") == "installed" else "not_confirmed",
        "installed_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "admin_url": admin_url,
        "user": username,
        "attempts": attempts,
    }
    update_initial_setup_plan(
        runtime,
        setup_id,
        "wordpress_instalado" if plan["wordpress_install"]["status"] == "installed" else str(row.get("status") or "gtm_aplicado"),
        plan,
    )
    return {"status": "ok", "setup_id": setup_id, "wordpress": plan["wordpress_install"], "validation": validation.get("wordpress", {})}


def final_check_initial_setup(runtime: dict[str, Any], setup_id: int) -> dict[str, Any]:
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    ads = plan.get("google_ads_apply") or {}
    gtm = plan.get("gtm_apply") or {}
    wpv = plan.get("wordpress_validation") or {}
    ga4 = plan.get("ga4") or {}
    clarity = plan.get("clarity") or {}
    confirmations = plan.get("manual_confirmations") or {}
    gtm_status = str(gtm.get("status") or "")
    gtm_has_code = bool(gtm.get("container_id") and (gtm.get("snippets") or {}).get("head"))
    wp_installed = wpv.get("status") == "installed"
    gtm_auto_valid = gtm_status == "success" and gtm_has_code and wp_installed
    clarity_project_id = str(clarity.get("project_id") or "").strip()
    clarity_tag_status = str(((gtm.get("steps") or {}).get("clarity_tag") or {}).get("status") or "")
    clarity_api_status = str((clarity.get("api_validation") or {}).get("status") or "")

    checks = [
        {
            "item": "Google Ads conversoes e tracking template",
            "status": "ok" if ads.get("status") == "success" else "pending",
            "detail": "Aplicado pela API" if ads.get("status") == "success" else "Clique em Aplicar Google Ads.",
        },
        {
            "item": "GTM container e codigo",
            "status": "ok" if gtm_has_code else "pending",
            "detail": str(gtm.get("container_id") or "Clique em Criar GTM."),
        },
        {
            "item": "Tags do GTM publicadas",
            "status": "ok" if gtm_status == "success" else ("manual" if gtm_status == "container_ready_tags_pending" else "pending"),
            "detail": "Publicadas pela API" if gtm_status == "success" else ("Permissao do container pendente para criar/publicar tags." if gtm_status == "container_ready_tags_pending" else "Clique em Criar GTM."),
        },
        {
            "item": "WPCode instalado no site",
            "status": "ok" if wpv.get("status") == "installed" else "pending",
            "detail": "GTM encontrado no HTML publico" if wpv.get("status") == "installed" else "Clique em Instalar WPCode e Validar WP.",
        },
        {
            "item": "GA4 Measurement ID",
            "status": "ok" if str(ga4.get("measurement_id") or "").strip().startswith("G-") or confirmations.get("ga4") else "manual",
            "detail": str(ga4.get("measurement_id") or "Criar propriedade GA4 e preencher G-XXXXXXXXXX."),
        },
        {
            "item": "Clarity Project ID",
            "status": "ok" if clarity_project_id or confirmations.get("clarity") else "waiting_input",
            "detail": clarity_project_id or "Informe o Project ID; ao aplicar tudo o sistema instala a tag automaticamente no GTM.",
        },
        {
            "item": "Clarity Data Export API",
            "status": "ok" if clarity_api_status == "ok" else "waiting_input",
            "detail": "Token validado na API do Clarity" if clarity_api_status == "ok" else "Informe o token Data Export API do projeto para validar acesso e leitura de dados.",
        },
        {
            "item": "Validacao GTM automatizada",
            "status": "ok" if gtm_auto_valid else "pending",
            "detail": "Container publicado e detectado no HTML publico" if gtm_auto_valid else "Aguardando GTM publicado e WPCode validado no HTML publico.",
        },
    ]
    blockers = [item for item in checks if item["status"] == "pending"]
    manual = [item for item in checks if item["status"] == "manual"]
    final_status = "blocked" if blockers else ("released_for_traffic" if not manual else "ready_for_manual_validation")
    plan["final_check"] = {
        "status": final_status,
        "checked_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
        "checks": checks,
        "blockers": blockers,
        "manual_required": manual,
    }
    status_name = "liberado_para_trafego" if final_status == "released_for_traffic" else ("pronto_para_validacao" if final_status != "blocked" else str(row.get("status") or "rascunho"))
    update_initial_setup_plan(runtime, setup_id, status_name, plan)
    return {"status": "ok", "setup_id": setup_id, "final_check": plan["final_check"]}


def confirm_initial_setup_step(runtime: dict[str, Any], setup_id: int, step: str) -> dict[str, Any]:
    allowed = {
        "ga4": "GA4 Measurement ID",
        "clarity": "Clarity Project ID",
    }
    if step not in allowed:
        return {"status": "error", "error": "Confirmacao invalida"}
    row = load_initial_setup_plan(runtime, setup_id)
    if not row:
        return {"status": "error", "error": "Setup nao encontrado"}
    plan = json.loads(row.get("plan_json") or "{}")
    confirmations = plan.setdefault("manual_confirmations", {})
    confirmations[step] = {
        "label": allowed[step],
        "status": "confirmed",
        "confirmed_at": datetime.now(APP_TZ).isoformat(timespec="seconds"),
    }
    update_initial_setup_plan(runtime, setup_id, str(row.get("status") or "rascunho"), plan)
    return final_check_initial_setup(runtime, setup_id)

def _load_attr_aliases(runtime: dict[str, Any]) -> dict[tuple[str, str], str]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    ensure_attr_alias_store(db_path)
    aliases: dict[tuple[str, str], str] = {}
    with sqlite3.connect(db_path, timeout=30) as conn:
        for site, source_attr_id, target_attr_id in conn.execute(
            """
            SELECT site, source_attr_id, target_attr_id
            FROM attr_aliases
            WHERE source_attr_id <> '' AND target_attr_id <> ''
            """
        ):
            aliases[(str(site or "").strip(), str(source_attr_id or "").strip())] = str(target_attr_id or "").strip()
    return aliases


def _ads_rows_with_demand_gen_ad_groups(runtime: dict[str, Any], ads_rows: list[Any], start_date: str, end_date: str) -> list[Any]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return ads_rows
    query = (
        "select day, customer_id, campaign_id, campaign, ad_group_id, ad_group_name, site, channel_type, cost, conversions, daily_budget "
        "from google_ads_campaign_settings_daily "
        "where day between ? and ? and ad_group_id != '' and upper(channel_type) = 'DEMAND_GEN'"
    )
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        try:
            settings_rows = conn.execute(query, (start_date, end_date)).fetchall()
        except sqlite3.OperationalError:
            return ads_rows
    if not settings_rows:
        return ads_rows
    campaign_meta = {}
    campaigns_with_ad_group_ads: set[tuple[str, str, str]] = set()
    for row in ads_rows:
        key = (str(getattr(row, "day", "") or ""), str(getattr(row, "customer_id", "") or ""), str(getattr(row, "campaign_id", "") or ""))
        campaign_meta[key] = row
        if str(getattr(row, "ad_group_id", "") or "").strip():
            campaigns_with_ad_group_ads.add(key)
    demand_keys = {(str(row["day"] or ""), str(row["customer_id"] or ""), str(row["campaign_id"] or "")) for row in settings_rows}
    synthesize_keys = demand_keys - campaigns_with_ad_group_ads
    if not synthesize_keys:
        return ads_rows
    merged = [row for row in ads_rows if (str(getattr(row, "day", "") or ""), str(getattr(row, "customer_id", "") or ""), str(getattr(row, "campaign_id", "") or "")) not in synthesize_keys]
    for row in settings_rows:
        key = (str(row["day"] or ""), str(row["customer_id"] or ""), str(row["campaign_id"] or ""))
        if key not in synthesize_keys:
            continue
        meta = campaign_meta.get(key)
        ad_group_id = str(row["ad_group_id"] or "").strip()
        if not ad_group_id:
            continue
        merged.append(SimpleNamespace(
            day=str(row["day"] or ""),
            hour=-1,
            account=str(getattr(meta, "account", "") or ""),
            customer_id=str(row["customer_id"] or ""),
            campaign_id=str(row["campaign_id"] or ""),
            campaign=str(row["campaign"] or ""),
            ad_group_id=ad_group_id,
            ad_group_name=str(row["ad_group_name"] or ""),
            channel_type=str(row["channel_type"] or "DEMAND_GEN"),
            attr_level="ad_group",
            attr_id=ad_group_id,
            site=str(row["site"] or getattr(meta, "site", "") or ""),
            cost=_to_float(row["cost"]),
            clicks=0,
            impressions=0,
            conversions=_to_float(row["conversions"]),
            daily_budget=_to_float(row["daily_budget"]) or _to_float(getattr(meta, "daily_budget", 0.0)),
            campaign_status=str(getattr(meta, "campaign_status", "") or ""),
            ad_group_status="ENABLED",
        ))
    return merged

def _ad_group_campaign_map(runtime: dict[str, Any], start_date: str, end_date: str) -> dict[tuple[str, str], str]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    mapping: dict[tuple[str, str], str] = {}
    query = (
        "select day, ad_group_id, campaign_id "
        "from google_ads_campaign_settings_daily "
        "where day between ? and ? and ad_group_id != '' and campaign_id != ''"
    )
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(query, (start_date, end_date)).fetchall()
        except sqlite3.OperationalError:
            return {}
    for row in rows:
        day = str(row["day"] or "").strip()
        ad_group_id = str(row["ad_group_id"] or "").strip()
        campaign_id = str(row["campaign_id"] or "").strip()
        if day and ad_group_id and campaign_id:
            mapping[(day, ad_group_id)] = campaign_id
            # Some landing tags send utm_campaign as campaign_id + ad_group_id
            # without a separator, for example score_google_23862682697193889592862.
            # Map that composite key back to the campaign so GAM revenue is not orphaned.
            mapping[(day, f"{campaign_id}{ad_group_id}")] = campaign_id
            mapping[(day, f"{campaign_id}_{ad_group_id}")] = campaign_id
    return mapping


def _expand_gam_attr_candidates_with_google_ads_settings(runtime: dict[str, Any], gam_attr_rows: list[Any], start_date: str, end_date: str) -> list[Any]:
    ad_group_to_campaign = _ad_group_campaign_map(runtime, start_date, end_date)
    if not ad_group_to_campaign:
        return gam_attr_rows
    expanded: list[Any] = []
    for row in gam_attr_rows:
        day = str(getattr(row, "day", "") or "").strip()
        payload = _row_payload(row)
        candidates = [str(item).strip() for item in payload.get("attr_candidates", ()) or () if str(item).strip()]
        attr_id = str(payload.get("attr_id", "") or "").strip()
        if attr_id:
            candidates.insert(0, attr_id)
        changed = False
        for candidate in list(candidates):
            campaign_id = ad_group_to_campaign.get((day, candidate))
            if campaign_id and campaign_id not in candidates:
                candidates.insert(0, campaign_id)
                changed = True
            if campaign_id and candidate.startswith(campaign_id):
                suffix = candidate[len(campaign_id):].lstrip("_-")
                if suffix and suffix not in candidates:
                    candidates.append(suffix)
                    changed = True
        if not changed:
            expanded.append(row)
            continue
        candidates = list(dict.fromkeys(candidates))
        payload["attr_candidates"] = tuple(candidates)
        if not str(payload.get("attr_id", "") or "").strip() and candidates:
            payload["attr_id"] = candidates[0]
        expanded.append(SimpleNamespace(**payload))
    return expanded

def _apply_attr_aliases(runtime: dict[str, Any], gam_attr_rows: list[Any]) -> list[Any]:
    aliases = _load_attr_aliases(runtime)
    if not aliases:
        return gam_attr_rows
    normalized: list[Any] = []
    for row in gam_attr_rows:
        site = str(getattr(row, "site", "") or "").strip()
        source_attr_id = str(getattr(row, "attr_id", "") or "").strip()
        target_attr_id = aliases.get((site, source_attr_id)) or aliases.get(("", source_attr_id))
        if not target_attr_id:
            normalized.append(row)
            continue
        payload = _row_payload(row)
        candidates = [str(item).strip() for item in payload.get("attr_candidates", ()) or () if str(item).strip()]
        candidates = list(dict.fromkeys([target_attr_id, source_attr_id, *candidates]))
        payload["attr_id"] = target_attr_id
        payload["attr_candidates"] = tuple(candidates)
        payload["attr_alias_source"] = source_attr_id
        normalized.append(SimpleNamespace(**payload))
    return normalized


def load_last_updated(runtime: dict[str, Any]) -> str:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return ""
    return datetime.fromtimestamp(db_path.stat().st_mtime, tz=timezone.utc).astimezone(APP_TZ).strftime("%d/%m/%Y %H:%M:%S")


def _allowed_projects(runtime: dict[str, Any], today: date) -> set[str]:
    excluded_projects = {str(value).strip().lower() for value in (runtime.get("excluded_projects") or []) if str(value).strip()}

    def allowed(project: Any) -> bool:
        value = str(project or "").strip()
        return bool(value) and value.lower() not in excluded_projects

    start_date, end_date = recent_30_day_range(today)
    ads_rows = ads_rows_from_store(
        read_ads_rows_operational(runtime["db_path"], start_date, end_date, operational_day=today.isoformat())
    )
    fact_rows = build_fact_rows(ads_rows, [], runtime["site_mapping"], runtime["dimension_overrides"])
    projects = {row.site for row in fact_rows if (row.cost > 0 or row.revenue > 0) and allowed(row.site)}
    projects.update(str(site).strip() for site in (runtime.get("site_mapping") or {}).values() if allowed(site))
    for row in read_table(runtime["db_path"], "sites"):
        project_id = str(row.get("project_id", "") or "").strip()
        if allowed(project_id):
            projects.add(project_id)
    return projects


def _project_family_projects(runtime: dict[str, Any], project: str) -> set[str]:
    site_to_project, project_to_sites = _site_project_maps(runtime)
    family = {project}
    family.update(project_to_sites.get(project, set()))
    family.update(site_to_project.get(site, "") for site in project_to_sites.get(project, set()))
    return {item for item in family if str(item or "").strip()}


def _load_period_rows(
    runtime: dict[str, Any],
    start_date: str,
    end_date: str,
    operational: bool = False,
) -> tuple[list[Any], list[Any], list[Any]]:
    return _runtime_cache_get(
        runtime,
        ("period_rows", start_date, end_date, operational),
        lambda: _load_period_rows_uncached(runtime, start_date, end_date, operational),
    )


def _load_period_rows_uncached(
    runtime: dict[str, Any],
    start_date: str,
    end_date: str,
    operational: bool = False,
) -> tuple[list[Any], list[Any], list[Any]]:
    if ((runtime.get("config") or {}).get("google_ads_script") or {}).get("enabled"):
        ads_rows = ads_rows_from_store(read_ads_rows(runtime["db_path"], start_date, end_date))
        ads_rows = _ads_rows_with_demand_gen_ad_groups(runtime, ads_rows, start_date, end_date)
    else:
        ads_reader = read_ads_rows_operational if operational else read_ads_rows_reporting
        ads_rows = ads_rows_from_store(ads_reader(runtime["db_path"], start_date, end_date, operational_day=end_date))
    gam_site_rows = gam_rows_from_store(read_gam_rows(runtime["db_path"], "gam_site_daily", start_date, end_date))
    gam_attr_rows = gam_rows_from_store(read_gam_rows(runtime["db_path"], "gam_attr_daily", start_date, end_date))
    gam_attr_rows = _expand_gam_attr_candidates_with_google_ads_settings(runtime, gam_attr_rows, start_date, end_date)
    gam_attr_rows = _apply_attr_aliases(runtime, gam_attr_rows)
    return ads_rows, gam_site_rows, gam_attr_rows


def _gam_row_exact_attr_id(row: Any) -> str:
    attr_id = str(getattr(row, "attr_id", "") or "").strip()
    if not attr_id:
        return ""
    attr_raw = str(getattr(row, "attr_raw", "") or "").strip()
    if not attr_raw:
        return ""
    if attr_raw == attr_id:
        return attr_id
    match = re.search(r"(?:^|[?&])utm_campaign=([^&#\s]+)(?:$|[?&#])", attr_raw, re.IGNORECASE)
    if match and match.group(1).strip() == attr_id:
        return attr_id
    return ""


def _prefer_exact_gam_rows(runtime: dict[str, Any], gam_rows: list[Any]) -> list[Any]:
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    exact_keys: set[tuple[str, str, str]] = set()

    for row in gam_rows:
        exact_attr_id = _gam_row_exact_attr_id(row)
        if not exact_attr_id:
            continue
        dims = enrich_dimensions(getattr(row, "site", "") or getattr(row, "breakdown", ""), runtime["site_mapping"], runtime["dimension_overrides"])
        row_site = str(dims.site or getattr(row, "site", "") or "").strip()
        row_project = (
            site_to_project.get(row_site)
            or site_to_project.get(str(getattr(row, "site", "") or "").strip())
            or str(runtime.get("site_mapping", {}).get(row_site, "") or "").strip()
            or row_site
        )
        exact_keys.add((str(getattr(row, "day", "")), row_project, exact_attr_id))

    filtered_rows: list[Any] = []
    for row in gam_rows:
        attr_id = str(getattr(row, "attr_id", "") or "").strip()
        if not attr_id:
            filtered_rows.append(row)
            continue

        exact_attr_id = _gam_row_exact_attr_id(row)
        if not exact_attr_id:
            dims = enrich_dimensions(getattr(row, "site", "") or getattr(row, "breakdown", ""), runtime["site_mapping"], runtime["dimension_overrides"])
            row_site = str(dims.site or getattr(row, "site", "") or "").strip()
            row_project = (
                site_to_project.get(row_site)
                or site_to_project.get(str(getattr(row, "site", "") or "").strip())
                or str(runtime.get("site_mapping", {}).get(row_site, "") or "").strip()
                or row_site
            )
            if (str(getattr(row, "day", "")), row_project, attr_id) in exact_keys:
                continue
        filtered_rows.append(row)
    return filtered_rows


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _extract_utm_params(attr_raw: Any) -> dict[str, str]:
    raw = str(attr_raw or "").strip()
    if not raw:
        return {}
    if "?" in raw:
        raw = raw.split("?", 1)[1]
    raw = raw.lstrip("?")
    return {str(key).strip().lower(): str(val).strip() for key, val in parse_qsl(raw, keep_blank_values=True)}


def _site_channel_hint(site: Any) -> str:
    host = _normalize_token(site)
    if not host:
        return ""
    primary = host.split(".", 1)[0]
    if primary.endswith("-g"):
        primary = primary[:-2]
    return primary


def _site_project_maps(runtime: dict[str, Any]) -> tuple[dict[str, str], dict[str, set[str]]]:
    site_to_project: dict[str, str] = {}
    project_to_sites: dict[str, set[str]] = {}
    for row in read_table(runtime["db_path"], "sites"):
        site_id = str(row.get("site_id", "") or "").strip()
        project_id = str(row.get("project_id", "") or "").strip()
        if not site_id or not project_id:
            continue
        site_to_project[site_id] = project_id
        project_to_sites.setdefault(project_id, set()).add(site_id)

    for alias, mapped in (runtime.get("site_mapping") or {}).items():
        alias_site = str(alias or "").strip()
        mapped_site = str(mapped or "").strip()
        if not alias_site or not mapped_site:
            continue
        project_id = site_to_project.get(alias_site) or site_to_project.get(mapped_site) or mapped_site
        site_to_project.setdefault(alias_site, project_id)
        site_to_project.setdefault(mapped_site, project_id)
        project_to_sites.setdefault(project_id, set()).update({alias_site, mapped_site})
    return site_to_project, project_to_sites


def _classify_push_channel(site: Any, attr_raw: Any) -> tuple[str, dict[str, str]]:
    params = _extract_utm_params(attr_raw)
    source = _normalize_token(params.get("utm_source"))
    medium = _normalize_token(params.get("utm_medium"))
    campaign = _normalize_token(params.get("utm_campaign"))
    host_hint = _site_channel_hint(site)
    source_is_push = source in PUSH_CONFIRMED_SOURCES
    medium_is_push = medium in PUSH_CONFIRMED_MEDIUMS
    campaign_is_push = any(campaign.startswith(prefix) for prefix in PUSH_CAMPAIGN_PREFIXES)
    campaign_is_google = "_google_" in campaign
    campaign_is_email = campaign.endswith("_email") or "_email" in campaign
    campaign_is_push_message = "_msg" in campaign or "welcom_msg" in campaign

    if campaign_is_email:
        return "", params
    if campaign_is_google:
        return "", params

    # Push revenue must be reconciled by UTM behavior, not by project/site name.
    # Counting every "quali" or "por" source/site as Push inflates the metric.
    if campaign_is_push_message and campaign_is_push:
        suffix = source or host_hint or "push"
        return f"push/{suffix}", params
    if campaign_is_push:
        suffix = source or medium or host_hint or "campaign"
        return f"push/{suffix}", params
    if medium_is_push and campaign_is_push:
        suffix = source or "atc"
        return f"push/{suffix}", params
    if source_is_push and (medium_is_push or campaign_is_push):
        return f"push/{source}", params
    return "", params


def _push_campaign_name(attr_raw: Any, params: dict[str, str]) -> str:
    campaign = str(params.get("utm_campaign", "") or "").strip()
    if campaign:
        return campaign
    raw = str(attr_raw or "").strip()
    if raw and "=" not in raw and "&" not in raw and "?" not in raw:
        return raw
    return ""


def _parse_push_campaign(campaign: str) -> dict[str, str]:
    normalized = _normalize_token(campaign)
    tokens = [token for token in re.split(r"[_\-\s]+", normalized) if token]
    message_number = ""
    message_match = re.search(r"msg(\d+)", normalized)
    if message_match:
        message_number = message_match.group(1)

    family_parts: list[str] = []
    if tokens:
        family_parts.append(tokens[0])
    if "welcom" in normalized or "welcome" in normalized:
        family_parts.append("welcome")
    elif len(tokens) > 1 and not tokens[1].isdigit():
        family_parts.append(tokens[1])
    message_family = "_".join(family_parts)

    send_time = ""
    schedule_hint = ""
    if message_match:
        before_msg = normalized[: message_match.start()]
        numeric_tokens = [int(token) for token in re.findall(r"(?<!\d)(\d{1,2})(?!\d)", before_msg)]
        if len(numeric_tokens) >= 2:
            hour, minute = numeric_tokens[-2], numeric_tokens[-1]
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                send_time = f"{hour:02d}:{minute:02d}"
        schedule_hint = before_msg.strip("_- ")

    return {
        "message_family": message_family,
        "message_number": message_number,
        "schedule_hint": schedule_hint,
        "send_time": send_time,
    }


def _push_message_id(project_id: str, site: str, campaign: str) -> str:
    import hashlib

    raw = f"{project_id}|{site}|{campaign}".encode("utf-8", "ignore")
    return hashlib.sha1(raw).hexdigest()[:20]


def sync_push_message_library(runtime: dict[str, Any], today: date | None = None) -> dict[str, Any]:
    current_day = today or app_today()
    month_start = current_day.replace(day=1).isoformat()
    month_end = current_day.isoformat()
    _ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(runtime, month_start, month_end)
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    ensure_push_control_store(runtime["db_path"])

    daily: dict[tuple[str, str], dict[str, Any]] = {}
    library: dict[str, dict[str, Any]] = {}
    links: dict[str, dict[str, Any]] = {}

    for row in gam_attr_rows:
        channel, params = _classify_push_channel(row.site, row.attr_raw)
        if not channel:
            continue
        campaign = _push_campaign_name(row.attr_raw, params)
        if not campaign:
            continue
        site = str(row.site or "").strip()
        project_id = site_to_project.get(site) or str(runtime.get("site_mapping", {}).get(site, "") or "").strip() or site
        message_id = _push_message_id(project_id, site, campaign)
        parsed = _parse_push_campaign(campaign)
        revenue = _net_gam_revenue(row.revenue)
        impressions = _to_float(getattr(row, "impressions", 0))
        clicks = _to_float(getattr(row, "clicks", 0))
        requests = _to_float(getattr(row, "total_requests", 0))
        unfilled = _to_float(getattr(row, "unfilled_impressions", 0))
        denominator = requests if requests > 0 else impressions + unfilled
        pmr = impressions / denominator if denominator > 0 else 0.0
        ecpm = revenue / impressions * 1000.0 if impressions > 0 else 0.0

        lib = library.setdefault(
            message_id,
            {
                "message_id": message_id,
                "project_id": project_id,
                "site": site,
                "utm_campaign": campaign,
                "utm_source": str(params.get("utm_source", "") or ""),
                "utm_medium": str(params.get("utm_medium", "") or ""),
                "message_family": parsed["message_family"],
                "message_number": parsed["message_number"],
                "schedule_hint": parsed["schedule_hint"],
                "send_time": parsed["send_time"],
                "first_seen_day": str(row.day or ""),
                "last_seen_day": str(row.day or ""),
                "total_revenue": 0.0,
                "total_impressions": 0.0,
                "total_clicks": 0.0,
                "metadata": {
                    "channel": channel,
                    "classification": "push_confirmed_from_gam_utm",
                    "raw_attr": str(row.attr_raw or ""),
                },
            },
        )
        lib["first_seen_day"] = min(str(lib["first_seen_day"] or row.day), str(row.day or ""))
        lib["last_seen_day"] = max(str(lib["last_seen_day"] or row.day), str(row.day or ""))
        lib["total_revenue"] += revenue
        lib["total_impressions"] += impressions
        lib["total_clicks"] += clicks

        key = (str(row.day or ""), message_id)
        metric = daily.setdefault(
            key,
            {
                "day": str(row.day or ""),
                "message_id": message_id,
                "project_id": project_id,
                "site": site,
                "utm_campaign": campaign,
                "revenue": 0.0,
                "impressions": 0.0,
                "clicks": 0.0,
                "pmr_numerator": 0.0,
                "pmr_denominator": 0.0,
            },
        )
        metric["revenue"] += revenue
        metric["impressions"] += impressions
        metric["clicks"] += clicks
        metric["pmr_numerator"] += impressions
        metric["pmr_denominator"] += denominator

        # First practical association layer: bind the push message to the acquisition
        # attribution id when the UTM itself is already numeric.
        attr_id = str(getattr(row, "attr_id", "") or "").strip()
        if attr_id.isdigit():
            link_id = f"{message_id}:{attr_id}"
            links[link_id] = {
                "link_id": link_id,
                "message_id": message_id,
                "project_id": project_id,
                "acquisition_platform": "googleads",
                "acquisition_campaign_id": "",
                "acquisition_attr_id": attr_id,
                "link_method": "direct_gam_attr_id",
                "confidence": 0.55,
                "metadata": {
                    "reason": "GAM attr_id available on push UTM row",
                    "site": site,
                    "utm_campaign": campaign,
                },
            }

    acquisition_by_project: dict[str, list[dict[str, Any]]] = {}
    for project_id, campaign_rows in _project_campaign_analysis(runtime, current_day).items():
        candidates: list[dict[str, Any]] = []
        for row in campaign_rows:
            today_cost = _to_float(row[9])
            month_cost = _to_float(row[24])
            if today_cost <= 0 and month_cost <= 0:
                continue
            candidates.append(
                {
                    "attr_id": str(row[3] or "").strip(),
                    "campaign_id": str(row[5] or "").strip(),
                    "campaign": str(row[6] or "").strip(),
                    "today_cost": today_cost,
                    "month_cost": month_cost,
                    "roi_today": _to_float(row[11]),
                }
            )
        candidates.sort(key=lambda item: (item["today_cost"], item["month_cost"]), reverse=True)
        acquisition_by_project[project_id] = candidates[:5]

    for item in library.values():
        project_candidates = acquisition_by_project.get(str(item["project_id"])) or []
        family_root = str(item["message_family"] or "").split("_", 1)[0].lower()
        for rank, candidate in enumerate(project_candidates[:3], start=1):
            campaign_name = str(candidate["campaign"] or "").lower()
            confidence = 0.30
            if family_root and family_root in campaign_name:
                confidence += 0.25
            if rank == 1:
                confidence += 0.10
            attr_id = str(candidate["attr_id"] or "")
            if not attr_id:
                continue
            link_id = f"{item['message_id']}:{attr_id}:project_mix"
            links.setdefault(
                link_id,
                {
                    "link_id": link_id,
                    "message_id": item["message_id"],
                    "project_id": item["project_id"],
                    "acquisition_platform": "googleads",
                    "acquisition_campaign_id": str(candidate["campaign_id"] or ""),
                    "acquisition_attr_id": attr_id,
                    "link_method": "project_campaign_mix",
                    "confidence": min(confidence, 0.75),
                    "metadata": {
                        "reason": "Push UTM has no acquisition id; linked to active paid acquisition candidate by project and family",
                        "campaign": candidate["campaign"],
                        "today_cost": candidate["today_cost"],
                        "month_cost": candidate["month_cost"],
                        "roi_today": candidate["roi_today"],
                        "family_root": family_root,
                    },
                },
            )

    linked_message_ids = {str(item["message_id"]) for item in links.values()}
    for item in library.values():
        item["acquisition_link_status"] = "candidate" if item["message_id"] in linked_message_ids else "unlinked"

    with sqlite3.connect(runtime["db_path"]) as conn:
        for item in library.values():
            conn.execute(
                """
                INSERT INTO push_message_library (
                    message_id, project_id, site, utm_campaign, utm_source, utm_medium,
                    message_family, message_number, schedule_hint, send_time,
                    first_seen_day, last_seen_day, total_revenue, total_impressions,
                    total_clicks, acquisition_link_status, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    project_id=excluded.project_id,
                    site=excluded.site,
                    utm_source=excluded.utm_source,
                    utm_medium=excluded.utm_medium,
                    message_family=excluded.message_family,
                    message_number=excluded.message_number,
                    schedule_hint=excluded.schedule_hint,
                    send_time=excluded.send_time,
                    first_seen_day=excluded.first_seen_day,
                    last_seen_day=excluded.last_seen_day,
                    last_seen_at=CURRENT_TIMESTAMP,
                    total_revenue=excluded.total_revenue,
                    total_impressions=excluded.total_impressions,
                    total_clicks=excluded.total_clicks,
                    acquisition_link_status=excluded.acquisition_link_status,
                    metadata_json=excluded.metadata_json
                """,
                (
                    item["message_id"],
                    item["project_id"],
                    item["site"],
                    item["utm_campaign"],
                    item["utm_source"],
                    item["utm_medium"],
                    item["message_family"],
                    item["message_number"],
                    item["schedule_hint"],
                    item["send_time"],
                    item["first_seen_day"],
                    item["last_seen_day"],
                    item["total_revenue"],
                    item["total_impressions"],
                    item["total_clicks"],
                    item["acquisition_link_status"],
                    json.dumps(item["metadata"], ensure_ascii=False),
                ),
            )
        for item in daily.values():
            impressions = _to_float(item["impressions"])
            pmr_denominator = _to_float(item["pmr_denominator"])
            pmr = _to_float(item["pmr_numerator"]) / pmr_denominator if pmr_denominator > 0 else 0.0
            ecpm = _to_float(item["revenue"]) / impressions * 1000.0 if impressions > 0 else 0.0
            conn.execute(
                """
                INSERT INTO push_message_daily_metrics (
                    day, message_id, project_id, site, utm_campaign, revenue,
                    impressions, clicks, pmr, ecpm, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(day, message_id) DO UPDATE SET
                    revenue=excluded.revenue,
                    impressions=excluded.impressions,
                    clicks=excluded.clicks,
                    pmr=excluded.pmr,
                    ecpm=excluded.ecpm,
                    metadata_json=excluded.metadata_json
                """,
                (
                    item["day"],
                    item["message_id"],
                    item["project_id"],
                    item["site"],
                    item["utm_campaign"],
                    item["revenue"],
                    impressions,
                    item["clicks"],
                    pmr,
                    ecpm,
                    json.dumps({"source": "gam_attr_daily"}, ensure_ascii=False),
                ),
            )
        for item in links.values():
            conn.execute(
                """
                INSERT INTO push_acquisition_links (
                    link_id, message_id, project_id, acquisition_platform,
                    acquisition_campaign_id, acquisition_attr_id, link_method,
                    confidence, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(link_id) DO UPDATE SET
                    acquisition_campaign_id=excluded.acquisition_campaign_id,
                    acquisition_attr_id=excluded.acquisition_attr_id,
                    link_method=excluded.link_method,
                    confidence=excluded.confidence,
                    last_seen_at=CURRENT_TIMESTAMP,
                    metadata_json=excluded.metadata_json
                """,
                (
                    item["link_id"],
                    item["message_id"],
                    item["project_id"],
                    item["acquisition_platform"],
                    item.get("acquisition_campaign_id", ""),
                    item["acquisition_attr_id"],
                    item.get("link_method", "project_day_window"),
                    item["confidence"],
                    json.dumps(item["metadata"], ensure_ascii=False),
                ),
            )

    return {
        "messages": len(library),
        "daily_rows": len(daily),
        "candidate_links": len(links),
        "period_start": month_start,
        "period_end": month_end,
    }


def load_push_control_summary(runtime: dict[str, Any], today: date | None = None, limit: int = 80) -> dict[str, Any]:
    sync_result = sync_push_message_library(runtime, today)
    current_day = today or app_today()
    today_iso = current_day.isoformat()
    month_start = current_day.replace(day=1).isoformat()
    ensure_push_control_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        top_messages = [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                    l.message_id,
                    l.project_id,
                    l.site,
                    l.utm_campaign,
                    l.message_family,
                    l.message_number,
                    l.send_time,
                    l.acquisition_link_status,
                    l.total_revenue,
                    l.total_impressions,
                    l.total_clicks,
                    COALESCE(t.revenue, 0) AS revenue_today,
                    COALESCE(t.clicks, 0) AS clicks_today,
                    COALESCE(t.ecpm, 0) AS ecpm_today
                FROM push_message_library l
                LEFT JOIN push_message_daily_metrics t
                    ON t.message_id = l.message_id AND t.day = ?
                ORDER BY l.total_revenue DESC
                LIMIT ?
                """,
                (today_iso, int(limit)),
            ).fetchall()
        ]
        project_rows = [
            dict(row)
            for row in conn.execute(
                """
                WITH all_projects AS (
                    SELECT DISTINCT project_id FROM push_message_daily_metrics
                    UNION
                    SELECT DISTINCT project_id FROM push_send_queue
                    UNION
                    SELECT DISTINCT project_id FROM push_message_library
                )
                SELECT
                    p.project_id,
                    COALESCE(m.messages, 0) AS messages,
                    COALESCE(m.revenue_today, 0) AS revenue_today,
                    COALESCE(m.revenue_month, 0) AS revenue_month,
                    COALESCE(m.clicks_today, 0) AS clicks_today,
                    COALESCE(q.scheduled, 0) AS scheduled,
                    COALESCE(q.sent, 0) AS sent,
                    COALESCE(q.errors, 0) AS errors
                FROM all_projects p
                LEFT JOIN (
                    SELECT
                        project_id,
                        COUNT(DISTINCT message_id) AS messages,
                        SUM(CASE WHEN day = ? THEN revenue ELSE 0 END) AS revenue_today,
                        SUM(CASE WHEN day BETWEEN ? AND ? THEN revenue ELSE 0 END) AS revenue_month,
                        SUM(CASE WHEN day = ? THEN clicks ELSE 0 END) AS clicks_today
                    FROM push_message_daily_metrics
                    GROUP BY project_id
                ) m ON m.project_id = p.project_id
                LEFT JOIN (
                    SELECT
                        project_id,
                        SUM(CASE WHEN status = 'scheduled' THEN 1 ELSE 0 END) AS scheduled,
                        SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) AS sent,
                        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors
                    FROM push_send_queue
                    GROUP BY project_id
                ) q ON q.project_id = p.project_id
                ORDER BY revenue_today DESC, revenue_month DESC
                """,
                (today_iso, month_start, today_iso, today_iso),
            ).fetchall()
        ]
        link_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_acquisition_links
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        ]
    return {
        "sync": sync_result,
        "projects": project_rows,
        "messages": top_messages,
        "links": link_rows,
    }


def _push_score(revenue: float, ecpm: float, clicks: float) -> float:
    return round((revenue * 1.0) + (ecpm * 0.12) + (clicks * 0.05), 6)


def _next_weekday_date(day: date) -> date:
    return day + timedelta(days=7)


def _push_test_slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", _normalize_token(value)).strip("_")
    return text[:36] or "push_test"


def check_push_site_installation(runtime: dict[str, Any], project_id: str, timeout: int = 12) -> dict[str, Any]:
    ensure_push_control_store(runtime["db_path"])
    host = str(project_id or "").strip().strip("/")
    if not host:
        return {"project_id": project_id, "is_installed": False, "result": "missing_project_id"}
    sw_url = f"https://{host}/firebase-messaging-sw.js"
    checked_at = datetime.now(APP_TZ).replace(microsecond=0).isoformat()
    status = 0
    installed = False
    result = "not_checked"
    error = ""
    try:
        request = urllib.request.Request(sw_url, headers={"User-Agent": "NexusPushInstallCheck/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(getattr(response, "status", 0) or 0)
            body = response.read(250000).decode("utf-8", "replace")
            installed = status == 200 and (
                "firebase" in body.lower()
                or "messaging" in body.lower()
                or "importscripts" in body.lower()
                or "push" in body.lower()
            )
            result = "installed" if installed else "script_without_expected_push_markers"
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        result = "not_installed" if status == 404 else "http_error"
        error = str(exc)
    except Exception as exc:  # noqa: BLE001
        result = "request_failed"
        error = str(exc)

    row = {
        "project_id": host,
        "checked_at": checked_at,
        "sw_url": sw_url,
        "http_status": status,
        "is_installed": bool(installed),
        "result": result,
        "error": error,
    }
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO push_site_installation_checks (
                project_id, checked_at, sw_url, http_status, is_installed, result, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (host, checked_at, sw_url, status, 1 if installed else 0, result, error[:500]),
        )
    return row


def load_push_installation_checks(runtime: dict[str, Any], projects: list[str] | None = None) -> list[dict[str, Any]]:
    ensure_push_control_store(runtime["db_path"])
    project_list = projects or [
        str(row.get("Projeto", "") or "")
        for row in load_project_kpis(runtime, app_today())
        if str(row.get("Projeto", "") or "").strip()
    ]
    current: list[dict[str, Any]] = []
    for project in project_list:
        current.append(check_push_site_installation(runtime, project))
    return current


def build_push_daily_optimization_plans(
    runtime: dict[str, Any],
    today: date | None = None,
    min_impressions: int = 100,
    required_sends: int = 8,
) -> dict[str, Any]:
    current_day = today or app_today()
    source_day = current_day - timedelta(days=1)
    next_week_day = _next_weekday_date(current_day)
    sync_push_message_library(runtime, current_day)
    ensure_push_control_store(runtime["db_path"])

    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        project_ids = [
            str(row["project_id"])
            for row in conn.execute(
                """
                SELECT DISTINCT project_id
                FROM push_message_daily_metrics
                WHERE day = ?
                ORDER BY project_id
                """,
                (source_day.isoformat(),),
            ).fetchall()
        ]
        plans: list[dict[str, Any]] = []
        for project_id in project_ids:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT
                        m.message_id,
                        m.project_id,
                        m.site,
                        m.utm_campaign,
                        m.revenue,
                        m.impressions,
                        m.clicks,
                        m.ecpm,
                        m.pmr,
                        l.message_family,
                        l.message_number,
                        l.send_time
                    FROM push_message_daily_metrics m
                    JOIN push_message_library l ON l.message_id = m.message_id
                    WHERE m.project_id = ? AND m.day = ?
                    ORDER BY m.ecpm DESC, m.revenue DESC
                    """,
                    (project_id, source_day.isoformat()),
                ).fetchall()
            ]
            enriched: list[dict[str, Any]] = []
            for row in rows:
                impressions = _to_float(row.get("impressions"))
                revenue = _to_float(row.get("revenue"))
                ecpm = _to_float(row.get("ecpm"))
                clicks = _to_float(row.get("clicks"))
                row["eligible_for_content_decision"] = impressions >= float(min_impressions)
                row["score"] = _push_score(revenue, ecpm, clicks)
                enriched.append(row)

            eligible = [row for row in enriched if row["eligible_for_content_decision"]]
            winners = sorted(eligible, key=lambda item: (item["score"], _to_float(item.get("ecpm")), _to_float(item.get("revenue"))), reverse=True)[:3]
            losers = sorted(eligible, key=lambda item: (item["score"], _to_float(item.get("ecpm")), _to_float(item.get("revenue"))))[:3]
            best_slots = [
                str(row.get("send_time") or "")
                for row in sorted(eligible, key=lambda item: (item["score"], _to_float(item.get("ecpm"))), reverse=True)
                if str(row.get("send_time") or "")
            ][:required_sends]
            best_slots = list(dict.fromkeys(best_slots))
            plan_id = f"push_opt_{project_id}_{current_day.isoformat()}".replace(".", "_").replace("-", "_")
            actions: list[dict[str, Any]] = []

            for row in losers:
                action_id = f"{plan_id}_pause_{row['message_id']}"
                payload = {
                    "rule": "pause_bottom_3_after_100_gam_impressions",
                    "source_day": source_day.isoformat(),
                    "minimum_impressions": min_impressions,
                }
                action = {
                    "action_id": action_id,
                    "plan_id": plan_id,
                    "project_id": project_id,
                    "source_message_id": row["message_id"],
                    "source_utm_campaign": row["utm_campaign"],
                    "action_type": "pause_content",
                    "status": "proposed",
                    "reason": "bottom_3_ecpm_revenue_after_100_impressions",
                    "score": row["score"],
                    "ecpm": _to_float(row.get("ecpm")),
                    "revenue": _to_float(row.get("revenue")),
                    "impressions": _to_float(row.get("impressions")),
                    "clicks": _to_float(row.get("clicks")),
                    "title_test": "",
                    "description_test": "",
                    "icon_test": "",
                    "target_send_slot": "",
                    "target_utm_campaign": "",
                    "payload_json": json.dumps(payload, ensure_ascii=False),
                }
                actions.append(action)

            for index, winner in enumerate(winners, start=1):
                slot = best_slots[(index - 1) % len(best_slots)] if best_slots else str(winner.get("send_time") or "")
                base = _push_test_slug(str(winner.get("utm_campaign") or winner.get("message_family") or "push"))
                utm = f"{base}_nw_{next_week_day.strftime('%d_%m')}_msg{index}"
                action_id = f"{plan_id}_new_{index}_{winner['message_id']}"
                payload = {
                    "rule": "create_3_new_contents_from_top_ecpm",
                    "source_day": source_day.isoformat(),
                    "next_same_weekday": next_week_day.isoformat(),
                    "base_message": winner,
                    "tests": ["title", "description", "icon"],
                }
                action = {
                    "action_id": action_id,
                    "plan_id": plan_id,
                    "project_id": project_id,
                    "source_message_id": winner["message_id"],
                    "source_utm_campaign": winner["utm_campaign"],
                    "action_type": "create_content_variant",
                    "status": "proposed",
                    "reason": "derived_from_top_3_ecpm_revenue",
                    "score": winner["score"],
                    "ecpm": _to_float(winner.get("ecpm")),
                    "revenue": _to_float(winner.get("revenue")),
                    "impressions": _to_float(winner.get("impressions")),
                    "clicks": _to_float(winner.get("clicks")),
                    "title_test": f"variant_title_from_{winner.get('message_family') or 'winner'}",
                    "description_test": f"variant_description_from_{winner.get('message_family') or 'winner'}",
                    "icon_test": f"variant_icon_from_{winner.get('message_family') or 'winner'}",
                    "target_send_slot": slot,
                    "target_utm_campaign": utm,
                    "payload_json": json.dumps(payload, ensure_ascii=False),
                }
                actions.append(action)

            payload = {
                "rule_version": "push_8_sends_v1",
                "required_sends": required_sends,
                "source_day": source_day.isoformat(),
                "next_same_weekday": next_week_day.isoformat(),
                "minimum_impressions": min_impressions,
                "best_slots": best_slots,
                "winners": winners,
                "losers": losers,
                "note": "Mantem 8 envios por dia; horarios podem mudar; os 3 piores conteudos sao pausados e substituidos por 3 variacoes baseadas nos melhores eCPMs.",
            }
            conn.execute(
                """
                INSERT INTO push_daily_optimization_plans (
                    plan_id, project_id, plan_day, source_day, required_sends,
                    eligible_messages, keep_count, pause_count, new_content_count,
                    status, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(plan_id) DO UPDATE SET
                    eligible_messages=excluded.eligible_messages,
                    keep_count=excluded.keep_count,
                    pause_count=excluded.pause_count,
                    new_content_count=excluded.new_content_count,
                    status=excluded.status,
                    payload_json=excluded.payload_json
                """,
                (
                    plan_id,
                    project_id,
                    current_day.isoformat(),
                    source_day.isoformat(),
                    required_sends,
                    len(eligible),
                    len(winners),
                    len(losers),
                    len(winners),
                    "proposed",
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            for action in actions:
                conn.execute(
                    """
                    INSERT INTO push_content_actions (
                        action_id, plan_id, project_id, source_message_id, source_utm_campaign,
                        action_type, status, reason, score, ecpm, revenue, impressions, clicks,
                        title_test, description_test, icon_test, target_send_slot,
                        target_utm_campaign, payload_json
                    )
                    VALUES (
                        :action_id, :plan_id, :project_id, :source_message_id, :source_utm_campaign,
                        :action_type, :status, :reason, :score, :ecpm, :revenue, :impressions, :clicks,
                        :title_test, :description_test, :icon_test, :target_send_slot,
                        :target_utm_campaign, :payload_json
                    )
                    ON CONFLICT(action_id) DO UPDATE SET
                        status=excluded.status,
                        reason=excluded.reason,
                        score=excluded.score,
                        ecpm=excluded.ecpm,
                        revenue=excluded.revenue,
                        impressions=excluded.impressions,
                        clicks=excluded.clicks,
                        title_test=excluded.title_test,
                        description_test=excluded.description_test,
                        icon_test=excluded.icon_test,
                        target_send_slot=excluded.target_send_slot,
                        target_utm_campaign=excluded.target_utm_campaign,
                        payload_json=excluded.payload_json
                    """,
                    action,
                )
            plans.append({"plan_id": plan_id, "project_id": project_id, "payload": payload, "actions": actions})
    return {
        "plan_day": current_day.isoformat(),
        "source_day": source_day.isoformat(),
        "required_sends": required_sends,
        "minimum_impressions": min_impressions,
        "plans": plans,
    }


def load_push_optimization_summary(runtime: dict[str, Any], today: date | None = None, limit: int = 80) -> dict[str, Any]:
    result = build_push_daily_optimization_plans(runtime, today)
    ensure_push_control_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        plan_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_daily_optimization_plans
                ORDER BY plan_day DESC, project_id
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        ]
        action_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_content_actions
                ORDER BY created_at DESC, score ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        ]
    return {"build": result, "plans": plan_rows, "actions": action_rows}


def _parse_push_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo:
        return parsed.astimezone(APP_TZ)
    return parsed.replace(tzinfo=APP_TZ)


def _push_day_key(value: Any) -> str:
    parsed = _parse_push_datetime(value)
    if not parsed:
        return "sem_data"
    return ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"][parsed.weekday()]


def _push_day_from_campaign(value: Any) -> str:
    text = _normalize_token(value)
    if re.search(r"(^|_)(seg|segunda)(_|$)", text):
        return "segunda"
    if re.search(r"(^|_)(ter|terc|terca)(_|$)", text):
        return "terca"
    if re.search(r"(^|_)(qua|quar|quarta)(_|$)", text):
        return "quarta"
    if re.search(r"(^|_)(qui|quinta)(_|$)", text):
        return "quinta"
    if re.search(r"(^|_)(sex|sexta)(_|$)", text):
        return "sexta"
    if re.search(r"(^|_)(sab|sabado)(_|$)", text):
        return "sabado"
    if re.search(r"(^|_)(dom|domingo)(_|$)", text):
        return "domingo"
    return "sem_data"


def _push_time(value: Any, fallback: Any = "") -> str:
    parsed = _parse_push_datetime(value)
    if parsed:
        return parsed.strftime("%H:%M")
    return str(fallback or "")


def _push_host_from_link(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    match = re.match(r"^https?://([^/?#]+)", raw, re.I)
    return match.group(1).replace("www.", "") if match else ""


def _push_topic(title: Any, link: Any, utm_campaign: Any) -> str:
    text = _normalize_token(f"{title} {link} {utm_campaign}")
    if "tarjeta" in text:
        return "Tarjetas"
    if "hipotec" in text or "viviend" in text:
        return "Hipoteca"
    if "quirografario" in text:
        return "Quirografario"
    if "paypal" in text:
        return "PayPal"
    if "dni" in text:
        return "DNI"
    if "jubilacion" in text:
        return "Jubilacion"
    if "buro" in text or "veraz" in text or "deuda" in text:
        return "Veraz / Deuda"
    if "credito" in text or "prestamo" in text:
        return "Credito / Prestamo"
    return "Outros"


def _push_tone(title: Any, body: Any, payload: dict[str, Any]) -> str:
    text = _normalize_token(f"{title} {body} {payload.get('button', '')} {payload.get('button_text', '')}")
    hard_words = ["ya", "hoy", "cash", "secreto", "rompe", "quema", "vip", "elegido", "ultima", "dinero", "plata", "cobra"]
    quality_words = ["orden", "entender", "comparar", "evita", "paso", "opciones", "error", "criterio", "proceso", "decidir"]
    hard = sum(1 for word in hard_words if word in text)
    quality = sum(1 for word in quality_words if word in text)
    if quality >= hard and quality > 0:
        return "Orden / Diagnostico"
    if hard >= 2:
        return "Hard / Urgencia"
    return "Misto"


def _push_risk(title: Any, body: Any, payload: dict[str, Any]) -> str:
    text = _normalize_token(f"{title} {body} {payload.get('button', '')} {payload.get('button_text', '')}")
    risky = ["secreto", "sin preguntas", "quema", "rompe", "cash", "elegido", "hasta", "no sabra", "sin permiso", "facil", "ya", "hoy", "desbloqueado"]
    score = sum(1 for word in risky if word in text)
    if score >= 3:
        return "Alto"
    if score >= 1:
        return "Medio"
    return "Baixo"


def _safe_json(raw: Any, default: Any) -> Any:
    try:
        return json.loads(str(raw or ""))
    except Exception:
        return default


def load_push_site_dashboard(runtime: dict[str, Any], project_id: str, today: date | None = None) -> dict[str, Any]:
    current_day = today or app_today()
    today_iso = current_day.isoformat()
    month_start = current_day.replace(day=1).isoformat()
    ensure_push_control_store(runtime["db_path"])
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        metrics_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_message_daily_metrics
                WHERE project_id = ?
                ORDER BY day DESC, revenue DESC, ecpm DESC
                """,
                (project_id,),
            ).fetchall()
        ]
        library_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_message_library
                WHERE project_id = ?
                ORDER BY total_revenue DESC, last_seen_day DESC
                """,
                (project_id,),
            ).fetchall()
        ]
        queue_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_send_queue
                WHERE project_id = ?
                ORDER BY scheduled_at DESC, created_at DESC
                """,
                (project_id,),
            ).fetchall()
        ]
        plan_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM push_daily_optimization_plans
                WHERE project_id = ?
                ORDER BY plan_day DESC
                LIMIT 8
                """,
                (project_id,),
            ).fetchall()
        ]

    metrics_by_utm: dict[str, list[dict[str, Any]]] = {}
    for row in metrics_rows:
        metrics_by_utm.setdefault(str(row.get("utm_campaign") or ""), []).append(row)

    def aggregate_metric(utm: str) -> dict[str, float]:
        rows = metrics_by_utm.get(utm, [])
        today_rows = [row for row in rows if str(row.get("day") or "") == today_iso]
        month_rows = [row for row in rows if month_start <= str(row.get("day") or "") <= today_iso]
        today_impressions = sum(_to_float(row.get("impressions")) for row in today_rows)
        today_revenue = sum(_to_float(row.get("revenue")) for row in today_rows)
        today_clicks = sum(_to_float(row.get("clicks")) for row in today_rows)
        month_impressions = sum(_to_float(row.get("impressions")) for row in month_rows)
        month_revenue = sum(_to_float(row.get("revenue")) for row in month_rows)
        return {
            "revenue_today": today_revenue,
            "impressions_today": today_impressions,
            "clicks_today": today_clicks,
            "ecpm_today": (today_revenue / today_impressions * 1000) if today_impressions else 0.0,
            "pmr_today": max((_to_float(row.get("pmr")) for row in today_rows), default=0.0),
            "revenue_month": month_revenue,
            "impressions_month": month_impressions,
            "ecpm_month": (month_revenue / month_impressions * 1000) if month_impressions else 0.0,
        }

    messages: list[dict[str, Any]] = []
    seen_utms: set[str] = set()
    for row in queue_rows:
        payload = _safe_json(row.get("payload_json"), {})
        utm = str(row.get("utm_campaign") or "").strip()
        seen_utms.add(utm)
        metric = aggregate_metric(utm)
        title = str(row.get("title") or "")
        body = str(row.get("body") or "")
        link = str(row.get("link") or "")
        messages.append(
            {
                "source": "queue",
                "project_id": project_id,
                "send_id": row.get("send_id", ""),
                "status": row.get("status", ""),
                "scheduled_at": row.get("scheduled_at", ""),
                "day": _push_day_key(row.get("scheduled_at")),
                "time": _push_time(row.get("scheduled_at")),
                "title": title,
                "body": body,
                "button": str(payload.get("button") or payload.get("button_text") or ""),
                "link": link,
                "host": _push_host_from_link(link),
                "utm_campaign": utm,
                "tone": _push_tone(title, body, payload),
                "risk": _push_risk(title, body, payload),
                "topic": _push_topic(title, link, utm),
                "error": row.get("error", ""),
                **metric,
            }
        )

    for row in library_rows:
        utm = str(row.get("utm_campaign") or "").strip()
        if utm in seen_utms:
            continue
        metric = aggregate_metric(utm)
        parsed = _parse_push_campaign(utm)
        messages.append(
            {
                "source": "gam",
                "project_id": project_id,
                "send_id": row.get("message_id", ""),
                "status": row.get("status", "observed"),
                "scheduled_at": row.get("last_seen_day", ""),
                "day": _push_day_from_campaign(utm),
                "time": str(row.get("send_time") or ""),
                "title": "",
                "body": "",
                "button": "",
                "link": "",
                "host": str(row.get("site") or ""),
                "utm_campaign": utm,
                "tone": "Sem copy",
                "risk": "Baixo",
                "topic": _push_topic("", "", utm),
                "error": "",
                **metric,
            }
        )

    day_order = ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo", "sem_data"]
    by_day: list[dict[str, Any]] = []
    for day_key in day_order:
        rows = [row for row in messages if row["day"] == day_key]
        if not rows:
            continue
        revenue = sum(_to_float(row.get("revenue_today")) for row in rows)
        impressions = sum(_to_float(row.get("impressions_today")) for row in rows)
        by_day.append(
            {
                "day": day_key,
                "pushes": len(rows),
                "hard": sum(1 for row in rows if row.get("tone") == "Hard / Urgencia"),
                "order": sum(1 for row in rows if row.get("tone") == "Orden / Diagnostico"),
                "high_risk": sum(1 for row in rows if row.get("risk") == "Alto"),
                "revenue_today": revenue,
                "impressions_today": impressions,
                "ecpm_today": (revenue / impressions * 1000) if impressions else 0.0,
                "start": min((str(row.get("time") or "99:99") for row in rows if row.get("time")), default=""),
                "end": max((str(row.get("time") or "") for row in rows if row.get("time")), default=""),
            }
        )

    def count_by(key: str) -> list[dict[str, Any]]:
        counts: dict[str, int] = {}
        for row in messages:
            value = str(row.get(key) or "Nao informado")
            counts[value] = counts.get(value, 0) + 1
        return [{"label": item[0], "value": item[1]} for item in sorted(counts.items(), key=lambda item: item[1], reverse=True)]

    for row in messages:
        impressions = _to_float(row.get("impressions_today"))
        revenue = _to_float(row.get("revenue_today"))
        ecpm = _to_float(row.get("ecpm_today"))
        pmr = _to_float(row.get("pmr_today"))
        if impressions < 100:
            row["decision"] = "aguardar_base"
            row["decision_reason"] = "menos de 100 impressoes GAM"
            row["decision_priority"] = 40
        elif revenue > 0 and ecpm > 0 and pmr >= 50:
            row["decision"] = "manter_replicar"
            row["decision_reason"] = "base minima com receita/eCPM e PMR saudavel"
            row["decision_priority"] = 90
        elif revenue <= 0 or ecpm <= 0 or pmr < 30:
            row["decision"] = "pausar_testar_novo"
            row["decision_reason"] = "base minima com receita/eCPM/PMR fraco"
            row["decision_priority"] = 80
        else:
            row["decision"] = "observar"
            row["decision_reason"] = "resultado intermediario"
            row["decision_priority"] = 60

    eligible_messages = [
        row for row in messages
        if _to_float(row.get("impressions_today")) >= 100
    ]
    winners = sorted(
        eligible_messages,
        key=lambda row: (
            _to_float(row.get("revenue_today")),
            _to_float(row.get("ecpm_today")),
            _to_float(row.get("pmr_today")),
        ),
        reverse=True,
    )[:5]
    losers = sorted(
        eligible_messages,
        key=lambda row: (
            _to_float(row.get("ecpm_today")),
            _to_float(row.get("revenue_today")),
            _to_float(row.get("pmr_today")),
        ),
    )[:3]
    pending_base = sorted(
        [row for row in messages if _to_float(row.get("impressions_today")) < 100],
        key=lambda row: _to_float(row.get("impressions_today")),
        reverse=True,
    )[:8]

    recommended_variations: list[dict[str, Any]] = []
    for idx, row in enumerate(winners[:3], start=1):
        recommended_variations.append(
            {
                "base_utm": row.get("utm_campaign", ""),
                "topic": row.get("topic", ""),
                "host": row.get("host", ""),
                "title": row.get("title", ""),
                "reason": "criar variacao baseada em vencedor por receita/eCPM",
                "slot": idx,
            }
        )

    revenue_today = sum(_to_float(row.get("revenue_today")) for row in messages)
    impressions_today = sum(_to_float(row.get("impressions_today")) for row in messages)
    return {
        "project_id": project_id,
        "today": today_iso,
        "messages": messages,
        "by_day": by_day,
        "domains": count_by("host"),
        "topics": count_by("topic"),
        "winners": winners,
        "losers": losers,
        "pending_base": pending_base,
        "recommended_variations": recommended_variations,
        "plans": plan_rows,
        "kpis": {
            "pushes": len(messages),
            "scheduled": sum(1 for row in messages if row.get("status") == "scheduled"),
            "sent": sum(1 for row in messages if row.get("status") == "sent"),
            "errors": sum(1 for row in messages if row.get("status") == "error"),
            "eligible": len(eligible_messages),
            "pending_base": sum(1 for row in messages if _to_float(row.get("impressions_today")) < 100),
            "winners": len(winners),
            "losers": len(losers),
            "hard": sum(1 for row in messages if row.get("tone") == "Hard / Urgencia"),
            "order": sum(1 for row in messages if row.get("tone") == "Orden / Diagnostico"),
            "high_risk": sum(1 for row in messages if row.get("risk") == "Alto"),
            "revenue_today": revenue_today,
            "impressions_today": impressions_today,
            "ecpm_today": (revenue_today / impressions_today * 1000) if impressions_today else 0.0,
        },
    }


def _mgid_revenue_by_project_periods(
    runtime: dict[str, Any],
    current_day: date,
    allowed_projects: set[str] | None = None,
) -> dict[str, dict[str, float]]:
    month_start = current_day.replace(day=1)
    yesterday = current_day - timedelta(days=1)
    seven_day_start = current_day - timedelta(days=6)
    history_start = min(month_start, yesterday, seven_day_start)
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    rows = read_mgid_rows(runtime["db_path"], history_start.isoformat(), current_day.isoformat())
    daily: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        row_day = date.fromisoformat(str(row.get("day", "") or "1900-01-01")[:10])
        if not row_day or row_day < history_start or row_day > current_day:
            continue
        domain = str(row.get("domain", "") or "").strip()
        if not domain:
            continue
        project = site_to_project.get(domain) or runtime.get("site_mapping", {}).get(domain) or domain
        project = str(project or domain).strip()
        if allowed_projects is not None and project not in allowed_projects:
            continue
        daily[project][row_day] += _to_float(row.get("revenue"))

    def total_between(project: str, start: date, end: date) -> float:
        total = 0.0
        current = start
        while current <= end:
            total += _to_float(daily.get(project, {}).get(current, 0.0))
            current += timedelta(days=1)
        return round(total, 6)

    result: dict[str, dict[str, float]] = {}
    for project in daily:
        result[project] = {
            "hoje": total_between(project, current_day, current_day),
            "ontem": total_between(project, yesterday, yesterday),
            "7d": total_between(project, seven_day_start, current_day),
            "mes": total_between(project, month_start, current_day),
        }
    return result


def _period_totals_from_daily(daily: dict[str, dict[date, float]], current_day: date) -> dict[str, dict[str, float]]:
    month_start = current_day.replace(day=1)
    yesterday = current_day - timedelta(days=1)
    seven_day_start = current_day - timedelta(days=6)

    def total_between(key: str, start: date, end: date) -> float:
        total = 0.0
        current = start
        while current <= end:
            total += _to_float(daily.get(key, {}).get(current, 0.0))
            current += timedelta(days=1)
        return round(total, 6)

    result: dict[str, dict[str, float]] = {}
    for key in daily:
        result[key] = {
            "hoje": total_between(key, current_day, current_day),
            "ontem": total_between(key, yesterday, yesterday),
            "7d": total_between(key, seven_day_start, current_day),
            "mes": total_between(key, month_start, current_day),
        }
    return result


def ensure_traffic_session_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS traffic_sessions (
                session_id TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                day TEXT NOT NULL,
                site TEXT NOT NULL,
                attr_id TEXT NOT NULL,
                placement TEXT NOT NULL DEFAULT '',
                url_path TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_traffic_sessions_attr "
            "ON traffic_sessions(site, attr_id, day, placement)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS traffic_session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                day TEXT NOT NULL,
                site TEXT NOT NULL,
                attr_id TEXT NOT NULL,
                placement TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT 'page_view',
                url_path TEXT NOT NULL DEFAULT '',
                target_url TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_traffic_session_events_attr "
            "ON traffic_session_events(site, attr_id, day, session_id, event_type)"
        )
        conn.commit()


def ensure_gam_country_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gam_country_daily (
                day TEXT NOT NULL,
                network TEXT NOT NULL,
                attr_id TEXT NOT NULL,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'USD',
                impressions INTEGER NOT NULL DEFAULT 0,
                clicks INTEGER NOT NULL DEFAULT 0,
                revenue REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (day, network, attr_id, country_code)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gam_country_attr "
            "ON gam_country_daily(attr_id, day, impressions)"
        )
        conn.commit()


def load_gam_top_countries(
    runtime: dict[str, Any],
    attr_id: str,
    days: int = 7,
) -> list[dict[str, Any]]:
    ensure_gam_country_store(runtime["db_path"])
    normalized_attr = re.sub(r"\D+", "", str(attr_id or ""))[:32]
    if not normalized_attr:
        return []
    period_days = max(1, min(int(days or 7), 31))
    start_day = (app_today() - timedelta(days=period_days - 1)).isoformat()
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT country_code, country_name, currency,
                   SUM(impressions) AS impressions,
                   SUM(clicks) AS clicks,
                   SUM(revenue) AS revenue
            FROM gam_country_daily
            WHERE attr_id=? AND day>=?
            GROUP BY country_code, country_name, currency
            ORDER BY impressions DESC, revenue DESC, country_name
            LIMIT 12
            """,
            (normalized_attr, start_day),
        ).fetchall()
    total_impressions = sum(int(row["impressions"] or 0) for row in rows)
    return [
        {
            "country_code": str(row["country_code"] or "").upper(),
            "country_name": str(row["country_name"] or ""),
            "currency": str(row["currency"] or "USD"),
            "impressions": int(row["impressions"] or 0),
            "clicks": int(row["clicks"] or 0),
            "revenue": round(_to_float(row["revenue"]), 6),
            "share": (int(row["impressions"] or 0) / total_impressions) if total_impressions else 0.0,
        }
        for row in rows
    ]


def record_traffic_session(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    ensure_traffic_session_store(runtime["db_path"])
    session_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(payload.get("session_id") or ""))[:100]
    site = str(payload.get("site") or "").strip().lower()
    site = re.sub(r"^https?://", "", site).split("/", 1)[0].split(":", 1)[0]
    attr_id = re.sub(r"\D+", "", str(payload.get("attr_id") or ""))[:32]
    placement = str(payload.get("placement") or "").strip().lower()[:500]
    if attr_id and placement.startswith(attr_id):
        placement = placement[len(attr_id):].lstrip("_-")
    url_path = str(payload.get("url_path") or "/").strip()[:1000] or "/"
    event_type = str(payload.get("event_type") or "page_view").strip().lower()
    if event_type not in {"page_view", "link_click"}:
        event_type = "page_view"
    target_url = str(payload.get("target_url") or "").strip()[:2000]
    if len(session_id) < 16 or not site or not attr_id:
        raise ValueError("sessao, site e utm_campaign sao obrigatorios")
    now = datetime.now(APP_TZ).isoformat(timespec="seconds")
    day = app_today().isoformat()
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        if event_type == "page_view":
            conn.execute(
                """
                INSERT INTO traffic_sessions (
                    session_id, first_seen_at, last_seen_at, day, site, attr_id, placement, url_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    url_path=excluded.url_path
                """,
                (session_id, now, now, day, site, attr_id, placement, url_path),
            )
        else:
            conn.execute(
                "UPDATE traffic_sessions SET last_seen_at=? WHERE session_id=?",
                (now, session_id),
            )
        conn.execute(
            """
            INSERT INTO traffic_session_events (
                session_id, occurred_at, day, site, attr_id, placement,
                event_type, url_path, target_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (session_id, now, day, site, attr_id, placement, event_type, url_path, target_url),
        )
        conn.commit()
    return {"status": "ok", "session_id": session_id}


def _traffic_sessions_by_attr_periods(
    runtime: dict[str, Any],
    current_day: date,
    project: str,
) -> dict[str, dict[str, float]]:
    ensure_traffic_session_store(runtime["db_path"])
    related_projects = sorted(_project_family_projects(runtime, project))
    placeholders = ",".join("?" for _ in related_projects)
    history_start = min(current_day.replace(day=1), current_day - timedelta(days=6))
    daily: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        rows = conn.execute(
            f"""
            SELECT day, attr_id, COUNT(DISTINCT session_id)
            FROM traffic_sessions
            WHERE site IN ({placeholders}) AND day BETWEEN ? AND ?
            GROUP BY day, attr_id
            """,
            [*related_projects, history_start.isoformat(), current_day.isoformat()],
        ).fetchall()
    for day_text, attr_id, sessions in rows:
        try:
            row_day = date.fromisoformat(str(day_text))
        except ValueError:
            continue
        daily[str(attr_id)][row_day] += _to_float(sessions)
    return _period_totals_from_daily(daily, current_day)


def _traffic_sessions_by_placement(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    start_day: date,
    end_day: date,
) -> dict[str, int]:
    ensure_traffic_session_store(runtime["db_path"])
    related_projects = sorted(_project_family_projects(runtime, project))
    placeholders = ",".join("?" for _ in related_projects)
    result: dict[str, int] = defaultdict(int)
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        rows = conn.execute(
            f"""
            SELECT placement, COUNT(DISTINCT session_id)
            FROM traffic_sessions
            WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
            GROUP BY placement
            """,
            [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
        ).fetchall()
    for placement, sessions in rows:
        key = str(placement or "").strip().lower()
        if key:
            result[key] += int(sessions or 0)
    return dict(result)


def traffic_session_project_periods(
    runtime: dict[str, Any],
    project: str,
    current_day: date,
) -> dict[str, float]:
    by_attr = _traffic_sessions_by_attr_periods(runtime, current_day, project)
    result = {"hoje": 0.0, "ontem": 0.0, "7d": 0.0, "mes": 0.0}
    for periods in by_attr.values():
        for key in result:
            result[key] += _to_float(periods.get(key))
    return result


def load_attr_funnel(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    current_day: date,
) -> dict[str, Any]:
    ensure_traffic_session_store(runtime["db_path"])
    related_projects = sorted(_project_family_projects(runtime, project))
    placeholders = ",".join("?" for _ in related_projects)
    campaign_rows = load_project_campaign_rows(runtime, project, current_day)
    campaign = next((row for row in campaign_rows if str(row.get("attr_id")) == str(attr_id)), None)
    if not campaign:
        return {}

    landing_url = str(campaign.get("landing_url") or "").strip()
    if not landing_url:
        landing_url = _google_ads_landing_url(runtime, project, str(campaign.get("campaign_id") or ""), str(attr_id))
        if landing_url:
            campaign["landing_url"] = landing_url
    landing_path = urlparse(landing_url).path or "/"
    page_intelligence = load_page_intelligence_map(runtime, set(related_projects) | {project})
    page_post_ids = _wordpress_post_ids_by_path(runtime, project)
    periods = {
        "hoje": (current_day, current_day),
        "ontem": (current_day - timedelta(days=1), current_day - timedelta(days=1)),
        "7d": (current_day - timedelta(days=6), current_day),
        "mes": (current_day.replace(day=1), current_day),
    }
    history_start = min(start for start, _end in periods.values())
    daily_revenue: dict[str, float] = defaultdict(float)
    daily_gam_impressions: dict[str, float] = defaultdict(float)
    stage_gam: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: {"revenue": 0.0, "impressions": 0.0, "clicks": 0.0}))
    ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(
        runtime,
        history_start.isoformat(),
        current_day.isoformat(),
    )
    gam_stage_records = []
    gam_content_records = []
    try:
        gam_stage_records = read_gam_rows(runtime["db_path"], "gam_stage_daily", history_start.isoformat(), current_day.isoformat())
    except Exception:
        gam_stage_records = []
    try:
        gam_content_records = read_gam_rows(runtime["db_path"], "gam_content_daily", history_start.isoformat(), current_day.isoformat())
    except Exception:
        gam_content_records = []
    detail_rows, _unresolved = _build_attribution_detail_rows(
        ads_rows,
        gam_attr_rows,
        runtime["site_mapping"],
        runtime["dimension_overrides"],
    )
    for row in detail_rows:
        if str(row.attr_id) != str(attr_id):
            continue
        if str(row.site_group) not in related_projects and str(row.site) not in related_projects:
            continue
        daily_revenue[str(row.date)] += _to_float(row.revenue)
        daily_gam_impressions[str(row.date)] += _to_float(getattr(row, "impressions", 0))
    for record in gam_stage_records:
        record_attr_id = str(record.get("attr_id") or "")
        if record_attr_id and record_attr_id != str(attr_id):
            continue
        raw = str(record.get("attr_raw") or "")
        raw_lower = raw.lower()
        is_stage_key = any(key in raw_lower for key in ("land_uri=", "request_uri=", "utm_source_land_uri="))
        record_site = str(record.get("site") or "")
        if record_site and record_site not in related_projects and not record_site.startswith("/") and not is_stage_key:
            continue
        stage = ""
        post_id = ""
        for token in re.split(r"[|,]", raw):
            token = token.strip()
            lower = token.lower()
            if lower.startswith("land_uri=") or lower.startswith("request_uri="):
                stage = token.split("=", 1)[1].strip()
        stage_key = stage or (f"post:{post_id}" if post_id else "")
        if not stage_key:
            continue
        day_key = str(record.get("day") or "")
        bucket = stage_gam[stage_key][day_key]
        bucket["revenue"] += _to_float(record.get("revenue"))
        bucket["impressions"] += _to_float(record.get("impressions"))
        bucket["clicks"] += _to_float(record.get("clicks"))
    for record in gam_content_records:
        match = re.search(r"(?:^|[|,])\s*id_post_wp=(\d+)", str(record.get("attr_raw") or ""), re.IGNORECASE)
        if not match:
            continue
        day_key = str(record.get("day") or "")
        bucket = stage_gam[f"post:{match.group(1)}"][day_key]
        bucket["revenue"] += _to_float(record.get("revenue"))
        bucket["impressions"] += _to_float(record.get("impressions"))
        bucket["clicks"] += _to_float(record.get("clicks"))

    def normalize_target(value: Any) -> tuple[str, bool]:
        raw = str(value or "").strip()
        if not raw:
            return "", False
        if raw.startswith("/"):
            return (urlparse(raw).path or "/"), False
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = str(parsed.hostname or "").strip().lower()
        if host in related_projects or host == project:
            return (parsed.path or "/"), False
        return raw, True

    result_periods: dict[str, dict[str, Any]] = {}
    with sqlite3.connect(runtime["db_path"], timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        coverage = conn.execute(
            f"""
            SELECT MIN(first_seen_at) AS first_seen_at, MAX(last_seen_at) AS last_seen_at
            FROM traffic_sessions
            WHERE site IN ({placeholders}) AND attr_id=?
            """,
            [*related_projects, str(attr_id)],
        ).fetchone()
        for key, (start_day, end_day) in periods.items():
            ads = conn.execute(
                f"""
                WITH ranked AS (
                    SELECT day, clicks, impressions, cost, conversions,
                           ROW_NUMBER() OVER (
                               PARTITION BY day
                               ORDER BY CASE WHEN COALESCE(ad_group_id, '')='' THEN 0 ELSE 1 END,
                                        clicks DESC
                           ) AS row_number
                    FROM ads_daily
                    WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                )
                SELECT COALESCE(SUM(clicks),0) AS clicks,
                       COALESCE(SUM(impressions),0) AS impressions,
                       COALESCE(SUM(cost),0) AS cost,
                       COALESCE(SUM(conversions),0) AS conversions
                FROM ranked
                WHERE row_number=1
                """,
                [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
            ).fetchone()
            sessions = conn.execute(
                f"""
                SELECT COUNT(DISTINCT session_id) AS sessions
                FROM traffic_sessions
                WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                """,
                [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
            ).fetchone()
            paths = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT url_path, COUNT(DISTINCT session_id) AS sessions
                    FROM traffic_sessions
                    WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                    GROUP BY url_path
                    ORDER BY sessions DESC, url_path
                    """,
                    [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
                ).fetchall()
            ]
            event_rows = conn.execute(
                f"""
                SELECT id, session_id, occurred_at, day, event_type, url_path, target_url
                FROM traffic_session_events
                WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                ORDER BY session_id, occurred_at, id
                """,
                [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
            ).fetchall()
            legacy_rows = conn.execute(
                f"""
                SELECT session_id, day, url_path
                FROM traffic_sessions
                WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                """,
                [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
            ).fetchall()
            ads_clicks = int(ads["clicks"] or 0)
            real_sessions = int(sessions["sessions"] or 0)
            sequences: dict[str, list[str]] = defaultdict(list)
            exact_sessions: set[str] = set()
            external_targets: set[str] = set()
            daily_advanced: dict[str, set[str]] = defaultdict(set)
            advanced_sessions: set[str] = set()

            def append_step(session_id: str, target: str) -> None:
                if target and (not sequences[session_id] or sequences[session_id][-1] != target):
                    sequences[session_id].append(target)

            for event in event_rows:
                session_id = str(event["session_id"] or "")
                exact_sessions.add(session_id)
                event_day = str(event["day"] or "")
                event_type = str(event["event_type"] or "page_view")
                current_path, _current_external = normalize_target(event["url_path"] or "/")
                if event_type == "page_view":
                    append_step(session_id, current_path)
                    continue
                append_step(session_id, current_path)
                target, is_external = normalize_target(event["target_url"])
                append_step(session_id, target)
                if target:
                    daily_advanced[event_day].add(session_id)
                    advanced_sessions.add(session_id)
                if is_external and target:
                    external_targets.add(target)

            for legacy in legacy_rows:
                session_id = str(legacy["session_id"] or "")
                if session_id in exact_sessions:
                    continue
                observed_path, _is_external = normalize_target(legacy["url_path"] or "/")
                if observed_path:
                    append_step(session_id, observed_path)

            off_landing_counts: dict[str, int] = defaultdict(int)
            if landing_path != "/":
                for session_id, sequence in list(sequences.items()):
                    if not sequence or sequence[0] != landing_path:
                        off_landing_counts[sequence[0] if sequence else "(sem pagina)"] += 1
                        del sequences[session_id]
            official_session_ids = set(sequences)
            funnel_sessions = len(official_session_ids)
            advanced_sessions.intersection_update(official_session_ids)

            node_sessions: dict[str, set[str]] = defaultdict(set)
            node_positions: dict[str, list[int]] = defaultdict(list)
            transitions: dict[tuple[str, str], set[str]] = defaultdict(set)
            transition_forward_sessions: dict[tuple[str, str], set[str]] = defaultdict(set)
            transition_return_sessions: dict[tuple[str, str], set[str]] = defaultdict(set)
            transition_return_stage: dict[tuple[str, str], int] = {}
            source_advanced: dict[str, set[str]] = defaultdict(set)
            position_nodes: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
            session_days = {str(row["session_id"]): str(row["day"]) for row in legacy_rows}
            for session_id, sequence in sequences.items():
                seen_nodes: dict[str, int] = {}
                advance_position = -1
                previous_node = ""
                for node in sequence:
                    if node not in seen_nodes:
                        advance_position += 1
                        position_nodes[advance_position][node].add(session_id)
                        node_sessions[node].add(session_id)
                        node_positions[node].append(advance_position)
                        seen_nodes[node] = advance_position
                    if previous_node:
                        source = previous_node
                        if source != node:
                            transitions[(source, node)].add(session_id)
                            if node in seen_nodes and seen_nodes[node] < advance_position:
                                transition_return_sessions[(source, node)].add(session_id)
                                transition_return_stage[(source, node)] = min(
                                    transition_return_stage.get((source, node), seen_nodes[node] + 1),
                                    seen_nodes[node] + 1,
                                )
                            else:
                                transition_forward_sessions[(source, node)].add(session_id)
                                source_advanced[source].add(session_id)
                                daily_advanced[session_days.get(session_id, "")].add(session_id)
                    previous_node = node

            def stage_days_for_node(node: str) -> tuple[dict[str, dict[str, float]], str]:
                if node in stage_gam:
                    return stage_gam.get(node, {}), "real por URL exata"
                candidates = [
                    key for key in stage_gam.keys()
                    if key.startswith("/") and key != "/" and node.startswith(key)
                ]
                if len(candidates) == 1:
                    return stage_gam.get(candidates[0], {}), "real por prefixo unico do GAM"
                post_id = page_post_ids.get(node)
                if post_id and f"post:{post_id}" in stage_gam:
                    return stage_gam.get(f"post:{post_id}", {}), f"real GAM por id_post_wp={post_id}; total da pagina"
                return {}, "sem dado real por etapa"

            stage_rows = []
            for node, session_ids in node_sessions.items():
                intelligence = page_intelligence.get(node) or {}
                stage_days, stage_metric_scope = stage_days_for_node(node)
                stage_revenue = sum(_to_float(values.get("revenue")) for day, values in stage_days.items() if start_day.isoformat() <= day <= end_day.isoformat())
                stage_impressions = sum(_to_float(values.get("impressions")) for day, values in stage_days.items() if start_day.isoformat() <= day <= end_day.isoformat())
                has_stage_measurement = any(start_day.isoformat() <= day <= end_day.isoformat() for day in stage_days)
                attr_specific_stage = "id_post_wp=" not in stage_metric_scope
                stage_rows.append(
                    {
                        "url": node,
                        "sessions": len(session_ids),
                        "position": (sum(node_positions[node]) / len(node_positions[node])) if node_positions[node] else 0,
                        "landing_rate": len(session_ids) / funnel_sessions if funnel_sessions else 0.0,
                        "external": node in external_targets,
                        "revenue": stage_revenue if has_stage_measurement else None,
                        "gam_impressions": stage_impressions if has_stage_measurement else None,
                        "ecpm": (stage_revenue / stage_impressions * 1000.0) if stage_impressions > 0 else None,
                        "rps": (stage_revenue / len(session_ids)) if attr_specific_stage and stage_revenue > 0 and len(session_ids) else None,
                        "metric_scope": stage_metric_scope if has_stage_measurement else "sem dado real por etapa",
                        "funnel_stage": str(intelligence.get("funnel_stage") or ""),
                        "iab_content_id": str(intelligence.get("iab_content_id") or ""),
                        "iab_content_label": str(intelligence.get("iab_content_label") or ""),
                        "commercial_signal_type": str(intelligence.get("commercial_signal_type") or ""),
                        "commercial_intent_score": _to_float(intelligence.get("commercial_intent_score")),
                        "audience_intent_label": str(intelligence.get("audience_intent_label") or ""),
                        "recommended_next_page": str(intelligence.get("recommended_next_page") or ""),
                    }
                )
            stage_rows.sort(key=lambda item: (item["position"], -item["sessions"], item["url"]))
            journey_columns = []
            for position in sorted(position_nodes)[:5]:
                ordered_nodes = sorted(
                    position_nodes[position].items(),
                    key=lambda item: (-len(item[1]), item[0]),
                )
                journey_columns.append(
                    {
                        "position": position + 1,
                        "items": [
                            {"path": node, "sessions": len(session_ids)}
                            for node, session_ids in ordered_nodes
                        ],
                    }
                )


            route_rows = []
            for (source, target), session_ids in transitions.items():
                source_count = len(node_sessions.get(source, set()))
                return_count = len(transition_return_sessions.get((source, target), set()))
                forward_count = len(transition_forward_sessions.get((source, target), set()))
                route_rows.append(
                    {
                        "source": source,
                        "target": target,
                        "sessions": len(session_ids),
                        "forward_sessions": forward_count,
                        "return_sessions": return_count,
                        "conversion_rate": len(session_ids) / source_count if source_count else 0.0,
                        "drop": max(source_count - len(session_ids), 0),
                        "external": target in external_targets,
                        "return_to_stage": transition_return_stage.get((source, target)) if return_count and not forward_count else None,
                    }
                )
            route_rows.sort(key=lambda item: (-item["sessions"], item["source"], item["target"]))

            bottleneck_candidates = []
            if ads_clicks:
                bottleneck_candidates.append(
                    {
                        "stage": "Google Ads → pagina de entrada",
                        "source": "Google Ads",
                        "sessions": ads_clicks,
                        "advanced": real_sessions,
                        "drop": max(ads_clicks - real_sessions, 0),
                        "drop_rate": max(ads_clicks - real_sessions, 0) / ads_clicks,
                    }
                )
            for source, source_sessions in node_sessions.items():
                total = len(source_sessions)
                advanced = len(source_advanced.get(source, set()))
                bottleneck_candidates.append(
                    {
                        "stage": source,
                        "source": source,
                        "sessions": total,
                        "advanced": advanced,
                        "drop": max(total - advanced, 0),
                        "drop_rate": max(total - advanced, 0) / total if total else 0.0,
                    }
                )
            bottleneck = max(bottleneck_candidates, key=lambda item: (item["drop"], item["drop_rate"]), default={})
            bottleneck_source = str(bottleneck.get("source") or "")
            if bottleneck_source == "Google Ads":
                recommendation = "Prioridade: revisar velocidade, redirecionamentos e carregamento da pagina de entrada."
            elif bottleneck_source:
                recommendation = f"Prioridade: revisar promessa, conteudo e CTA em {bottleneck_source}."
            else:
                recommendation = "Ainda nao ha volume suficiente para apontar um gargalo."

            daily_ads = {
                str(row["day"]): row
                for row in conn.execute(
                    f"""
                    WITH ranked AS (
                        SELECT day, clicks, impressions, cost,
                               ROW_NUMBER() OVER (
                                   PARTITION BY day
                                   ORDER BY CASE WHEN COALESCE(ad_group_id, '')='' THEN 0 ELSE 1 END,
                                            clicks DESC
                               ) AS row_number
                        FROM ads_daily
                        WHERE site IN ({placeholders}) AND attr_id=? AND day BETWEEN ? AND ?
                    )
                    SELECT day, clicks, impressions, cost FROM ranked WHERE row_number=1
                    """,
                    [*related_projects, str(attr_id), start_day.isoformat(), end_day.isoformat()],
                ).fetchall()
            }
            daily_sessions: dict[str, int] = defaultdict(int)
            for session_id in official_session_ids:
                daily_sessions[session_days.get(session_id, "")] += 1
            daily_rows = []
            cursor = start_day
            while cursor <= end_day:
                day_key = cursor.isoformat()
                ads_day = daily_ads.get(day_key)
                clicks_day = int(ads_day["clicks"] or 0) if ads_day else 0
                sessions_day = int(daily_sessions.get(day_key, 0))
                advances_day = len(daily_advanced.get(day_key, set()) & official_session_ids)
                revenue_day = _to_float(daily_revenue.get(day_key))
                gam_impressions_day = _to_float(daily_gam_impressions.get(day_key))
                daily_rows.append(
                    {
                        "day": day_key,
                        "ads_clicks": clicks_day,
                        "sessions": sessions_day,
                        "advances": advances_day,
                        "revenue": revenue_day,
                        "gam_impressions": gam_impressions_day,
                        "ecpm": revenue_day / gam_impressions_day * 1000.0 if gam_impressions_day else 0.0,
                        "rps": revenue_day / sessions_day if sessions_day else 0.0,
                        "ads_session_rate": sessions_day / clicks_day if clicks_day else 0.0,
                        "session_advance_rate": advances_day / sessions_day if sessions_day else 0.0,
                    }
                )
                cursor += timedelta(days=1)

            entry_nodes = position_nodes.get(0, {})
            primary_entry_path = max(entry_nodes, key=lambda node: len(entry_nodes[node])) if entry_nodes else ""
            destination_rows = [
                {"url_path": row["target"], "sessions": row["sessions"]}
                for row in route_rows
                if row["source"] == primary_entry_path
            ]
            button_clicks = len(advanced_sessions)
            final_clicks = len({session_id for target in external_targets for session_id in node_sessions.get(target, set())})
            period_revenue = sum(_to_float(row["revenue"]) for row in daily_rows)
            period_gam_impressions = sum(_to_float(row.get("gam_impressions")) for row in daily_rows)
            result_periods[key] = {
                "start_day": start_day.isoformat(),
                "end_day": end_day.isoformat(),
                "ads_clicks": ads_clicks,
                "ads_impressions": int(ads["impressions"] or 0),
                "ads_cost": _to_float(ads["cost"]),
                "ads_conversions": _to_float(ads["conversions"]),
                "sessions": funnel_sessions,
                "associated_sessions": real_sessions,
                "off_landing_sessions": sum(off_landing_counts.values()),
                "off_landing_entries": [
                    {"url": path, "sessions": count}
                    for path, count in sorted(off_landing_counts.items(), key=lambda item: (-item[1], item[0]))
                ],
                "ads_session_loss": max(ads_clicks - funnel_sessions, 0),
                "ads_session_loss_rate": ((ads_clicks - funnel_sessions) / ads_clicks) if ads_clicks > 0 else 0.0,
                "button_clicks": button_clicks,
                "session_button_rate": button_clicks / funnel_sessions if funnel_sessions > 0 else 0.0,
                "ads_button_rate": button_clicks / ads_clicks if ads_clicks > 0 else 0.0,
                "landing_sessions": funnel_sessions,
                "destinations": destination_rows,
                "final_clicks": final_clicks,
                "session_final_rate": final_clicks / funnel_sessions if funnel_sessions else 0.0,
                "revenue": period_revenue,
                "gam_impressions": period_gam_impressions,
                "ecpm": period_revenue / period_gam_impressions * 1000.0 if period_gam_impressions else 0.0,
                "rps": period_revenue / funnel_sessions if funnel_sessions else 0.0,
                "journey_stages": stage_rows,
                "journey_columns": journey_columns,
                "primary_entry_path": primary_entry_path,
                "routes": route_rows,
                "bottleneck": bottleneck,
                "recommendation": recommendation,
                "daily": daily_rows,
                "event_coverage": len(exact_sessions & official_session_ids) / funnel_sessions if funnel_sessions else 0.0,
            }
    return {
        "project": project,
        "attr_id": str(attr_id),
        "campaign": campaign,
        "landing_path": landing_path,
        "tracking_first_seen_at": str(coverage["first_seen_at"] or "") if coverage else "",
        "tracking_last_seen_at": str(coverage["last_seen_at"] or "") if coverage else "",
        "periods": result_periods,
    }


def ensure_mgid_attr_mapping_store(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mgid_widget_attr_map (
                domain TEXT NOT NULL,
                widget_id TEXT NOT NULL,
                attr_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'manual',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (domain, widget_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mgid_widget_attr_map_attr ON mgid_widget_attr_map(attr_id, status)")
        conn.commit()


def _mgid_widget_attr_map(runtime: dict[str, Any], project: str) -> dict[tuple[str, str], str]:
    mapping: dict[tuple[str, str], str] = {}
    cfg = (runtime.get("config") or {}).get("mgid") or {}
    for item in cfg.get("widget_attr_map") or []:
        domain = str(item.get("domain") or item.get("site") or "").strip()
        widget_id = str(item.get("widget_id") or item.get("widgetId") or "").strip()
        attr_id = re.sub(r"\D+", "", str(item.get("attr_id") or item.get("attrId") or ""))
        if domain and widget_id and attr_id:
            mapping[(domain, widget_id)] = attr_id
    db_path = Path(runtime["db_path"])
    if db_path.exists():
        try:
            ensure_mgid_attr_mapping_store(db_path)
            with sqlite3.connect(db_path, timeout=20) as conn:
                conn.row_factory = sqlite3.Row
                for row in conn.execute("SELECT domain, widget_id, attr_id FROM mgid_widget_attr_map WHERE status='ACTIVE' AND attr_id != ''"):
                    domain = str(row["domain"] or "").strip()
                    widget_id = str(row["widget_id"] or "").strip()
                    attr_id = re.sub(r"\D+", "", str(row["attr_id"] or ""))
                    if domain and widget_id and attr_id:
                        mapping[(domain, widget_id)] = attr_id
        except Exception:
            pass
    return mapping


def _mgid_revenue_by_attr_periods(runtime: dict[str, Any], current_day: date, project: str) -> dict[str, dict[str, float]]:
    month_start = current_day.replace(day=1)
    yesterday = current_day - timedelta(days=1)
    seven_day_start = current_day - timedelta(days=6)
    history_start = min(month_start, yesterday, seven_day_start)
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    rows = read_mgid_rows(runtime["db_path"], history_start.isoformat(), current_day.isoformat())
    widget_map = _mgid_widget_attr_map(runtime, project)
    daily: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        try:
            row_day = date.fromisoformat(str(row.get("day", "") or "1900-01-01")[:10])
        except Exception:
            continue
        if row_day < history_start or row_day > current_day:
            continue
        domain = str(row.get("domain", "") or "").strip()
        row_project = str(site_to_project.get(domain) or runtime.get("site_mapping", {}).get(domain) or domain).strip()
        if row_project != project:
            continue
        attr_id = re.sub(r"\D+", "", str(row.get("sub_id") or ""))
        if not attr_id:
            attr_id = widget_map.get((domain, str(row.get("widget_id") or "").strip()), "")
        if not attr_id:
            continue
        daily[attr_id][row_day] += _to_float(row.get("revenue"))
    return _period_totals_from_daily(daily, current_day)


def _push_revenue_by_attr_periods(runtime: dict[str, Any], current_day: date, project: str) -> dict[str, dict[str, float]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    month_start = current_day.replace(day=1)
    yesterday = current_day - timedelta(days=1)
    seven_day_start = current_day - timedelta(days=6)
    history_start = min(month_start, yesterday, seven_day_start)
    daily: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    try:
        ensure_push_control_store(db_path)
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT m.day, m.revenue, l.acquisition_attr_id
                FROM push_message_daily_metrics m
                JOIN push_acquisition_links l ON l.message_id = m.message_id AND l.project_id = m.project_id
                WHERE m.project_id = ?
                  AND m.day BETWEEN ? AND ?
                  AND l.link_method = 'direct_gam_attr_id'
                  AND l.acquisition_attr_id != ''
                """,
                (project, history_start.isoformat(), current_day.isoformat()),
            ).fetchall()
    except Exception:
        return {}
    for row in rows:
        try:
            row_day = date.fromisoformat(str(row["day"] or "")[:10])
        except Exception:
            continue
        attr_id = re.sub(r"\D+", "", str(row["acquisition_attr_id"] or ""))
        if not attr_id:
            continue
        daily[attr_id][row_day] += _to_float(row["revenue"])
    return _period_totals_from_daily(daily, current_day)


def _campaign_ecpm_with_aux(row_ecpm: Any, adsense_revenue: Any, total_revenue: Any) -> float:
    old_ecpm = _to_float(row_ecpm)
    adsense = _to_float(adsense_revenue)
    total = _to_float(total_revenue)
    if old_ecpm <= 0 or adsense <= 0 or total <= 0:
        return old_ecpm
    impressions = adsense / old_ecpm * 1000.0
    if impressions <= 0:
        return old_ecpm
    return total / impressions * 1000.0


def _build_push_reconciliation(
    runtime: dict[str, Any],
    current_day: date,
    gam_attr_rows: list[Any],
    allowed_projects: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    today_iso = current_day.isoformat()
    month_start = current_day.replace(day=1).isoformat()
    site_to_project, project_to_sites = _site_project_maps(runtime)
    reconciliation: dict[str, dict[str, Any]] = {}

    def ensure(project_id: str) -> dict[str, Any]:
        return reconciliation.setdefault(
            project_id,
            {
                "receita_push_hoje": 0.0,
                "receita_push_mes": 0.0,
                "fontes_push": set(),
                "hosts_push": set(),
                "campanhas_push": set(),
                "utm_mediums_push": set(),
                "sitios_projeto": sorted(project_to_sites.get(project_id, set())),
            },
        )

    for row in gam_attr_rows:
        site = str(row.site or "").strip()
        project_id = site_to_project.get(site) or str(runtime.get("site_mapping", {}).get(site, "") or "").strip() or site
        if allowed_projects and project_id not in allowed_projects:
            continue
        channel, params = _classify_push_channel(site, row.attr_raw)
        if not channel:
            continue
        revenue = _net_gam_revenue(row.revenue)
        if revenue <= 0:
            continue
        bucket = ensure(project_id)
        bucket["hosts_push"].add(site)
        bucket["fontes_push"].add(str(params.get("utm_source", "") or channel.split("/", 1)[-1]))
        bucket["utm_mediums_push"].add(str(params.get("utm_medium", "") or ""))
        campaign_name = str(params.get("utm_campaign", "") or "")
        if campaign_name:
            bucket["campanhas_push"].add(campaign_name)
        if str(row.day or "") == today_iso:
            bucket["receita_push_hoje"] += revenue
        if month_start <= str(row.day or "") <= today_iso:
            bucket["receita_push_mes"] += revenue

    for project_id, bucket in reconciliation.items():
        bucket["fontes_push"] = sorted(item for item in bucket["fontes_push"] if item)
        bucket["hosts_push"] = sorted(item for item in bucket["hosts_push"] if item)
        bucket["utm_mediums_push"] = sorted(item for item in bucket["utm_mediums_push"] if item)
        bucket["campanhas_push"] = sorted(item for item in bucket["campanhas_push"] if item)
    return reconciliation


def _gam_pmr_by_attr_periods(
    runtime: dict[str, Any],
    current_day: date,
    project: str,
) -> dict[str, dict[str, float]]:
    site_to_project, project_to_sites = _site_project_maps(runtime)
    sites = sorted(project_to_sites.get(project, set()) or {project})
    if not sites:
        return {}

    periods = {
        "hoje": (current_day, current_day),
        "ontem": (current_day - timedelta(days=1), current_day - timedelta(days=1)),
        "7d": (current_day - timedelta(days=6), current_day),
        "mes": (current_day.replace(day=1), current_day),
    }
    placeholders = ",".join("?" for _ in sites)
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}

    result: dict[str, dict[str, float]] = {}
    aliases = _load_attr_aliases(runtime)
    with sqlite3.connect(db_path, timeout=30) as conn:
        has_attr_rate = conn.execute(
            "select 1 from sqlite_master where type='table' and name='gam_attr_rate_daily'"
        ).fetchone() is not None
        for period_key, (start_day, end_day) in periods.items():
            if has_attr_rate:
                period_has_attr_rate = False
                rate_query = (
                    "select attr_id, sum(match_rate * impressions), sum(impressions) "
                    "from gam_attr_rate_daily "
                    "where day between ? and ? and match_rate > 0 "
                    "group by attr_id"
                )
                for attr_id, rate_sum, rate_weight in conn.execute(
                    rate_query,
                    (start_day.isoformat(), end_day.isoformat()),
                ):
                    normalized_attr = aliases.get(("", str(attr_id or "").strip())) or str(attr_id)
                    weight = _to_float(rate_weight)
                    if weight > 0:
                        result.setdefault(str(normalized_attr), {})[period_key] = _to_float(rate_sum) / weight
                        period_has_attr_rate = True
                if period_has_attr_rate:
                    continue
            query = (
                "select attr_id, sum(impressions), sum(unfilled_impressions), sum(total_requests) "
                "from gam_attr_daily "
                f"where site in ({placeholders}) and day between ? and ? "
                "group by attr_id"
            )
            params = [*sites, start_day.isoformat(), end_day.isoformat()]
            for attr_id, impressions, unfilled, total_requests in conn.execute(query, params):
                attr_id = aliases.get(("", str(attr_id or "").strip())) or aliases.get((str(sites[0] if len(sites) == 1 else ""), str(attr_id or "").strip())) or str(attr_id)
                filled = _to_float(impressions)
                unfilled_count = _to_float(unfilled)
                requests = _to_float(total_requests)
                denominator = requests if requests > 0 else filled + unfilled_count
                pmr = filled / denominator if denominator > 0 else 0.0
                result.setdefault(str(attr_id), {})[period_key] = pmr
    return result


def _gam_delivery_by_project_periods(
    runtime: dict[str, Any],
    current_day: date,
    gam_attr_rows: list[Any],
    allowed_projects: set[str] | None = None,
) -> dict[str, dict[str, dict[str, float]]]:
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    periods = {
        "Hoje": (current_day, current_day),
        "Ontem": (current_day - timedelta(days=1), current_day - timedelta(days=1)),
        "Ultimos 7 Dias": (current_day - timedelta(days=6), current_day),
        "Mes": (current_day.replace(day=1), current_day),
    }
    totals: dict[str, dict[str, dict[str, float]]] = {}
    for row in gam_attr_rows:
        try:
            row_day = date.fromisoformat(str(row.day or ""))
        except ValueError:
            continue
        site = str(row.site or "").strip()
        project_id = site_to_project.get(site) or str(runtime.get("site_mapping", {}).get(site, "") or "").strip() or site
        if allowed_projects and project_id not in allowed_projects:
            continue
        for period_key, (start_day, end_day) in periods.items():
            if not (start_day <= row_day <= end_day):
                continue
            bucket = totals.setdefault(project_id, {}).setdefault(
                period_key,
                {"revenue": 0.0, "impressions": 0.0, "unfilled": 0.0, "requests": 0.0},
            )
            bucket["revenue"] += _net_gam_revenue(row.revenue)
            bucket["impressions"] += _to_float(row.impressions)
            bucket["unfilled"] += _to_float(getattr(row, "unfilled_impressions", 0))
            bucket["requests"] += _to_float(getattr(row, "total_requests", 0))

    result: dict[str, dict[str, dict[str, float]]] = {}
    for project_id, period_totals in totals.items():
        for period_key, values in period_totals.items():
            impressions = values["impressions"]
            denominator = values["requests"] if values["requests"] > 0 else impressions + values["unfilled"]
            pmr = impressions / denominator if denominator > 0 else 0.0
            ecpm = (values["revenue"] / impressions * 1000.0) if impressions > 0 else 0.0
            result.setdefault(project_id, {})[period_key] = {
                "pmr": pmr,
                "ecpm": ecpm,
            }
    return result


def _divergence_report(project_row: dict[str, Any], push_data: dict[str, Any]) -> str:
    receita_api = _to_float(project_row.get("Receita Hoje API"))
    receita_campanhas = _to_float(project_row.get("Receita Hoje Campanhas"))
    receita_push = _to_float(push_data.get("receita_push_hoje"))
    fontes = ", ".join(push_data.get("fontes_push") or []) or "sem fonte push confirmada"
    hosts = ", ".join(push_data.get("hosts_push") or []) or "sem host push"
    campanhas = ", ".join((push_data.get("campanhas_push") or [])[:3]) or "sem campanha push catalogada"
    return (
        f"Receita oficial hoje {_to_float(receita_api):.2f} x leitura operacional {_to_float(receita_campanhas):.2f}. "
        f"Push confirmado hoje {_to_float(receita_push):.2f}. "
        f"Fontes push: {fontes}. Hosts push: {hosts}. Campanhas exemplo: {campanhas}."
    )


def _project_campaign_analysis(
    runtime: dict[str, Any],
    current_day: date,
    allowed_projects: set[str] | None = None,
) -> dict[str, list[list[Any]]]:
    allowed_key = tuple(sorted(allowed_projects)) if allowed_projects else ()
    return _runtime_cache_get(
        runtime,
        ("project_campaign_analysis", current_day.isoformat(), allowed_key),
        lambda: _project_campaign_analysis_uncached(runtime, current_day, allowed_projects),
    )


def _project_campaign_analysis_uncached(
    runtime: dict[str, Any],
    current_day: date,
    allowed_projects: set[str] | None = None,
) -> dict[str, list[list[Any]]]:
    month_start = current_day.replace(day=1).isoformat()
    yesterday = (current_day - timedelta(days=1)).isoformat()
    seven_day_start = (current_day - timedelta(days=6)).isoformat()
    history_start = min(month_start, yesterday, seven_day_start)
    month_end = current_day.isoformat()
    ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(runtime, history_start, month_end)
    return project_campaign_analysis_rows(
        ads_rows,
        gam_attr_rows,
        None,
        runtime["site_mapping"],
        current_day,
        runtime["dimension_overrides"],
        allowed_projects=allowed_projects,
    )



def _google_ads_script_freshness_by_project(runtime: dict[str, Any]) -> dict[str, dict[str, Any]]:
    cfg = (runtime.get("config") or {}).get("google_ads_script") or {}
    if not cfg.get("enabled"):
        return {}
    max_stale = max(180, int(_to_float(cfg.get("max_stale_minutes") or 15) or 15))
    customer_to_project: dict[str, str] = {}
    for item in ((runtime.get("config") or {}).get("google_ads") or {}).get("customers") or []:
        customer_id = re.sub(r"\D+", "", str(item.get("customer_id") or ""))
        site = str(item.get("site") or item.get("name") or "").strip()
        if str(item.get("status") or "").lower() == "suspended" or bool(item.get("ads_actions_blocked")):
            continue
        if customer_id and site:
            customer_to_project[customer_id] = site
    out: dict[str, dict[str, Any]] = {}
    now = datetime.now(APP_TZ)
    try:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            if not conn.execute("select 1 from sqlite_master where type='table' and name='google_ads_script_runs'").fetchone():
                return {}
            rows = conn.execute("""
                select r.customer_id,
                       max(r.account) as account,
                       max(r.created_at) as last_run,
                       max(s.updated_at) as last_settings_at,
                       count(s.campaign_id) as rows_today
                from google_ads_script_runs r
                left join google_ads_campaign_settings_daily s
                  on s.customer_id = r.customer_id and s.day = ?
                where r.status = 'success' and r.customer_id != ''
                group by r.customer_id
            """, (app_today().isoformat(),)).fetchall()
    except Exception:
        return {}
    for row in rows:
        customer_id = re.sub(r"\D+", "", str(row["customer_id"] or ""))
        if customer_id not in customer_to_project:
            continue
        project = customer_to_project.get(customer_id) or find_site(str(row["account"] or ""), runtime.get("site_mapping") or {})
        if not project or project == "SEM_MAPEAMENTO":
            continue
        last_run = str(row["last_run"] or "")
        last_settings_at = str(row["last_settings_at"] or "")
        last_seen = max([value for value in (last_run, last_settings_at) if value] or [""])
        try:
            parsed = datetime.fromisoformat(last_seen)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=APP_TZ)
            parsed = parsed.astimezone(APP_TZ)
            age = max(0, int((now - parsed).total_seconds() // 60))
        except Exception:
            age = 999999
        rows_today = int(_to_float(row["rows_today"]))
        status = "stale" if rows_today <= 0 or age > max_stale else "fresh"
        out[project] = {"last_run": last_run, "last_settings_at": last_settings_at, "last_seen": last_seen, "age_minutes": age, "status": status, "max_stale_minutes": max_stale, "rows_today": rows_today, "source": "apps_script"}
    try:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            if conn.execute("select 1 from sqlite_master where type='table' and name='ads_daily'").fetchone():
                rows = conn.execute(
                    """
                    select customer_id, site, sum(cost) as cost
                    from ads_daily
                    where day = ?
                    group by customer_id, site
                    """,
                    (app_today().isoformat(),),
                ).fetchall()
                for row in rows:
                    customer_id = re.sub(r"\D+", "", str(row["customer_id"] or ""))
                    project = customer_to_project.get(customer_id) or str(row["site"] or "").strip()
                    if not project or project == "SEM_MAPEAMENTO":
                        continue
                    if abs(_to_float(row["cost"])) <= 0.000001:
                        continue
                    if project not in out:
                        out[project] = {"last_run": "", "last_settings_at": "", "last_seen": "", "age_minutes": 999999, "status": "stale", "max_stale_minutes": max_stale, "rows_today": 0, "source": "apps_script_missing"}
    except Exception:
        pass
    return out

def load_project_kpis(
    runtime: dict[str, Any],
    today: date | None = None,
    operational: bool = False,
) -> list[dict[str, Any]]:
    current_day = today or app_today()
    return _runtime_cache_get(
        runtime,
        ("project_kpis", current_day.isoformat(), operational),
        lambda: _load_project_kpis_uncached(runtime, current_day, operational),
    )


def _load_project_kpis_uncached(
    runtime: dict[str, Any],
    current_day: date,
    operational: bool = False,
) -> list[dict[str, Any]]:
    month_start = current_day.replace(day=1).isoformat()
    yesterday = (current_day - timedelta(days=1)).isoformat()
    seven_day_start = (current_day - timedelta(days=6)).isoformat()
    history_start = min(month_start, yesterday, seven_day_start)
    month_end = current_day.isoformat()
    ads_rows, gam_site_rows, gam_attr_rows = _load_period_rows(runtime, history_start, month_end, operational=operational)
    fact_rows = build_fact_rows(ads_rows, gam_site_rows, runtime["site_mapping"], runtime["dimension_overrides"])
    allowed_projects = _allowed_projects(runtime, current_day)
    values = build_project_kpi_summary(fact_rows, current_day, allowed_projects=allowed_projects)
    rows = [row for row in _project_kpi_dicts(values) if str(row.get("Projeto", "") or "").strip().upper() != "TOTAL GERAL"]
    present_projects = {str(row.get("Projeto", "") or "").strip() for row in rows}
    for project in sorted(allowed_projects - present_projects):
        rows.append({
            "Projeto": project,
            "Sites Considerados": project,
            "Blocos eCPM": "",
            "Investimento Hoje": 0.0,
            "Receita Hoje": 0.0,
            "ROI Hoje": 0.0,
            "eCPM Hoje": 0.0,
            "Taxa de correspondencia do Ad Exchange Hoje": 0.0,
            "Investimento Ontem": 0.0,
            "Receita Ontem": 0.0,
            "ROI Ontem": 0.0,
            "eCPM Ontem": 0.0,
            "Taxa de correspondencia do Ad Exchange Ontem": 0.0,
            "Investimento Ultimos 7 Dias": 0.0,
            "Receita Ultimos 7 Dias": 0.0,
            "ROI Ultimos 7 Dias": 0.0,
            "eCPM Ultimos 7 Dias": 0.0,
            "Taxa de correspondencia do Ad Exchange Ultimos 7 Dias": 0.0,
            "Investimento Mes": 0.0,
            "Receita Mes": 0.0,
            "ROI Mes": 0.0,
            "eCPM Mes": 0.0,
            "Taxa de correspondencia do Ad Exchange Mes": 0.0,
            "Validacao Dados": "aguardando_primeiras_metricas",
        })
    ads_script_freshness = _google_ads_script_freshness_by_project(runtime)
    campaign_analysis = _project_campaign_analysis(runtime, current_day, allowed_projects=allowed_projects)
    push_reconciliation = _build_push_reconciliation(runtime, current_day, gam_attr_rows, allowed_projects=allowed_projects)
    gam_delivery = _gam_delivery_by_project_periods(runtime, current_day, gam_attr_rows, allowed_projects=allowed_projects)
    mgid_revenue = _mgid_revenue_by_project_periods(runtime, current_day, allowed_projects=allowed_projects)
    campaign_summaries: dict[str, dict[str, float]] = {}
    for project, project_rows in campaign_analysis.items():
        summary = {
            "investimento_hoje": 0.0,
            "receita_hoje": 0.0,
            "roi_hoje": 0.0,
            "investimento_ontem": 0.0,
            "receita_ontem": 0.0,
            "roi_ontem": 0.0,
            "investimento_7d": 0.0,
            "receita_7d": 0.0,
            "roi_7d": 0.0,
            "investimento_mes": 0.0,
            "receita_mes": 0.0,
            "roi_mes": 0.0,
        }
        for campaign_row in project_rows:
            summary["investimento_hoje"] += _to_float(campaign_row[9])
            summary["receita_hoje"] += _to_float(campaign_row[10])
            summary["investimento_ontem"] += _to_float(campaign_row[14])
            summary["receita_ontem"] += _to_float(campaign_row[15])
            summary["investimento_7d"] += _to_float(campaign_row[19])
            summary["receita_7d"] += _to_float(campaign_row[20])
            summary["investimento_mes"] += _to_float(campaign_row[24])
            summary["receita_mes"] += _to_float(campaign_row[25])
        for period in ("hoje", "ontem", "7d", "mes"):
            spend = summary[f"investimento_{period}"]
            revenue = summary[f"receita_{period}"]
            summary[f"roi_{period}"] = ((revenue - spend) / spend) if spend > 0 else 0.0
        campaign_summaries[project] = summary
    for row in rows:
        project = str(row.get("Projeto", "") or "")
        gam_status = _gam_sync_status_for_project(runtime, project, current_day)
        row["GAM Snapshot Status"] = gam_status.get("status", "")
        row["GAM Snapshot Message"] = gam_status.get("message", "")
        gam_today_unavailable = gam_status.get("status") != "ok"
        freshness = ads_script_freshness.get(project) or {}
        if freshness:
            row["Google Ads Snapshot At"] = freshness.get("last_run", "")
            row["Google Ads Snapshot Age Minutes"] = freshness.get("age_minutes", 0)
            row["Google Ads Snapshot Status"] = freshness.get("status", "")
            if freshness.get("status") == "stale":
                row["Validacao Dados"] = "ads_snapshot_desatualizado"
                row["Laudo Divergencia"] = f"Google Ads Apps Script desatualizado: ultimo snapshot ha {freshness.get('age_minutes')} min. Atualize o script antes de confiar em investimento/ROI."
        campaign_summary = campaign_summaries.get(project) or {}
        if not campaign_summary:
            row["Validacao Dados"] = "sem_resumo_campanhas"
            continue
        row["Investimento Hoje API"] = row.get("Investimento Hoje", 0)
        row["Receita Hoje API"] = row.get("Receita Hoje", 0)
        row["ROI Hoje API"] = row.get("ROI Hoje", 0)
        row["Investimento Ontem API"] = row.get("Investimento Ontem", 0)
        row["Receita Ontem API"] = row.get("Receita Ontem", 0)
        row["ROI Ontem API"] = row.get("ROI Ontem", 0)
        row["Investimento 7 Dias API"] = row.get("Investimento Ultimos 7 Dias", 0)
        row["Receita 7 Dias API"] = row.get("Receita Ultimos 7 Dias", 0)
        row["ROI 7 Dias API"] = row.get("ROI Ultimos 7 Dias", 0)
        row["Investimento Mes API"] = row.get("Investimento Mes", 0)
        row["Receita Mes API"] = row.get("Receita Mes", 0)
        row["ROI Mes API"] = row.get("ROI Mes", 0)
        for period_label in ("Hoje", "Ontem", "Ultimos 7 Dias", "Mes"):
            delivery = (gam_delivery.get(project) or {}).get(period_label) or {}
            if delivery:
                row[f"Taxa de correspondencia do Ad Exchange {period_label}"] = delivery.get("pmr", 0.0)
                row[f"eCPM {period_label}"] = delivery.get("ecpm", 0.0)
        revenue_gap = abs(_to_float(row["Receita Hoje API"]) - _to_float(campaign_summary["receita_hoje"]))
        spend_gap = abs(_to_float(row["Investimento Hoje API"]) - _to_float(campaign_summary["investimento_hoje"]))
        if gam_today_unavailable:
            row["Validacao Dados"] = "gam_snapshot_desatualizado"
            row["GAM Hoje Indisponivel"] = "sim"
            row["Laudo Divergencia"] = gam_status.get("message", "Sem snapshot GAM hoje.")
        elif row.get("Google Ads Snapshot Status") == "stale":
            row["Validacao Dados"] = "ads_snapshot_desatualizado"
        else:
            row["Validacao Dados"] = "divergente" if (revenue_gap > 0.01 or spend_gap > 0.01) else "ok"
        row["Receita Hoje Campanhas"] = campaign_summary["receita_hoje"]
        row["Investimento Hoje Campanhas"] = campaign_summary["investimento_hoje"]
        row["ROI Hoje Campanhas"] = campaign_summary["roi_hoje"]
        row["Receita Ontem Campanhas"] = campaign_summary["receita_ontem"]
        row["Investimento Ontem Campanhas"] = campaign_summary["investimento_ontem"]
        row["ROI Ontem Campanhas"] = campaign_summary["roi_ontem"]
        row["Receita 7 Dias Campanhas"] = campaign_summary["receita_7d"]
        row["Investimento 7 Dias Campanhas"] = campaign_summary["investimento_7d"]
        row["ROI 7 Dias Campanhas"] = campaign_summary["roi_7d"]
        row["Receita Mes Campanhas"] = campaign_summary["receita_mes"]
        row["Investimento Mes Campanhas"] = campaign_summary["investimento_mes"]
        row["ROI Mes Campanhas"] = campaign_summary["roi_mes"]
        row["Investimento Hoje"] = campaign_summary["investimento_hoje"]
        row["Receita Hoje"] = max(_to_float(row.get("Receita Hoje API")), campaign_summary["receita_hoje"])
        row["ROI Hoje"] = ((row["Receita Hoje"] - row["Investimento Hoje"]) / row["Investimento Hoje"]) if row["Investimento Hoje"] > 0 else 0.0
        row["Investimento Ontem"] = campaign_summary["investimento_ontem"]
        row["Receita Ontem"] = max(_to_float(row.get("Receita Ontem API")), campaign_summary["receita_ontem"])
        row["ROI Ontem"] = ((row["Receita Ontem"] - row["Investimento Ontem"]) / row["Investimento Ontem"]) if row["Investimento Ontem"] > 0 else 0.0
        row["Investimento Ultimos 7 Dias"] = campaign_summary["investimento_7d"]
        row["Receita Ultimos 7 Dias"] = max(_to_float(row.get("Receita 7 Dias API")), campaign_summary["receita_7d"])
        row["ROI Ultimos 7 Dias"] = ((row["Receita Ultimos 7 Dias"] - row["Investimento Ultimos 7 Dias"]) / row["Investimento Ultimos 7 Dias"]) if row["Investimento Ultimos 7 Dias"] > 0 else 0.0
        row["Investimento Mes"] = campaign_summary["investimento_mes"]
        row["Receita Mes"] = max(_to_float(row.get("Receita Mes API")), campaign_summary["receita_mes"])
        row["ROI Mes"] = ((row["Receita Mes"] - row["Investimento Mes"]) / row["Investimento Mes"]) if row["Investimento Mes"] > 0 else 0.0
        push_data = push_reconciliation.get(project) or {}
        mgid_data = mgid_revenue.get(project) or {}
        receita_push_hoje = _to_float(push_data.get("receita_push_hoje"))
        receita_push_mes = _to_float(push_data.get("receita_push_mes"))
        receita_mgid_hoje = _to_float(mgid_data.get("hoje"))
        receita_mgid_ontem = _to_float(mgid_data.get("ontem"))
        receita_mgid_7d = _to_float(mgid_data.get("7d"))
        receita_mgid_mes = _to_float(mgid_data.get("mes"))
        receita_total_hoje = _to_float(row["Receita Hoje"]) + receita_mgid_hoje
        receita_total_ontem = _to_float(row["Receita Ontem"]) + receita_mgid_ontem
        receita_total_7d = _to_float(row["Receita Ultimos 7 Dias"]) + receita_mgid_7d
        receita_total_mes = _to_float(row["Receita Mes"]) + receita_mgid_mes
        row["Receita MGID Hoje"] = receita_mgid_hoje
        row["Receita MGID Ontem"] = receita_mgid_ontem
        row["Receita MGID 7 Dias"] = receita_mgid_7d
        row["Receita MGID Mes"] = receita_mgid_mes
        row["Receita Push Hoje"] = receita_push_hoje
        row["Receita Push Mes"] = receita_push_mes
        row["Receita Main Hoje"] = max(_to_float(row["Receita Hoje"]) - receita_push_hoje, 0.0)
        row["Receita Main Mes"] = max(_to_float(row["Receita Mes"]) - receita_push_mes, 0.0)
        row["Receita Hoje"] = receita_total_hoje
        row["Receita Ontem"] = receita_total_ontem
        row["Receita Ultimos 7 Dias"] = receita_total_7d
        row["Receita Mes"] = receita_total_mes
        row["ROI Hoje"] = ((receita_total_hoje - _to_float(row["Investimento Hoje"])) / _to_float(row["Investimento Hoje"])) if _to_float(row["Investimento Hoje"]) > 0 else 0.0
        row["ROI Ontem"] = ((receita_total_ontem - _to_float(row["Investimento Ontem"])) / _to_float(row["Investimento Ontem"])) if _to_float(row["Investimento Ontem"]) > 0 else 0.0
        row["ROI Ultimos 7 Dias"] = ((receita_total_7d - _to_float(row["Investimento Ultimos 7 Dias"])) / _to_float(row["Investimento Ultimos 7 Dias"])) if _to_float(row["Investimento Ultimos 7 Dias"]) > 0 else 0.0
        row["ROI Mes"] = ((receita_total_mes - _to_float(row["Investimento Mes"])) / _to_float(row["Investimento Mes"])) if _to_float(row["Investimento Mes"]) > 0 else 0.0
        row["Receita Total Consolidada Hoje"] = receita_total_hoje
        row["Receita Total Consolidada Mes"] = receita_total_mes
        row["Fontes Push"] = ", ".join(push_data.get("fontes_push") or [])
        row["Hosts Push"] = ", ".join(push_data.get("hosts_push") or [])
        row["Campanhas Push Exemplo"] = ", ".join((push_data.get("campanhas_push") or [])[:3])
        row["Laudo Divergencia"] = _divergence_report(row, push_data)
    metric_overrides: dict[tuple[str, str], dict[str, float]] = {}
    db_path = Path(runtime["db_path"])
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS project_metric_overrides (project TEXT NOT NULL, day TEXT NOT NULL, investment REAL, revenue REAL, reason TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (project, day))")
                for override_row in conn.execute("SELECT project, day, investment, revenue FROM project_metric_overrides"):
                    metric_overrides[(str(override_row[0]), str(override_row[1]))] = {
                        "investment": _to_float(override_row[2]),
                        "revenue": _to_float(override_row[3]),
                    }
        except Exception:
            metric_overrides = {}
    if metric_overrides:
        period_days = {"Hoje": current_day.isoformat(), "Ontem": yesterday}
        for row in rows:
            project_id = str(row.get("Projeto", "") or "").strip()
            for period_label, day_key in period_days.items():
                override = metric_overrides.get((project_id, day_key))
                if not override:
                    continue
                investment = _to_float(override.get("investment"))
                revenue = _to_float(override.get("revenue"))
                if investment > 0:
                    row[f"Investimento {period_label}"] = investment
                if revenue > 0:
                    row[f"Receita {period_label}"] = revenue
                spend = _to_float(row.get(f"Investimento {period_label}"))
                rec = _to_float(row.get(f"Receita {period_label}"))
                row[f"ROI {period_label}"] = ((rec - spend) / spend) if spend > 0 else 0.0
                row[f"Ajuste Manual {period_label}"] = "sim"
    rows.sort(key=lambda row: _to_float(row.get("Investimento Hoje")), reverse=True)
    return rows


def load_dashboard_summary(
    runtime: dict[str, Any],
    today: date | None = None,
    operational: bool = False,
) -> dict[str, Any]:
    rows = load_project_kpis(runtime, today, operational=operational)
    rows = [
        row for row in rows
        if any(_to_float(row.get(key)) > 0 for key in (
            "Investimento Hoje",
            "Investimento Ontem",
            "Investimento Ultimos 7 Dias",
            "Investimento Mes",
        ))
    ]
    summary = {
        "total_projects": len(rows),
        "investimento_hoje": 0.0,
        "receita_hoje": 0.0,
        "roi_hoje": 0.0,
        "investimento_ontem": 0.0,
        "receita_ontem": 0.0,
        "roi_ontem": 0.0,
        "investimento_7d": 0.0,
        "receita_7d": 0.0,
        "roi_7d": 0.0,
        "investimento_mes": 0.0,
        "receita_mes": 0.0,
        "roi_mes": 0.0,
        "updated_at": load_last_updated(runtime),
        "gam_hoje_indisponivel": False,
        "gam_hoje_message": "",
    }
    for row in rows:
        summary["investimento_hoje"] += _to_float(row.get("Investimento Hoje"))
        summary["receita_hoje"] += _to_float(row.get("Receita Hoje"))
        summary["investimento_ontem"] += _to_float(row.get("Investimento Ontem"))
        summary["receita_ontem"] += _to_float(row.get("Receita Ontem"))
        summary["investimento_7d"] += _to_float(row.get("Investimento Ultimos 7 Dias"))
        summary["receita_7d"] += _to_float(row.get("Receita Ultimos 7 Dias"))
        summary["investimento_mes"] += _to_float(row.get("Investimento Mes"))
        summary["receita_mes"] += _to_float(row.get("Receita Mes"))
        if str(row.get("GAM Hoje Indisponivel") or "").lower() == "sim" and _to_float(row.get("Investimento Hoje")) > 0:
            summary["gam_hoje_indisponivel"] = True
            if not summary["gam_hoje_message"]:
                summary["gam_hoje_message"] = str(row.get("GAM Snapshot Message") or "GAM sem snapshot hoje.")
    summary["roi_hoje"] = (
        (summary["receita_hoje"] - summary["investimento_hoje"]) / summary["investimento_hoje"]
        if summary["investimento_hoje"] > 0
        else 0.0
    )
    summary["roi_ontem"] = (
        (summary["receita_ontem"] - summary["investimento_ontem"]) / summary["investimento_ontem"]
        if summary["investimento_ontem"] > 0
        else 0.0
    )
    summary["roi_7d"] = (
        (summary["receita_7d"] - summary["investimento_7d"]) / summary["investimento_7d"]
        if summary["investimento_7d"] > 0
        else 0.0
    )
    summary["roi_mes"] = (
        (summary["receita_mes"] - summary["investimento_mes"]) / summary["investimento_mes"]
        if summary["investimento_mes"] > 0
        else 0.0
    )
    return summary



def load_campaign_metric_history(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    metric: str,
    today: date | None = None,
    days: int = 30,
    scope: str = "days",
) -> dict[str, Any]:
    current_day = today or app_today()
    days = max(1, min(int(days or 30), 60))
    start_day = current_day - timedelta(days=days - 1)
    start_iso = start_day.isoformat()
    end_iso = current_day.isoformat()
    metric = str(metric or "").strip().lower()
    allowed_metrics = {
        "investment": ("Investimento", "money"),
        "revenue": ("Receita", "money"),
        "roi": ("ROI", "percent"),
        "pmr": ("PMR GAM", "percent"),
        "ecpm": ("eCPM", "money"),
        "cost_per_conversion": ("Custo/Conv.", "money"),
    }
    if metric not in allowed_metrics:
        metric = "investment"
    db_path = Path(runtime["db_path"])
    if str(scope or "").lower() == "today":
        label, unit = allowed_metrics[metric]
        now_hour = datetime.now(APP_TZ).hour
        points = [{"day": f"{hour:02d}h", "hour": hour, "value": 0.0} for hour in range(now_hour + 1)]
        by_hour = {item["hour"]: item for item in points}
        if db_path.exists():
            aliases = _load_attr_aliases(runtime)
            attr_ids = {str(attr_id or "").strip()}
            for (_site, raw_attr), normalized in aliases.items():
                if str(normalized or "").strip() == str(attr_id or "").strip():
                    attr_ids.add(str(raw_attr or "").strip())
            attr_ids = {item for item in attr_ids if item}
            with sqlite3.connect(db_path, timeout=20) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA busy_timeout=20000")
                has_roi_hourly = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='roi_control_hourly_metrics'").fetchone() is not None
                if has_roi_hourly and attr_ids:
                    placeholders = ",".join("?" for _ in attr_ids)
                    query = f"""
                        SELECT local_hour,
                               SUM(cost) cost,
                               SUM(revenue) revenue,
                               SUM(impressions) impressions,
                               AVG(NULLIF(roi, 0)) avg_roi,
                               AVG(NULLIF(pmr, 0)) avg_pmr,
                               AVG(NULLIF(ecpm, 0)) avg_ecpm,
                               SUM(pmr * CASE WHEN impressions > 0 THEN impressions ELSE 1 END) pmr_weighted,
                               SUM(CASE WHEN impressions > 0 THEN impressions ELSE 1 END) pmr_weight
                        FROM roi_control_hourly_metrics
                        WHERE local_date=? AND project_name=?
                          AND (entity_id IN ({placeholders}) OR campaign_id IN ({placeholders}) OR group_id IN ({placeholders}))
                        GROUP BY local_hour
                    """
                    params = [current_day.isoformat(), project, *attr_ids, *attr_ids, *attr_ids]
                    for row in conn.execute(query, params).fetchall():
                        bucket = by_hour.get(int(row["local_hour"] or 0))
                        if bucket is None:
                            continue
                        cost = _to_float(row["cost"])
                        revenue = _to_float(row["revenue"])
                        impressions = _to_float(row["impressions"])
                        avg_roi = _to_float(row["avg_roi"])
                        avg_pmr = _to_float(row["avg_pmr"])
                        avg_ecpm = _to_float(row["avg_ecpm"])
                        weighted_pmr = (_to_float(row["pmr_weighted"]) / _to_float(row["pmr_weight"])) if _to_float(row["pmr_weight"]) > 0 else 0.0
                        pmr = weighted_pmr or avg_pmr
                        bucket["revenue_hour"] = revenue
                        values = {
                            "investment": cost,
                            "revenue": revenue,
                            "roi": ((revenue - cost) / cost * 100.0) if cost > 0 else (avg_roi * 100.0 if abs(avg_roi) <= 10 else avg_roi),
                            "pmr": (pmr * 100.0) if abs(pmr) <= 1 else pmr,
                            "ecpm": (revenue / impressions * 1000.0) if impressions > 0 else avg_ecpm,
                        }
                        bucket["value"] = values[metric]
                if metric in {"investment", "roi"} and attr_ids:
                    hourly_cost_found = False
                    has_ads_hourly = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ads_hourly'").fetchone() is not None
                    if has_ads_hourly:
                        placeholders = ",".join("?" for _ in attr_ids)
                        query = f"SELECT hour, SUM(cost) cost FROM ads_hourly WHERE day=? AND attr_id IN ({placeholders}) GROUP BY hour"
                        for row in conn.execute(query, [current_day.isoformat(), *attr_ids]).fetchall():
                            bucket = by_hour.get(int(row["hour"] or 0))
                            cost = _to_float(row["cost"])
                            if bucket is not None and cost > 0:
                                hourly_cost_found = True
                                bucket["cost_hour"] = cost
                                if metric == "investment":
                                    bucket["value"] = cost
                                elif metric == "roi":
                                    current_revenue = _to_float(bucket.get("revenue_hour", 0.0))
                                    bucket["value"] = ((current_revenue - cost) / cost * 100.0) if cost > 0 else bucket["value"]
                    if not hourly_cost_found:
                        placeholders = ",".join("?" for _ in attr_ids)
                        has_settings = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_daily'").fetchone() is not None
                        if has_settings and placeholders:
                            row = conn.execute(f"SELECT SUM(cost) cost FROM google_ads_campaign_settings_daily WHERE day=? AND attr_id IN ({placeholders})", [current_day.isoformat(), *attr_ids]).fetchone()
                            fallback_cost = _to_float(row["cost"] if row else 0.0)
                            if fallback_cost > 0 and points:
                                bucket = points[-1]
                                bucket["cost_hour"] = fallback_cost
                                if metric == "investment":
                                    bucket["value"] = fallback_cost
                                elif metric == "roi":
                                    current_revenue = _to_float(bucket.get("revenue_hour", 0.0))
                                    bucket["value"] = ((current_revenue - fallback_cost) / fallback_cost * 100.0) if fallback_cost > 0 else bucket["value"]
        if db_path.exists() and attr_ids and points and metric in {"roi", "pmr", "ecpm"}:
            with sqlite3.connect(db_path, timeout=20) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA busy_timeout=20000")
                has_gam_daily = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gam_attr_daily'").fetchone() is not None
                if has_gam_daily:
                    placeholders = ",".join("?" for _ in attr_ids)
                    row = conn.execute(
                        f"""
                        SELECT SUM(revenue) revenue,
                               SUM(impressions) impressions,
                               SUM(match_rate * impressions) pmr_weighted,
                               SUM(impressions) pmr_weight
                        FROM gam_attr_daily
                        WHERE day=? AND attr_id IN ({placeholders})
                        """,
                        [current_day.isoformat(), *attr_ids],
                    ).fetchone()
                    current_revenue = _net_gam_revenue(row["revenue"] if row else 0.0)
                    impressions = _to_float(row["impressions"] if row else 0.0)
                    pmr_weight = _to_float(row["pmr_weight"] if row else 0.0)
                    pmr = (_to_float(row["pmr_weighted"] if row else 0.0) / pmr_weight) if pmr_weight > 0 else 0.0
                    bucket = points[-1]
                    if metric == "roi" and _to_float(bucket.get("cost_hour")) > 0:
                        cost = _to_float(bucket.get("cost_hour"))
                        bucket["value"] = ((current_revenue - cost) / cost * 100.0) if cost > 0 else bucket["value"]
                    elif metric == "pmr" and not any(_to_float(point["value"]) for point in points):
                        bucket["value"] = pmr * 100.0
                    elif metric == "ecpm" and not any(_to_float(point["value"]) for point in points):
                        bucket["value"] = (current_revenue / impressions * 1000.0) if impressions > 0 else 0.0
        values = [_to_float(point["value"]) for point in points]
        return {"metric": metric, "label": label, "unit": unit, "scope": "today", "period_label": "00h ate agora", "points": points, "summary": {"current": values[-1] if values else 0.0, "average": (sum(values) / len(values)) if values else 0.0, "minimum": min(values) if values else 0.0, "maximum": max(values) if values else 0.0}}
    points = [
        {"day": (start_day + timedelta(days=offset)).isoformat(), "investment": 0.0, "conversions": 0.0, "revenue": 0.0, "impressions": 0.0, "unfilled": 0.0, "requests": 0.0, "pmr_weighted": 0.0, "pmr_weight": 0.0}
        for offset in range(days)
    ]
    by_day = {item["day"]: item for item in points}
    if not db_path.exists():
        label, unit = allowed_metrics[metric]
        return {"metric": metric, "label": label, "unit": unit, "period_label": f"ultimos {days} dias", "points": [{"day": p["day"], "value": 0.0} for p in points]}
    related_projects = _project_family_projects(runtime, project)
    _site_to_project, project_to_sites = _site_project_maps(runtime)
    sites: set[str] = set()
    for related in related_projects:
        sites.add(related)
        sites.update(project_to_sites.get(related, set()))
    sites = {str(site or "").strip() for site in sites if str(site or "").strip()}
    aliases = _load_attr_aliases(runtime)
    attr_ids = {str(attr_id or "").strip()}
    for (_site, raw_attr), normalized in aliases.items():
        if str(normalized or "").strip() == str(attr_id or "").strip():
            attr_ids.add(str(raw_attr or "").strip())
    attr_ids = {item for item in attr_ids if item}
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=20000")
        attr_placeholders = ",".join("?" for _ in attr_ids)
        if attr_placeholders:
            ads_query = "select day, sum(cost) as cost, sum(conversions) as conversions from ads_daily " f"where day between ? and ? and attr_id in ({attr_placeholders}) group by day"
            for row in conn.execute(ads_query, [start_iso, end_iso, *attr_ids]).fetchall():
                bucket = by_day.get(str(row["day"]))
                if bucket is not None:
                    bucket["investment"] = _to_float(row["cost"])
                    bucket["conversions"] = _to_float(row["conversions"])
            has_settings_daily = conn.execute("select 1 from sqlite_master where type='table' and name='google_ads_campaign_settings_daily'").fetchone() is not None
            if has_settings_daily:
                settings_query = "select day, sum(cost) as cost, sum(conversions) as conversions from google_ads_campaign_settings_daily " f"where day between ? and ? and attr_id in ({attr_placeholders}) group by day"
                for row in conn.execute(settings_query, [start_iso, end_iso, *attr_ids]).fetchall():
                    bucket = by_day.get(str(row["day"]))
                    if bucket is not None and _to_float(row["cost"]) > 0:
                        bucket["investment"] = _to_float(row["cost"])
                        bucket["conversions"] = _to_float(row["conversions"])
            has_hourly = conn.execute("select 1 from sqlite_master where type='table' and name='ads_hourly'").fetchone() is not None
            if has_hourly:
                hourly_query = "select day, sum(cost) as cost from ads_hourly " f"where day = ? and attr_id in ({attr_placeholders}) group by day"
                for row in conn.execute(hourly_query, [end_iso, *attr_ids]).fetchall():
                    bucket = by_day.get(str(row["day"]))
                    if bucket is not None and _to_float(row["cost"]) > 0:
                        bucket["investment"] = _to_float(row["cost"])
            site_filter = ""
            params: list[Any] = [start_iso, end_iso, *attr_ids]
            if sites:
                site_placeholders = ",".join("?" for _ in sites)
                site_filter = f" and site in ({site_placeholders})"
                params.extend(sorted(sites))
            gam_query = (
                "select day, sum(revenue) as revenue, sum(impressions) as impressions, "
                "sum(unfilled_impressions) as unfilled, sum(total_requests) as requests, "
                "sum(match_rate * impressions) as pmr_weighted, sum(impressions) as pmr_weight "
                "from gam_attr_daily "
                f"where day between ? and ? and attr_id in ({attr_placeholders}){site_filter} group by day"
            )
            for row in conn.execute(gam_query, params).fetchall():
                bucket = by_day.get(str(row["day"]))
                if bucket is not None:
                    bucket["revenue"] = _net_gam_revenue(row["revenue"])
                    bucket["impressions"] = _to_float(row["impressions"])
                    bucket["unfilled"] = _to_float(row["unfilled"])
                    bucket["requests"] = _to_float(row["requests"])
                    bucket["pmr_weighted"] = _to_float(row["pmr_weighted"])
                    bucket["pmr_weight"] = _to_float(row["pmr_weight"])
    output_points: list[dict[str, Any]] = []
    for item in points:
        investment = _to_float(item["investment"])
        revenue = _to_float(item["revenue"])
        impressions = _to_float(item["impressions"])
        requests = _to_float(item["requests"])
        denominator = requests if requests > 0 else impressions + _to_float(item["unfilled"])
        pmr = (_to_float(item["pmr_weighted"]) / _to_float(item["pmr_weight"])) if _to_float(item["pmr_weight"]) > 0 and _to_float(item["pmr_weighted"]) > 0 else (impressions / denominator if denominator > 0 else 0.0)
        conversions = _to_float(item.get("conversions"))
        values = {
            "investment": investment,
            "revenue": revenue,
            "roi": ((revenue - investment) / investment * 100.0) if investment > 0 else 0.0,
            "pmr": pmr * 100.0,
            "ecpm": (revenue / impressions * 1000.0) if impressions > 0 else 0.0,
            "cost_per_conversion": (investment / conversions) if conversions > 0 else 0.0,
        }
        output_points.append({"day": item["day"], "value": values[metric]})
    values = [_to_float(point["value"]) for point in output_points]
    label, unit = allowed_metrics[metric]
    return {"metric": metric, "label": label, "unit": unit, "period_label": f"ultimos {days} dias", "points": output_points, "summary": {"current": values[-1] if values else 0.0, "average": (sum(values) / len(values)) if values else 0.0, "minimum": min(values) if values else 0.0, "maximum": max(values) if values else 0.0}}


def _ads_campaign_settings_by_campaign(runtime: dict[str, Any], current_day: date) -> dict[str, dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_daily'").fetchone()
            if not exists:
                return {}
            rows = conn.execute(
                """
                SELECT *
                FROM google_ads_campaign_settings_daily
                WHERE day <= ?
                ORDER BY day DESC, updated_at DESC
                """,
                (current_day.isoformat(),),
            ).fetchall()
    except Exception:
        return {}
    output: dict[str, dict[str, Any]] = {}
    campaign_goals: dict[str, dict[str, Any]] = {}
    campaign_ad_group_targets: dict[str, dict[str, Any]] = {}
    exact_goals = _exact_conversion_goals_by_campaign(runtime, current_day)
    for row in rows:
        data = dict(row)
        campaign_id = str(data.get("campaign_id") or "")
        has_goal = bool(_conversion_goal_name_raw(data))
        if campaign_id and has_goal and campaign_id not in campaign_goals:
            campaign_goals[campaign_id] = data
        if campaign_id and str(data.get("ad_group_id") or "").strip() and _to_float(data.get("ad_group_target_cpa")) > 0 and campaign_id not in campaign_ad_group_targets:
            campaign_ad_group_targets[campaign_id] = data
        for key_name in ("ad_group_id", "attr_id", "campaign_id"):
            key_value = str(data.get(key_name) or "")
            current = output.get(key_value)
            if key_value and (current is None or (not _conversion_goal_name_raw(current) and _conversion_goal_name_raw(data))):
                output[key_value] = data
    for campaign_id, ad_group_data in campaign_ad_group_targets.items():
        current = dict(output.get(campaign_id) or ad_group_data)
        current["target_cpa"] = ad_group_data.get("ad_group_target_cpa") or ad_group_data.get("target_cpa") or current.get("target_cpa")
        current["ad_group_target_cpa"] = ad_group_data.get("ad_group_target_cpa") or current.get("ad_group_target_cpa")
        current["campaign_target_cpa"] = ad_group_data.get("campaign_target_cpa") or current.get("campaign_target_cpa")
        current["ad_group_id"] = ad_group_data.get("ad_group_id") or current.get("ad_group_id")
        current["ad_group_name"] = ad_group_data.get("ad_group_name") or current.get("ad_group_name")
        current["updated_at"] = ad_group_data.get("updated_at") or current.get("updated_at")
        output[campaign_id] = current
    for key, data in list(output.items()):
        campaign_id = str(data.get("campaign_id") or "")
        exact_goal = exact_goals.get(campaign_id)
        if exact_goal:
            goal_name = str(exact_goal.get("conversion_goal_name") or "").strip()
            data["primary_conversion_action"] = goal_name
            data["configured_conversion_actions_json"] = json.dumps([goal_name], ensure_ascii=False)
            data["observed_conversion_actions_json"] = data.get("observed_conversion_actions_json") or "[]"
            data["conversion_goal_source"] = "google_ads_conversion_goal_campaign_config"
        campaign_goal = campaign_goals.get(campaign_id)
        if campaign_goal and not _conversion_goal_name_raw(data):
            data["primary_conversion_action"] = campaign_goal.get("primary_conversion_action") or ""
            data["configured_conversion_actions_json"] = campaign_goal.get("configured_conversion_actions_json") or "[]"
            data["observed_conversion_actions_json"] = campaign_goal.get("observed_conversion_actions_json") or "[]"
    manual_goals = _manual_conversion_goals()
    for key, goal in manual_goals.items():
        if key in output:
            output[key]["manual_conversion_goal"] = goal
            if not str(output[key].get("primary_conversion_action") or ""):
                output[key]["primary_conversion_action"] = goal
    return output


def _latest_target_cpa_changes(runtime: dict[str, Any], current_day: date) -> dict[str, dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    changes: dict[str, dict[str, Any]] = {}
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        action_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='campaign_bid_manual_actions'"
        ).fetchone()
        if action_table:
            rows = conn.execute(
                """
                SELECT campaign_id, attr_id, old_value, new_value,
                       COALESCE(NULLIF(verified_at, ''), NULLIF(applied_at, ''), created_at) AS changed_at
                FROM campaign_bid_manual_actions
                WHERE status IN ('applied', 'success')
                  AND old_value > 0 AND new_value > 0
                  AND ABS(old_value - new_value) >= 0.000001
                  AND SUBSTR(COALESCE(NULLIF(verified_at, ''), NULLIF(applied_at, ''), created_at), 1, 10) <= ?
                ORDER BY changed_at DESC, created_at DESC
                """,
                (current_day.isoformat(),),
            ).fetchall()
            for row in rows:
                payload = {
                    "old_source": "anterior",
                    "new_source": "atual",
                    "old_value": round(_to_float(row["old_value"]), 6),
                    "new_value": round(_to_float(row["new_value"]), 6),
                    "changed_at": str(row["changed_at"] or ""),
                }
                for key in (str(row["campaign_id"] or ""), str(row["attr_id"] or "")):
                    if key and key not in changes:
                        changes[key] = payload
        settings_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_daily'"
        ).fetchone()
        if not settings_table:
            return changes
        rows = conn.execute(
            """
            SELECT day, campaign_id, ad_group_id, attr_id,
                   target_cpa, campaign_target_cpa, ad_group_target_cpa, updated_at
            FROM google_ads_campaign_settings_daily
            WHERE day <= ?
            ORDER BY campaign_id, ad_group_id, day DESC, updated_at DESC
            """,
            (current_day.isoformat(),),
        ).fetchall()
    campaigns_with_ad_group_cpa = {
        str(row["campaign_id"] or "")
        for row in rows
        if str(row["ad_group_id"] or "") and _to_float(row["ad_group_target_cpa"]) > 0
    }
    history: dict[str, list[tuple[float, str]]] = {}
    for row in rows:
        campaign_id = str(row["campaign_id"] or "")
        ad_group_id = str(row["ad_group_id"] or "")
        attr_id = str(row["attr_id"] or "")
        if not ad_group_id and campaign_id in campaigns_with_ad_group_cpa:
            continue
        if ad_group_id:
            value = _to_float(row["ad_group_target_cpa"]) or _to_float(row["target_cpa"])
        else:
            value = _to_float(row["campaign_target_cpa"]) or _to_float(row["target_cpa"])
        if value <= 0:
            continue
        keys = [campaign_id]
        if ad_group_id:
            keys.extend([ad_group_id, attr_id])
        for key in keys:
            if not key:
                continue
            values = history.setdefault(key, [])
            if not values or abs(values[-1][0] - value) >= 0.000001:
                values.append((value, str(row["updated_at"] or row["day"] or "")))
    for key, values in history.items():
        if len(values) < 2:
            continue
        manual_change = changes.get(key) or {}
        if manual_change and abs(_to_float(manual_change.get("new_value")) - values[0][0]) < 0.000001:
            continue
        changes[key] = {
            "old_source": "anterior",
            "new_source": "atual",
            "old_value": round(values[1][0], 6),
            "new_value": round(values[0][0], 6),
            "changed_at": values[0][1],
        }
    return changes


def _manual_conversion_goals() -> dict[str, str]:
    return {}


def _conversion_goal_name_raw(settings: dict[str, Any]) -> str:
    primary = str(settings.get("primary_conversion_action") or "").strip()
    if primary and primary != "meta_google_ads":
        return primary
    for key in ("configured_conversion_actions_json", "observed_conversion_actions_json"):
        try:
            values = json.loads(str(settings.get(key) or "[]"))
        except Exception:
            values = []
        for value in values if isinstance(values, list) else []:
            text = str(value or "").strip()
            if text and text != "meta_google_ads":
                return text
    return ""


def _conversion_goal_name(settings: dict[str, Any]) -> str:
    try:
        configured = json.loads(str(settings.get("configured_conversion_actions_json") or "[]"))
    except Exception:
        configured = []
    try:
        observed = json.loads(str(settings.get("observed_conversion_actions_json") or "[]"))
    except Exception:
        observed = []
    if not configured and isinstance(observed, list) and len([item for item in observed if str(item or "").strip()]) > 1:
        return "Todas as conversoes"
    raw = _conversion_goal_name_raw(settings)
    if raw:
        return raw
    manual = str(settings.get("manual_conversion_goal") or "").strip()
    if manual:
        return manual
    campaign = str(settings.get("campaign") or "").lower()
    if "prestamo veraz" in campaign or "pr?stamo veraz" in campaign:
        return "Convers?o prestamo-veraz"
    return "Meta n?o identificada"


def _ads_today_metrics_by_attr(runtime: dict[str, Any], current_day: date, project: str) -> dict[str, dict[str, float]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    out: dict[str, dict[str, float]] = {}
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT attr_id, SUM(cost) AS cost, SUM(conversions) AS conversions, SUM(clicks) AS clicks FROM ads_daily WHERE day = ? AND site = ? GROUP BY attr_id", (current_day.isoformat(), project)).fetchall()
    except Exception:
        return {}
    for row in rows:
        attr_id = str(row["attr_id"] or "").strip()
        if not attr_id:
            continue
        cost = _to_float(row["cost"])
        conversions = _to_float(row["conversions"])
        clicks = _to_float(row["clicks"])
        out[attr_id] = {"cost": cost, "conversions": conversions, "clicks": clicks, "avg_cpc": (cost / clicks) if clicks > 0 else 0.0, "cost_per_conversion": (cost / conversions) if conversions > 0 else 0.0}
    return out


def _campaign_efficiency_payload(settings: dict[str, Any], row_data: dict[str, Any]) -> dict[str, Any]:
    if not settings:
        return {"state": "neutral", "badge": "Sem dados", "diagnosis": "Sem snapshot Ads", "goal": "", "target_cpa": 0.0, "avg_cpc": 0.0, "cost_per_conversion": 0.0, "ratio": 0.0}
    goal = _conversion_goal_name(settings)
    target_cpa = _to_float(settings.get("target_cpa"))
    campaign_target_cpa = _to_float(settings.get("campaign_target_cpa"))
    ad_group_target_cpa = _to_float(settings.get("ad_group_target_cpa"))
    avg_cpc = _to_float(settings.get("avg_cpc"))
    cost = _to_float(settings.get("cost"))
    conversions = _to_float(settings.get("conversions"))
    if _to_float(row_data.get("ads_conversions_today")) > 0:
        cost = _to_float(row_data.get("ads_cost_today")) or _to_float(row_data.get("spend_today"))
        conversions = _to_float(row_data.get("ads_conversions_today"))
    if avg_cpc <= 0 and _to_float(row_data.get("ads_avg_cpc_today")) > 0:
        avg_cpc = _to_float(row_data.get("ads_avg_cpc_today"))
    cost_per_conversion = (cost / conversions) if conversions > 0 else 0.0
    roi_today = _agent_roi_pct(row_data.get("roi_today")) if "_agent_roi_pct" in globals() else _to_float(row_data.get("roi_today"))
    roi_7d = _agent_roi_pct(row_data.get("roi_7d")) if "_agent_roi_pct" in globals() else _to_float(row_data.get("roi_7d"))
    pmr_today = _agent_percent(row_data.get("pmr_today")) if "_agent_percent" in globals() else _to_float(row_data.get("pmr_today"))
    pmr_yesterday = _agent_percent(row_data.get("pmr_yesterday")) if "_agent_percent" in globals() else _to_float(row_data.get("pmr_yesterday"))
    pmr_7d = _agent_percent(row_data.get("pmr_7d")) if "_agent_percent" in globals() else _to_float(row_data.get("pmr_7d"))
    revenue_today = _to_float(row_data.get("revenue_today"))
    revenue_7d = _to_float(row_data.get("revenue_7d"))
    ratio = (cost_per_conversion / target_cpa * 100.0) if target_cpa > 0 and cost_per_conversion > 0 else 0.0
    state = "neutral"
    badge = "Monitorar"
    diagnosis = "Acompanhar CPA/CPC contra ROI e GAM"
    has_revenue_signal = revenue_today > 0 or revenue_7d > 0
    floor_first = (
        ((roi_today < 0 and pmr_today >= 90) or (roi_7d < 0 and pmr_7d >= 90) or (roi_today < 0 and pmr_yesterday >= 90))
        and has_revenue_signal
    )
    if floor_first:
        state = "warn"
        badge = "Priorizar floor"
        diagnosis = "PMR alto com ROI negativo; revisar regra de preco GAM antes de mexer em CPA/CPC"
    elif roi_today < 0:
        state = "warn" if roi_7d > 0 else "bad"
        badge = "Hoje negativo"
        diagnosis = "ROI de hoje negativo; nao classificar como ROI ok ate validar fechamento do dia"
    elif target_cpa <= 0 and avg_cpc <= 0:
        badge = "Sem CPA/CPC"
        diagnosis = "Campanha sem CPA alvo ou custo por conversao no snapshot"
    elif roi_7d >= 40 and pmr_7d >= 80 and (ratio == 0 or ratio <= 100):
        state = "good"
        badge = "Escalar"
        diagnosis = "ROI e GAM saudaveis para avaliar escala"
    elif roi_7d > 0 and pmr_7d >= 75 and (ratio == 0 or ratio <= 120):
        state = "good"
        badge = "ROI ok"
        diagnosis = "Retorno positivo com GAM saudavel"
    elif roi_7d < 0 and ratio >= 80:
        state = "bad"
        badge = "CPC pressiona"
        diagnosis = "Custo por conversao proximo/acima do CPA alvo com ROI negativo"
    elif roi_7d <= -50:
        state = "bad"
        badge = "Reduzir"
        diagnosis = "ROI abaixo do limite operacional"
    elif pmr_7d < 70 and pmr_7d > 0:
        state = "warn"
        badge = "GAM fraco"
        diagnosis = "PMR baixo pode limitar receita"
    elif ratio >= 100:
        state = "warn"
        badge = "CPA alto"
        diagnosis = "Custo por conversao encostando no CPA alvo"
    target_cpa_change = dict(settings.get("target_cpa_change") or {})
    return {
        "state": state,
        "badge": badge,
        "diagnosis": diagnosis,
        "goal": goal,
        "target_cpa": round(target_cpa, 6),
        "campaign_target_cpa": round(campaign_target_cpa, 6),
        "ad_group_target_cpa": round(ad_group_target_cpa, 6),
        "target_cpa_change": target_cpa_change,
        "avg_cpc": round(avg_cpc, 6),
        "cost_per_conversion": round(cost_per_conversion, 6),
        "ratio": round(ratio, 6),
        "roi_today": round(roi_today, 6),
        "bidding_strategy_type": str(settings.get("bidding_strategy_type") or ""),
        "settings_day": str(settings.get("day") or ""),
        "observed_conversion_actions": str(settings.get("observed_conversion_actions_json") or "[]"),
    }


def load_project_campaign_rows(
    runtime: dict[str, Any],
    project: str,
    today: date | None = None,
    operational: bool = False,
) -> list[dict[str, Any]]:
    current_day = today or app_today()
    related_projects = _project_family_projects(runtime, project)
    project_rows = _project_campaign_analysis(runtime, current_day, allowed_projects=related_projects)
    rows = project_rows.get(project, [])
    pmr_by_attr = _gam_pmr_by_attr_periods(runtime, current_day, project)
    month_start = current_day.replace(day=1).isoformat()
    yesterday = (current_day - timedelta(days=1)).isoformat()
    seven_day_start = (current_day - timedelta(days=6)).isoformat()
    history_start = min(month_start, yesterday, seven_day_start)
    month_end = current_day.isoformat()
    _ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(runtime, history_start, month_end, operational=operational)
    project_delivery = _gam_delivery_by_project_periods(runtime, current_day, gam_attr_rows, allowed_projects=related_projects).get(project, {})
    mgid_by_attr = _mgid_revenue_by_attr_periods(runtime, current_day, project)
    push_by_attr = _push_revenue_by_attr_periods(runtime, current_day, project)
    sessions_by_attr = _traffic_sessions_by_attr_periods(runtime, current_day, project)
    budgets = _ads_daily_budget_by_attr(runtime, current_day, project)
    campaign_settings = _ads_campaign_settings_by_campaign(runtime, current_day)
    target_cpa_changes = _latest_target_cpa_changes(runtime, current_day)
    ads_today_metrics = _ads_today_metrics_by_attr(runtime, current_day, project)

    def pmr_value(attr_pmr: dict[str, float], period_key: str, project_period: str) -> float:
        attr_value = _to_float(attr_pmr.get(period_key))
        if attr_value > 0:
            return attr_value
        return _to_float((project_delivery.get(project_period) or {}).get("pmr"))

    def merge_settings(*items: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for item in items:
            if not item:
                continue
            for key, value in item.items():
                if value not in (None, "", "[]") or key not in merged:
                    if value not in (None, ""):
                        merged[key] = value
                    elif key not in merged:
                        merged[key] = value
        return merged

    result: list[dict[str, Any]] = []
    for row in rows:
        attr_id = str(row[3])
        attr_pmr = pmr_by_attr.get(attr_id, {})
        push_periods = push_by_attr.get(attr_id, {})
        mgid_periods = mgid_by_attr.get(attr_id, {})
        session_periods = sessions_by_attr.get(attr_id, {})
        push_hoje = _to_float(push_periods.get("hoje"))
        push_ontem = _to_float(push_periods.get("ontem"))
        push_7d = _to_float(push_periods.get("7d"))
        push_mes = _to_float(push_periods.get("mes"))
        mgid_hoje = _to_float(mgid_periods.get("hoje"))
        mgid_ontem = _to_float(mgid_periods.get("ontem"))
        mgid_7d = _to_float(mgid_periods.get("7d"))
        mgid_mes = _to_float(mgid_periods.get("mes"))
        total_hoje = _to_float(row[10]) + push_hoje + mgid_hoje
        total_ontem = _to_float(row[15]) + push_ontem + mgid_ontem
        total_7d = _to_float(row[20]) + push_7d + mgid_7d
        total_mes = _to_float(row[25]) + push_mes + mgid_mes
        sessions_hoje = _to_float(session_periods.get("hoje"))
        sessions_ontem = _to_float(session_periods.get("ontem"))
        sessions_7d = _to_float(session_periods.get("7d"))
        sessions_mes = _to_float(session_periods.get("mes"))
        ads_today = ads_today_metrics.get(attr_id, {})
        settings = merge_settings(
            campaign_settings.get(str(row[5]) or "", {}),
            campaign_settings.get(str(row[3]) or "", {}),
        )
        settings["target_cpa_change"] = (
            target_cpa_changes.get(str(row[3]) or "")
            or target_cpa_changes.get(str(row[5]) or "")
            or {}
        )
        landing_url = str(settings.get("landing_url") or "").strip() or _fallback_campaign_landing_url(project, row[6])
        result.append(
            {
                "site": row[0],
                "channel_type": row[1],
                "attr_level": row[2],
                "attr_id": attr_id,
                "campaign_id": row[5],
                "campaign": str(settings.get("campaign") or row[6]),
                "ad_group": str(settings.get("ad_group_name") or row[8]),
                "google_ads_url": _google_ads_campaign_ui_url(runtime, settings.get("customer_id"), row[5]),
                "landing_url": landing_url,
                "daily_budget": budgets.get(str(row[3]), 0.0),
                "efficiency": _campaign_efficiency_payload(settings, {"roi_today": row[11], "roi_7d": ((total_7d - _to_float(row[19])) / _to_float(row[19])) if _to_float(row[19]) > 0 else 0.0, "pmr_today": pmr_value(attr_pmr, "hoje", "Hoje"), "pmr_yesterday": pmr_value(attr_pmr, "ontem", "Ontem"), "pmr_7d": pmr_value(attr_pmr, "7d", "Ultimos 7 Dias"), "revenue_today": row[10], "revenue_7d": row[20], "spend_today": row[9], "spend_7d": row[19], "ads_cost_today": ads_today.get("cost"), "ads_conversions_today": ads_today.get("conversions"), "ads_avg_cpc_today": ads_today.get("avg_cpc")}),
                "investimento_hoje": row[9],
                "receita_adsense_hoje": row[10],
                "receita_push_hoje": push_hoje,
                "receita_mgid_hoje": mgid_hoje,
                "receita_total_hoje": total_hoje,
                "receita_hoje": total_hoje,
                "sessoes_hoje": sessions_hoje,
                "rps_hoje": total_hoje / sessions_hoje if sessions_hoje > 0 else 0.0,
                "roi_hoje": ((total_hoje - _to_float(row[9])) / _to_float(row[9])) if _to_float(row[9]) > 0 else 0.0,
                "ecpm_hoje": _campaign_ecpm_with_aux(row[12], row[10], total_hoje),
                "pmr_hoje": pmr_value(attr_pmr, "hoje", "Hoje"),
                "taxa_hoje": row[13],
                "investimento_ontem": row[14],
                "receita_adsense_ontem": row[15],
                "receita_push_ontem": push_ontem,
                "receita_mgid_ontem": mgid_ontem,
                "receita_total_ontem": total_ontem,
                "receita_ontem": total_ontem,
                "sessoes_ontem": sessions_ontem,
                "rps_ontem": total_ontem / sessions_ontem if sessions_ontem > 0 else 0.0,
                "roi_ontem": ((total_ontem - _to_float(row[14])) / _to_float(row[14])) if _to_float(row[14]) > 0 else 0.0,
                "ecpm_ontem": _campaign_ecpm_with_aux(row[17], row[15], total_ontem),
                "pmr_ontem": pmr_value(attr_pmr, "ontem", "Ontem"),
                "taxa_ontem": row[18],
                "investimento_7d": row[19],
                "receita_adsense_7d": row[20],
                "receita_push_7d": push_7d,
                "receita_mgid_7d": mgid_7d,
                "receita_total_7d": total_7d,
                "receita_7d": total_7d,
                "sessoes_7d": sessions_7d,
                "rps_7d": total_7d / sessions_7d if sessions_7d > 0 else 0.0,
                "roi_7d": ((total_7d - _to_float(row[19])) / _to_float(row[19])) if _to_float(row[19]) > 0 else 0.0,
                "ecpm_7d": _campaign_ecpm_with_aux(row[22], row[20], total_7d),
                "pmr_7d": pmr_value(attr_pmr, "7d", "Ultimos 7 Dias"),
                "taxa_7d": row[23],
                "investimento_mes": row[24],
                "receita_adsense_mes": row[25],
                "receita_push_mes": push_mes,
                "receita_mgid_mes": mgid_mes,
                "receita_total_mes": total_mes,
                "receita_mes": total_mes,
                "sessoes_mes": sessions_mes,
                "rps_mes": total_mes / sessions_mes if sessions_mes > 0 else 0.0,
                "roi_mes": ((total_mes - _to_float(row[24])) / _to_float(row[24])) if _to_float(row[24]) > 0 else 0.0,
                "ecpm_mes": _campaign_ecpm_with_aux(row[27], row[25], total_mes),
                "pmr_mes": pmr_value(attr_pmr, "mes", "Mes"),
                "taxa_mes": row[28],
            }
        )
    settings_campaigns: dict[str, dict[str, Any]] = {}
    for settings_row in campaign_settings.values():
        settings_site = str(settings_row.get("site") or "").strip()
        campaign_id = str(settings_row.get("campaign_id") or "").strip()
        if settings_site not in related_projects or not campaign_id:
            continue
        current = settings_campaigns.get(campaign_id)
        if current is None or str(settings_row.get("updated_at") or "") > str(current.get("updated_at") or ""):
            settings_campaigns[campaign_id] = settings_row
    represented_campaigns = {str(item.get("campaign_id") or "").strip() for item in result}
    for campaign_id, settings in settings_campaigns.items():
        if campaign_id in represented_campaigns:
            continue
        attr_id = str(settings.get("attr_id") or settings.get("ad_group_id") or campaign_id).strip()
        settings["target_cpa_change"] = (
            target_cpa_changes.get(attr_id)
            or target_cpa_changes.get(campaign_id)
            or {}
        )
        landing_url = str(settings.get("landing_url") or "").strip() or _fallback_campaign_landing_url(
            project,
            settings.get("campaign"),
        )
        result.append(
            {
                "site": project,
                "channel_type": str(settings.get("channel_type") or ""),
                "attr_level": "campaign",
                "attr_id": attr_id,
                "campaign_id": campaign_id,
                "campaign": str(settings.get("campaign") or campaign_id),
                "ad_group": str(settings.get("ad_group_name") or ""),
                "google_ads_url": _google_ads_campaign_ui_url(runtime, settings.get("customer_id"), campaign_id),
                "landing_url": landing_url,
                "daily_budget": max(
                    _to_float(settings.get("daily_budget")),
                    _to_float(budgets.get(attr_id)),
                    _to_float(budgets.get(campaign_id)),
                ),
                "efficiency": _campaign_efficiency_payload(
                    settings,
                    {
                        "roi_today": 0.0,
                        "roi_7d": 0.0,
                        "pmr_today": 0.0,
                        "pmr_yesterday": 0.0,
                        "pmr_7d": 0.0,
                        "revenue_today": 0.0,
                        "revenue_7d": 0.0,
                        "spend_today": 0.0,
                        "spend_7d": 0.0,
                        "ads_cost_today": settings.get("cost"),
                        "ads_conversions_today": settings.get("conversions"),
                        "ads_avg_cpc_today": settings.get("avg_cpc"),
                    },
                ),
                "_settings_snapshot": True,
            }
        )
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    additive_fields = [
        "investimento_hoje", "receita_adsense_hoje", "receita_push_hoje", "receita_mgid_hoje", "receita_total_hoje", "receita_hoje",
        "investimento_ontem", "receita_adsense_ontem", "receita_push_ontem", "receita_mgid_ontem", "receita_total_ontem", "receita_ontem",
        "investimento_7d", "receita_adsense_7d", "receita_push_7d", "receita_mgid_7d", "receita_total_7d", "receita_7d",
        "investimento_mes", "receita_adsense_mes", "receita_push_mes", "receita_mgid_mes", "receita_total_mes", "receita_mes",
    ]
    for item in result:
        key = (str(item.get("campaign_id") or ""), str(item.get("attr_id") or ""), str(item.get("channel_type") or ""))
        current = deduped.get(key)
        if current is None:
            deduped[key] = dict(item)
            continue
        for field in additive_fields:
            current[field] = _to_float(current.get(field)) + _to_float(item.get(field))
        current["daily_budget"] = max(_to_float(current.get("daily_budget")), _to_float(item.get("daily_budget")))
        for field in ("sessoes_hoje", "sessoes_ontem", "sessoes_7d", "sessoes_mes"):
            current[field] = max(_to_float(current.get(field)), _to_float(item.get(field)))
        for period in ("hoje", "ontem", "7d", "mes"):
            spend = _to_float(current.get(f"investimento_{period}"))
            revenue = _to_float(current.get(f"receita_{period}"))
            current[f"roi_{period}"] = ((revenue - spend) / spend) if spend > 0 else 0.0
            sessions = _to_float(current.get(f"sessoes_{period}"))
            current[f"rps_{period}"] = revenue / sessions if sessions > 0 else 0.0
        for field in ("pmr_hoje", "pmr_ontem", "pmr_7d", "pmr_mes", "ecpm_hoje", "ecpm_ontem", "ecpm_7d", "ecpm_mes", "taxa_hoje", "taxa_ontem", "taxa_7d", "taxa_mes"):
            current[field] = max(_to_float(current.get(field)), _to_float(item.get(field)))
        if _to_float(item.get("investimento_hoje")) > _to_float(current.get("investimento_hoje")):
            current["efficiency"] = item.get("efficiency", current.get("efficiency"))
        if not str(current.get("landing_url") or "").strip() and str(item.get("landing_url") or "").strip():
            current["landing_url"] = str(item.get("landing_url") or "").strip()
        if not str(current.get("google_ads_url") or "").strip() and str(item.get("google_ads_url") or "").strip():
            current["google_ads_url"] = str(item.get("google_ads_url") or "").strip()
    result = [
        item
        for item in deduped.values()
        if item.pop("_settings_snapshot", False)
        or str(item.get("campaign_id") or "").strip() in settings_campaigns
        or any(
            _to_float(item.get(field)) > 0
            for field in (
                "investimento_hoje", "receita_hoje",
                "investimento_ontem", "receita_ontem",
                "investimento_7d", "receita_7d",
                "investimento_mes", "receita_mes",
            )
        )
    ]
    optimization_windows = _display_campaign_optimization_windows(runtime, current_day, project, result)
    for item in result:
        for period in ("hoje", "ontem", "7d", "mes"):
            for metric in ("investimento", "receita_adsense", "receita_push", "receita_mgid", "receita_total", "receita", "sessoes", "rps", "roi", "ecpm", "pmr", "taxa"):
                item.setdefault(f"{metric}_{period}", 0.0)
        if str(item.get("channel_type", "")).upper() == "DISPLAY":
            item["optimization_lock"] = optimization_windows.get(str(item.get("campaign_id") or ""), {})
    return sorted(result, key=lambda item: _to_float(item.get("investimento_hoje")), reverse=True)


def _ads_daily_budget_by_attr(runtime: dict[str, Any], current_day: date, project: str) -> dict[str, float]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return {}
    _site_to_project, project_to_sites = _site_project_maps(runtime)
    sites = sorted(project_to_sites.get(project, set()) or {project})
    if not sites:
        return {}
    placeholders = ",".join("?" for _ in sites)
    day = current_day.isoformat()
    history_start = (current_day - timedelta(days=30)).isoformat()
    budgets: dict[str, float] = {}
    campaign_budget: dict[str, float] = {}
    group_to_campaign: dict[str, str] = {}
    project_campaign_ids: set[str] = set()
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        table = "ads_hourly" if conn.execute("select 1 from sqlite_master where type='table' and name='ads_hourly'").fetchone() else "ads_daily"
        has_rows = conn.execute(f"select 1 from {table} where day=? limit 1", (day,)).fetchone()
        if not has_rows:
            table = "ads_daily"
        columns = {row[1] for row in conn.execute(f"pragma table_info({table})").fetchall()}
        if "daily_budget" in columns:
            rows = conn.execute(
                f"select attr_id, campaign_id, max(daily_budget) as daily_budget from {table} "
                f"where site in ({placeholders}) and day=? group by attr_id, campaign_id",
                [*sites, day],
            ).fetchall()
            for row in rows:
                value = _to_float(row["daily_budget"])
                attr_id = str(row["attr_id"] or "")
                campaign_id = str(row["campaign_id"] or "")
                if campaign_id:
                    project_campaign_ids.add(campaign_id)
                if value > 0 and attr_id:
                    budgets[attr_id] = value
                if value > 0 and campaign_id:
                    campaign_budget[campaign_id] = max(campaign_budget.get(campaign_id, 0.0), value)
            fallback_rows = conn.execute(
                f"select attr_id, campaign_id, daily_budget from ads_daily "
                f"where site in ({placeholders}) and day between ? and ? order by day desc",
                [*sites, history_start, day],
            ).fetchall()
            for row in fallback_rows:
                attr_id = str(row["attr_id"] or "")
                campaign_id = str(row["campaign_id"] or "")
                value = _to_float(row["daily_budget"])
                if campaign_id:
                    project_campaign_ids.add(campaign_id)
                if value > 0 and attr_id and attr_id not in budgets:
                    budgets[attr_id] = value
                if value > 0 and campaign_id and campaign_id not in campaign_budget:
                    campaign_budget[campaign_id] = value
        if conn.execute("select 1 from sqlite_master where type='table' and name='google_ads_campaign_settings_daily'").fetchone():
            try:
                setting_rows = conn.execute(
                    f"select ad_group_id, campaign_id from google_ads_campaign_settings_daily where site in ({placeholders}) and day between ? and ? order by day desc",
                    [*sites, history_start, day],
                ).fetchall()
                for row in setting_rows:
                    campaign_id = str(row["campaign_id"] or "")
                    ad_group_id = str(row["ad_group_id"] or "")
                    if campaign_id:
                        project_campaign_ids.add(campaign_id)
                    if ad_group_id and campaign_id:
                        group_to_campaign.setdefault(ad_group_id, campaign_id)
            except sqlite3.OperationalError:
                pass
        if conn.execute("select 1 from sqlite_master where type='table' and name='google_ads_campaign_budgets_daily'").fetchone() and project_campaign_ids:
            campaign_placeholders = ",".join("?" for _ in project_campaign_ids)
            exact_rows = conn.execute(
                f"select campaign_id, daily_budget from google_ads_campaign_budgets_daily "
                f"where campaign_id in ({campaign_placeholders}) and day <= ? order by day desc, updated_at desc",
                [*sorted(project_campaign_ids), day],
            ).fetchall()
            for row in exact_rows:
                campaign_id = str(row["campaign_id"] or "")
                value = _to_float(row["daily_budget"])
                if campaign_id and value > 0 and campaign_id not in campaign_budget:
                    campaign_budget[campaign_id] = value
                    budgets[campaign_id] = max(_to_float(budgets.get(campaign_id)), value)
        if conn.execute("select 1 from sqlite_master where type='table' and name='campaign_budget_manual_actions'").fetchone():
            rows = conn.execute(
                """
                select attr_id, campaign_id, new_budget
                from campaign_budget_manual_actions
                where project = ? and status = 'applied' and new_budget > 0
                order by applied_at desc, created_at desc
                """,
                (project,),
            ).fetchall()
            for row in rows:
                value = _to_float(row["new_budget"])
                attr_id = str(row["attr_id"] or "")
                campaign_id = str(row["campaign_id"] or "")
                if value > 0 and campaign_id:
                    campaign_budget[campaign_id] = max(_to_float(campaign_budget.get(campaign_id)), value)
                    budgets[campaign_id] = max(_to_float(budgets.get(campaign_id)), value)
                if value > 0 and attr_id:
                    budgets[attr_id] = max(_to_float(budgets.get(attr_id)), value)
    for ad_group_id, campaign_id in group_to_campaign.items():
        if ad_group_id and campaign_id in campaign_budget:
            budgets[ad_group_id] = max(_to_float(budgets.get(ad_group_id)), _to_float(campaign_budget.get(campaign_id)))
    return budgets


def _parse_ads_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _google_ads_client_from_runtime(runtime: dict[str, Any]) -> Any | None:
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except Exception:
        return None
    ads_cfg = (runtime.get("config") or {}).get("google_ads") or {}
    required = ["developer_token", "client_id", "client_secret", "refresh_token"]
    if not all(ads_cfg.get(key) for key in required):
        return None
    client_cfg = {
        "developer_token": ads_cfg["developer_token"],
        "client_id": ads_cfg["client_id"],
        "client_secret": ads_cfg["client_secret"],
        "refresh_token": ads_cfg["refresh_token"],
        "use_proto_plus": True,
    }
    if ads_cfg.get("login_customer_id"):
        client_cfg["login_customer_id"] = re.sub(r"\D+", "", str(ads_cfg["login_customer_id"]))
    try:
        return GoogleAdsClient.load_from_dict(client_cfg, version=ads_cfg.get("api_version", "v23"))
    except Exception:
        return None


def _display_campaign_optimization_windows(
    runtime: dict[str, Any],
    current_day: date,
    project: str,
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    display_campaigns: dict[str, set[str]] = {}
    for row in rows:
        if str(row.get("channel_type", "")).upper() != "DISPLAY":
            continue
        campaign_id = str(row.get("campaign_id") or "").strip()
        attr_id = str(row.get("attr_id") or "").strip()
        if campaign_id:
            display_campaigns.setdefault(campaign_id, set())
            if attr_id:
                display_campaigns[campaign_id].add(attr_id)
    if not display_campaigns:
        return {}
    cache_key = ("display_campaign_optimization_windows", current_day.isoformat(), project, tuple(sorted((cid, tuple(sorted(attrs))) for cid, attrs in display_campaigns.items())))
    return _runtime_cache_get(runtime, cache_key, lambda: _display_campaign_optimization_windows_uncached(runtime, current_day, display_campaigns))


def _display_campaign_optimization_windows_uncached(
    runtime: dict[str, Any],
    current_day: date,
    campaign_ids: dict[str, set[str]] | list[str],
) -> dict[str, dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if isinstance(campaign_ids, dict):
        campaign_attr_ids = {str(cid).strip(): {str(attr).strip() for attr in attrs if str(attr).strip()} for cid, attrs in campaign_ids.items() if str(cid).strip()}
    else:
        campaign_attr_ids = {str(item).strip(): set() for item in campaign_ids if str(item).strip()}
    campaign_set = set(campaign_attr_ids)
    local: dict[str, dict[str, str]] = {}
    if db_path.exists() and campaign_set:
        placeholders = ",".join("?" for _ in campaign_set)
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"select campaign_id, customer_id, min(day) as first_seen_day from ads_daily "
                f"where campaign_id in ({placeholders}) group by campaign_id, customer_id",
                list(campaign_set),
            ).fetchall()
        for row in rows:
            cid = str(row["campaign_id"] or "").strip()
            if not cid:
                continue
            first_seen = str(row["first_seen_day"] or "").strip()
            customer_id = re.sub(r"\D+", "", str(row["customer_id"] or ""))
            current = local.get(cid)
            if not current or first_seen < current.get("first_seen_day", "9999-99-99"):
                local[cid] = {"customer_id": customer_id, "first_seen_day": first_seen}

    setting_dates: dict[str, date] = {}
    block_dates: dict[str, date] = {}
    if db_path.exists() and campaign_set:
        placeholders = ",".join("?" for _ in campaign_set)
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(
                f"select campaign_id, start_date from google_ads_campaign_settings_daily where campaign_id in ({placeholders}) and start_date != '' order by day desc",
                list(campaign_set),
            ).fetchall():
                cid = str(row["campaign_id"] or "").strip()
                parsed = _parse_ads_date(row["start_date"])
                if cid and parsed and cid not in setting_dates:
                    setting_dates[cid] = parsed
            attr_to_campaign: dict[str, str] = {}
            for cid, attrs in campaign_attr_ids.items():
                for attr in attrs:
                    attr_to_campaign[attr] = cid
            if attr_to_campaign:
                attr_placeholders = ",".join("?" for _ in attr_to_campaign)
                for row in conn.execute(
                    f"select breakdown, attr_candidates_json, attr_raw from gam_attr_daily where attr_candidates_json != '' and (" + " OR ".join(["attr_candidates_json like ?" for _ in attr_to_campaign]) + ")",
                    [f'%{attr}%' for attr in attr_to_campaign],
                ).fetchall():
                    text = f"{row['breakdown'] or ''} {row['attr_candidates_json'] or ''} {row['attr_raw'] or ''}"
                    found_attrs = [attr for attr in attr_to_campaign if attr in text]
                    if not found_attrs:
                        continue
                    dates = []
                    for match in re.finditer(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])([0-2]\d|3[01])(?!\d)", text):
                        parsed = _parse_ads_date(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
                        if parsed:
                            dates.append(parsed)
                    if not dates:
                        continue
                    inferred = min(dates)
                    for attr in found_attrs:
                        cid = attr_to_campaign[attr]
                        current = block_dates.get(cid)
                        if not current or inferred < current:
                            block_dates[cid] = inferred

    output: dict[str, dict[str, Any]] = {}
    for cid in campaign_set:
        source = "settings_start_date"
        start_day = setting_dates.get(cid)
        if not start_day:
            inferred_block_day = block_dates.get(cid)
            output[cid] = {
                "start_date": inferred_block_day.isoformat() if inferred_block_day else "",
                "unlock_date": "",
                "days_remaining": None,
                "can_optimize": False,
                "label": "Aguardando data real do Google Ads",
                "source": "unreliable_gam_block_date" if inferred_block_day else "unavailable",
            }
            continue
        unlock_day = start_day + timedelta(days=24)
        remaining = max(0, (unlock_day - current_day).days)
        can_optimize = current_day >= unlock_day
        output[cid] = {
            "start_date": start_day.isoformat(),
            "unlock_date": unlock_day.isoformat(),
            "days_remaining": remaining,
            "can_optimize": can_optimize,
            "label": "Liberado para otimizar" if can_optimize else f"Faltam {remaining} dia{'s' if remaining != 1 else ''} para otimizar",
            "source": source,
        }
    return output

def _campaign_period_window(current_day: date, period: str) -> tuple[str, date, date]:
    normalized = str(period or "today").strip().lower()
    if normalized in {"today", "hoje"}:
        return "today", current_day, current_day
    if normalized in {"yesterday", "ontem"}:
        day = current_day - timedelta(days=1)
        return "yesterday", day, day
    if normalized in {"7d", "7 days", "ultimos 7 dias", "últimos 7 dias", "seven", "seven_days"}:
        return "7d", current_day - timedelta(days=6), current_day
    if normalized in {"month", "mes", "mês"}:
        return "month", current_day.replace(day=1), current_day
    raise ValueError(f"Periodo invalido: {period}")


def _dashboard_project_allowlist(runtime: dict[str, Any]) -> set[str]:
    config = runtime.get("config") or {}
    configured = config.get("dashboard_projects") or config.get("google_ads_projects") or []
    allowlist = {str(item).strip() for item in configured if str(item).strip()}
    if allowlist:
        return allowlist
    return {
        "esp-g.sonora123.com",
        "ph.institutodeidentificacao.com.br",
        "por-g.sonora123.com",
        "mx.investindoserio.com.br",
        "cl.promocaointernetfibra.com.br",
        "site.promocaointernetfibra.com.br",
          "za.institutodeidentificacao.com.br",
        "au.investindoserio.com.br",
    }



def _summarize_campaign_blocks_for_floor(runtime: dict[str, Any], project: str, attr_id: str, start_day: date, end_day: date) -> dict[str, dict[str, Any]]:
    try:
        _ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(runtime, start_day.isoformat(), end_day.isoformat())
    except Exception:
        return {}
    site_mapping = runtime["site_mapping"]
    dimension_overrides = runtime["dimension_overrides"]
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    blocks: dict[str, dict[str, Any]] = {}
    target_attr = str(attr_id).strip()
    for gam_row in gam_attr_rows:
        row_attr_id = str(getattr(gam_row, "attr_id", "") or "").strip()
        row_candidates = {
            str(value).strip()
            for value in (getattr(gam_row, "attr_candidates", ()) or ())
            if str(value).strip()
        }
        if row_attr_id != target_attr and target_attr not in row_candidates:
            continue
        dims = enrich_dimensions(getattr(gam_row, "site", ""), site_mapping, dimension_overrides)
        row_site = str(dims.site or getattr(gam_row, "site", "") or "").strip()
        row_project = (
            site_to_project.get(row_site)
            or site_to_project.get(str(getattr(gam_row, "site", "") or "").strip())
            or str(runtime.get("site_mapping", {}).get(row_site, "") or "").strip()
            or row_site
        )
        if row_project != project:
            continue
        block_name = str(getattr(gam_row, "breakdown", "") or getattr(gam_row, "site", "") or "Sem bloco").strip() or "Sem bloco"
        bucket = blocks.setdefault(block_name, {"clicks": 0, "impressions": 0, "unfilled_impressions": 0, "total_requests": 0, "matched_requests": 0.0, "match_rate_sum": 0.0, "match_rate_weight": 0})
        impressions = int(_to_float(getattr(gam_row, "impressions", 0)))
        unfilled = int(_to_float(getattr(gam_row, "unfilled_impressions", 0)))
        clicks = int(_to_float(getattr(gam_row, "clicks", 0)))
        total_requests, matched_requests = _resolve_request_metrics(
            impressions,
            unfilled,
            int(_to_float(getattr(gam_row, "total_requests", 0))),
            _to_float(getattr(gam_row, "match_rate", 0.0)),
        )
        bucket["clicks"] += clicks
        bucket["impressions"] += impressions
        bucket["unfilled_impressions"] += unfilled
        bucket["total_requests"] += total_requests
        bucket["matched_requests"] += matched_requests
        row_match_rate = _to_float(getattr(gam_row, "match_rate", 0.0))
        if row_match_rate > 0:
            weight = total_requests if total_requests > 0 else max(1, impressions)
            bucket["match_rate_sum"] += row_match_rate * weight
            bucket["match_rate_weight"] += weight
    for bucket in blocks.values():
        impressions = int(bucket["impressions"])
        unfilled = int(bucket["unfilled_impressions"])
        total_requests = int(bucket["total_requests"])
        matched_requests = _to_float(bucket["matched_requests"])
        if int(bucket["match_rate_weight"]) > 0:
            pmr = _to_float(bucket["match_rate_sum"]) / int(bucket["match_rate_weight"])
        elif total_requests > 0:
            pmr = matched_requests / total_requests
        elif impressions > 0:
            denominator = impressions + unfilled
            pmr = impressions / denominator if denominator > 0 else 0.0
        else:
            pmr = 0.0
        bucket["pmr"] = round(pmr * 100.0, 2)
    return blocks


def _floor_action_age_hours(action: dict[str, Any]) -> float:
    try:
        created = datetime.fromisoformat(str(action.get("created_at") or ""))
        if created.tzinfo is None:
            created = created.replace(tzinfo=APP_TZ)
        return max(0.0, (datetime.now(APP_TZ) - created.astimezone(APP_TZ)).total_seconds() / 3600.0)
    except Exception:
        return 0.0


def _floor_price_recommendation(action: dict[str, Any], today_stats: dict[str, Any] | None, two_day_stats: dict[str, Any] | None) -> dict[str, Any] | None:
    if not action or str(action.get("status") or "") == "disabled":
        return None
    current_floor = _to_float(action.get("new_floor"))
    if current_floor <= 0:
        return None
    if not _gam_floor_data_ready():
        return None
    age_hours = _floor_action_age_hours(action)
    today_stats = today_stats or {}
    two_day_stats = two_day_stats or {}

    stats = today_stats if int(_to_float(today_stats.get("impressions"))) >= 2 else two_day_stats
    source = "hoje" if stats is today_stats else "hoje+ontem"
    if int(_to_float(stats.get("impressions"))) < 2:
        return None

    pmr = _to_float(stats.get("pmr"))
    impressions = int(_to_float(stats.get("impressions")))
    metadata = _decode_action_metadata(action)
    impressions_since_registration = max(0.0, impressions - _to_float(metadata.get("impressions_at_registration")))

    if pmr > 0 and pmr < 50:
        return {"action": "disable", "change_pct": None, "new_floor": current_floor, "reason": f"{source}: {impressions} impressoes e PMR {pmr:.2f}% abaixo de 50%"}
    if 51 <= pmr < 70:
        if impressions_since_registration < 10:
            return None
        new_floor = _floor_bucket_shift(current_floor, "decrease", 1)
        return {"action": "decrease", "change_pct": round(((new_floor - current_floor) / current_floor) * 100.0, 2), "new_floor": new_floor, "reason": f"{source}: {impressions} impressoes e PMR {pmr:.2f}% entre 51% e 69%; descer 1 bucket"}
    if 70 <= pmr < 80:
        return None
    if pmr < 80:
        return None
    if impressions_since_registration < 10:
        return None

    new_floor = _floor_bucket_shift(current_floor, "increase", 1)
    return {
        "action": "increase",
        "change_pct": round(((new_floor - current_floor) / current_floor) * 100.0, 2) if current_floor > 0 else None,
        "new_floor": new_floor,
        "reason": f"{source}: {impressions} impressoes e PMR {pmr:.2f}% acima de 80%; subir 1 bucket",
    }

def load_project_campaign_blocks(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    period: str = "today",
    today: date | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    current_day = today or app_today()
    if start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("Informe data inicial e final")
        try:
            start_day = date.fromisoformat(str(start_date))
            end_day = date.fromisoformat(str(end_date))
        except ValueError as exc:
            raise ValueError("Data invalida") from exc
        if end_day < start_day:
            raise ValueError("Data final menor que a inicial")
        period_key = f"custom:{start_day.isoformat()}:{end_day.isoformat()}"
    else:
        period_key, start_day, end_day = _campaign_period_window(current_day, period)
    _ads_rows, _gam_site_rows, gam_attr_rows = _load_period_rows(runtime, start_day.isoformat(), end_day.isoformat())

    site_mapping = runtime["site_mapping"]
    dimension_overrides = runtime["dimension_overrides"]
    site_to_project, _project_to_sites = _site_project_maps(runtime)
    project_rows: dict[str, dict[str, Any]] = {}

    for gam_row in gam_attr_rows:
        row_attr_id = str(getattr(gam_row, "attr_id", "") or "").strip()
        row_candidates = {
            str(value).strip()
            for value in (getattr(gam_row, "attr_candidates", ()) or ())
            if str(value).strip()
        }
        if row_attr_id != str(attr_id).strip() and str(attr_id).strip() not in row_candidates:
            continue

        dims = enrich_dimensions(getattr(gam_row, "site", ""), site_mapping, dimension_overrides)
        row_site = str(dims.site or getattr(gam_row, "site", "") or "").strip()
        row_project = (
            site_to_project.get(row_site)
            or site_to_project.get(str(getattr(gam_row, "site", "") or "").strip())
            or str(runtime.get("site_mapping", {}).get(row_site, "") or "").strip()
            or row_site
        )
        if row_project != project:
            continue

        block_name = str(getattr(gam_row, "breakdown", "") or getattr(gam_row, "site", "") or "Sem bloco").strip() or "Sem bloco"
        bucket = project_rows.setdefault(
            block_name,
            {
                "block_name": block_name,
                "impressions": 0,
                "clicks": 0,
                "unfilled_impressions": 0,
                "total_requests": 0,
                "matched_requests": 0.0,
                "match_rate_sum": 0.0,
                "match_rate_weight": 0,
                "viewability_rate_sum": 0.0,
                "viewability_rate_weight": 0,
                "gross_revenue": 0.0,
                "gross_revenue_usd": 0.0,
                "net_revenue": 0.0,
            },
        )

        impressions = _to_float(getattr(gam_row, "impressions", 0))
        unfilled_impressions = _to_float(getattr(gam_row, "unfilled_impressions", 0))
        clicks = _to_float(getattr(gam_row, "clicks", 0))
        revenue = _to_float(getattr(gam_row, "revenue", 0.0))
        row_total_requests, row_matched_requests = _resolve_request_metrics(
            int(impressions),
            int(unfilled_impressions),
            int(_to_float(getattr(gam_row, "total_requests", 0))),
            _to_float(getattr(gam_row, "match_rate", 0.0)),
        )

        bucket["impressions"] += int(impressions)
        bucket["clicks"] += int(clicks)
        bucket["unfilled_impressions"] += int(unfilled_impressions)
        bucket["total_requests"] += row_total_requests
        bucket["matched_requests"] += row_matched_requests
        bucket["gross_revenue"] += revenue
        if _project_uses_usd_block_ecpm(row_project):
            bucket["gross_revenue_usd"] += _gam_brl_to_usd(revenue, getattr(gam_row, "day", current_day.isoformat()))
        bucket["net_revenue"] += _net_gam_revenue(revenue)

        row_match_rate = _to_float(getattr(gam_row, "match_rate", 0.0))
        if row_match_rate > 0:
            rate_weight = row_total_requests if row_total_requests > 0 else max(1, int(impressions))
            bucket["match_rate_sum"] += row_match_rate * rate_weight
            bucket["match_rate_weight"] += rate_weight
        row_viewability_rate = _to_float(getattr(gam_row, "viewability_rate", 0.0))
        if row_viewability_rate > 0:
            viewability_weight = max(1, int(impressions))
            bucket["viewability_rate_sum"] += row_viewability_rate * viewability_weight
            bucket["viewability_rate_weight"] += viewability_weight

    if not project_rows:
        project_sites = set((_project_to_sites.get(project) or []))
        project_sites.add(project)
        first_label = project.split(".", 1)[0].strip().lower()
        block_prefix = f"{first_label}f_%" if first_label else ""
        try:
            with sqlite3.connect(runtime["db_path"]) as conn:
                conn.row_factory = sqlite3.Row
                filters = []
                params: list[Any] = [start_day.isoformat(), end_day.isoformat()]
                if project_sites:
                    placeholders = ",".join("?" for _ in project_sites)
                    filters.append(f"site IN ({placeholders})")
                    filters.append(f"breakdown IN ({placeholders})")
                    params.extend(project_sites)
                    params.extend(project_sites)
                if block_prefix:
                    filters.append("site LIKE ?")
                    filters.append("breakdown LIKE ?")
                    params.extend([block_prefix, block_prefix])
                where_extra = " OR ".join(filters) or "1=0"
                for fallback_row in conn.execute(
                    f"""
                    SELECT
                        COALESCE(NULLIF(breakdown,''), NULLIF(site,'')) AS block_name,
                        MAX(day) AS day,
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks,
                        SUM(unfilled_impressions) AS unfilled_impressions,
                        SUM(total_requests) AS total_requests,
                        SUM(revenue) AS revenue,
                        AVG(match_rate) AS match_rate,
                        AVG(viewability_rate) AS viewability_rate
                    FROM gam_attr_daily
                    WHERE day BETWEEN ? AND ?
                      AND attr_id = ?
                      AND ({where_extra})
                    GROUP BY COALESCE(NULLIF(breakdown,''), NULLIF(site,''))
                    ORDER BY revenue DESC, block_name
                    """,
                    [params[0], params[1], str(attr_id).strip(), *params[2:]],
                ).fetchall():
                    block_name = str(fallback_row["block_name"] or "").strip()
                    if not block_name:
                        continue
                    impressions = int(_to_float(fallback_row["impressions"]))
                    unfilled_impressions = int(_to_float(fallback_row["unfilled_impressions"]))
                    total_requests, matched_requests = _resolve_request_metrics(
                        impressions,
                        unfilled_impressions,
                        int(_to_float(fallback_row["total_requests"])),
                        _to_float(fallback_row["match_rate"]),
                    )
                    revenue = _to_float(fallback_row["revenue"])
                    project_rows[block_name] = {
                        "block_name": block_name,
                        "impressions": impressions,
                        "clicks": int(_to_float(fallback_row["clicks"])),
                        "unfilled_impressions": unfilled_impressions,
                        "total_requests": total_requests,
                        "matched_requests": matched_requests,
                        "match_rate_sum": _to_float(fallback_row["match_rate"]) * (total_requests if total_requests > 0 else max(1, impressions)),
                        "match_rate_weight": total_requests if total_requests > 0 else max(1, impressions),
                        "viewability_rate_sum": _to_float(fallback_row["viewability_rate"]) * max(1, impressions),
                        "viewability_rate_weight": max(1, impressions),
                        "gross_revenue": revenue,
                        "gross_revenue_usd": _gam_brl_to_usd(revenue, fallback_row["day"]) if _project_uses_usd_block_ecpm(project) else 0.0,
                        "net_revenue": _net_gam_revenue(revenue),
                    }
        except Exception:
            pass

    results: list[dict[str, Any]] = []
    for block_name, bucket in sorted(project_rows.items(), key=lambda item: (-_to_float(item[1]["net_revenue"]), item[0].lower())):
        impressions = int(bucket["impressions"])
        clicks = int(bucket["clicks"])
        unfilled_impressions = int(bucket["unfilled_impressions"])
        total_requests = int(bucket["total_requests"])
        matched_requests = _to_float(bucket["matched_requests"])
        match_rate_sum = _to_float(bucket["match_rate_sum"])
        match_rate_weight = int(bucket["match_rate_weight"])
        pmr_value = 0.0
        if match_rate_weight > 0:
            pmr_value = match_rate_sum / match_rate_weight
        elif total_requests > 0:
            pmr_value = matched_requests / total_requests
        elif impressions > 0:
            denominator = impressions + unfilled_impressions
            pmr_value = impressions / denominator if denominator > 0 else 0.0
        viewability_value = 0.0
        if int(bucket.get("viewability_rate_weight", 0)) > 0:
            viewability_value = _to_float(bucket.get("viewability_rate_sum")) / int(bucket["viewability_rate_weight"])

        gross_revenue = round(_to_float(bucket["gross_revenue"]), 2)
        net_revenue = round(_to_float(bucket["net_revenue"]), 2)
        ctr_value = (clicks / impressions * 100.0) if impressions > 0 else 0.0
        cpc_value = (net_revenue / clicks) if clicks > 0 else 0.0
        ecpm_value = (net_revenue / impressions * 1000.0) if impressions > 0 else 0.0
        gross_revenue_usd_raw = _to_float(bucket.get("gross_revenue_usd"))
        gross_revenue_usd = round(gross_revenue_usd_raw, 2)
        use_gam_usd_ecpm = _project_uses_usd_block_ecpm(project)
        gam_ecpm_value = (gross_revenue_usd_raw / impressions * 1000.0) if use_gam_usd_ecpm and impressions > 0 else ((gross_revenue / impressions * 1000.0) if impressions > 0 else 0.0)

        results.append(
            {
                "block_name": block_name,
                "impressions": impressions,
                "clicks": clicks,
                "pmr": round(pmr_value * 100.0, 2),
                "viewability": round(viewability_value * 100.0 if abs(viewability_value) <= 1 else viewability_value, 2),
                "ctr": round(ctr_value, 2),
                "cpc": round(cpc_value, 2),
                "ecpm": round(gam_ecpm_value if use_gam_usd_ecpm else ecpm_value, 2),
                "ecpm_currency": "USD" if use_gam_usd_ecpm else "BRL",
                "gross_revenue": gross_revenue,
                "gross_revenue_usd": gross_revenue_usd,
                "net_revenue": net_revenue,
                "period": period_key,
                "campaign_revenue": 0.0,
            }
        )
    campaign_period_revenue = round(sum(_to_float(row["net_revenue"]) for row in results), 2)
    today_block_stats = _summarize_campaign_blocks_for_floor(runtime, project, attr_id, current_day, current_day)
    two_day_block_stats = _summarize_campaign_blocks_for_floor(runtime, project, attr_id, current_day - timedelta(days=1), current_day)
    latest_floor_actions: dict[str, dict[str, Any]] = {}
    if results:
        try:
            with sqlite3.connect(runtime["db_path"]) as conn:
                conn.row_factory = sqlite3.Row
                table_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'gam_floor_price_manual_actions'"
                ).fetchone()
                if table_exists:
                    placeholders = ",".join("?" for _ in results)
                    block_names = [row["block_name"] for row in results]
                    params = [project, str(attr_id).strip(), *block_names]
                    for action in conn.execute(
                        f"""
                        SELECT block_name, current_floor, new_floor, change_pct, created_at, pmr_at_registration, status, 1 AS priority
                        FROM gam_floor_price_manual_actions
                        WHERE project = ?
                          AND attr_id = ?
                          AND scope IN ('campaign_block', 'legacy', '')
                          AND block_name IN ({placeholders})
                        ORDER BY priority DESC, datetime(created_at) DESC
                        """,
                        params,
                    ):
                        block = str(action["block_name"] or "")
                        if block and block not in latest_floor_actions:
                            latest_floor_actions[block] = dict(action)
                    # Floor rules are intentionally scoped to project + attr_id + block_name.
                    # Do not fallback by block_name only, otherwise one Attr ID inherits another Attr ID rule.
        except Exception:
            latest_floor_actions = {}

    for row in results:
        row["campaign_revenue"] = campaign_period_revenue
        action = latest_floor_actions.get(row["block_name"])
        if _is_stale_disabled_floor_action(action, current_day):
            action = None
        if action:
            current_floor = _to_float(action.get("new_floor"))
            previous_floor = _to_float(action.get("current_floor"))
            if str(action.get("status") or "") == "disabled":
                row["floor_rule_status"] = f"Regra desligada; antes {previous_floor:.2f} -> OFF"
                current_floor = 0.0
            else:
                row["floor_rule_status"] = f"Registrada {previous_floor:.2f} -> {current_floor:.2f}"
            row["floor_current_value"] = round(current_floor, 2)
            row["floor_previous_value"] = round(previous_floor, 2)
            row["floor_registered_at"] = action.get("created_at")
            row["floor_change_pct"] = round(_to_float(action.get("change_pct")) * 100.0, 2)
            row["floor_registered_pmr"] = round(_to_float(action.get("pmr_at_registration")), 2) if action.get("pmr_at_registration") is not None else None
            recommendation = _floor_price_recommendation(
                dict(action),
                today_block_stats.get(row["block_name"]),
                two_day_block_stats.get(row["block_name"]),
            )
            if recommendation:
                row["floor_suggestion_action"] = recommendation.get("action")
                row["floor_suggestion_change_pct"] = recommendation.get("change_pct")
                row["floor_suggested_value"] = recommendation.get("new_floor")
                row["floor_suggestion_reason"] = recommendation.get("reason")
            else:
                row["floor_suggestion_action"] = None
                row["floor_suggestion_change_pct"] = None
                row["floor_suggested_value"] = None
                row["floor_suggestion_reason"] = None
        else:
            row["floor_rule_status"] = "Sem regra registrada"
            row["floor_current_value"] = None
            row["floor_previous_value"] = None
            row["floor_registered_at"] = None
            row["floor_change_pct"] = None
            row["floor_registered_pmr"] = None
            row["floor_suggestion_action"] = None
            row["floor_suggestion_change_pct"] = None
            row["floor_suggested_value"] = None
            row["floor_suggestion_reason"] = None
    return results


def _placement_domain_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text).split("/", 1)[0].strip()
    text = text.split(":", 1)[0].strip().strip(".")
    if text.startswith("www."):
        text = text[4:]
    return text


def _placement_content_keys(attr_id: str, placement: str, display_name: str = "") -> set[str]:
    attr = str(attr_id or "").strip()
    values: set[str] = set()
    for raw in (placement, display_name):
        text = _placement_domain_key(raw)
        if text and attr:
            values.add(f"utm_content={attr}_{text}")
            values.add(f"utm_content={attr}{text}")
    return values


def _placement_content_domains(placement: str, display_name: str = "") -> set[str]:
    return {domain for domain in (_placement_domain_key(placement), _placement_domain_key(display_name)) if domain}


def _gam_content_matches_placement(attr_id: str, attr_raw: Any, exact_keys: set[str], domains: set[str]) -> bool:
    raw = str(attr_raw or "").strip().lower()
    if not raw:
        return False
    if raw in exact_keys:
        return True
    attr = str(attr_id or "").strip().lower()
    if not attr or not domains or "utm_content=" not in raw:
        return False
    content = raw.split("utm_content=", 1)[1].split("&", 1)[0].split("#", 1)[0].strip()
    if content.startswith(attr + "_"):
        host = content[len(attr) + 1:]
    elif content.startswith(attr):
        host = content[len(attr):]
    else:
        return False
    host = _placement_domain_key(host.lstrip("_-."))
    if not host:
        return False
    return any(host == domain or host.endswith("." + domain) for domain in domains)


def _floor_bucket_code(value: float) -> str:
    cents = int(round(max(0.0, float(value or 0.0)) * 100))
    return f"fl_{cents}"


FLOOR_BUCKET_VALUES = [
    1, 2, 3, 5, 7.5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
    55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 110, 120,
    130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230,
    240, 250, 260, 270, 280, 290, 300, 310, 320, 330, 340,
    350, 360, 370, 380, 390, 400, 410, 420, 430, 440, 450,
    460, 470, 480, 490, 500, 550, 600, 650, 700, 750, 800,
    850, 900, 950, 1000,
    *range(1100, 5001, 100),
]


def _nearest_floor_bucket_at_or_above(value: float) -> float:
    number = round(float(value or 0.0), 2)
    if number <= 0:
        return 0.0
    for bucket in FLOOR_BUCKET_VALUES:
        if bucket >= number:
            return float(bucket)
    return float(FLOOR_BUCKET_VALUES[-1])


def _nearest_floor_bucket_above(value: float) -> float:
    number = round(float(value or 0.0), 2)
    if number <= 0:
        return 0.0
    for bucket in FLOOR_BUCKET_VALUES:
        if bucket > number:
            return float(bucket)
    return float(FLOOR_BUCKET_VALUES[-1])


def _nearest_floor_bucket_at_or_below(value: float) -> float:
    number = round(float(value or 0.0), 2)
    if number <= 0:
        return 0.0
    selected = 0.0
    for bucket in FLOOR_BUCKET_VALUES:
        if bucket <= number:
            selected = float(bucket)
        else:
            break
    return selected


def _floor_from_ecpm(ecpm: float, impressions: float) -> float | None:
    if impressions < 2 or ecpm <= 0:
        return None
    return _nearest_floor_bucket_above(ecpm)


def _gam_floor_data_ready(now: datetime | None = None) -> bool:
    current = now or datetime.now(APP_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=APP_TZ)
    return current.astimezone(APP_TZ).hour >= 2


def _floor_bucket_shift(value: float, direction: str, steps: int = 1) -> float:
    number = round(float(value or 0.0), 2)
    if number <= 0:
        return 0.0
    steps = max(1, int(steps or 1))
    if direction == "increase":
        idx = 0
        for pos, bucket in enumerate(FLOOR_BUCKET_VALUES):
            if bucket >= number:
                idx = pos
                break
        else:
            idx = len(FLOOR_BUCKET_VALUES) - 1
        return float(FLOOR_BUCKET_VALUES[min(len(FLOOR_BUCKET_VALUES) - 1, idx + steps)])
    idx = 0
    for pos, bucket in enumerate(FLOOR_BUCKET_VALUES):
        if bucket <= number:
            idx = pos
        else:
            break
    return float(FLOOR_BUCKET_VALUES[max(0, idx - steps)])


def _decode_action_metadata(action: dict[str, Any] | sqlite3.Row | None) -> dict[str, Any]:
    if not action:
        return {}
    raw = ""
    try:
        raw = str(action.get("metadata_json") or "")  # type: ignore[union-attr]
    except AttributeError:
        try:
            raw = str(action["metadata_json"] or "")
        except Exception:
            raw = ""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _is_stale_disabled_floor_action(action: dict[str, Any] | None, current_day: date) -> bool:
    if not action or str(action.get("status") or "") != "disabled":
        return False
    try:
        created_day = datetime.fromisoformat(str(action.get("created_at") or "")).astimezone(APP_TZ).date()
    except Exception:
        return False
    return created_day < current_day


def _disabled_floor_can_reopen(action: dict[str, Any] | None, pmr: float, impressions: float) -> bool:
    if not action or str(action.get("status") or "") != "disabled":
        return False
    return impressions >= 2 and pmr >= 80


def _pmr_floor_decision(
    *,
    current_floor: float,
    ecpm: float,
    pmr: float,
    impressions: float,
    action: dict[str, Any] | None = None,
    is_demand_gen: bool = False,
) -> dict[str, Any]:
    metadata = _decode_action_metadata(action)
    action_status = str((action or {}).get("status") or "")
    impressions_at_registration = _to_float(metadata.get("impressions_at_registration"))
    impressions_since_registration = max(0.0, impressions - impressions_at_registration)
    if not _gam_floor_data_ready():
        return {
            "action_status": "hold",
            "direction": "wait",
            "new_floor": None,
            "change_pct": None,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": "GAM tem atraso de 2h; aguardar janela de dados ficar pronta antes de agir.",
        }

    if impressions < 2:
        return {
            "action_status": "hold",
            "direction": "observe",
            "new_floor": None,
            "change_pct": None,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": "Impressoes do bloco < 2; observar e nao alterar floor.",
        }

    if action_status == "disabled" and not _disabled_floor_can_reopen(action, pmr, impressions):
        return {
            "action_status": "hold",
            "direction": "disabled_until_tomorrow",
            "new_floor": None,
            "change_pct": None,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": "Regra desligada; volta somente no outro dia apos o reset de 00h.",
        }

    if action_status == "disabled" and _disabled_floor_can_reopen(action, pmr, impressions):
        current_floor = 0.0

    if current_floor <= 0:
        if pmr < 80:
            return {
                "action_status": "hold",
                "direction": "observe",
                "new_floor": None,
                "change_pct": None,
                "impressions_since_registration": round(impressions_since_registration, 2),
                "reason": f"Primeira analise so cria floor se PMR >= 80%; PMR atual {pmr:.2f}%.",
            }
        initial_floor = _floor_from_ecpm(ecpm, impressions)
        return {
            "action_status": "registered" if initial_floor is not None else "hold",
            "direction": "initial",
            "new_floor": round(initial_floor, 2) if initial_floor is not None else None,
            "change_pct": None,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": "Primeira analise com PMR >= 80%; usar eCPM atual para bucket inicial compativel/acima." if initial_floor is not None else "Aguardando impressoes e eCPM para primeiro floor.",
        }

    if pmr < 50:
        return {
            "action_status": "disabled",
            "direction": "disable",
            "new_floor": round(current_floor, 2),
            "change_pct": 0.0,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": "PMR abaixo de 50%; desligar regra e voltar somente no outro dia.",
        }

    if pmr < 70:
        if impressions_since_registration < 10:
            return {
                "action_status": "hold",
                "direction": "wait_volume",
                "new_floor": None,
                "change_pct": 0.0,
                "impressions_since_registration": round(impressions_since_registration, 2),
                "reason": f"PMR {pmr:.2f}% entre 51% e 69%, mas precisa de +10 impressoes desde a ultima alteracao; atual {impressions_since_registration:.0f}.",
            }
        next_floor = _floor_bucket_shift(current_floor, "decrease", 1)
        return {
            "action_status": "registered" if next_floor != current_floor else "hold",
            "direction": "decrease",
            "new_floor": round(next_floor, 2),
            "change_pct": round(((next_floor - current_floor) / current_floor) * 100.0, 2) if current_floor > 0 else None,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": f"PMR {pmr:.2f}% entre 51% e 69%; descer 1 bucket.",
        }

    if pmr < 80:
        return {
            "action_status": "hold",
            "direction": "keep",
            "new_floor": None,
            "change_pct": 0.0,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": f"PMR {pmr:.2f}% entre 70% e 79%; manter bucket atual.",
        }

    if impressions_since_registration < 10:
        return {
            "action_status": "hold",
            "direction": "wait_volume",
            "new_floor": None,
            "change_pct": 0.0,
            "impressions_since_registration": round(impressions_since_registration, 2),
            "reason": f"PMR {pmr:.2f}% >= 80%, mas precisa de +10 impressoes desde a ultima alteracao; atual {impressions_since_registration:.0f}.",
        }

    next_floor = _floor_bucket_shift(current_floor, "increase", 1)
    return {
        "action_status": "registered" if next_floor != current_floor else "hold",
        "direction": "increase",
        "new_floor": round(next_floor, 2),
        "change_pct": round(((next_floor - current_floor) / current_floor) * 100.0, 2) if current_floor > 0 else None,
        "impressions_since_registration": round(impressions_since_registration, 2),
        "reason": f"PMR {pmr:.2f}% acima de 80%; subir 1 bucket sobre o atual.",
    }


def load_project_campaign_placements(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    period: str = "today",
    today: date | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    current_day = today or app_today()
    if start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("Informe data inicial e final")
        try:
            start_day = date.fromisoformat(str(start_date))
            end_day = date.fromisoformat(str(end_date))
        except ValueError as exc:
            raise ValueError("Data invalida") from exc
        if end_day < start_day:
            raise ValueError("Data final menor que a inicial")
        period_key = f"custom:{start_day.isoformat()}:{end_day.isoformat()}"
    else:
        period_key, start_day, end_day = _campaign_period_window(current_day, period)
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return []
    site_to_project, project_to_sites = _site_project_maps(runtime)
    site_mapping = runtime["site_mapping"]
    dimension_overrides = runtime["dimension_overrides"]
    sites = sorted(project_to_sites.get(project, set()) or {project})
    if not sites:
        return []
    placeholders = ",".join("?" for _ in sites)
    placements: dict[str, dict[str, Any]] = {}
    placement_sessions = _traffic_sessions_by_placement(runtime, project, attr_id, start_day, end_day)
    ensure_gam_floor_manual_action_store(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        if not conn.execute("select 1 from sqlite_master where type='table' and name='ads_placement_daily'").fetchone():
            return []
        query = (
            "select placement, placement_display_name, placement_type, target_url, campaign, "
            "sum(cost) cost, sum(clicks) clicks, sum(impressions) impressions, sum(conversions) conversions "
            "from ads_placement_daily "
            f"where site in ({placeholders}) and attr_id=? and day between ? and ? "
            "group by placement, placement_display_name, placement_type, target_url, campaign "
            "order by cost desc"
        )
        for row in conn.execute(query, [*sites, str(attr_id), start_day.isoformat(), end_day.isoformat()]):
            placement = str(row["placement"] or row["placement_display_name"] or "").strip()
            if not placement:
                continue
            key = placement.lower()
            placements[key] = {
                "placement": placement,
                "display_name": str(row["placement_display_name"] or placement),
                "placement_type": str(row["placement_type"] or ""),
                "target_url": str(row["target_url"] or ""),
                "campaign": str(row["campaign"] or ""),
                "cost": _to_float(row["cost"]),
                "clicks": int(_to_float(row["clicks"])),
                "impressions": int(_to_float(row["impressions"])),
                "conversions": _to_float(row["conversions"]),
                "revenue": 0.0,
                "gross_revenue_usd": 0.0,
                "gam_impressions": 0,
                "gam_clicks": 0,
                "content_keys": _placement_content_keys(str(attr_id), placement, str(row["placement_display_name"] or "")),
                "content_domains": _placement_content_domains(placement, str(row["placement_display_name"] or "")),
                "ad_blocks": {},
            }
        if placements:
            gam_query = (
                "select day, attr_raw, breakdown, site, sum(revenue) revenue, sum(impressions) impressions, sum(clicks) clicks, "
                "sum(match_rate * impressions) rate_sum, sum(impressions) rate_weight, "
                "sum(viewability_rate * impressions) viewability_sum, sum(impressions) viewability_weight "
                "from gam_content_daily "
                "where lower(attr_raw) like ? and day between ? and ? "
                "group by day, attr_raw, breakdown, site"
            )
            gam_rows = conn.execute(
                gam_query,
                [f"%utm_content={str(attr_id).strip().lower()}%", start_day.isoformat(), end_day.isoformat()],
            ).fetchall()
            for item in placements.values():
                exact_keys = {str(key).lower() for key in item.get("content_keys", set())}
                domains = set(item.get("content_domains", set()))
                matching_rows = [
                    gam_row
                    for gam_row in gam_rows
                    if _gam_content_matches_placement(str(attr_id), gam_row["attr_raw"], exact_keys, domains)
                ]
                detailed_rows = [
                    gam_row
                    for gam_row in matching_rows
                    if "=" not in str(gam_row["breakdown"] or "")
                    and not str(gam_row["breakdown"] or "").lower().startswith(("utm_", "floor_"))
                ]
                for gam_row in detailed_rows or matching_rows:
                    row_revenue = _net_gam_revenue(gam_row["revenue"])
                    row_revenue_usd = (
                        _gam_brl_to_usd(_to_float(gam_row["revenue"]), gam_row["day"])
                        if _project_uses_usd_block_ecpm(project)
                        else 0.0
                    )
                    row_impressions = _to_float(gam_row["impressions"])
                    row_clicks = _to_float(gam_row["clicks"])
                    row_rate_sum = _to_float(gam_row["rate_sum"])
                    row_rate_weight = _to_float(gam_row["rate_weight"])
                    row_viewability_sum = _to_float(gam_row["viewability_sum"])
                    row_viewability_weight = _to_float(gam_row["viewability_weight"])
                    item["revenue"] += row_revenue
                    item["gross_revenue_usd"] += row_revenue_usd
                    item["gam_impressions"] += int(row_impressions)
                    item["gam_clicks"] += int(row_clicks)
                    if row_rate_weight > 0:
                        item["_rate_sum"] = _to_float(item.get("_rate_sum")) + row_rate_sum
                        item["_rate_weight"] = _to_float(item.get("_rate_weight")) + row_rate_weight
                    if row_viewability_weight > 0:
                        item["_viewability_sum"] = _to_float(item.get("_viewability_sum")) + row_viewability_sum
                        item["_viewability_weight"] = _to_float(item.get("_viewability_weight")) + row_viewability_weight
                    block_name = str(gam_row["breakdown"] or gam_row["site"] or "Sem bloco").strip() or "Sem bloco"
                    block_bucket = item["ad_blocks"].setdefault(
                        block_name,
                        {
                            "block_name": block_name,
                            "impressions": 0.0,
                            "clicks": 0.0,
                            "revenue": 0.0,
                            "gross_revenue_usd": 0.0,
                            "rate_sum": 0.0,
                            "rate_weight": 0.0,
                            "viewability_sum": 0.0,
                            "viewability_weight": 0.0,
                        },
                    )
                    block_bucket["revenue"] += row_revenue
                    block_bucket["gross_revenue_usd"] += row_revenue_usd
                    block_bucket["impressions"] += row_impressions
                    block_bucket["clicks"] += row_clicks
                    if row_rate_weight > 0:
                        block_bucket["rate_sum"] += row_rate_sum
                        block_bucket["rate_weight"] += row_rate_weight
                    if row_viewability_weight > 0:
                        block_bucket["viewability_sum"] += row_viewability_sum
                        block_bucket["viewability_weight"] += row_viewability_weight
            project_block_rows: dict[str, dict[str, Any]] = {}
            for gam_row in _load_period_rows(runtime, start_day.isoformat(), end_day.isoformat())[2]:
                row_attr_id = str(getattr(gam_row, "attr_id", "") or "").strip()
                row_candidates = {
                    str(value).strip()
                    for value in (getattr(gam_row, "attr_candidates", ()) or ())
                    if str(value).strip()
                }
                if row_attr_id != str(attr_id).strip() and str(attr_id).strip() not in row_candidates:
                    continue

                dims = enrich_dimensions(getattr(gam_row, "site", ""), site_mapping, dimension_overrides)
                row_site = str(dims.site or getattr(gam_row, "site", "") or "").strip()
                row_project = (
                    site_to_project.get(row_site)
                    or site_to_project.get(str(getattr(gam_row, "site", "") or "").strip())
                    or str(runtime.get("site_mapping", {}).get(row_site, "") or "").strip()
                    or row_site
                )
                if row_project != project:
                    continue

                block_name = str(getattr(gam_row, "breakdown", "") or getattr(gam_row, "site", "") or "Sem bloco").strip() or "Sem bloco"
                bucket = project_block_rows.setdefault(
                    block_name,
                    {
                        "block_name": block_name,
                        "impressions": 0.0,
                        "clicks": 0.0,
                        "unfilled_impressions": 0.0,
                        "total_requests": 0.0,
                        "matched_requests": 0.0,
                        "match_rate_sum": 0.0,
                        "match_rate_weight": 0.0,
                        "viewability_rate_sum": 0.0,
                        "viewability_rate_weight": 0.0,
                        "gross_revenue": 0.0,
                        "gross_revenue_usd": 0.0,
                        "net_revenue": 0.0,
                    },
                )

                impressions = _to_float(getattr(gam_row, "impressions", 0))
                unfilled_impressions = _to_float(getattr(gam_row, "unfilled_impressions", 0))
                clicks = _to_float(getattr(gam_row, "clicks", 0))
                revenue = _to_float(getattr(gam_row, "revenue", 0.0))
                row_total_requests, row_matched_requests = _resolve_request_metrics(
                    int(impressions),
                    int(unfilled_impressions),
                    int(_to_float(getattr(gam_row, "total_requests", 0))),
                    _to_float(getattr(gam_row, "match_rate", 0.0)),
                )

                bucket["impressions"] += impressions
                bucket["clicks"] += clicks
                bucket["unfilled_impressions"] += unfilled_impressions
                bucket["total_requests"] += row_total_requests
                bucket["matched_requests"] += row_matched_requests
                bucket["gross_revenue"] += revenue
                bucket["net_revenue"] += _net_gam_revenue(revenue)
                if _project_uses_usd_block_ecpm(project):
                    bucket["gross_revenue_usd"] += _gam_brl_to_usd(revenue, getattr(gam_row, "day", current_day.isoformat()))
                row_match_rate = _to_float(getattr(gam_row, "match_rate", 0.0))
                if row_match_rate > 0:
                    rate_weight = row_total_requests if row_total_requests > 0 else max(1, int(impressions))
                    bucket["match_rate_sum"] += row_match_rate * rate_weight
                    bucket["match_rate_weight"] += rate_weight
                row_viewability_rate = _to_float(getattr(gam_row, "viewability_rate", 0.0))
                if row_viewability_rate > 0:
                    viewability_weight = max(1, int(impressions))
                    bucket["viewability_rate_sum"] += row_viewability_rate * viewability_weight
                    bucket["viewability_rate_weight"] += viewability_weight

            if not project_block_rows:
                for gam_row in _load_period_rows(runtime, start_day.isoformat(), end_day.isoformat())[2]:
                    row_attr_id = str(getattr(gam_row, "attr_id", "") or "").strip()
                    row_candidates = {
                        str(value).strip()
                        for value in (getattr(gam_row, "attr_candidates", ()) or ())
                        if str(value).strip()
                    }
                    if row_attr_id != str(attr_id).strip() and str(attr_id).strip() not in row_candidates:
                        continue

                    block_name = str(getattr(gam_row, "breakdown", "") or getattr(gam_row, "site", "") or "Sem bloco").strip() or "Sem bloco"
                    bucket = project_block_rows.setdefault(
                        block_name,
                        {
                            "block_name": block_name,
                            "impressions": 0.0,
                            "clicks": 0.0,
                            "unfilled_impressions": 0.0,
                            "total_requests": 0.0,
                            "matched_requests": 0.0,
                            "match_rate_sum": 0.0,
                            "match_rate_weight": 0.0,
                            "viewability_rate_sum": 0.0,
                            "viewability_rate_weight": 0.0,
                            "gross_revenue": 0.0,
                            "gross_revenue_usd": 0.0,
                            "net_revenue": 0.0,
                        },
                    )
                    impressions = _to_float(getattr(gam_row, "impressions", 0))
                    unfilled_impressions = _to_float(getattr(gam_row, "unfilled_impressions", 0))
                    clicks = _to_float(getattr(gam_row, "clicks", 0))
                    revenue = _to_float(getattr(gam_row, "revenue", 0.0))
                    row_total_requests, row_matched_requests = _resolve_request_metrics(
                        int(impressions),
                        int(unfilled_impressions),
                        int(_to_float(getattr(gam_row, "total_requests", 0))),
                        _to_float(getattr(gam_row, "match_rate", 0.0)),
                    )
                    bucket["impressions"] += impressions
                    bucket["clicks"] += clicks
                    bucket["unfilled_impressions"] += unfilled_impressions
                    bucket["total_requests"] += row_total_requests
                    bucket["matched_requests"] += row_matched_requests
                    bucket["gross_revenue"] += revenue
                    bucket["net_revenue"] += _net_gam_revenue(revenue)
                    if _project_uses_usd_block_ecpm(project):
                        bucket["gross_revenue_usd"] += _gam_brl_to_usd(revenue, getattr(gam_row, "day", current_day.isoformat()))
                    row_match_rate = _to_float(getattr(gam_row, "match_rate", 0.0))
                    if row_match_rate > 0:
                        rate_weight = row_total_requests if row_total_requests > 0 else max(1, int(impressions))
                        bucket["match_rate_sum"] += row_match_rate * rate_weight
                        bucket["match_rate_weight"] += rate_weight
                    row_viewability_rate = _to_float(getattr(gam_row, "viewability_rate", 0.0))
                    if row_viewability_rate > 0:
                        viewability_weight = max(1, int(impressions))
                        bucket["viewability_rate_sum"] += row_viewability_rate * viewability_weight
                        bucket["viewability_rate_weight"] += viewability_weight

            latest_floor_actions: dict[str, dict[str, Any]] = {}
            block_names = [block_name for block_name in project_block_rows.keys() if block_name]
            if block_names:
                floor_placeholders = ",".join("?" for _ in block_names)
                floor_rows = conn.execute(
                    f"""
                    SELECT placement, block_name, current_floor, new_floor, change_pct, created_at, pmr_at_registration, status, metadata_json
                    FROM gam_floor_price_manual_actions
                    WHERE project = ?
                      AND attr_id = ?
                      AND scope = 'placement_block'
                      AND placement != ''
                      AND block_name IN ({floor_placeholders})
                    ORDER BY datetime(created_at) DESC
                    """,
                    [project, str(attr_id), *block_names],
                ).fetchall()
                for floor_row in floor_rows:
                    action_key = "|".join(
                        [
                            str(floor_row["placement"] or "").strip().lower(),
                            str(floor_row["block_name"] or "").strip().lower(),
                        ]
                    )
                    if action_key != "|" and action_key not in latest_floor_actions:
                        latest_floor_actions[action_key] = dict(floor_row)

                actions_today_rows = conn.execute(
                    f"""
                    SELECT placement, block_name, COUNT(*) AS actions_today
                    FROM gam_floor_price_manual_actions
                    WHERE project = ?
                      AND attr_id = ?
                      AND scope = 'placement_block'
                      AND placement != ''
                      AND block_name IN ({floor_placeholders})
                      AND substr(created_at, 1, 10) = ?
                    GROUP BY placement, block_name
                    """,
                    [project, str(attr_id), *block_names, app_today().isoformat()],
                ).fetchall()
                actions_today_map = {
                    "|".join([str(row["placement"] or "").strip().lower(), str(row["block_name"] or "").strip().lower()]): int(_to_float(row["actions_today"]))
                    for row in actions_today_rows
                }
            else:
                actions_today_map = {}

            ad_blocks: list[dict[str, Any]] = []
            for block_name, bucket in sorted(project_block_rows.items(), key=lambda item: (-_to_float(item[1]["net_revenue"]), item[0].lower())):
                impressions = int(round(_to_float(bucket["impressions"])))
                clicks = int(round(_to_float(bucket["clicks"])))
                unfilled_impressions = int(round(_to_float(bucket["unfilled_impressions"])))
                total_requests = int(round(_to_float(bucket["total_requests"])))
                matched_requests = _to_float(bucket["matched_requests"])
                match_rate_sum = _to_float(bucket["match_rate_sum"])
                match_rate_weight = _to_float(bucket["match_rate_weight"])
                pmr_value = 0.0
                if match_rate_weight > 0:
                    pmr_value = match_rate_sum / match_rate_weight
                elif total_requests > 0:
                    pmr_value = matched_requests / total_requests
                elif impressions > 0:
                    denominator = impressions + unfilled_impressions
                    pmr_value = impressions / denominator if denominator > 0 else 0.0
                viewability_value = 0.0
                if _to_float(bucket.get("viewability_rate_weight")) > 0:
                    viewability_value = _to_float(bucket.get("viewability_rate_sum")) / _to_float(bucket.get("viewability_rate_weight"))

                gross_revenue = round(_to_float(bucket["gross_revenue"]), 2)
                gross_revenue_usd = round(_to_float(bucket.get("gross_revenue_usd")), 6)
                net_revenue = round(_to_float(bucket["net_revenue"]), 2)
                use_gam_usd_ecpm = _project_uses_usd_block_ecpm(project)
                ecpm_value = (
                    (gross_revenue_usd / impressions * 1000.0)
                    if use_gam_usd_ecpm and impressions > 0
                    else ((net_revenue / impressions * 1000.0) if impressions > 0 else 0.0)
                )
                block_action = None
                if _is_stale_disabled_floor_action(block_action, current_day):
                    block_action = None
                is_demand_gen = _is_high_risk_campaign_type(item.get("channel_type"), item.get("campaign"))
                floor_status = "Sem regra registrada"
                current_floor = None
                previous_floor = None
                registered_at = None
                change_pct = None
                registered_pmr = None
                action_status = "hold"
                suggestion_direction = ""
                suggestion_reason = ""
                suggestion_change_pct = None
                suggestion_new_value = None
                impressions_since_registration = 0.0
                initial_decision = _pmr_floor_decision(
                    current_floor=0.0,
                    ecpm=ecpm_value,
                    pmr=pmr_value * 100.0,
                    impressions=impressions,
                    action=None,
                    is_demand_gen=is_demand_gen,
                )
                action_status = initial_decision["action_status"]
                suggestion_direction = initial_decision["direction"]
                suggestion_reason = initial_decision["reason"]
                suggestion_change_pct = initial_decision["change_pct"]
                suggestion_new_value = initial_decision["new_floor"]
                impressions_since_registration = initial_decision["impressions_since_registration"]
                if block_action:
                    block_action["actions_today_count"] = 0
                    current_floor_value = _to_float(block_action.get("new_floor"))
                    previous_floor_value = _to_float(block_action.get("current_floor"))
                    if str(block_action.get("status") or "") == "disabled":
                        floor_status = f"Regra desligada; antes {previous_floor_value:.2f} -> OFF"
                        current_floor_value = 0.0
                    else:
                        floor_status = f"Registrada {previous_floor_value:.2f} -> {current_floor_value:.2f}"
                    current_floor = round(current_floor_value, 2)
                    previous_floor = round(previous_floor_value, 2)
                    registered_at = block_action.get("created_at")
                    change_pct = round(_to_float(block_action.get("change_pct")) * 100.0, 2)
                    registered_pmr = round(_to_float(block_action.get("pmr_at_registration")), 2) if block_action.get("pmr_at_registration") is not None else None
                    decision = _pmr_floor_decision(
                        current_floor=current_floor_value,
                        ecpm=ecpm_value,
                        pmr=pmr_value * 100.0,
                        impressions=impressions,
                        action=block_action,
                        is_demand_gen=is_demand_gen,
                    )
                    action_status = decision["action_status"]
                    suggestion_direction = decision["direction"]
                    suggestion_reason = decision["reason"]
                    suggestion_change_pct = decision["change_pct"]
                    suggestion_new_value = decision["new_floor"]
                    impressions_since_registration = decision["impressions_since_registration"]
                    if str(block_action.get("status") or "") == "disabled" and decision.get("direction") == "initial" and decision.get("new_floor"):
                        floor_status = f"Regra desligada; antes {previous_floor_value:.2f} -> OFF | Reabrir: {float(decision.get('new_floor') or 0):.2f}"
                ad_blocks.append(
                    {
                        "block_name": block_name,
                        "revenue": round(net_revenue, 2),
                        "impressions": impressions,
                        "clicks": clicks,
                        "pmr": round(pmr_value * 100.0, 2),
                        "viewability": round(viewability_value * 100.0 if abs(viewability_value) <= 1 else viewability_value, 2),
                        "ecpm": round(ecpm_value, 2),
                        "floor_suggested_action_status": action_status,
                        "floor_suggested_change_pct": suggestion_change_pct,
                        "floor_suggestion_direction": suggestion_direction,
                        "floor_impressions_since_registration": impressions_since_registration,
                        "floor_suggestion_reason": suggestion_reason,
                        "floor_rule_status": floor_status,
                        "floor_current_value": current_floor,
                        "floor_current_bucket": _floor_bucket_code(current_floor) if current_floor and current_floor > 0 else "",
                        "floor_previous_value": previous_floor,
                        "floor_registered_at": registered_at,
                        "floor_change_pct": change_pct,
                        "floor_registered_pmr": registered_pmr,
                        "floor_suggested_new_value": round(suggestion_new_value, 2) if suggestion_new_value is not None else None,
                    }
                )
            action_rows = conn.execute(
                """
                SELECT placement, block_name, current_floor, new_floor, change_pct,
                       created_at, pmr_at_registration, status
                FROM gam_floor_price_manual_actions
                WHERE project = ?
                  AND attr_id = ?
                  AND scope = 'placement_block'
                ORDER BY datetime(created_at) DESC
                """,
                [project, str(attr_id)],
            ).fetchall()
            latest_actions: dict[tuple[str, str], dict[str, Any]] = {}
            for action_row in action_rows:
                action_key = (
                    str(action_row["placement"] or "").strip().lower(),
                    str(action_row["block_name"] or "").strip().lower(),
                )
                if all(action_key) and action_key not in latest_actions:
                    latest_actions[action_key] = dict(action_row)

            for item in placements.values():
                placement_key = str(item.get("placement") or "").strip().lower()
                raw_blocks = item.get("ad_blocks") or {}
                placement_blocks: list[dict[str, Any]] = []
                for block_name, metrics in raw_blocks.items():
                    if "=" in str(block_name) or str(block_name).lower().startswith(("utm_", "floor_")):
                        continue
                    impressions = int(round(_to_float(metrics.get("impressions"))))
                    clicks = int(round(_to_float(metrics.get("clicks"))))
                    revenue = round(_to_float(metrics.get("revenue")), 2)
                    gross_revenue_usd = _to_float(metrics.get("gross_revenue_usd"))
                    rate_weight = _to_float(metrics.get("rate_weight"))
                    pmr = (
                        _to_float(metrics.get("rate_sum")) / rate_weight * 100.0
                        if rate_weight > 0
                        else 0.0
                    )
                    viewability_weight = _to_float(metrics.get("viewability_weight"))
                    viewability = (
                        _to_float(metrics.get("viewability_sum")) / viewability_weight * 100.0
                        if viewability_weight > 0
                        else 0.0
                    )
                    use_gam_usd_ecpm = _project_uses_usd_block_ecpm(project)
                    ecpm = (
                        (gross_revenue_usd / impressions * 1000.0)
                        if use_gam_usd_ecpm and impressions > 0
                        else ((revenue / impressions * 1000.0) if impressions > 0 else 0.0)
                    )
                    block_row = {
                        "block_name": block_name,
                        "impressions": impressions,
                        "clicks": clicks,
                        "ctr": round((clicks / impressions * 100.0) if impressions > 0 else 0.0, 2),
                        "cpc": round((revenue / clicks) if clicks > 0 else 0.0, 2),
                        "net_revenue": revenue,
                        "pmr": round(pmr, 2),
                        "viewability": round(viewability, 2),
                        "ecpm": round(ecpm, 2),
                        "ecpm_currency": "USD" if use_gam_usd_ecpm else "BRL",
                        "floor_rule_status": "Sem regra registrada",
                        "floor_current_value": None,
                        "floor_previous_value": None,
                        "floor_registered_at": None,
                        "floor_change_pct": None,
                        "floor_registered_pmr": None,
                    }
                    action = latest_actions.get((placement_key, str(block_name).strip().lower()))
                    if action:
                        previous_floor = _to_float(action.get("current_floor"))
                        current_floor = _to_float(action.get("new_floor"))
                        if str(action.get("status") or "") == "disabled":
                            block_row["floor_rule_status"] = f"Regra desligada; antes {previous_floor:.2f} -> OFF"
                            current_floor = 0.0
                        else:
                            block_row["floor_rule_status"] = f"Registrada {previous_floor:.2f} -> {current_floor:.2f}"
                        block_row["floor_current_value"] = round(current_floor, 2)
                        block_row["floor_previous_value"] = round(previous_floor, 2)
                        block_row["floor_registered_at"] = action.get("created_at")
                        block_row["floor_change_pct"] = round(_to_float(action.get("change_pct")) * 100.0, 2)
                        block_row["floor_registered_pmr"] = (
                            round(_to_float(action.get("pmr_at_registration")), 2)
                            if action.get("pmr_at_registration") is not None
                            else None
                        )
                    placement_blocks.append(block_row)
                placement_blocks.sort(
                    key=lambda block: (_to_float(block.get("net_revenue")), _to_float(block.get("impressions"))),
                    reverse=True,
                )
                item["ad_blocks"] = placement_blocks
                active_count = sum(
                    1 for block in placement_blocks
                    if str(block.get("floor_rule_status") or "").startswith("Registrada")
                )
                item["floor_rule_status"] = (
                    f"{active_count} regra(s) ativa(s) por bloco"
                    if active_count
                    else "Configure nos blocos"
                )
                item["floor_current_value"] = None
                item["floor_previous_value"] = None
                item["floor_registered_at"] = None
                item["floor_change_pct"] = None
                item["floor_registered_pmr"] = None
    results: list[dict[str, Any]] = []
    for item in placements.values():
        impressions = int(item["impressions"])
        clicks = int(item["clicks"])
        conversions = _to_float(item["conversions"])
        cost = _to_float(item["cost"])
        revenue = _to_float(item["revenue"])
        gross_revenue_usd = _to_float(item.get("gross_revenue_usd"))
        gam_impressions = int(item["gam_impressions"])
        gam_clicks = int(item["gam_clicks"])
        rate_weight = _to_float(item.get("_rate_weight"))
        viewability_weight = _to_float(item.get("_viewability_weight"))
        has_exact_gam = revenue > 0 or gam_impressions > 0 or gam_clicks > 0 or rate_weight > 0
        placement_key = str(item["placement"] or "").strip().lower()
        sessions = sum(
            count
            for session_placement, count in placement_sessions.items()
            if session_placement == placement_key or session_placement.endswith(f".{placement_key}")
        )
        results.append(
            {
                "placement": item["placement"],
                "display_name": item["display_name"],
                "placement_type": item["placement_type"],
                "target_url": item["target_url"],
                "cost": round(cost, 2),
                "clicks": clicks,
                "impressions": impressions,
                "ctr": round((clicks / impressions * 100.0) if impressions > 0 else 0.0, 2),
                "cpc": round((cost / clicks) if clicks > 0 else 0.0, 2),
                "conversions": round(conversions, 2),
                "conversion_rate": round((conversions / clicks * 100.0) if clicks > 0 else 0.0, 2),
                "revenue": round(revenue, 2),
                "sessions": sessions,
                "rps": round(revenue / sessions, 2) if sessions > 0 else 0.0,
                "roi": round(((revenue - cost) / cost * 100.0) if cost > 0 else 0.0, 2),
                "gam_impressions": gam_impressions,
                "gam_clicks": gam_clicks,
                "pmr": round((_to_float(item.get("_rate_sum")) / rate_weight * 100.0) if rate_weight > 0 else 0.0, 2),
                "viewability": round((_to_float(item.get("_viewability_sum")) / viewability_weight * 100.0) if viewability_weight > 0 else 0.0, 2),
                "ecpm": round(
                    (gross_revenue_usd / gam_impressions * 1000.0)
                    if _project_uses_usd_block_ecpm(project) and gam_impressions > 0
                    else ((revenue / gam_impressions * 1000.0) if gam_impressions > 0 else 0.0),
                    2,
                ),
                "ecpm_currency": "USD" if _project_uses_usd_block_ecpm(project) else "BRL",
                "gam_data_exact": has_exact_gam,
                "gam_metric_source": "content_exact" if has_exact_gam else "",
                "gam_data_note": "" if has_exact_gam else "Sem dado exato de placement no GAM",
                "ad_blocks": item.get("ad_blocks", []),
                "floor_rule_status": item.get("floor_rule_status", "Sem regra registrada"),
                "floor_current_value": item.get("floor_current_value"),
                "floor_previous_value": item.get("floor_previous_value"),
                "floor_registered_at": item.get("floor_registered_at"),
                "floor_change_pct": item.get("floor_change_pct"),
                "floor_registered_pmr": item.get("floor_registered_pmr"),
                "period": period_key,
            }
        )
    results.sort(key=lambda row: _to_float(row["cost"]), reverse=True)
    return results


def exclude_project_campaign_placements(
    runtime: dict[str, Any],
    project: str,
    attr_id: str,
    placements: list[str],
) -> dict[str, Any]:
    selected = [str(item or "").strip() for item in placements if str(item or "").strip()]
    if not selected:
        raise ValueError("Nenhum placement selecionado.")
    db_path = Path(runtime["db_path"])
    site_to_project, project_to_sites = _site_project_maps(runtime)
    sites = sorted(project_to_sites.get(project, set()) or {project})
    if not sites:
        raise ValueError("Projeto sem sites mapeados.")
    placeholders = ",".join("?" for _ in sites)
    selected_lookup = {item.lower(): item for item in selected}
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "select customer_id, campaign_id, placement, placement_display_name, target_url "
            "from ads_placement_daily "
            f"where site in ({placeholders}) and attr_id=? "
            "order by day desc",
            [*sites, str(attr_id)],
        ).fetchall()
    targets: dict[tuple[str, str, str], str] = {}
    for row in rows:
        candidates = [
            str(row["placement"] or "").strip(),
            str(row["placement_display_name"] or "").strip(),
            str(row["target_url"] or "").strip(),
        ]
        if not any(candidate.lower() in selected_lookup for candidate in candidates if candidate):
            continue
        url = str(row["target_url"] or row["placement"] or row["placement_display_name"] or "").strip()
        if not url:
            continue
        targets[(str(row["customer_id"]), str(row["campaign_id"]), url)] = selected_lookup.get(url.lower(), url)
    if not targets:
        raise ValueError("Placements selecionados nao foram encontrados para essa campanha.")

    ensure_google_ads_script_store(db_path)
    created_at = datetime.now(APP_TZ).isoformat(timespec="seconds")
    created: list[str] = []
    with sqlite3.connect(db_path, timeout=20) as conn:
        conn.execute("PRAGMA busy_timeout=20000")
        for customer_id, campaign_id, url in targets:
            action_key = hashlib.sha256(f"{created_at}|placement_exclusion|{project}|{attr_id}|{customer_id}|{campaign_id}|{url}".encode()).hexdigest()
            conn.execute("INSERT OR IGNORE INTO google_ads_placement_exclusion_actions (action_key, created_at, project, attr_id, customer_id, campaign_id, placement, status, source) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_apply', 'manual_ui')", (action_key, created_at, project, attr_id, customer_id, campaign_id, url))
            created.append(action_key)
        conn.commit()
    return {"requested": selected, "created": created, "errors": [], "ok": bool(created), "status": "pending_apply"}


def load_project_summary(
    runtime: dict[str, Any],
    project: str,
    today: date | None = None,
    operational: bool = False,
    project_rows: list[dict[str, Any]] | None = None,
    project_kpis: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    current_day = today or app_today()
    if project_rows is None:
        project_rows = load_project_campaign_rows(runtime, project, current_day, operational=operational)
    if project_kpis is None:
        project_kpis = load_project_kpis(runtime, current_day, operational=operational)
    kpi_rows = project_kpis
    project_kpi = next((row for row in kpi_rows if str(row.get("Projeto", "") or "") == project), None)
    if not project_kpi:
        return {}
    gam_status = _gam_sync_status_for_project(runtime, project, current_day)
    session_periods = traffic_session_project_periods(runtime, project, current_day)
    summary = {
        "project": project,
        "campaigns": len(project_rows),
        "investimento_hoje": _to_float(project_kpi.get("Investimento Hoje")),
        "receita_hoje": _to_float(project_kpi.get("Receita Hoje")),
        "roi_hoje": _to_float(project_kpi.get("ROI Hoje")),
        "sessoes_hoje": _to_float(session_periods.get("hoje")),
        "investimento_ontem": _to_float(project_kpi.get("Investimento Ontem")),
        "receita_ontem": _to_float(project_kpi.get("Receita Ontem")),
        "roi_ontem": _to_float(project_kpi.get("ROI Ontem")),
        "sessoes_ontem": _to_float(session_periods.get("ontem")),
        "investimento_7d": _to_float(project_kpi.get("Investimento Ultimos 7 Dias")),
        "receita_7d": _to_float(project_kpi.get("Receita Ultimos 7 Dias")),
        "roi_7d": _to_float(project_kpi.get("ROI Ultimos 7 Dias")),
        "sessoes_7d": _to_float(session_periods.get("7d")),
        "investimento_mes": _to_float(project_kpi.get("Investimento Mes")),
        "receita_mes": _to_float(project_kpi.get("Receita Mes")),
        "roi_mes": _to_float(project_kpi.get("ROI Mes")),
        "sessoes_mes": _to_float(session_periods.get("mes")),
        "receita_push_hoje": _to_float(project_kpi.get("Receita Push Hoje")),
        "receita_push_mes": _to_float(project_kpi.get("Receita Push Mes")),
        "receita_mgid_hoje": _to_float(project_kpi.get("Receita MGID Hoje")),
        "receita_mgid_ontem": _to_float(project_kpi.get("Receita MGID Ontem")),
        "receita_mgid_7d": _to_float(project_kpi.get("Receita MGID 7 Dias")),
        "receita_mgid_mes": _to_float(project_kpi.get("Receita MGID Mes")),
        "receita_main_hoje": _to_float(project_kpi.get("Receita Main Hoje")),
        "receita_main_mes": _to_float(project_kpi.get("Receita Main Mes")),
        "receita_total_consolidada_hoje": _to_float(project_kpi.get("Receita Total Consolidada Hoje")),
        "receita_total_consolidada_mes": _to_float(project_kpi.get("Receita Total Consolidada Mes")),
        "fontes_push": str(project_kpi.get("Fontes Push", "") or ""),
        "hosts_push": str(project_kpi.get("Hosts Push", "") or ""),
        "campanhas_push_exemplo": str(project_kpi.get("Campanhas Push Exemplo", "") or ""),
        "laudo_divergencia": str(project_kpi.get("Laudo Divergencia", "") or ""),
        "updated_at": load_last_updated(runtime),
        "validacao_dados": str(project_kpi.get("Validacao Dados", "") or ""),
        "google_ads_snapshot_at": str(project_kpi.get("Google Ads Snapshot At", "") or ""),
        "google_ads_snapshot_age_minutes": _to_float(project_kpi.get("Google Ads Snapshot Age Minutes")),
        "google_ads_snapshot_status": str(project_kpi.get("Google Ads Snapshot Status", "") or ""),
        "gam_snapshot_status": gam_status.get("status", ""),
        "gam_snapshot_message": gam_status.get("message", ""),
    }
    for period in ("hoje", "ontem", "7d", "mes"):
        sessions = _to_float(summary.get(f"sessoes_{period}"))
        summary[f"rps_{period}"] = (
            _to_float(summary.get(f"receita_{period}")) / sessions if sessions > 0 else 0.0
        )
    return summary


def _gam_sync_status_for_project(runtime: dict[str, Any], project: str, current_day: date) -> dict[str, str]:
    log_path = Path(runtime["db_path"]).parent / "logs" / "fast_task.log"
    try:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-50000:].lower()
    except Exception:
        tail = ""
    last_start = tail.rfind("fast start")
    active_run = tail[last_start:] if last_start >= 0 else tail
    error_markers = (
        "aviso gam:",
        "invalid_grant",
        "no_networks_to_access",
        "unauthorized",
        "authenticationerror",
    )
    recovered_markers = (
        "linhas de gam.",
        "linhas de gam",
    )
    has_gam_recovery = any(marker in active_run for marker in recovered_markers) and " 0 linhas de gam" not in active_run
    if any(marker in active_run for marker in error_markers) and not has_gam_recovery:
        return {
            "status": "stale",
            "message": "GAM OAuth/API com erro no ultimo sync. Nao usar receita, ROI, PMR ou floor para decisao ate renovar o acesso e sincronizar.",
        }
    try:
        with sqlite3.connect(runtime["db_path"], timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=20000")
            row = conn.execute(
                "SELECT MAX(day) AS last_day, COUNT(*) AS rows_today FROM gam_site_daily WHERE site=? AND day=?",
                (project, current_day.isoformat()),
            ).fetchone()
            rows_today = int(row["rows_today"] or 0) if row else 0
            if rows_today <= 0:
                attributed_row = conn.execute(
                    """
                    SELECT COUNT(*) AS rows_today
                    FROM gam_attr_daily gam
                    WHERE gam.day=?
                      AND gam.attr_id<>''
                      AND gam.attr_id IN (
                          SELECT DISTINCT ads.attr_id
                          FROM ads_daily ads
                          WHERE ads.day=? AND ads.site=? AND ads.attr_id<>''
                      )
                    """,
                    (current_day.isoformat(), current_day.isoformat(), project),
                ).fetchone()
                rows_today = int(attributed_row["rows_today"] or 0) if attributed_row else 0
            if rows_today <= 0:
                return {
                    "status": "stale",
                    "message": "Sem snapshot GAM do projeto hoje. Nao usar receita/ROI/floor ate o sync GAM confirmar dados.",
                }
    except Exception:
        return {
            "status": "stale",
            "message": "Nao foi possivel validar o snapshot GAM. Bloquear decisoes de receita/floor ate verificacao.",
        }
    return {"status": "fresh", "message": ""}


def load_action_rows(runtime: dict[str, Any], limit: int = 50) -> list[dict[str, Any]]:
    rows = read_table(runtime["db_path"], "actions")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("payload_json")
        payload_data = {}
        if payload:
            try:
                import json

                payload_data = json.loads(str(payload or "{}"))
            except Exception:
                payload_data = {}
        normalized.append(
            {
                "action_id": str(row.get("action_id", "") or ""),
                "status": str(row.get("status", "") or ""),
                "action_type": str(row.get("action_type", "") or ""),
                "entity_type": str(row.get("entity_type", "") or ""),
                "entity_id": str(row.get("entity_id", "") or ""),
                "old_value": _to_float(row.get("old_value")),
                "new_value": _to_float(row.get("new_value")),
                "delta_pct": _to_float(row.get("delta_pct")),
                "created_at": str(row.get("created_at", "") or ""),
                "executed_at": str(row.get("executed_at", "") or ""),
                "payload": payload_data,
            }
        )
    normalized.sort(key=lambda row: row["created_at"], reverse=True)
    return normalized[:limit]


def _read_optional_table(runtime: dict[str, Any], table_name: str) -> list[dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(row) for row in rows]


def load_roi_control_runs(runtime: dict[str, Any], limit: int = 30) -> list[dict[str, Any]]:
    rows = _read_optional_table(runtime, "roi_control_runs")
    rows.sort(key=lambda row: str(row.get("generated_at", "") or ""), reverse=True)
    return rows[:limit]


def load_roi_control_actions(runtime: dict[str, Any], limit: int = 80) -> list[dict[str, Any]]:
    rows = _read_optional_table(runtime, "roi_control_actions")
    rows.sort(key=lambda row: str(row.get("generated_at", "") or ""), reverse=True)
    return rows[:limit]


def load_roi_control_outcomes(runtime: dict[str, Any], limit: int = 80) -> list[dict[str, Any]]:
    rows = _read_optional_table(runtime, "roi_control_action_outcomes")
    rows.sort(key=lambda row: str(row.get("measured_at", "") or ""), reverse=True)
    return rows[:limit]


def load_roi_control_latest_metrics(runtime: dict[str, Any], limit: int = 120) -> list[dict[str, Any]]:
    db_path = Path(runtime["db_path"])
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='roi_control_entity_metrics'"
        ).fetchone()
        if not exists:
            return []
        latest_run_ids = [
            str(row["run_id"])
            for row in conn.execute(
                """
                SELECT run_id
                FROM roi_control_runs
                ORDER BY generated_at DESC
                LIMIT 4
                """
            ).fetchall()
        ]
        if not latest_run_ids:
            return []
        placeholders = ",".join("?" for _ in latest_run_ids)
        rows = conn.execute(
            f"""
            SELECT *
            FROM roi_control_entity_metrics
            WHERE run_id IN ({placeholders}) AND period='hoje'
            ORDER BY generated_at DESC, cost DESC
            LIMIT ?
            """,
            (*latest_run_ids, int(limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def load_gam_sync_rows(runtime: dict[str, Any], limit: int = 50) -> list[dict[str, Any]]:
    rows = read_table(runtime["db_path"], "gam_sync_queue")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("payload_json")
        payload_data = {}
        if payload:
            try:
                import json

                payload_data = json.loads(str(payload or "{}"))
            except Exception:
                payload_data = {}
        normalized.append(
            {
                "sync_key": str(row.get("sync_key", "") or ""),
                "status": str(row.get("status", "") or ""),
                "site_name": str(row.get("site_name", "") or ""),
                "block_name": str(row.get("block_name", "") or ""),
                "attr_id": str(row.get("attr_id", "") or ""),
                "current_rule_key": str(row.get("current_rule_key", "") or ""),
                "target_rule_key": str(row.get("target_rule_key", "") or ""),
                "target_rule_name": str(row.get("target_rule_name", "") or ""),
                "current_floor_brl": _to_float(row.get("current_floor_brl")),
                "target_floor_brl": _to_float(row.get("target_floor_brl")),
                "created_at": str(row.get("created_at", "") or ""),
                "synced_at": str(row.get("synced_at", "") or ""),
                "payload": payload_data,
            }
        )
    normalized.sort(key=lambda row: row["created_at"], reverse=True)
    return normalized[:limit]



def sync_nexus_recommendations(runtime: dict[str, Any], today: date | None = None) -> list[dict[str, Any]]:
    current_day = today or app_today()
    projects = load_project_kpis(runtime, current_day, operational=True)
    existing_actions = {str(row.get('action_id', '') or ''): row for row in read_table(runtime['db_path'], 'actions')}
    existing_decisions = {str(row.get('decision_id', '') or ''): row for row in read_table(runtime['db_path'], 'decisions')}
    decision_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []

    for project in projects:
        project_id = str(project.get('Projeto', '') or '')
        gam_status = _gam_sync_status_for_project(runtime, project_id, current_day)
        decision_guard = evaluate_project_decision_guard(project, gam_status)
        spend_today = _to_float(project.get('Investimento Hoje'))
        spend_yesterday = _to_float(project.get('Investimento Ontem'))
        spend_7d = _to_float(project.get('Investimento Ultimos 7 Dias'))
        roi_today = _to_float(project.get('ROI Hoje'))
        roi_7d = _to_float(project.get('ROI Ultimos 7 Dias'))
        validation = str(project.get('Validacao Dados', '') or '')
        if not decision_guard["allowed"]:
            LOGGER.warning(
                "AUTOMATION_DATA_GUARD_BLOCKED project=%s codes=%s",
                project_id,
                ",".join(decision_guard["codes"]),
            )
            candidates.append(build_data_guard_reconciliation_candidate(project_id, project, decision_guard))
            continue
        for campaign_row in load_project_campaign_rows(runtime, project_id, current_day, operational=True):
            pause_candidate = _build_lossy_campaign_pause_candidate(project_id, campaign_row, current_day)
            if pause_candidate:
                candidates.append(pause_candidate)
        if spend_yesterday >= 50 and spend_today <= 0:
            candidates.append({
                'project_id': project_id,
                'agent_slug': 'trafego-pago',
                'action_type': 'investigate_delivery_drop',
                'priority': 'HIGH',
                'expected_reward': spend_yesterday,
                'probability_success': 0.89,
                'reason': f'Projeto investiu {spend_yesterday:.2f} ontem e zerou hoje.',
                'message': f'Investigar queda de entrega em {project_id}, revisar status, budget, sinais e grupos ativos.',
            })
        if spend_today >= 20 and roi_today < 0.40 and validation != 'divergente':
            candidates.append({
                'project_id': project_id,
                'agent_slug': 'trafego-pago',
                'action_type': 'optimize_project_below_target_roi',
                'priority': 'HIGH' if roi_today < 0.10 else 'MEDIUM',
                'expected_reward': abs(0.40 - roi_today),
                'probability_success': 0.84,
                'reason': f'Projeto abaixo da meta de ROI de 40% com {spend_today:.2f} investidos.',
                'message': f'Otimizar {project_id} para recuperar ROI alvo de 40%, com foco em corte de perdas e redistribuicao de verba.',
            })
        if spend_7d >= 200 and roi_7d < 0.10 and validation != 'divergente':
            candidates.append({
                'project_id': project_id,
                'agent_slug': 'orquestrador',
                'action_type': 'restructure_project_strategy',
                'priority': 'HIGH',
                'expected_reward': abs(0.10 - roi_7d),
                'probability_success': 0.86,
                'reason': f'Projeto consumiu {spend_7d:.2f} nos ultimos 7 dias com ROI de {roi_7d*100:.2f}%.',
                'message': f'Reestruturar estrategia de {project_id}, porque o resultado de 7 dias nao sustenta escala nem manutencao passiva.',
            })
        if spend_today >= 50 and roi_today <= -0.08:
            candidates.append({
                'project_id': project_id,
                'agent_slug': 'orquestrador',
                'action_type': 'review_loss_project',
                'priority': 'HIGH',
                'expected_reward': abs(roi_today),
                'probability_success': 0.74,
                'reason': f'Projeto com gasto relevante e ROI hoje em {roi_today*100:.2f}%.',
                'message': f'Revisar trafego, monetizacao e funil do projeto {project_id} para conter prejuizo.',
            })
        elif spend_today >= 50 and roi_today >= 0.10:
            candidates.append({
                'project_id': project_id,
                'agent_slug': 'trafego-pago',
                'action_type': 'scale_project_safely',
                'priority': 'MEDIUM',
                'expected_reward': roi_today,
                'probability_success': 0.68,
                'reason': f'Projeto com ROI hoje em {roi_today*100:.2f}% e volume suficiente para escala controlada.',
                'message': f'Avaliar escala controlada do projeto {project_id} mantendo guardrails de custo.',
            })

    spend_map = {str(row.get('Projeto', '') or ''): _to_float(row.get('Investimento Hoje')) for row in projects}
    filtered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in sorted(candidates, key=lambda c: (spend_map.get(c['project_id'], 0.0), 1 if c['priority'] == 'HIGH' else 0), reverse=True):
        dedupe = f"{item['project_id']}::{item.get('entity_type', 'project')}::{item.get('entity_id', item['project_id'])}::{item['action_type']}"
        if dedupe in seen:
            continue
        seen.add(dedupe)
        filtered.append(item)
    filtered = filtered[:8]

    created_at = datetime.now(timezone.utc).astimezone(APP_TZ).isoformat(timespec='seconds')
    day_key = current_day.isoformat()
    for item in filtered:
        project_id = item['project_id']
        agent_slug = str(item.get('agent_slug') or 'orquestrador')
        decision_id = f"nexus-decision-{item['action_type']}-{project_id}-{day_key}"
        action_id = f"nexus-action-{item['action_type']}-{project_id}-{day_key}"
        existing_action = existing_actions.get(action_id) or {}
        existing_decision = existing_decisions.get(decision_id) or {}
        status = str(existing_action.get('status', '') or existing_decision.get('status', '') or 'AWAITING_APPROVAL')
        rationale = {
            'agent_slug': agent_slug,
            'project_id': project_id,
            'priority': item['priority'],
            'reason': item['reason'],
            'message': item['message'],
            'requires_approval': True,
        }
        decision_rows.append({
            'decision_id': decision_id,
            'snapshot_id': f'snapshot-{project_id}-{day_key}',
            'entity_type': 'project',
            'entity_id': project_id,
            'action_type': item['action_type'],
            'old_value': 0,
            'proposed_value': 0,
            'probability_success': item['probability_success'],
            'expected_reward': item['expected_reward'],
            'rationale': rationale,
            'status': status,
            'created_at': str(existing_decision.get('created_at', '') or created_at),
        })
        action_rows.append({
            'action_id': action_id,
            'decision_id': decision_id,
            'entity_type': 'project',
            'entity_id': project_id,
            'action_type': item['action_type'],
            'old_value': 0,
            'new_value': 0,
            'delta_pct': item['expected_reward'],
            'trigger_source': 'nexus_agents_auto',
            'status': status,
            'created_at': str(existing_action.get('created_at', '') or created_at),
            'executed_at': str(existing_action.get('executed_at', '') or ''),
            'payload': rationale,
        })

    if decision_rows:
        insert_decisions(runtime['db_path'], decision_rows)
    if action_rows:
        insert_actions(runtime['db_path'], action_rows)
    return action_rows


def sync_agent_learnings(runtime: dict[str, Any], today: date | None = None) -> list[dict[str, Any]]:
    current_day = today or app_today()
    learned_at = datetime.now(timezone.utc).astimezone(APP_TZ).strftime("%d/%m/%Y %H:%M:%S")
    learnings = [
        {
            "learning_key": "push-guardian:daily-8-unique-minute-host",
            "agent_slug": "push-guardian",
            "scope": "push_send_queue",
            "rule": "Cada subdominio deve ter 8 disparos por dia, nenhum horario pode repetir e o host do link deve bater com o project_id.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "required_sends_per_subdomain": 8,
                    "unique_schedule_minute": True,
                    "host_must_match_project_id": True,
                    "severity": "HIGH",
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "orquestrador:tomorrow-financial-plan-required",
            "agent_slug": "orquestrador",
            "scope": "financial_planning",
            "rule": "Todo ciclo deve avaliar ROI, investimento, receita main/push e gerar plano de acao para o dia seguinte.",
            "learned_at": learned_at,
            "payload_json": json.dumps({"day": current_day.isoformat(), "tomorrow": (current_day + timedelta(days=1)).isoformat()}, ensure_ascii=False),
        },
        {
            "learning_key": "orquestrador:company-profit-first-operating-rule",
            "agent_slug": "orquestrador",
            "scope": "company_operating_model",
            "rule": "A empresa opera com lucro como objetivo principal, sobrevivencia como limite e autonomia apenas quando a acao melhora resultado ou aprendizado. Qualquer escrita em producao precisa de trilha, risco claro e responsavel definido.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "priority_order": ["profit", "survival", "cash_efficiency", "budget_protection", "learning", "scale"],
                    "live_writes_require_approval": True,
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "orquestrador:default-area-owner-agent-rule",
            "agent_slug": "orquestrador",
            "scope": "agent_operating_model",
            "rule": "Toda area operacional deve ter um agente dono por padrao. Esse agente monitora KPIs, identifica risco/oportunidade, quantifica impacto financeiro, recomenda acao concreta, reporta ao responsavel e mede o resultado para se autoaprimorar continuamente.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "responsible_owner": "Luciano",
                    "default_required": True,
                    "reporting_required": True,
                    "profit_first": True,
                    "agent_contract": [
                        "monitorar area especifica continuamente",
                        "analisar hoje, ontem, 7 dias e mes",
                        "diferenciar falha de dado, falha operacional e oportunidade",
                        "calcular impacto esperado em lucro, caixa, risco e aprendizado",
                        "reportar ao responsavel com evidencia e pergunta clara",
                        "acompanhar resultado depois da acao",
                        "registrar aprendizado e ajustar a propria regra",
                    ],
                    "area_owner_map": {
                        "dados_e_governanca": "governanca-dados",
                        "tracking_utms": "tracking-utms",
                        "painel_operacional": "painel-operacional",
                        "ux": "ux",
                        "trafego_pago": "trafego-pago",
                        "monetizacao_floor_placements": "monetizacao",
                        "ofertas_receita": "ofertas-receita",
                        "funil": "funil",
                        "conteudo_criativos": "conteudo-criativos",
                        "experimentos": "experimentacao",
                        "aprendizado": "aprendizado",
                        "auditoria": "auditor",
                        "orquestracao": "orquestrador",
                    },
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "ux:leigo-first-communication-rule",
            "agent_slug": "ux",
            "scope": "communication_design",
            "rule": "Toda mensagem importante deve ser traduzida para linguagem de leigo, com o que aconteceu, por que importa e o que fazer agora. O UX prepara a comunicacao, mas o orquestrador continua sendo o aprovador final quando houver impacto operacional.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "layers": ["o_que_aconteceu", "por_que_importa", "o_que_fazer_agora"],
                    "approval_owner": "orquestrador",
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "agent-scoreboard:profit-linked-autonomy-rule",
            "agent_slug": "agent-scoreboard-agent",
            "scope": "score_governance",
            "rule": "Scores de agente devem subir quando a acao melhora lucro, cair quando reduz lucro sem aprendizado e forcar reevaluacao abaixo do limite de confianca.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "minimum_positive_outcome_probability": 80,
                    "company_survival_gate": "negative_weekly_roi_blocks_growth",
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "profit-gate:luciano-decision-package-required",
            "agent_slug": "orquestrador",
            "scope": "agent_approval_governance",
            "rule": "Nada deve ser enviado para Luciano se nao houver informacoes suficientes para decisao: projeto, attr_id, dados de hoje, dados de 7 dias, impacto financeiro, risco, acao concreta e pergunta clara do que aprovar/reprovar.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "luciano_feedback": "reprovou todas as pendentes porque nao havia informacoes suficientes para decidir",
                    "hard_gate": True,
                    "required_decision_package": ["project", "attr_id", "today_data", "7d_data", "financial_impact", "risk", "concrete_action", "clear_decision_question"],
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "profit-gate:dual-author-responsible-score-rule",
            "agent_slug": "orquestrador",
            "scope": "agent_score_governance",
            "rule": "Quando Luciano aprova ou recusa uma melhoria, pontuam o agente autor e o agente responsavel pelo filtro. O responsavel mede taxa de acerto: aprovacoes de Luciano dividido por envios para Luciano; abaixo de 60% entra em estudo obrigatorio.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "author_score": {"responsible_passed": 1, "luciano_approved": 2, "profit_proven": 5, "luciano_rejected": -2, "loss_proven": -5},
                    "responsible_score": {"luciano_approved_filtered": 1, "profit_proven": 3, "luciano_rejected_filtered": -2, "loss_proven": -4, "blocked_bad_idea": 1, "blocked_useful_idea": -1},
                    "responsible_accuracy_minimum": 60,
                    "goal": "poucos envios, alta taxa de aprovacao e lucro comprovado",
                },
                ensure_ascii=False,
            ),
        },
        {
            "learning_key": "profit-gate:central-agent-approval-score-rule",
            "agent_slug": "orquestrador",
            "scope": "agent_approval_governance",
            "rule": "Nenhum agente aprova melhoria se nao responder claramente como protege ou aumenta lucro. Score >= 70 envia para Luciano; 50-69 fica em observacao; abaixo de 50 reprova e registra aprendizado.",
            "learned_at": learned_at,
            "payload_json": json.dumps(
                {
                    "day": current_day.isoformat(),
                    "score_model": {"financial_impact": 40, "data_evidence": 25, "controlled_risk": 20, "action_clarity": 15},
                    "agent_points": {"responsible_approved": 1, "luciano_approved": 2, "profit_proven": 5, "observation": -1, "responsible_rejected": -2, "loss_after_luciano": -5},
                    "mandatory_study_after_consecutive_rejections": 3,
                },
                ensure_ascii=False,
            ),
        },
    ]
    with sqlite3.connect(runtime["db_path"]) as conn:
        for row in learnings:
            conn.execute(
                """
                INSERT INTO nexus_agent_learnings (
                    learning_key, agent_slug, scope, rule, learned_at, payload_json
                )
                VALUES (
                    :learning_key, :agent_slug, :scope, :rule, :learned_at, :payload_json
                )
                ON CONFLICT(learning_key) DO UPDATE SET
                    learned_at=excluded.learned_at,
                    payload_json=excluded.payload_json
                """,
                row,
            )
    return learnings


def sync_tomorrow_financial_plan(runtime: dict[str, Any], today: date | None = None) -> list[dict[str, Any]]:
    current_day = today or app_today()
    tomorrow = current_day + timedelta(days=1)
    projects = load_project_kpis(runtime, current_day, operational=True)
    existing_actions = {str(row.get("action_id", "") or ""): row for row in read_table(runtime["db_path"], "actions")}
    existing_decisions = {str(row.get("decision_id", "") or ""): row for row in read_table(runtime["db_path"], "decisions")}
    created_at = datetime.now(timezone.utc).astimezone(APP_TZ).strftime("%d/%m/%Y %H:%M:%S")
    decision_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []

    for project in projects:
        project_id = str(project.get("Projeto", "") or "")
        if not project_id:
            continue
        gam_status = _gam_sync_status_for_project(runtime, project_id, current_day)
        decision_guard = evaluate_project_decision_guard(project, gam_status)
        if not decision_guard["allowed"]:
            LOGGER.warning(
                "TOMORROW_PLAN_DATA_GUARD_BLOCKED project=%s codes=%s",
                project_id,
                ",".join(decision_guard["codes"]),
            )
            reconciliation = build_data_guard_reconciliation_candidate(project_id, project, decision_guard)
            decision_id = f"nexus-decision-tomorrow-{reconciliation['action_type']}-{project_id}-{tomorrow.isoformat()}"
            action_id = f"nexus-action-tomorrow-{reconciliation['action_type']}-{project_id}-{tomorrow.isoformat()}"
            existing_action = existing_actions.get(action_id) or {}
            existing_decision = existing_decisions.get(decision_id) or {}
            status = str(existing_action.get("status", "") or existing_decision.get("status", "") or "AWAITING_APPROVAL")
            payload = {
                "agent_slug": reconciliation["agent_slug"],
                "project_id": project_id,
                "priority": reconciliation["priority"],
                "plan": reconciliation["action_type"],
                "tomorrow": tomorrow.isoformat(),
                "reason": reconciliation["reason"],
                "message": reconciliation["message"],
                "requires_approval": True,
            }
            decision_rows.append(
                {
                    "decision_id": decision_id,
                    "snapshot_id": f"snapshot-{project_id}-{current_day.isoformat()}",
                    "entity_type": "project",
                    "entity_id": project_id,
                    "action_type": f"tomorrow_{reconciliation['action_type']}",
                    "old_value": 0,
                    "proposed_value": 0,
                    "probability_success": reconciliation["probability_success"],
                    "expected_reward": reconciliation["expected_reward"],
                    "rationale": payload,
                    "status": status,
                    "created_at": created_at,
                }
            )
            action_rows.append(
                {
                    "action_id": action_id,
                    "decision_id": decision_id,
                    "entity_type": "project",
                    "entity_id": project_id,
                    "action_type": f"tomorrow_{reconciliation['action_type']}",
                    "old_value": 0,
                    "new_value": 0,
                    "delta_pct": reconciliation["expected_reward"],
                    "trigger_source": "nexus_agents_tomorrow_plan",
                    "status": status,
                    "created_at": created_at,
                    "executed_at": str(existing_action.get("executed_at", "") or ""),
                    "payload": payload,
                }
            )
            continue
        spend_today = _to_float(project.get("Investimento Hoje"))
        revenue_today = _to_float(project.get("Receita Hoje"))
        roi_today = _to_float(project.get("ROI Hoje"))
        roi_7d = _to_float(project.get("ROI Ultimos 7 Dias"))
        push_today = _to_float(project.get("Receita Push Hoje"))
        validation = str(project.get("Validacao Dados", "") or "")

        if validation == "divergente":
            plan = "bloquear_escala_e_reconciliar"
            priority = "HIGH"
            agent = "governanca-dados"
            reason = "dados divergentes antes do plano de amanha"
            reward = 0.0
        for campaign_row in load_project_campaign_rows(runtime, project_id, current_day, operational=True):
            pause_candidate = _build_lossy_campaign_pause_candidate(project_id, campaign_row, current_day)
            if not pause_candidate:
                continue
            campaign_decision_id = f"nexus-decision-{pause_candidate['action_type']}-{project_id}-{pause_candidate['entity_id']}-{tomorrow.isoformat()}"
            campaign_action_id = f"nexus-action-{pause_candidate['action_type']}-{project_id}-{pause_candidate['entity_id']}-{tomorrow.isoformat()}"
            existing_action = existing_actions.get(campaign_action_id) or {}
            existing_decision = existing_decisions.get(campaign_decision_id) or {}
            status = str(existing_action.get("status", "") or existing_decision.get("status", "") or "AWAITING_APPROVAL")
            payload = {
                "agent_slug": "trafego-pago",
                "project_id": project_id,
                "campaign_id": pause_candidate["campaign_id"],
                "campaign_name": pause_candidate["campaign_name"],
                "channel_type": pause_candidate["channel_type"],
                "priority": pause_candidate["priority"],
                "plan": pause_candidate["action_type"],
                "tomorrow": tomorrow.isoformat(),
                "reason": pause_candidate["reason"],
                "message": pause_candidate["message"],
                "requires_approval": True,
            }
            decision_rows.append(
                {
                    "decision_id": campaign_decision_id,
                    "snapshot_id": f"snapshot-{project_id}-{current_day.isoformat()}",
                    "entity_type": pause_candidate["entity_type"],
                    "entity_id": pause_candidate["entity_id"],
                    "action_type": pause_candidate["action_type"],
                    "old_value": 0,
                    "proposed_value": 0,
                    "probability_success": pause_candidate["probability_success"],
                    "expected_reward": pause_candidate["expected_reward"],
                    "rationale": payload,
                    "status": status,
                    "created_at": created_at,
                }
            )
            action_rows.append(
                {
                    "action_id": campaign_action_id,
                    "decision_id": campaign_decision_id,
                    "entity_type": pause_candidate["entity_type"],
                    "entity_id": pause_candidate["entity_id"],
                    "action_type": pause_candidate["action_type"],
                    "old_value": 0,
                    "new_value": 0,
                    "delta_pct": pause_candidate["expected_reward"],
                    "trigger_source": "nexus_agents_tomorrow_plan",
                    "status": status,
                    "created_at": created_at,
                    "executed_at": str(existing_action.get("executed_at", "") or ""),
                    "payload": payload,
                }
            )
        if validation != "divergente":
            if spend_today >= 50 and roi_today <= -0.08:
                plan = "reduzir_ou_pausar_perdedores"
                priority = "HIGH"
                agent = "trafego-pago"
                reason = f"ROI hoje {roi_today*100:.2f}% com investimento {spend_today:.2f}"
                reward = abs(roi_today)
            elif spend_today >= 50 and roi_today >= 0.10 and roi_7d >= 0:
                plan = "escalar_controlado_amanha"
                priority = "MEDIUM"
                agent = "trafego-pago"
                reason = f"ROI hoje {roi_today*100:.2f}% e 7d {roi_7d*100:.2f}%"
                reward = roi_today
            elif push_today > 0:
                plan = "manter_e_otimizar_push"
                priority = "MEDIUM"
                agent = "monetizacao"
                reason = f"Push gerou {push_today:.2f} hoje; manter 8 disparos e testar melhores slots"
                reward = push_today
            else:
                plan = "manter_observacao"
                priority = "INFO"
                agent = "orquestrador"
                reason = "sem sinal financeiro forte para escala ou corte"
                reward = 0.0

        decision_id = f"nexus-decision-tomorrow-{plan}-{project_id}-{tomorrow.isoformat()}"
        action_id = f"nexus-action-tomorrow-{plan}-{project_id}-{tomorrow.isoformat()}"
        payload = {
            "agent_slug": agent,
            "project_id": project_id,
            "priority": priority,
            "plan": plan,
            "tomorrow": tomorrow.isoformat(),
            "reason": reason,
            "message": (
                f"Amanha {tomorrow.isoformat()}: {plan} em {project_id}. "
                f"Hoje investiu {spend_today:.2f}, receita {revenue_today:.2f}, ROI {roi_today*100:.2f}%, push {push_today:.2f}."
            ),
            "requires_approval": True,
        }
        existing_action = existing_actions.get(action_id) or {}
        existing_decision = existing_decisions.get(decision_id) or {}
        status = str(existing_action.get("status", "") or existing_decision.get("status", "") or "AWAITING_APPROVAL")
        decision_rows.append(
            {
                "decision_id": decision_id,
                "snapshot_id": f"snapshot-{project_id}-{current_day.isoformat()}",
                "entity_type": "project",
                "entity_id": project_id,
                "action_type": f"tomorrow_{plan}",
                "old_value": 0,
                "proposed_value": 0,
                "probability_success": 0.72,
                "expected_reward": reward,
                "rationale": payload,
                "status": status,
                "created_at": created_at,
            }
        )
        action_rows.append(
            {
                "action_id": action_id,
                "decision_id": decision_id,
                "entity_type": "project",
                "entity_id": project_id,
                "action_type": f"tomorrow_{plan}",
                "old_value": 0,
                "new_value": 0,
                "delta_pct": reward,
                "trigger_source": "nexus_agents_tomorrow_plan",
                "status": status,
                "created_at": created_at,
                "executed_at": str(existing_action.get("executed_at", "") or ""),
                "payload": payload,
            }
        )

    if decision_rows:
        insert_decisions(runtime["db_path"], decision_rows)
    if action_rows:
        insert_actions(runtime["db_path"], action_rows)
    return action_rows


def _parse_push_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(APP_TZ)


def build_push_queue_guardian_events(
    runtime: dict[str, Any],
    today: date | None = None,
    created_at: str = "",
) -> list[dict[str, Any]]:
    current_day = today or app_today()
    day_key = current_day.isoformat()
    created_at = created_at or datetime.now(timezone.utc).astimezone(APP_TZ).isoformat(timespec="seconds")
    ignored_statuses = {"cancelled", "canceled", "skipped"}
    rows = []
    with sqlite3.connect(runtime["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        queue_rows = [dict(row) for row in conn.execute("SELECT * FROM push_send_queue").fetchall()]
    for row in queue_rows:
        status = str(row.get("status", "") or "").lower()
        if status in ignored_statuses:
            continue
        scheduled = _parse_push_datetime(row.get("scheduled_at"))
        sent = _parse_push_datetime(row.get("sent_at"))
        if (scheduled and scheduled.date() == current_day) or (sent and sent.date() == current_day):
            row = dict(row)
            row["_scheduled_local"] = scheduled
            rows.append(row)

    events: list[dict[str, Any]] = []
    by_project: dict[str, list[dict[str, Any]]] = {}
    by_minute: dict[str, list[dict[str, Any]]] = {}
    mismatches: list[dict[str, str]] = []

    for row in rows:
        project_id = str(row.get("project_id", "") or "").strip().lower()
        by_project.setdefault(project_id, []).append(row)
        scheduled = row.get("_scheduled_local")
        if isinstance(scheduled, datetime):
            minute_key = scheduled.strftime("%H:%M")
            by_minute.setdefault(minute_key, []).append(row)
        link_host = urlparse(str(row.get("link", "") or "")).netloc.lower().split("@")[-1].split(":")[0]
        if link_host.startswith("www."):
            link_host = link_host[4:]
        if project_id and link_host and link_host != project_id:
            mismatches.append(
                {
                    "send_id": str(row.get("send_id", "") or ""),
                    "project_id": project_id,
                    "link_host": link_host,
                }
            )

    for project_id, project_rows in sorted(by_project.items()):
        count = len(project_rows)
        if project_id and count != 8:
            events.append(
                {
                    "event_id": f"push-guardian-count-{project_id}-{day_key}",
                    "agent_slug": "push-guardian",
                    "event_type": "push_queue_validation",
                    "priority": "HIGH",
                    "title": "Subdominio com quantidade errada de pushs",
                    "message": f"{project_id} tem {count} disparos para {day_key}; regra exige 8 por dia.",
                    "entity_type": "push_send_queue",
                    "entity_id": project_id,
                    "project_id": project_id,
                    "created_at": created_at,
                    "metadata": {"day": day_key, "count": count, "expected": 8},
                }
            )

    duplicated_minutes = {minute: items for minute, items in by_minute.items() if len(items) > 1}
    for minute, items in sorted(duplicated_minutes.items()):
        projects = ", ".join(sorted({str(item.get("project_id", "") or "") for item in items}))
        send_ids = ", ".join(str(item.get("send_id", "") or "") for item in items[:4])
        events.append(
            {
                "event_id": f"push-guardian-time-{day_key}-{minute.replace(':', '')}",
                "agent_slug": "push-guardian",
                "event_type": "push_queue_validation",
                "priority": "HIGH",
                "title": "Pushs marcados no mesmo horario",
                "message": f"{day_key} {minute} BRT tem {len(items)} disparos simultaneos: {projects}. Send IDs: {send_ids}.",
                "entity_type": "push_send_queue",
                "entity_id": f"{day_key}-{minute}",
                "project_id": projects,
                "created_at": created_at,
                "metadata": {"day": day_key, "minute": minute, "send_ids": [str(item.get("send_id", "") or "") for item in items]},
            }
        )

    for mismatch in mismatches[:10]:
        events.append(
            {
                "event_id": f"push-guardian-host-{day_key}-{mismatch['send_id']}",
                "agent_slug": "push-guardian",
                "event_type": "push_queue_validation",
                "priority": "HIGH",
                "title": "Push apontando para subdominio diferente",
                "message": f"{mismatch['send_id']} esta em {mismatch['project_id']}, mas o link aponta para {mismatch['link_host']}.",
                "entity_type": "push_send_queue",
                "entity_id": mismatch["send_id"],
                "project_id": mismatch["project_id"],
                "created_at": created_at,
                "metadata": mismatch,
            }
        )

    return events


def sync_nexus_agent_events(runtime: dict[str, Any], today: date | None = None) -> list[dict[str, Any]]:
    current_day = today or app_today()
    summary = load_dashboard_summary(runtime, current_day, operational=True)
    projects = load_project_kpis(runtime, current_day, operational=True)
    learnings = sync_agent_learnings(runtime, current_day)
    sync_nexus_recommendations(runtime, current_day)
    sync_tomorrow_financial_plan(runtime, current_day)
    actions = load_action_rows(runtime, limit=20)
    sync_rows = load_gam_sync_rows(runtime, limit=12)
    updated_at = str(summary.get("updated_at", "") or "")

    rows: list[dict[str, Any]] = []
    now_local = datetime.now(timezone.utc).astimezone(APP_TZ)
    updated_dt = None
    if updated_at:
        try:
            updated_dt = datetime.strptime(updated_at, "%d/%m/%Y %H:%M:%S").replace(tzinfo=APP_TZ)
        except ValueError:
            updated_dt = None

    rows.append(
        {
            "event_id": f"nexus-orq-{updated_at}",
            "agent_slug": "orquestrador",
            "event_type": "summary",
            "priority": "INFO",
            "title": "Resumo operacional do momento",
            "message": (
                f"Investimento hoje {_to_float(summary.get('investimento_hoje')):.2f} | "
                f"Receita hoje {_to_float(summary.get('receita_hoje')):.2f} | "
                f"ROI hoje {_to_float(summary.get('roi_hoje')) * 100:.2f}%"
            ),
            "entity_type": "dashboard",
            "entity_id": "overview",
            "project_id": "",
            "created_at": updated_at,
            "metadata": {"updated_at": updated_at},
        }
    )

    if updated_dt is not None:
        lag_minutes = max(0, int((now_local - updated_dt).total_seconds() // 60))
        if lag_minutes >= 60:
            rows.append(
                {
                    "event_id": f"nexus-data-stale-{updated_at}",
                    "agent_slug": "governanca-dados",
                    "event_type": "stale_data",
                    "priority": "HIGH",
                    "title": "Painel operando com dado envelhecido",
                    "message": f"O snapshot operacional esta {lag_minutes} minutos atrasado e precisa de intervencao no sync.",
                    "entity_type": "dashboard",
                    "entity_id": "overview",
                    "project_id": "",
                    "created_at": updated_at,
                    "metadata": {"lag_minutes": lag_minutes, "updated_at": updated_at},
                }
            )

    rows.extend(build_push_queue_guardian_events(runtime, current_day, updated_at))

    for learning in learnings:
        rows.append(
            {
                "event_id": f"nexus-learning-{learning['learning_key']}",
                "agent_slug": str(learning.get("agent_slug", "") or "orquestrador"),
                "event_type": "learning",
                "priority": "INFO",
                "title": "Aprendizado operacional registrado",
                "message": str(learning.get("rule", "") or ""),
                "entity_type": "agent_learning",
                "entity_id": str(learning.get("learning_key", "") or ""),
                "project_id": "",
                "created_at": str(learning.get("learned_at", "") or updated_at),
                "metadata": {"scope": str(learning.get("scope", "") or "")},
            }
        )

    for project in projects:
        project_id = str(project.get("Projeto", "") or "")
        spend_today = _to_float(project.get("Investimento Hoje"))
        spend_yesterday = _to_float(project.get("Investimento Ontem"))
        roi_today = _to_float(project.get("ROI Hoje"))
        validation = str(project.get("Validacao Dados", "") or "")
        if validation == "divergente":
            rows.append(
                {
                    "event_id": f"nexus-data-{project_id}-{updated_at}",
                    "agent_slug": "governanca-dados",
                    "event_type": "validation",
                    "priority": "HIGH",
                    "title": "Divergencia de dados detectada",
                    "message": (
                        f"Projeto {project_id} com diferenca entre fonte oficial e leitura operacional. "
                        "O painel deve seguir a fonte oficial enquanto a reconciliacao nao fecha. "
                        f"Laudo: {str(project.get('Laudo Divergencia', '') or 'sem laudo')}."
                    ),
                    "entity_type": "project",
                    "entity_id": project_id,
                    "project_id": project_id,
                    "created_at": updated_at,
                    "metadata": {
                        "investimento_hoje_api": _to_float(project.get("Investimento Hoje API")),
                        "receita_hoje_api": _to_float(project.get("Receita Hoje API")),
                        "investimento_hoje_campanhas": _to_float(project.get("Investimento Hoje Campanhas")),
                        "receita_hoje_campanhas": _to_float(project.get("Receita Hoje Campanhas")),
                        "receita_main_hoje": _to_float(project.get("Receita Main Hoje")),
                        "receita_push_hoje": _to_float(project.get("Receita Push Hoje")),
                        "receita_total_consolidada_hoje": _to_float(project.get("Receita Total Consolidada Hoje")),
                        "fontes_push": str(project.get("Fontes Push", "") or ""),
                        "hosts_push": str(project.get("Hosts Push", "") or ""),
                        "campanhas_push_exemplo": str(project.get("Campanhas Push Exemplo", "") or ""),
                    },
                }
            )
        if spend_yesterday >= 50 and spend_today <= 0:
            rows.append(
                {
                    "event_id": f"nexus-delivery-drop-{project_id}-{updated_at}",
                    "agent_slug": "trafego-pago",
                    "event_type": "delivery_drop",
                    "priority": "HIGH",
                    "title": "Queda de entrega detectada",
                    "message": f"{project_id} investiu {spend_yesterday:.2f} ontem e esta zerado hoje; revisar entrega imediatamente.",
                    "entity_type": "project",
                    "entity_id": project_id,
                    "project_id": project_id,
                    "created_at": updated_at,
                    "metadata": {"investimento_ontem": spend_yesterday, "investimento_hoje": spend_today},
                }
            )
        if spend_today >= 20 and roi_today < 0.40 and validation != "divergente":
            rows.append(
                {
                    "event_id": f"nexus-below-target-{project_id}-{updated_at}",
                    "agent_slug": "trafego-pago",
                    "event_type": "below_target_roi",
                    "priority": "HIGH" if roi_today < 0.10 else "MEDIUM",
                    "title": "Projeto abaixo da meta de ROI",
                    "message": f"{project_id} esta com ROI de {roi_today*100:.2f}% e precisa reagir para voltar a meta de 40%.",
                    "entity_type": "project",
                    "entity_id": project_id,
                    "project_id": project_id,
                    "created_at": updated_at,
                    "metadata": {"investimento_hoje": spend_today, "roi_hoje": roi_today, "target_roi": 0.40},
                }
            )

    for row in actions[:10]:
        payload = row.get("payload") or {}
        action_type = str(row.get("action_type", "") or "")
        status = str(row.get("status", "") or "")
        created_at_raw = str(row.get("created_at", "") or updated_at)
        created_dt = None
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                created_dt = datetime.strptime(created_at_raw, fmt)
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=APP_TZ)
                else:
                    created_dt = created_dt.astimezone(APP_TZ)
                break
            except ValueError:
                continue
        if status.upper() == "AWAITING_APPROVAL":
            rows.append(
                {
                    "event_id": f"nexus-approval-{row['action_id']}",
                    "agent_slug": str(payload.get("agent_slug", "orquestrador") or "orquestrador"),
                    "event_type": "approval_request",
                    "priority": str(payload.get("priority", "HIGH") or "HIGH"),
                    "title": f"Aprovacao necessaria: {action_type}",
                    "message": (
                        f"{str(payload.get('message', 'Acao recomendada'))} "
                        f"Motivo: {str(payload.get('reason', 'sem motivo'))}. "
                        
                        f"Aprovar: /aprovar {row['action_id']} | Rejeitar: /rejeitar {row['action_id']}"
                    ),
                    "entity_type": "approval",
                    "entity_id": str(row.get("action_id", "") or ""),
                    "project_id": str(payload.get("project_id", "") or ""),
                    "created_at": str(row.get("created_at", "") or updated_at),
                    "metadata": payload,
                }
            )
            if created_dt is not None:
                age_hours = max(0, int((now_local - created_dt).total_seconds() // 3600))
                if age_hours >= 6:
                    rows.append(
                        {
                            "event_id": f"nexus-approval-overdue-{row['action_id']}",
                            "agent_slug": "orquestrador",
                            "event_type": "approval_overdue",
                            "priority": "HIGH" if age_hours >= 24 else "MEDIUM",
                            "title": "Aprovacao parada na fila",
                            "message": f"{row['action_id']} aguarda aprovacao ha {age_hours} horas e precisa de follow-up.",
                            "entity_type": "approval",
                            "entity_id": str(row.get("action_id", "") or ""),
                            "project_id": str(payload.get("project_id", "") or ""),
                            "created_at": created_at_raw,
                            "metadata": {**payload, "age_hours": age_hours},
                        }
                    )
        else:
            rows.append(
                {
                    "event_id": f"nexus-action-{row['action_id']}",
                    "agent_slug": str(payload.get("agent_slug", "monetizacao") or "monetizacao"),
                    "event_type": "action",
                    "priority": "INFO" if status.upper().startswith("EXECUTED") or status.upper() == "APPROVED" else "MEDIUM",
                    "title": f"Acao monitorada: {action_type}",
                    "message": f"Status {status} | {str(payload.get('message', action_type) or action_type)}",
                    "entity_type": "action",
                    "entity_id": str(row.get("action_id", "") or ""),
                    "project_id": str(payload.get("project_id", "") or ""),
                    "created_at": str(row.get("created_at", "") or updated_at),
                    "metadata": payload,
                }
            )

    for row in sync_rows[:6]:
        rows.append(
            {
                "event_id": f"nexus-sync-{row['sync_key']}",
                "agent_slug": "auditor",
                "event_type": "execution",
                "priority": "HIGH" if str(row.get("status", "")).upper() == "SYNC_ERROR" else "INFO",
                "title": f"Fila GAM: {row['status']}",
                "message": (
                    f"{row['site_name']} | {row['block_name']} | "
                    f"{_to_float(row['current_floor_brl']):.2f} -> {_to_float(row['target_floor_brl']):.2f}"
                ),
                "entity_type": "gam_sync",
                "entity_id": str(row.get("sync_key", "") or ""),
                "project_id": str(row.get("site_name", "") or ""),
                "created_at": str(row.get("created_at", "") or updated_at),
                "metadata": row.get("payload") or {},
            }
        )

    try:
        webhook_info = get_telegram_webhook_info(runtime["config"])
    except Exception as exc:
        webhook_info = {"ok": False, "reason": str(exc)}

    try:
        bot_info = get_telegram_bot_info(runtime["config"])
    except Exception as exc:
        bot_info = {"ok": False, "reason": str(exc)}

    if not bool(webhook_info.get("ok")):
        rows.append(
            {
                "event_id": f"nexus-audit-telegram-health-{updated_at}",
                "agent_slug": "auditor",
                "event_type": "delivery_health",
                "priority": "HIGH",
                "title": "Telegram com falha de auditoria",
                "message": f"Nao foi possivel validar a saude do webhook do Telegram: {webhook_info.get('reason', 'erro_desconhecido')}",
                "entity_type": "telegram",
                "entity_id": "webhook",
                "project_id": "",
                "created_at": updated_at,
                "metadata": webhook_info,
            }
        )
    else:
        info = webhook_info.get("result") or {}
        pending = int(info.get("pending_update_count") or 0)
        last_error_message = str(info.get("last_error_message", "") or "")
        last_error_date = int(info.get("last_error_date") or 0)
        url = str(info.get("url", "") or "")
        error_age_minutes = None
        if last_error_date:
            error_age_minutes = max(
                0.0,
                (datetime.now(timezone.utc).timestamp() - float(last_error_date)) / 60.0,
            )
        active_error = bool(last_error_message and (error_age_minutes is None or error_age_minutes <= 15))
        stale_error = bool(last_error_message and not active_error)
        if not url or active_error or pending > 0 or stale_error:
            priority = "HIGH" if active_error or not url else ("MEDIUM" if pending > 0 else "INFO")
            details: list[str] = []
            if not url:
                details.append("webhook_sem_url")
            if pending > 0:
                details.append(f"fila_pendente={pending}")
            if active_error:
                details.append(f"erro_ativo={last_error_message}")
            elif stale_error:
                details.append(f"erro_historico={last_error_message}")
            if error_age_minutes is not None:
                details.append(f"idade_erro_min={error_age_minutes:.1f}")
            rows.append(
                {
                    "event_id": f"nexus-audit-telegram-webhook-{updated_at}",
                    "agent_slug": "auditor",
                    "event_type": "delivery_health",
                    "priority": priority,
                    "title": "Telegram exige atencao",
                    "message": " | ".join(details) or "Webhook do Telegram precisa de revisao.",
                    "entity_type": "telegram",
                    "entity_id": "webhook",
                    "project_id": "",
                    "created_at": updated_at,
                    "metadata": info,
                }
            )

    if not bool(bot_info.get("ok")):
        rows.append(
            {
                "event_id": f"nexus-audit-telegram-bot-{updated_at}",
                "agent_slug": "auditor",
                "event_type": "delivery_health",
                "priority": "HIGH",
                "title": "Telegram com leitura indefinida",
                "message": f"Nao foi possivel validar a leitura do bot no grupo: {bot_info.get('reason', 'erro_desconhecido')}",
                "entity_type": "telegram",
                "entity_id": "bot_privacy",
                "project_id": "",
                "created_at": updated_at,
                "metadata": bot_info,
            }
        )
    else:
        bot = bot_info.get("result") or {}
        can_read_all = bool(bot.get("can_read_all_group_messages"))
        if not can_read_all:
            rows.append(
                {
                    "event_id": f"nexus-audit-telegram-privacy-{updated_at}",
                    "agent_slug": "auditor",
                    "event_type": "delivery_health",
                    "priority": "HIGH",
                    "title": "Telegram com privacy mode ativo",
                    "message": "O bot nao esta lendo todas as mensagens do grupo. Desative em @BotFather > /setprivacy > nexuswarroom_bot > Disable.",
                    "entity_type": "telegram",
                    "entity_id": "bot_privacy",
                    "project_id": "",
                    "created_at": updated_at,
                    "metadata": bot,
                }
            )

    insert_nexus_agent_events(runtime["db_path"], rows)
    return rows


def load_nexus_agent_events(runtime: dict[str, Any], limit: int = 40, today: date | None = None) -> list[dict[str, Any]]:
    sync_nexus_agent_events(runtime, today or app_today())
    rows = read_table(runtime["db_path"], "nexus_agent_events")
    normalized = []
    for row in rows:
        normalized.append(
            {
                "event_id": str(row.get("event_id", "") or ""),
                "agent_slug": str(row.get("agent_slug", "") or ""),
                "event_type": str(row.get("event_type", "") or ""),
                "priority": str(row.get("priority", "") or ""),
                "title": str(row.get("title", "") or ""),
                "message": str(row.get("message", "") or ""),
                "entity_type": str(row.get("entity_type", "") or ""),
                "entity_id": str(row.get("entity_id", "") or ""),
                "project_id": str(row.get("project_id", "") or ""),
                "created_at": str(row.get("created_at", "") or ""),
            }
        )
    normalized.sort(key=lambda row: row["created_at"], reverse=True)
    return normalized[:limit]


def load_nexus_telegram_channels(runtime: dict[str, Any]) -> list[dict[str, Any]]:
    rows = read_table(runtime["db_path"], "nexus_telegram_channels")
    normalized = []
    for row in rows:
        normalized.append(
            {
                "channel_key": str(row.get("channel_key", "") or ""),
                "channel_type": str(row.get("channel_type", "") or ""),
                "channel_name": str(row.get("channel_name", "") or ""),
                "chat_id": str(row.get("chat_id", "") or ""),
                "is_enabled": bool(row.get("is_enabled", 0)),
            }
        )
    normalized.sort(key=lambda row: row["channel_name"])
    return normalized


def load_nexus_telegram_messages(runtime: dict[str, Any], limit: int = 40) -> list[dict[str, Any]]:
    rows = read_table(runtime["db_path"], "nexus_telegram_messages")
    normalized = []
    for row in rows:
        normalized.append(
            {
                "message_key": str(row.get("message_key", "") or ""),
                "event_id": str(row.get("event_id", "") or ""),
                "channel_key": str(row.get("channel_key", "") or ""),
                "status": str(row.get("status", "") or ""),
                "telegram_message_id": str(row.get("telegram_message_id", "") or ""),
                "body": str(row.get("body", "") or ""),
                "sent_at": str(row.get("sent_at", "") or ""),
            }
        )
    normalized.sort(key=lambda row: row["sent_at"] or row["message_key"], reverse=True)
    return normalized[:limit]


def load_nexus_telegram_updates(runtime: dict[str, Any], limit: int = 30) -> list[dict[str, Any]]:
    rows = read_table(runtime["db_path"], "nexus_telegram_updates")
    normalized = []
    for row in rows:
        normalized.append(
            {
                "update_id": str(row.get("update_id", "") or ""),
                "chat_id": str(row.get("chat_id", "") or ""),
                "chat_type": str(row.get("chat_type", "") or ""),
                "sender_name": str(row.get("sender_name", "") or ""),
                "text": str(row.get("text", "") or ""),
                "status": str(row.get("status", "") or ""),
                "received_at": str(row.get("received_at", "") or ""),
                "processed_at": str(row.get("processed_at", "") or ""),
            }
        )
    normalized.sort(key=lambda row: row["received_at"] or row["update_id"], reverse=True)
    return normalized[:limit]


def seed_nexus_war_room_messages(runtime: dict[str, Any], today: date | None = None) -> list[dict[str, Any]]:
    events = load_nexus_agent_events(runtime, limit=20, today=today or app_today())
    return queue_war_room_events(runtime["db_path"], runtime["config"], events=events)

# Phase 1 autonomous agents memory. Additive only: observes and records, does not execute actions.
def _agent_memory_db_path(runtime: dict[str, Any]) -> Path:
    return Path(runtime["db_path"]).with_name("agent_memory.sqlite3")


def ensure_agent_memory_store(runtime: dict[str, Any]) -> Path:
    db_path = _agent_memory_db_path(runtime)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT UNIQUE NOT NULL,
                instance TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_key TEXT UNIQUE NOT NULL,
                run_key TEXT NOT NULL,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                severity TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                metrics_json TEXT NOT NULL DEFAULT '{}',
                recommendation TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'observed',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_learnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                learning_key TEXT UNIQUE NOT NULL,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                title TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                confidence REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_alert_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feedback_key TEXT UNIQUE NOT NULL,
                observation_key TEXT NOT NULL,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL DEFAULT '',
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                decision TEXT NOT NULL DEFAULT 'invalid',
                note TEXT NOT NULL DEFAULT '',
                make_pattern INTEGER NOT NULL DEFAULT 0,
                agent_verdict TEXT NOT NULL DEFAULT 'pending',
                agent_verdict_reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                reviewed_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_improvement_suggestions (
                suggestion_key TEXT PRIMARY KEY,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                project TEXT NOT NULL DEFAULT '',
                attr_id TEXT NOT NULL DEFAULT '',
                area TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                problem TEXT NOT NULL DEFAULT '',
                evidence TEXT NOT NULL DEFAULT '',
                proposal TEXT NOT NULL DEFAULT '',
                expected_impact TEXT NOT NULL DEFAULT '',
                risk TEXT NOT NULL DEFAULT '',
                effort TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT '',
                source_scope TEXT NOT NULL DEFAULT 'internal_db,map,agent_history',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT '',
                decision_at TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_improvement_status ON agent_improvement_suggestions(instance, status, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_improvement_agent ON agent_improvement_suggestions(instance, agent_slug, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_alert_feedback_observation ON agent_alert_feedback(observation_key, decision)")
        feedback_columns = {row[1] for row in conn.execute("PRAGMA table_info(agent_alert_feedback)").fetchall()}
        feedback_migrations = {
            "agent_verdict": "ALTER TABLE agent_alert_feedback ADD COLUMN agent_verdict TEXT NOT NULL DEFAULT 'pending'",
            "agent_verdict_reason": "ALTER TABLE agent_alert_feedback ADD COLUMN agent_verdict_reason TEXT NOT NULL DEFAULT ''",
            "reviewed_at": "ALTER TABLE agent_alert_feedback ADD COLUMN reviewed_at TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in feedback_migrations.items():
            if column not in feedback_columns:
                conn.execute(statement)
        improvement_columns = {row[1] for row in conn.execute("PRAGMA table_info(agent_improvement_suggestions)").fetchall()}
        improvement_migrations = {
            "profit_answer": "ALTER TABLE agent_improvement_suggestions ADD COLUMN profit_answer TEXT NOT NULL DEFAULT ''",
            "score_financial_impact": "ALTER TABLE agent_improvement_suggestions ADD COLUMN score_financial_impact INTEGER NOT NULL DEFAULT 0",
            "score_evidence": "ALTER TABLE agent_improvement_suggestions ADD COLUMN score_evidence INTEGER NOT NULL DEFAULT 0",
            "score_risk": "ALTER TABLE agent_improvement_suggestions ADD COLUMN score_risk INTEGER NOT NULL DEFAULT 0",
            "score_clarity": "ALTER TABLE agent_improvement_suggestions ADD COLUMN score_clarity INTEGER NOT NULL DEFAULT 0",
            "score_total": "ALTER TABLE agent_improvement_suggestions ADD COLUMN score_total INTEGER NOT NULL DEFAULT 0",
            "responsible_owner": "ALTER TABLE agent_improvement_suggestions ADD COLUMN responsible_owner TEXT NOT NULL DEFAULT ''",
            "responsible_decision": "ALTER TABLE agent_improvement_suggestions ADD COLUMN responsible_decision TEXT NOT NULL DEFAULT ''",
            "responsible_reason": "ALTER TABLE agent_improvement_suggestions ADD COLUMN responsible_reason TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in improvement_migrations.items():
            if column not in improvement_columns:
                conn.execute(statement)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_score_events (
                event_key TEXT PRIMARY KEY,
                instance TEXT NOT NULL,
                agent_slug TEXT NOT NULL,
                suggestion_key TEXT NOT NULL DEFAULT '',
                points INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        score_columns = {row[1] for row in conn.execute("PRAGMA table_info(agent_score_events)").fetchall()}
        score_migrations = {
            "score_role": "ALTER TABLE agent_score_events ADD COLUMN score_role TEXT NOT NULL DEFAULT 'author'",
            "decision_type": "ALTER TABLE agent_score_events ADD COLUMN decision_type TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in score_migrations.items():
            if column not in score_columns:
                conn.execute(statement)
        conn.execute(
            """
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
            """
        )
        conn.commit()
    return db_path


def _agent_key(*parts: object) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _agent_record_observation(conn: sqlite3.Connection, row: dict[str, Any]) -> bool:
    columns = [
        "observation_key", "run_key", "instance", "agent_slug", "severity", "entity_type", "entity_id",
        "project", "attr_id", "title", "details", "metrics_json", "recommendation", "status", "created_at",
    ]
    values = [row.get(column, "") for column in columns]
    cur = conn.execute(
        f"INSERT OR IGNORE INTO agent_observations ({','.join(columns)}) VALUES ({','.join(['?'] * len(columns))})",
        values,
    )
    return cur.rowcount > 0


def _agent_percent(value: Any) -> float:
    value = _to_float(value)
    return value * 100 if 0 < abs(value) <= 1 else value


def _agent_roi_pct(value: Any) -> float:
    return _agent_percent(value)



def _agent_alert_type(agent_slug: str, title: str) -> tuple[str, str, str, bool, str]:
    title_l = str(title or "").lower()
    agent_l = str(agent_slug or "").lower()
    if agent_l == "governanca-dados" or "sem receita" in title_l:
        return (
            "tracking/dados",
            "Receita pode nao estar conciliada com Ads/GAM/push ou a UTM pode nao estar fechando.",
            "Validar sync, UTM, GAM e push antes de pausar, aumentar budget ou mexer em floor.",
            True,
            "proximo sync",
        )
    if agent_l == "roi":
        return (
            "performance",
            "A campanha esta consumindo verba com retorno abaixo do limite operacional.",
            "Diagnosticar receita, placements, PMR, budget e status no Google Ads antes de escalar.",
            False,
            "fechamento do dia",
        )
    if agent_l == "aprendizado-campanha":
        return (
            "aprendizado/conversao",
            "Campanha recente pode precisar comprar dados antes de encontrar publico qualificado; campanha antiga sem conversao indica problema real.",
            "Comparar idade, conversoes, taxa de conversao, gasto e ROI antes de pausar ou escalar.",
            False,
            "proxima janela de aprendizado",
        )
    if agent_l == "placements":
        return (
            "trafego/placements",
            "Ha sinais de canais consumindo verba com baixa receita ou ROI negativo.",
            "Abrir placements, filtrar cliques e ROI, selecionar canais ruins e avaliar exclusao.",
            False,
            "proxima revisao de placements",
        )
    if agent_l == "floor-price":
        return (
            "monetizacao/floor",
            "PMR, receita e historico indicam oportunidade ou risco na regra de preco.",
            "Abrir blocos GAM, conferir PMR/cliques/receita e registrar ajuste manual quando fizer sentido.",
            False,
            "24h apos registro",
        )
    if agent_l == "orcamento":
        return (
            "orcamento/escala",
            "Campanha pode estar chegando ao limite de budget com ROI positivo.",
            "Validar no primeiro horario do dia seguinte e medir se lucro e volume melhoraram.",
            False,
            "proximo dia",
        )
    return ("operacional", "Padrao operacional detectado pelos agentes.", "Abrir /agents para revisar o detalhe.", False, "proxima revisao")


def load_agent_global_alerts(runtime: dict[str, Any], active: str = "", limit: int = 8) -> list[dict[str, Any]]:
    db_path = ensure_agent_memory_store(runtime)
    severity_rank = {"CRITICAL": 5, "HIGH": 4, "WARN": 3, "OPPORTUNITY": 2, "INFO": 1}
    alerts: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT obs.* FROM agent_observations obs
                WHERE obs.severity IN ('CRITICAL','HIGH','WARN','OPPORTUNITY')
                  AND NOT EXISTS (
                    SELECT 1 FROM agent_alert_feedback fb
                    WHERE fb.decision = 'valid'
                      AND fb.agent_verdict IN ('pending','accepted')
                      AND (
                        fb.observation_key = obs.observation_key
                        OR (
                          fb.instance = obs.instance
                          AND fb.agent_slug = obs.agent_slug
                          AND fb.project = obs.project
                          AND fb.attr_id = obs.attr_id
                          AND fb.title = obs.title
                        )
                      )
                  )
                ORDER BY obs.id DESC
                LIMIT 300
                """
            ).fetchall()
            seen: set[tuple[str, str, str]] = set()
            for row in rows:
                agent_slug = str(row["agent_slug"] or "")
                title = str(row["title"] or "")
                project = str(row["project"] or "")
                key = (agent_slug, title, project)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    metrics = json.loads(row["metrics_json"] or "{}")
                except Exception:
                    metrics = {}
                spend_today = _to_float(metrics.get("spend_today"))
                spend_7d = _to_float(metrics.get("spend_7d"))
                revenue_today = _to_float(metrics.get("revenue_today"))
                roi_today = _to_float(metrics.get("roi_today"))
                impact_value = max(spend_today, min(spend_7d, spend_today + max(0.0, spend_7d / 7.0)))
                problem_type, hypothesis, next_action, automation_block, expires = _agent_alert_type(agent_slug, title)
                severity = str(row["severity"] or "INFO")
                priority_score = severity_rank.get(severity, 1) * 100000 + impact_value
                roi_context = ""
                if active == "roi-control":
                    if automation_block:
                        roi_context = "Controle ROI: nao aplicar decisao automatica ate validar dados."
                    elif agent_slug in {"roi", "placements"}:
                        roi_context = "Controle ROI: revisar esta entidade antes de aprovar novas acoes."
                    elif agent_slug == "orcamento":
                        roi_context = "Controle ROI: oportunidade de escala, mas medir resultado no dia seguinte."
                alerts.append({
                    "observation_key": str(row["observation_key"] or ""),
                    "severity": severity,
                    "agent_slug": agent_slug,
                    "title": title,
                    "project": project,
                    "attr_id": str(row["attr_id"] or ""),
                    "details": str(row["details"] or ""),
                    "recommendation": str(row["recommendation"] or ""),
                    "problem_type": problem_type,
                    "hypothesis": hypothesis,
                    "next_action": next_action,
                    "automation_block": automation_block,
                    "expires": expires,
                    "impact_value": round(impact_value, 2),
                    "roi_today": round(roi_today, 2),
                    "spend_today": round(spend_today, 2),
                    "revenue_today": round(revenue_today, 2),
                    "roi_context": roi_context,
                    "created_at": str(row["created_at"] or ""),
                    "priority_score": priority_score,
                })
    except Exception:
        return []
    alerts.sort(key=lambda item: (-_to_float(item.get("priority_score")), -_to_float(item.get("impact_value"))))
    return alerts[:limit]

def record_agent_alert_feedback(runtime: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    db_path = ensure_agent_memory_store(runtime)
    observation_key = str(payload.get("observation_key") or "").strip()
    decision = str(payload.get("decision") or "invalid").strip().lower()
    note = str(payload.get("note") or "").strip()
    if decision not in {"valid", "invalid"}:
        decision = "invalid"
    if not observation_key:
        raise ValueError("observation_key ausente")
    if len(note) < 8:
        raise ValueError("registre uma observacao mais clara")
    now_text = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
    make_pattern = 1 if payload.get("make_pattern") else 0
    last_error: Exception | None = None
    for attempt in range(6):
        try:
            with sqlite3.connect(db_path, timeout=90) as conn:
                conn.execute("PRAGMA busy_timeout=90000")
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM agent_observations WHERE observation_key=?", (observation_key,)).fetchone()
                if not row:
                    raise ValueError("alerta nao encontrado")
                feedback_key = _agent_key(observation_key, decision, note, make_pattern, now_text)
                conn.execute(
                    """
                    INSERT INTO agent_alert_feedback (
                        feedback_key, observation_key, instance, agent_slug, project, attr_id, title,
                        decision, note, make_pattern, agent_verdict, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        feedback_key,
                        observation_key,
                        str(row["instance"] or ""),
                        str(row["agent_slug"] or ""),
                        str(row["project"] or ""),
                        str(row["attr_id"] or ""),
                        str(row["title"] or ""),
                        decision,
                        note,
                        make_pattern,
                        now_text,
                    ),
                )
                if decision == "valid":
                    conn.execute("UPDATE agent_observations SET status='human_validated' WHERE observation_key=?", (observation_key,))
                    conn.execute(
                        "UPDATE agent_observations SET status='human_validated' WHERE instance=? AND agent_slug=? AND project=? AND attr_id=? AND title=?",
                        (str(row["instance"] or ""), str(row["agent_slug"] or ""), str(row["project"] or ""), str(row["attr_id"] or ""), str(row["title"] or "")),
                    )
                conn.commit()
                return {"ok": True, "decision": decision, "hidden": decision == "valid", "agent_verdict": "pending", "created_at": now_text}
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "locked" not in str(exc).lower() or attempt >= 5:
                break
            time.sleep(0.35 * (attempt + 1))
    raise last_error or RuntimeError("falha ao registrar feedback")

def _agent_campaign_learning_profile(runtime: dict[str, Any], attr_id: str, current_day: date, campaign: str = "", channel: str = "", campaign_id: str = "") -> dict[str, Any]:
    db_path = Path(runtime.get("db_path") or "")
    if not attr_id or not db_path.exists():
        return {}
    start_7d = (current_day - timedelta(days=6)).isoformat()
    start_3d = (current_day - timedelta(days=2)).isoformat()
    prev_3d_start = (current_day - timedelta(days=5)).isoformat()
    prev_3d_end = (current_day - timedelta(days=3)).isoformat()
    prev_7d_start = (current_day - timedelta(days=13)).isoformat()
    prev_7d_end = (current_day - timedelta(days=7)).isoformat()
    today_text = current_day.isoformat()

    def avg(values: list[float]) -> float:
        clean = [float(v) for v in values if v is not None]
        return sum(clean) / len(clean) if clean else 0.0

    def pct_delta(current_value: float, previous_value: float) -> float:
        if previous_value == 0:
            return 100.0 if current_value > 0 else 0.0
        return ((current_value - previous_value) / abs(previous_value)) * 100.0

    campaign_l = str(campaign or "").lower()
    channel_l = str(channel or "").lower()
    conversion_goal = "meta_google_ads"
    if "adviewinterstitial" in campaign_l or "interstitial" in campaign_l:
        conversion_goal = "adViewInterstitial"
    elif "adviewrewarded" in campaign_l or "reward" in campaign_l:
        conversion_goal = "adViewRewarded"
    elif "adview" in campaign_l or channel_l == "display":
        conversion_goal = "adView"
    elif "button" in campaign_l or "botao" in campaign_l or "bot?o" in campaign_l:
        conversion_goal = "clique_botao"
    settings: dict[str, Any] = {}

    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            first = conn.execute(
                """
                SELECT MIN(day) AS first_day
                FROM ads_daily
                WHERE attr_id = ?
                  AND (cost > 0 OR clicks > 0 OR impressions > 0 OR conversions > 0)
                """,
                (str(attr_id),),
            ).fetchone()
            ads_rows = conn.execute(
                """
                SELECT day, SUM(cost) AS cost, SUM(clicks) AS clicks, SUM(impressions) AS impressions, SUM(conversions) AS conversions
                FROM ads_daily
                WHERE attr_id = ? AND day BETWEEN ? AND ?
                GROUP BY day
                """,
                (str(attr_id), prev_7d_start, today_text),
            ).fetchall()
            gam_rows = conn.execute(
                """
                SELECT day,
                       SUM(revenue) AS revenue,
                       SUM(impressions) AS impressions,
                       SUM(clicks) AS clicks,
                       SUM(unfilled_impressions) AS unfilled_impressions,
                       SUM(total_requests) AS total_requests,
                       AVG(match_rate) AS avg_match_rate
                FROM gam_attr_daily
                WHERE attr_id = ? AND day BETWEEN ? AND ?
                GROUP BY day
                """,
                (str(attr_id), prev_7d_start, today_text),
            ).fetchall()
            settings_exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_daily'").fetchone()
            if settings_exists:
                lookup_ids = [str(value) for value in (campaign_id, attr_id) if str(value or "")]
                if lookup_ids:
                    placeholders = ",".join("?" for _ in lookup_ids)
                    row = conn.execute(
                        f"""
                        SELECT * FROM google_ads_campaign_settings_daily
                        WHERE campaign_id IN ({placeholders})
                           OR ad_group_id IN ({placeholders})
                           OR attr_id IN ({placeholders})
                        ORDER BY
                            CASE WHEN ad_group_id IN ({placeholders}) OR attr_id IN ({placeholders}) THEN 0 ELSE 1 END,
                            day DESC
                        LIMIT 1
                        """,
                        tuple(lookup_ids * 5),
                    ).fetchone()
                    if row:
                        settings = dict(row)
    except Exception:
        return {}

    first_seen_day = str(first["first_day"] or "") if first else ""
    campaign_start_day = _parse_ads_date(settings.get("start_date", ""))
    first_day = campaign_start_day.isoformat() if campaign_start_day else first_seen_day
    age_days = None
    if first_day:
        try:
            age_days = (current_day - date.fromisoformat(first_day)).days + 1
        except Exception:
            age_days = None
    learning_lock_days = 7
    learning_locked = age_days is not None and age_days <= learning_lock_days
    learning_unlock_date = ""
    if campaign_start_day:
        learning_unlock_date = (campaign_start_day + timedelta(days=learning_lock_days)).isoformat()

    daily: dict[str, dict[str, float]] = {}
    for row in ads_rows:
        day = str(row["day"])
        daily.setdefault(day, {})
        daily[day].update({
            "cost": _to_float(row["cost"]),
            "ads_clicks": _to_float(row["clicks"]),
            "ads_impressions": _to_float(row["impressions"]),
            "conversions": _to_float(row["conversions"]),
        })
    for row in gam_rows:
        day = str(row["day"])
        daily.setdefault(day, {})
        revenue = _to_float(row["revenue"])
        impressions = _to_float(row["impressions"])
        unfilled = _to_float(row["unfilled_impressions"])
        total_requests = _to_float(row["total_requests"])
        request_base = total_requests if total_requests > 0 else impressions + unfilled
        pmr = (impressions / request_base * 100.0) if request_base > 0 else _agent_percent(row["avg_match_rate"])
        daily[day].update({
            "revenue": revenue,
            "gam_impressions": impressions,
            "gam_clicks": _to_float(row["clicks"]),
            "pmr": pmr,
            "ecpm": (revenue / impressions * 1000.0) if impressions > 0 else 0.0,
        })
    for day, values in daily.items():
        cost = _to_float(values.get("cost"))
        revenue = _to_float(values.get("revenue"))
        values["roi"] = ((revenue - cost) / cost * 100.0) if cost > 0 else 0.0
        clicks = _to_float(values.get("ads_clicks"))
        conversions = _to_float(values.get("conversions"))
        values["conversion_rate"] = (conversions / clicks * 100.0) if clicks > 0 else 0.0

    days_7d = [(current_day - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    days_last_3 = [(current_day - timedelta(days=i)).isoformat() for i in range(2, -1, -1)]
    days_prev_3 = [(current_day - timedelta(days=i)).isoformat() for i in range(5, 2, -1)]

    def total(days: list[str], key: str) -> float:
        return sum(_to_float(daily.get(day, {}).get(key)) for day in days)

    def window_avg(days: list[str], key: str) -> float:
        return avg([_to_float(daily.get(day, {}).get(key)) for day in days if day in daily])

    cost_7d = total(days_7d, "cost")
    clicks_7d = total(days_7d, "ads_clicks")
    impressions_7d = total(days_7d, "ads_impressions")
    conversions_7d = total(days_7d, "conversions")
    revenue_7d = total(days_7d, "revenue")
    conversions_prev_7d = sum(_to_float(row["conversions"]) for row in ads_rows if prev_7d_start <= str(row["day"]) <= prev_7d_end)

    current_pmr = window_avg(days_last_3, "pmr")
    previous_pmr = window_avg(days_prev_3, "pmr")
    current_ecpm = window_avg(days_last_3, "ecpm")
    previous_ecpm = window_avg(days_prev_3, "ecpm")
    current_roi = window_avg(days_last_3, "roi")
    previous_roi = window_avg(days_prev_3, "roi")
    current_conversion_rate = window_avg(days_last_3, "conversion_rate")
    previous_conversion_rate = window_avg(days_prev_3, "conversion_rate")
    current_revenue = total(days_last_3, "revenue")
    previous_revenue = total(days_prev_3, "revenue")
    current_cost = total(days_last_3, "cost")
    previous_cost = total(days_prev_3, "cost")
    current_conversions = total(days_last_3, "conversions")
    previous_conversions = total(days_prev_3, "conversions")

    pmr_delta = current_pmr - previous_pmr
    ecpm_delta = pct_delta(current_ecpm, previous_ecpm)
    revenue_delta = pct_delta(current_revenue, previous_revenue)
    roi_delta = current_roi - previous_roi
    conversion_rate_delta = current_conversion_rate - previous_conversion_rate
    conversion_delta = pct_delta(current_conversions, previous_conversions)
    spend_delta = pct_delta(current_cost, previous_cost)

    score = 0
    for value in (pmr_delta, ecpm_delta, revenue_delta, roi_delta, conversion_rate_delta):
        if value > 5:
            score += 1
        elif value < -5:
            score -= 1
    if current_revenue > current_cost and current_cost > 0:
        score += 1
    if current_conversions >= previous_conversions and current_conversions > 0:
        score += 1

    if score >= 3:
        trend = "melhorando"
    elif score <= -2:
        trend = "piorando"
    else:
        trend = "estavel"

    daily_summary = [
        {
            "day": day,
            "cost": round(_to_float(daily.get(day, {}).get("cost")), 6),
            "revenue": round(_to_float(daily.get(day, {}).get("revenue")), 6),
            "roi": round(_to_float(daily.get(day, {}).get("roi")), 6),
            "pmr": round(_to_float(daily.get(day, {}).get("pmr")), 6),
            "ecpm": round(_to_float(daily.get(day, {}).get("ecpm")), 6),
            "conversions": round(_to_float(daily.get(day, {}).get("conversions")), 6),
            "conversion_rate": round(_to_float(daily.get(day, {}).get("conversion_rate")), 6),
        }
        for day in days_7d
        if day in daily
    ]

    exact_goal = _conversion_goal_name(settings)
    if exact_goal:
        conversion_goal = exact_goal
    return {
        "first_seen_day": first_seen_day,
        "age_days": age_days,
        "learning_locked": learning_locked,
        "learning_lock_days": learning_lock_days,
        "learning_unlock_date": learning_unlock_date,
        "conversion_goal": conversion_goal,
        "conversion_goal_source": settings.get("conversion_goal_source") or ("google_ads_snapshot" if exact_goal else "inferred"),
        "configured_conversion_actions": settings.get("configured_conversion_actions_json", "[]"),
        "observed_conversion_actions": settings.get("observed_conversion_actions_json", "[]"),
        "campaign_start_date": settings.get("start_date", ""),
        "bidding_strategy_type": settings.get("bidding_strategy_type", ""),
        "target_cpa": round(_to_float(settings.get("target_cpa")), 6),
        "avg_cpc_ads": round(_to_float(settings.get("avg_cpc")), 6),
        "ads_settings_day": settings.get("day", ""),
        "cost_7d": round(cost_7d, 6),
        "revenue_7d_ads_goal_window": round(revenue_7d, 6),
        "clicks_7d": round(clicks_7d, 6),
        "impressions_7d": round(impressions_7d, 6),
        "conversions_7d": round(conversions_7d, 6),
        "cost_3d": round(current_cost, 6),
        "clicks_3d": round(total(days_last_3, "ads_clicks"), 6),
        "conversions_3d": round(current_conversions, 6),
        "conversions_prev_7d": round(conversions_prev_7d, 6),
        "conversion_rate_7d": round((conversions_7d / clicks_7d * 100.0) if clicks_7d > 0 else 0.0, 6),
        "kpi_trend": trend,
        "kpi_improvement_score": score,
        "pmr_delta_3d_vs_prev3d": round(pmr_delta, 6),
        "ecpm_delta_pct_3d_vs_prev3d": round(ecpm_delta, 6),
        "revenue_delta_pct_3d_vs_prev3d": round(revenue_delta, 6),
        "spend_delta_pct_3d_vs_prev3d": round(spend_delta, 6),
        "roi_delta_3d_vs_prev3d": round(roi_delta, 6),
        "conversion_rate_delta_3d_vs_prev3d": round(conversion_rate_delta, 6),
        "conversion_delta_pct_3d_vs_prev3d": round(conversion_delta, 6),
        "daily_kpi_summary": daily_summary,
    }


def _agent_operational_change_observations(runtime: dict[str, Any], instance: str, run_key: str, now_text: str, current_day: date) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    main_db = Path(runtime["db_path"])
    campaign_map = _agent_known_campaign_map(runtime, current_day)
    try:
        with sqlite3.connect(main_db) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_changes'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM google_ads_campaign_settings_changes ORDER BY created_at DESC LIMIT 500"):
                    campaign_id = str(row["campaign_id"] or "")
                    ad_group_id = str(row["ad_group_id"] or "")
                    mapped = campaign_map.get(campaign_id) or campaign_map.get(ad_group_id) or {}
                    project = str(mapped.get("project") or str(row["campaign"] or ""))
                    attr_id = str(mapped.get("attr_id") or str(row["attr_id"] or ad_group_id or campaign_id))
                    old_value = _to_float(row["old_value"])
                    new_value = _to_float(row["new_value"])
                    metrics = {
                        "change_type": str(row["change_type"] or ""),
                        "old_source": str(row["old_source"] or ""),
                        "new_source": str(row["new_source"] or ""),
                        "old_value": old_value,
                        "new_value": new_value,
                        "campaign_id": campaign_id,
                        "ad_group_id": ad_group_id,
                    }
                    observations.append({
                        "observation_key": _agent_key(instance, "operational-change", str(row["change_key"] or "")),
                        "run_key": run_key,
                        "instance": instance,
                        "agent_slug": "trafego-pago",
                        "severity": "INFO",
                        "entity_type": "operational_change",
                        "entity_id": str(row["change_key"] or ""),
                        "project": project,
                        "attr_id": attr_id,
                        "title": "Mudanca operacional registrada",
                        "details": f"{row['reason']} | {row['old_source']} {old_value:.2f} -> {row['new_source']} {new_value:.2f}",
                        "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                        "recommendation": "Usar este registro como baseline para comparar ROI, receita, PMR e volume nas proximas janelas de aprendizado.",
                        "status": "observed",
                        "created_at": str(row["created_at"] or now_text),
                    })
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='campaign_budget_manual_actions'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM campaign_budget_manual_actions ORDER BY created_at DESC LIMIT 500"):
                    project = str(row["project"] or "")
                    attr_id = str(row["attr_id"] or "")
                    if project and attr_id:
                        actions.append({"action_key": str(row["action_key"]), "action_type": "manual_budget_increase", "project": project, "attr_id": attr_id, "entity_id": str(row["campaign_id"] or ""), "created_at": str(row["created_at"] or ""), "metadata": {"old_budget": _to_float(row["old_budget"]), "new_budget": _to_float(row["new_budget"]), "status": str(row["status"] or ""), "source": str(row["source"] or "manual_ui")}})
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='campaign_bid_manual_actions'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM campaign_bid_manual_actions ORDER BY created_at DESC LIMIT 500"):
                    metrics = {
                        "action_type": str(row["action_type"] or ""),
                        "old_value": _to_float(row["old_value"]),
                        "new_value": _to_float(row["new_value"]),
                        "status": str(row["status"] or ""),
                        "source": str(row["source"] or ""),
                    }
                    observations.append({
                        "observation_key": _agent_key(instance, "manual-bid-action", str(row["action_key"] or "")),
                        "run_key": run_key,
                        "instance": instance,
                        "agent_slug": "trafego-pago",
                        "severity": "INFO",
                        "entity_type": "operational_change",
                        "entity_id": str(row["action_key"] or ""),
                        "project": str(row["project"] or ""),
                        "attr_id": str(row["attr_id"] or ""),
                        "title": "Ajuste manual registrado",
                        "details": f"{row['action_type']} | {row['old_value']:.2f} -> {row['new_value']:.2f} | motivo: {row['reason']}",
                        "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                        "recommendation": "Avaliar resultado do ajuste antes de repetir o padrao em outras campanhas.",
                        "status": "observed",
                        "created_at": str(row["created_at"] or now_text),
                    })
    except Exception:
        return observations
    return observations


def _agent_campaign_observations(instance: str, run_key: str, now_text: str, project: str, rows: list[dict[str, Any]], runtime: dict[str, Any] | None = None, current_day: date | None = None) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []

    def add(agent: str, severity: str, entity_type: str, entity_id: str, attr_id: str, title: str, details: str, metrics: dict[str, Any], recommendation: str) -> None:
        observations.append(
            {
                "observation_key": _agent_key(instance, agent, project, attr_id, title, date.today().isoformat()),
                "run_key": run_key,
                "instance": instance,
                "agent_slug": agent,
                "severity": severity,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "project": project,
                "attr_id": attr_id,
                "title": title,
                "details": details,
                "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                "recommendation": recommendation,
                "status": "observed",
                "created_at": now_text,
            }
        )

    for item in rows:
        attr_id = str(item.get("attr_id") or item.get("campaign_id") or "")
        campaign = str(item.get("campaign") or item.get("site") or project)
        spend_today = _to_float(item.get("investimento_hoje"))
        revenue_today = _to_float(item.get("receita_total_hoje", item.get("receita_hoje")))
        roi_today = _agent_roi_pct(item.get("roi_hoje"))
        spend_7d = _to_float(item.get("investimento_7d"))
        revenue_7d = _to_float(item.get("receita_total_7d", item.get("receita_7d")))
        roi_7d = _agent_roi_pct(item.get("roi_7d"))
        pmr_today = _agent_percent(item.get("pmr_hoje"))
        budget = _to_float(item.get("daily_budget"))
        channel = str(item.get("channel_type") or "").upper()
        learning_profile = _agent_campaign_learning_profile(runtime or {}, attr_id, current_day or app_today(), campaign, channel, str(item.get("campaign_id") or "")) if runtime else {}
        age_days = learning_profile.get("age_days")
        first_seen_day = str(learning_profile.get("first_seen_day") or "").strip()
        conversions_7d = _to_float(learning_profile.get("conversions_7d"))
        clicks_7d = _to_float(learning_profile.get("clicks_7d"))
        conversion_rate_7d = _to_float(learning_profile.get("conversion_rate_7d"))
        metrics = {
            "campaign": campaign,
            "channel": channel,
            "spend_today": spend_today,
            "revenue_today": revenue_today,
            "roi_today": roi_today,
            "spend_7d": spend_7d,
            "revenue_7d": revenue_7d,
            "roi_7d": roi_7d,
            "pmr_today": pmr_today,
            "daily_budget": budget,
            **learning_profile,
        }
        kpi_trend = str(learning_profile.get("kpi_trend") or "")
        kpi_score = int(_to_float(learning_profile.get("kpi_improvement_score")))
        conversion_goal = str(learning_profile.get("conversion_goal") or "Meta n?o identificada")
        pmr_delta = _to_float(learning_profile.get("pmr_delta_3d_vs_prev3d"))
        ecpm_delta = _to_float(learning_profile.get("ecpm_delta_pct_3d_vs_prev3d"))
        revenue_delta = _to_float(learning_profile.get("revenue_delta_pct_3d_vs_prev3d"))
        roi_delta = _to_float(learning_profile.get("roi_delta_3d_vs_prev3d"))
        conversion_rate_delta = _to_float(learning_profile.get("conversion_rate_delta_3d_vs_prev3d"))
        trend_details = (
            f"Meta Google Ads={conversion_goal}; tendencia={kpi_trend}; "
            f"PMR 3d vs ant={pmr_delta:+.2f}pp; eCPM={ecpm_delta:+.2f}%; "
            f"receita={revenue_delta:+.2f}%; ROI={roi_delta:+.2f}pp; taxa conv.={conversion_rate_delta:+.2f}pp."
        )
        if age_days is not None and age_days <= 7 and spend_today > 0 and kpi_score >= 2:
            add(
                "aprendizado-campanha",
                "INFO",
                "campaign",
                campaign,
                attr_id,
                "Campanha recente com sinais de aprendizado",
                f"Campanha com {age_days} dia(s) e KPIs melhorando. {trend_details}",
                metrics,
                "Manter observacao com limite de gasto; ainda nao transformar ROI inicial negativo em decisao definitiva se PMR/eCPM/receita/conversoes estiverem evoluindo.",
            )
        if age_days is not None and age_days >= 8 and spend_7d >= 10 and kpi_score <= -2:
            add(
                "aprendizado-campanha",
                "HIGH",
                "campaign",
                campaign,
                attr_id,
                "Campanha com KPIs piorando apos aprendizado",
                f"Campanha com {age_days} dia(s), gasto 7d relevante e KPIs piorando. {trend_details}",
                metrics,
                "Priorizar diagnostico de meta de conversao, criativo, placements, PMR, eCPM e pagina; se nao houver reversao, reduzir ou pausar conforme ROI.",
            )
        if age_days is not None and age_days <= 7 and spend_today > 0 and roi_today < 0:
            add(
                "aprendizado-campanha",
                "WARN",
                "campaign",
                campaign,
                attr_id,
                "Campanha recente em fase de aprendizado",
                f"Campanha com {age_days} dia(s) desde a primeira entrega. Google Ads ainda pode estar comprando informacao para qualificar publico. Meta={conversion_goal}. Conv. 7d={conversions_7d:.2f}, clicks 7d={clicks_7d:.0f}, taxa conv. 7d={conversion_rate_7d:.2f}%. {trend_details}",
                metrics,
                "Observar curva por mais alguns dias antes de pausar se nao houver gasto excessivo; priorizar leitura de placements, criativo e qualidade do trafego.",
            )
        if age_days is not None and age_days >= 8 and spend_7d >= 10 and conversions_7d < 5 and roi_7d < 0:
            add(
                "aprendizado-campanha",
                "WARN",
                "campaign",
                campaign,
                attr_id,
                "Campanha nao saiu bem do aprendizado",
                f"Campanha com {age_days} dia(s), gasto 7d relevante e baixa conversao na meta {conversion_goal}. Conv. 7d={conversions_7d:.2f}, clicks 7d={clicks_7d:.0f}, taxa conv. 7d={conversion_rate_7d:.2f}%. {trend_details}",
                metrics,
                "Revisar segmentacao, placements, criativo, pagina e objetivo antes de aumentar verba; se continuar negativa, reduzir ou pausar conforme regra de ROI.",
            )
        target_cpa = _to_float(learning_profile.get("target_cpa"))
        avg_cpc_ads = _to_float(learning_profile.get("avg_cpc_ads"))
        if target_cpa > 0 and avg_cpc_ads > 0 and roi_7d < 0:
            ratio = (avg_cpc_ads / target_cpa * 100.0) if target_cpa > 0 else 0.0
            add(
                "aprendizado-campanha",
                "WARN",
                "campaign",
                campaign,
                attr_id,
                "CPA/CPC desalinhado com ROI",
                f"Meta={conversion_goal}; CPA alvo={target_cpa:.2f}; CPC medio Ads={avg_cpc_ads:.2f}; CPC/CPA={ratio:.2f}%; ROI 7d={roi_7d:.2f}%. Verificar se o lance esta comprando clique caro sem receita suficiente no GAM.",
                metrics,
                "Comparar CPA alvo, CPC medio, PMR, eCPM e receita GAM por dia antes de aumentar budget ou mexer em floor.",
            )
        if spend_today > 0 and revenue_today <= 0:
            add("governanca-dados", "HIGH", "campaign", campaign, attr_id, "Investimento sem receita hoje", "Ha gasto hoje sem receita atribuida no painel.", metrics, "Conferir sync GAM/push e mapeamento de UTM antes de tomar decisao de ROI.")
        if spend_today >= 10 and roi_today <= -50:
            add("roi", "CRITICAL", "campaign", campaign, attr_id, "ROI hoje abaixo do limite operacional", "Campanha passou de R$10 hoje e esta com ROI <= -50%.", metrics, "Revisar para pausa/reducao conforme regra operacional, sem executar automaticamente na Fase 1.")
        if spend_7d >= 10 and roi_7d <= -50:
            add("roi", "HIGH", "campaign", campaign, attr_id, "ROI 7 dias abaixo do limite", "Janela de 7 dias passou de R$10 e esta com ROI <= -50%.", metrics, "Priorizar analise de causa: receita, placements, PMR, budget e status Google Ads.")
        if learning_profile.get("learning_locked"):
            add(
                "aprendizado-campanha",
                "WARN",
                "campaign",
                campaign,
                attr_id,
                "Campanha em aprendizado: bloquear alteracoes de lance/orcamento",
                f"Campanha com {age_days} dia(s) desde a data real do Google Ads ({learning_profile.get('campaign_start_date') or first_seen_day}). Nao alterar orcamento, CPC ou CPA ate sair da janela inicial.",
                metrics,
                "Manter orcamento, CPC e CPA estaveis durante a janela de aprendizado; revisar apenas dados, tracking, PMR e placements sem alterar lance.",
            )
        if not learning_profile.get("learning_locked") and budget > 0 and spend_today >= budget * 0.8 and roi_today > 0:
            add("orcamento", "OPPORTUNITY", "campaign", campaign, attr_id, "Campanha perto do limite com ROI positivo", "Investimento hoje atingiu 80% ou mais do orcamento e ROI esta positivo.", metrics, "Candidata a aumento no primeiro horario do dia seguinte, conforme regra atual.")
        if pmr_today >= 90 and revenue_today > 0:
            add("floor-price", "OPPORTUNITY", "campaign", campaign, attr_id, "PMR alto sugere avaliar floor", "PMR hoje esta acima de 90% com receita.", metrics, "Avaliar blocos GAM e historico de floor antes de sugerir aumento.")
        if 0 < pmr_today < 60 and revenue_today > 0:
            add("floor-price", "WARN", "campaign", campaign, attr_id, "PMR baixo pode indicar trava de entrega", "PMR hoje abaixo de 60% com receita existente.", metrics, "Verificar se floor recente reduziu fill/receita antes de reduzir/desligar regra.")
        if channel == "DISPLAY" and roi_today < 0 and spend_today > 0:
            add("placements", "WARN", "campaign", campaign, attr_id, "Display negativo precisa leitura de placements", "Campanha Display esta negativa hoje.", metrics, "Abrir placements, filtrar cliques/ROI e avaliar exclusoes manualmente.")
    return observations



def _agent_metric_snapshot(runtime: dict[str, Any], project: str, attr_id: str, day: date, period: str = "today") -> dict[str, Any]:
    try:
        rows = load_project_campaign_rows(runtime, project, day, operational=True)
    except Exception:
        rows = []
    found = next((row for row in rows if str(row.get("attr_id") or row.get("campaign_id") or "") == str(attr_id)), None)
    if not found:
        return {"found": False, "day": day.isoformat(), "period": period}
    suffix = "7d" if period == "7d" else "hoje"
    spend = _to_float(found.get(f"investimento_{suffix}"))
    revenue = _to_float(found.get(f"receita_total_{suffix}", found.get(f"receita_{suffix}")))
    roi = _agent_roi_pct(found.get(f"roi_{suffix}"))
    pmr = _agent_percent(found.get(f"pmr_{suffix}"))
    clicks = _to_float(found.get(f"clicks_{suffix}", 0))
    profit = revenue - spend
    return {
        "found": True,
        "day": day.isoformat(),
        "period": period,
        "project": project,
        "attr_id": str(attr_id),
        "campaign": str(found.get("campaign") or ""),
        "spend": round(spend, 6),
        "revenue": round(revenue, 6),
        "profit": round(profit, 6),
        "roi": round(roi, 6),
        "pmr": round(pmr, 6),
        "volume": round(spend, 6),
        "daily_budget": _to_float(found.get("daily_budget")),
    }


def _agent_outcome_result(baseline: dict[str, Any], outcome: dict[str, Any]) -> tuple[str, float, float, float]:
    if not baseline.get("found") or not outcome.get("found"):
        return "pending", 0.0, 0.0, 0.0
    profit_delta = _to_float(outcome.get("profit")) - _to_float(baseline.get("profit"))
    roi_delta = _to_float(outcome.get("roi")) - _to_float(baseline.get("roi"))
    volume_delta = _to_float(outcome.get("volume")) - _to_float(baseline.get("volume"))
    if profit_delta > 0 and (roi_delta >= -5 or volume_delta > 0):
        result = "improved"
    elif profit_delta < 0 and roi_delta < 0:
        result = "worse"
    else:
        result = "neutral"
    return result, round(profit_delta, 6), round(roi_delta, 6), round(volume_delta, 6)


def _agent_known_campaign_map(runtime: dict[str, Any], current_day: date) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    try:
        project_rows = load_project_kpis(runtime, current_day, operational=True)
    except Exception:
        project_rows = []
    projects = [str(row.get("Projeto") or row.get("project") or "") for row in project_rows]
    for project in [item for item in projects if item]:
        try:
            for row in load_project_campaign_rows(runtime, project, current_day, operational=True):
                campaign_id = str(row.get("campaign_id") or row.get("attr_id") or "")
                attr_id = str(row.get("attr_id") or campaign_id)
                if campaign_id:
                    mapping[campaign_id] = {"project": project, "attr_id": attr_id, "campaign": str(row.get("campaign") or "")}
        except Exception:
            continue
    return mapping


def _agent_collect_action_candidates(runtime: dict[str, Any], current_day: date) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    main_db = Path(runtime["db_path"])
    campaign_map = _agent_known_campaign_map(runtime, current_day)
    try:
        with sqlite3.connect(main_db) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gam_floor_price_manual_actions'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM gam_floor_price_manual_actions ORDER BY created_at DESC LIMIT 500"):
                    project = str(row["project"] or "")
                    attr_id = str(row["attr_id"] or "")
                    if project and attr_id:
                        actions.append({
                            "action_key": str(row["action_key"]),
                            "action_type": "floor_price",
                            "project": project,
                            "attr_id": attr_id,
                            "entity_id": str(row["block_name"] or ""),
                            "created_at": str(row["created_at"] or ""),
                            "metadata": {"current_floor": _to_float(row["current_floor"]), "new_floor": _to_float(row["new_floor"]), "status": str(row["status"] or "")},
                        })
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='budget_increase_actions'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM budget_increase_actions ORDER BY created_at DESC LIMIT 500"):
                    campaign_id = str(row["campaign_id"] or "")
                    mapped = campaign_map.get(campaign_id) or campaign_map.get("".join(ch for ch in campaign_id if ch.isdigit())) or {}
                    project = str(mapped.get("project") or "")
                    attr_id = str(mapped.get("attr_id") or campaign_id)
                    if project and attr_id:
                        actions.append({
                            "action_key": f"budget:{row['day']}:{row['customer_id']}:{campaign_id}",
                            "action_type": "budget_increase",
                            "project": project,
                            "attr_id": attr_id,
                            "entity_id": campaign_id,
                            "created_at": str(row["created_at"] or row["day"] or ""),
                            "metadata": {"old_budget": _to_float(row["old_budget"]), "new_budget": _to_float(row["new_budget"]), "status": str(row["status"] or "")},
                        })
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='campaign_bid_manual_actions'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM campaign_bid_manual_actions ORDER BY created_at DESC LIMIT 500"):
                    project = str(row["project"] or "")
                    attr_id = str(row["attr_id"] or "")
                    if project and attr_id:
                        actions.append({
                            "action_key": str(row["action_key"]),
                            "action_type": str(row["action_type"] or "target_cpa_change"),
                            "project": project,
                            "attr_id": attr_id,
                            "entity_id": str(row["campaign_id"] or ""),
                            "created_at": str(row["created_at"] or ""),
                            "metadata": {"old_value": _to_float(row["old_value"]), "new_value": _to_float(row["new_value"]), "status": str(row["status"] or ""), "source": str(row["source"] or "manual_ui")},
                        })
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='google_ads_campaign_settings_changes'").fetchone()
            if exists:
                for row in conn.execute("SELECT * FROM google_ads_campaign_settings_changes ORDER BY created_at DESC LIMIT 500"):
                    campaign_id = str(row["campaign_id"] or "")
                    ad_group_id = str(row["ad_group_id"] or "")
                    mapped = campaign_map.get(campaign_id) or campaign_map.get(ad_group_id) or {}
                    project = str(mapped.get("project") or "")
                    attr_id = str(mapped.get("attr_id") or str(row["attr_id"] or ad_group_id or campaign_id))
                    if project and attr_id:
                        actions.append({
                            "action_key": str(row["change_key"]),
                            "action_type": str(row["change_type"] or "google_ads_setting_change"),
                            "project": project,
                            "attr_id": attr_id,
                            "entity_id": ad_group_id or campaign_id,
                            "created_at": str(row["created_at"] or ""),
                            "metadata": {"old_source": str(row["old_source"] or ""), "new_source": str(row["new_source"] or ""), "old_value": _to_float(row["old_value"]), "new_value": _to_float(row["new_value"]), "reason": str(row["reason"] or "")},
                        })
    except Exception:
        return actions
    return actions


def _agent_collect_approved_improvement_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    rows = conn.execute(
        """
        SELECT * FROM agent_improvement_suggestions
        WHERE status='approved' AND project!='' AND attr_id!='' AND decision_at!=''
        ORDER BY datetime(decision_at) DESC
        LIMIT 500
        """
    ).fetchall()
    for row in rows:
        actions.append({
            "action_key": str(row["suggestion_key"] or ""),
            "action_type": "approved_improvement",
            "project": str(row["project"] or ""),
            "attr_id": str(row["attr_id"] or ""),
            "entity_id": str(row["suggestion_key"] or ""),
            "created_at": str(row["decision_at"] or row["created_at"] or ""),
            "metadata": {
                "agent_slug": str(row["agent_slug"] or ""),
                "responsible_owner": str(row["responsible_owner"] or ""),
                "score_total": int(row["score_total"] or 0),
                "title": str(row["title"] or ""),
            },
        })
    return actions


def _agent_parse_action_day(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate).astimezone(APP_TZ).date()
        except Exception:
            pass
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _agent_evaluate_action_outcomes(runtime: dict[str, Any], conn: sqlite3.Connection, instance: str, current_day: date, now_text: str) -> dict[str, Any]:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_action_outcomes (
            outcome_key TEXT PRIMARY KEY,
            instance TEXT NOT NULL,
            action_key TEXT NOT NULL,
            action_type TEXT NOT NULL,
            project TEXT NOT NULL DEFAULT '',
            attr_id TEXT NOT NULL DEFAULT '',
            entity_id TEXT NOT NULL DEFAULT '',
            evaluation_window TEXT NOT NULL,
            due_day TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT NOT NULL DEFAULT 'pending',
            baseline_json TEXT NOT NULL DEFAULT '{}',
            outcome_json TEXT NOT NULL DEFAULT '{}',
            profit_delta REAL NOT NULL DEFAULT 0,
            roi_delta REAL NOT NULL DEFAULT 0,
            volume_delta REAL NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_action_outcomes_action ON agent_action_outcomes(action_key, evaluation_window)")
    windows = [("24h", 1, "today"), ("48h", 2, "today"), ("7d", 7, "7d")]
    total = evaluated = pending = improved = worse = 0
    action_candidates = _agent_collect_action_candidates(runtime, current_day) + _agent_collect_approved_improvement_candidates(conn)
    for action in action_candidates:
        action_day = _agent_parse_action_day(action.get("created_at"))
        if not action_day:
            continue
        baseline_period = "7d" if action.get("action_type") == "floor_price" else "today"
        baseline = _agent_metric_snapshot(runtime, action["project"], action["attr_id"], action_day, baseline_period)
        for window_name, offset_days, period in windows:
            total += 1
            due_day = action_day + timedelta(days=offset_days)
            outcome_key = _agent_key(instance, action.get("action_key"), window_name)
            if current_day < due_day:
                result, status, outcome = "pending", "pending", {}
                p_delta = r_delta = v_delta = 0.0
                pending += 1
            else:
                outcome = _agent_metric_snapshot(runtime, action["project"], action["attr_id"], due_day, period)
                result, p_delta, r_delta, v_delta = _agent_outcome_result(baseline, outcome)
                status = "evaluated" if result != "pending" else "pending"
                evaluated += 1 if status == "evaluated" else 0
                improved += 1 if result == "improved" else 0
                worse += 1 if result == "worse" else 0
            conn.execute(
                """
                INSERT INTO agent_action_outcomes (
                    outcome_key, instance, action_key, action_type, project, attr_id, entity_id,
                    evaluation_window, due_day, status, result, baseline_json, outcome_json,
                    profit_delta, roi_delta, volume_delta, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(outcome_key) DO UPDATE SET
                    status=excluded.status,
                    result=excluded.result,
                    baseline_json=excluded.baseline_json,
                    outcome_json=excluded.outcome_json,
                    profit_delta=excluded.profit_delta,
                    roi_delta=excluded.roi_delta,
                    volume_delta=excluded.volume_delta,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    outcome_key, instance, action["action_key"], action["action_type"], action["project"], action["attr_id"], action["entity_id"],
                    window_name, due_day.isoformat(), status, result, json.dumps(baseline, ensure_ascii=False, sort_keys=True),
                    json.dumps(outcome, ensure_ascii=False, sort_keys=True), p_delta, r_delta, v_delta,
                    json.dumps(action.get("metadata") or {}, ensure_ascii=False, sort_keys=True), now_text, now_text,
                ),
            )
            if action.get("action_type") == "approved_improvement" and status == "evaluated" and result in {"improved", "worse"}:
                metadata = action.get("metadata") or {}
                author = str(metadata.get("agent_slug") or "")
                responsible = str(metadata.get("responsible_owner") or "")
                if result == "improved":
                    _agent_record_score_event(conn, instance, author, action["action_key"], 5, "melhoria aprovada comprovou lucro", now_text, metadata, "author", f"profit_{window_name}")
                    _agent_record_score_event(conn, instance, responsible, action["action_key"], 3, "filtro aprovou melhoria que comprovou lucro", now_text, metadata, "responsible", f"profit_{window_name}")
                else:
                    _agent_record_score_event(conn, instance, author, action["action_key"], -5, "melhoria aprovada deu prejuizo", now_text, metadata, "author", f"loss_{window_name}")
                    _agent_record_score_event(conn, instance, responsible, action["action_key"], -4, "filtro deixou passar melhoria que deu prejuizo", now_text, metadata, "responsible", f"loss_{window_name}")
    return {"action_outcomes_total": total, "action_outcomes_evaluated": evaluated, "action_outcomes_pending": pending, "action_outcomes_improved": improved, "action_outcomes_worse": worse}


def _agent_responsible_accuracy_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT agent_slug,
               SUM(CASE WHEN decision_type='luciano_approved' THEN 1 ELSE 0 END) AS approved,
               SUM(CASE WHEN decision_type='luciano_rejected' THEN 1 ELSE 0 END) AS rejected,
               COUNT(*) AS sent
        FROM agent_score_events
        WHERE score_role='responsible' AND decision_type IN ('luciano_approved','luciano_rejected')
        GROUP BY agent_slug
        ORDER BY sent DESC, approved DESC
        """
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        sent = int(row["sent"] or 0)
        approved = int(row["approved"] or 0)
        rejected = int(row["rejected"] or 0)
        rate = (approved / sent * 100) if sent else 0.0
        result.append({"agent_slug": str(row["agent_slug"] or ""), "sent": sent, "approved": approved, "rejected": rejected, "accuracy_rate": round(rate, 2), "study_required": bool(sent >= 5 and rate < 60)})
    return result


def _agent_create_responsible_accuracy_study_tasks(conn: sqlite3.Connection, instance: str, now_text: str) -> int:
    created = 0
    for row in _agent_responsible_accuracy_rows(conn):
        if not row.get("study_required"):
            continue
        agent_slug = str(row.get("agent_slug") or "")
        task_key = _agent_key(instance, agent_slug, "responsible-accuracy", now_text[:10])
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO agent_study_tasks (
                task_key, instance, agent_slug, status, title, error_committed, missing_evidence,
                new_rule_needed, apply_example, avoid_example, created_at, payload_json
            ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_key,
                instance,
                agent_slug,
                "Modo estudo obrigatorio: taxa de acerto do responsavel abaixo de 60%",
                "O responsavel deixou chegar ruido demais ao Luciano ou aprovou filtro fraco.",
                "Faltou exigir resposta clara de lucro, dados hoje + 7 dias, risco controlado e acao especifica antes de enviar.",
                "Maximizar poucos envios, alta taxa de aprovacao e lucro comprovado; nao maximizar quantidade de sugestoes.",
                "Enviar quando a melhoria tem score >=70, evidencia suficiente e risco reversivel.",
                "Nao enviar aumento de gasto sem receita comprovada, cosmetica ou campanha nova sem dados.",
                now_text,
                json.dumps(row, ensure_ascii=False, sort_keys=True),
            ),
        )
        if cur.rowcount:
            created += 1
    return created


def _agent_phase_readiness(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='evaluated' THEN 1 ELSE 0 END) AS evaluated,
            SUM(CASE WHEN result='improved' THEN 1 ELSE 0 END) AS improved,
            SUM(CASE WHEN result='worse' THEN 1 ELSE 0 END) AS worse
        FROM agent_action_outcomes
        """
    ).fetchone()
    total = int(row["total"] or 0) if row else 0
    evaluated = int(row["evaluated"] or 0) if row else 0
    improved = int(row["improved"] or 0) if row else 0
    worse = int(row["worse"] or 0) if row else 0
    enough = evaluated >= 30
    safety = worse <= max(3, int(evaluated * 0.25)) if evaluated else False
    return {
        "phase": 2,
        "next_phase": 3,
        "ready_for_next_phase": bool(enough and safety),
        "criteria": "mudar para Fase 2 de ML/score quando houver pelo menos 30 resultados avaliados e menos de 25% piora nas ações",
        "evaluated": evaluated,
        "total": total,
        "improved": improved,
        "worse": worse,
    }

def _agent_consolidate_learnings(conn: sqlite3.Connection, instance: str, now_text: str) -> int:
    rows = conn.execute(
        """
        SELECT agent_slug, title, project, severity, COUNT(*) AS hits,
               COUNT(DISTINCT attr_id) AS affected_attrs,
               MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
        FROM agent_observations
        WHERE instance = ?
        GROUP BY agent_slug, title, project, severity
        HAVING hits >= 2 OR affected_attrs >= 2
        """,
        (instance,),
    ).fetchall()
    changed = 0
    for row in rows:
        agent_slug = str(row["agent_slug"] or "")
        title = str(row["title"] or "")
        project = str(row["project"] or "")
        severity = str(row["severity"] or "")
        hits = int(row["hits"] or 0)
        affected_attrs = int(row["affected_attrs"] or 0)
        first_seen = str(row["first_seen"] or "")
        last_seen = str(row["last_seen"] or "")
        if agent_slug == "governanca-dados" and title == "Investimento sem receita hoje":
            learning_title = "Gasto sem receita recorrente precisa bloquear decisao automatica"
            rule = "Antes de pausar, aumentar budget ou mexer em floor, validar UTM/GAM/push quando houver investimento recorrente sem receita."
            confidence = min(0.95, 0.45 + hits * 0.05 + affected_attrs * 0.04)
        elif agent_slug == "roi" and "ROI" in title:
            learning_title = "ROI negativo recorrente precisa diagnostico antes de escala"
            rule = "Se ROI segue negativo em hoje/7d, priorizar causa raiz: receita, PMR, placements, budget e status Ads."
            confidence = min(0.95, 0.50 + hits * 0.05 + affected_attrs * 0.03)
        elif agent_slug == "placements":
            learning_title = "Display negativo recorrente deve passar por triagem de placements"
            rule = "Campanhas Display negativas devem abrir placements, filtrar cliques/ROI e registrar exclusoes antes de nova escala."
            confidence = min(0.95, 0.50 + hits * 0.05 + affected_attrs * 0.03)
        elif agent_slug == "floor-price":
            learning_title = "Floor price deve considerar PMR, cliques, receita e historico registrado"
            rule = "PMR alto vira oportunidade apenas se volume/receita sustentam; PMR baixo apos ajuste indica reduzir ou desligar regra."
            confidence = min(0.95, 0.50 + hits * 0.04 + affected_attrs * 0.03)
        elif agent_slug == "aprendizado-campanha" and "bloquear" in title.lower():
            learning_title = "Campanha nova nao deve receber aumento de orcamento CPA ou CPC"
            rule = "Durante os primeiros 7 dias desde a data real do Google Ads, bloquear aumento de orcamento, CPA e CPC; avaliar apenas tracking, dados, PMR e placements."
            confidence = min(0.95, 0.60 + hits * 0.05 + affected_attrs * 0.04)
        elif agent_slug == "orcamento":
            learning_title = "Aumento de orcamento deve ser validado no dia seguinte"
            rule = "Quando campanha bate 80% do budget com ROI positivo, aumentar somente no proximo dia e medir se lucro/volume melhoraram."
            confidence = min(0.95, 0.55 + hits * 0.05 + affected_attrs * 0.04)
        else:
            continue
        learning_key = _agent_key(instance, agent_slug, learning_title, project)
        evidence = {
            "scope": "project_pattern",
            "project": project,
            "severity": severity,
            "hits": hits,
            "affected_attrs": affected_attrs,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "rule": rule,
        }
        cur = conn.execute(
            """
            INSERT INTO agent_learnings (learning_key, instance, agent_slug, title, evidence_json, confidence, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(learning_key) DO UPDATE SET
                evidence_json = excluded.evidence_json,
                confidence = excluded.confidence,
                updated_at = excluded.updated_at
            """,
            (learning_key, instance, agent_slug, learning_title, json.dumps(evidence, ensure_ascii=False, sort_keys=True), confidence, now_text, now_text),
        )
        if cur.rowcount:
            changed += 1
    return changed


def _agent_review_alert_feedback(runtime: dict[str, Any], conn: sqlite3.Connection, current_day: date, now_text: str) -> dict[str, int]:
    reviewed = 0
    accepted = 0
    rejected = 0
    rows = conn.execute(
        """
        SELECT fb.*, obs.metrics_json, obs.details, obs.recommendation
        FROM agent_alert_feedback fb
        JOIN agent_observations obs ON obs.observation_key = fb.observation_key
        WHERE fb.decision = 'valid'
          AND fb.agent_verdict = 'pending'
        ORDER BY fb.id ASC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        reviewed += 1
        title = str(row["title"] or "")
        project = str(row["project"] or "")
        attr_id = str(row["attr_id"] or "")
        try:
            metrics = json.loads(row["metrics_json"] or "{}")
        except Exception:
            metrics = {}
        period = "7d" if "7 dias" in title.lower() else "today"
        current = _agent_metric_snapshot(runtime, project, attr_id, current_day, period=period)
        verdict = "accepted"
        reason = "informacao humana aceita; nao ha sinal operacional pior que invalide a justificativa."
        if current.get("found"):
            current_spend = _to_float(current.get("spend"))
            current_revenue = _to_float(current.get("revenue"))
            current_roi = _to_float(current.get("roi"))
            current_profit = _to_float(current.get("profit"))
            baseline_spend = _to_float(metrics.get("spend_7d" if period == "7d" else "spend_today"))
            baseline_revenue = _to_float(metrics.get("revenue_7d" if period == "7d" else "revenue_today"))
            baseline_roi = _to_float(metrics.get("roi_7d" if period == "7d" else "roi_today"))
            baseline_profit = baseline_revenue - baseline_spend
            if "Investimento sem receita" in title and current_spend > 0 and current_revenue <= 0:
                verdict = "rejected"
                reason = "o problema continuou: ainda ha investimento sem receita atribuida."
            elif "ROI hoje" in title and current_spend >= 10 and current_roi <= -50:
                verdict = "rejected"
                reason = "o ROI continua abaixo do limite com gasto relevante; a justificativa nao protegeu lucro."
            elif "ROI 7 dias" in title and current_spend >= 10 and current_roi <= -50:
                verdict = "rejected"
                reason = "a janela de 7 dias continua negativa abaixo do limite; manter alerta."
            elif current_profit < baseline_profit and current_roi < baseline_roi:
                verdict = "rejected"
                reason = "lucro e ROI pioraram apos a justificativa; nao aceitar como padrao."
            elif current_revenue > baseline_revenue or current_profit >= baseline_profit or current_roi > baseline_roi:
                verdict = "accepted"
                reason = "resultado melhorou ou estabilizou; justificativa permanece aceita."
        else:
            verdict = "pending"
            reason = "sem snapshot atual suficiente para revisar."
        if verdict == "pending":
            continue
        conn.execute(
            "UPDATE agent_alert_feedback SET agent_verdict=?, agent_verdict_reason=?, reviewed_at=? WHERE id=?",
            (verdict, reason, now_text, row["id"]),
        )
        if verdict == "accepted":
            accepted += 1
            if int(row["make_pattern"] or 0):
                learning_key = _agent_key(str(row["instance"] or ""), "feedback-pattern", str(row["agent_slug"] or ""), title, project, attr_id)
                evidence = {
                    "feedback_note": str(row["note"] or ""),
                    "verdict_reason": reason,
                    "project": project,
                    "attr_id": attr_id,
                    "title": title,
                }
                conn.execute(
                    """
                    INSERT INTO agent_learnings (learning_key, instance, agent_slug, title, evidence_json, confidence, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(learning_key) DO UPDATE SET evidence_json=excluded.evidence_json, confidence=excluded.confidence, updated_at=excluded.updated_at
                    """,
                    (learning_key, str(row["instance"] or ""), str(row["agent_slug"] or ""), f"Padrao validado: {title}", json.dumps(evidence, ensure_ascii=False, sort_keys=True), 0.62, now_text, now_text),
                )
        else:
            rejected += 1
    return {"feedback_reviewed": reviewed, "feedback_accepted": accepted, "feedback_rejected": rejected}


def _agent_improvement_area(agent_slug: str) -> str:
    mapping = {
        "roi": "ROI e lucro",
        "placements": "Qualidade de trafego",
        "floor-price": "GAM floor price",
        "orcamento": "Orcamento e escala",
        "governanca-dados": "Governanca de dados",
        "aprendizado-campanha": "Aprendizado de campanha",
    }
    return mapping.get(str(agent_slug or ""), "Operacao")


def _agent_improvement_impact(agent_slug: str) -> str:
    mapping = {
        "roi": "proteger verba e priorizar campanhas com maior chance de lucro",
        "placements": "reduzir trafego ruim e melhorar ROI sem perder volume bom",
        "floor-price": "aumentar receita por bloco mantendo PMR e volume saudaveis",
        "orcamento": "escalar campanhas positivas sem aumentar risco operacional",
        "governanca-dados": "evitar decisoes erradas por dado atrasado, UTM falha ou receita nao conciliada",
        "aprendizado-campanha": "separar aprendizado normal de campanha ruim antes de pausar ou escalar",
    }
    return mapping.get(str(agent_slug or ""), "melhorar eficiencia operacional com aprendizado registrado")


def _agent_improvement_risk(agent_slug: str, severity: str) -> str:
    if severity in {"CRITICAL", "HIGH"}:
        return "risco alto se ignorar; revisar antes da proxima decisao operacional"
    if agent_slug == "floor-price":
        return "ajuste agressivo pode derrubar PMR, volume e receita total"
    if agent_slug == "orcamento":
        return "escala sem validacao pode aumentar gasto antes da receita consolidar"
    return "risco moderado; aplicar somente apos autorizacao humana"


def _agent_responsible_owner(agent_slug: str) -> str:
    mapping = {
        "roi": "trafego-pago",
        "placements": "trafego-pago",
        "orcamento": "trafego-pago",
        "aprendizado-campanha": "trafego-pago",
        "floor-price": "monetizacao",
        "monetizacao": "monetizacao",
        "governanca-dados": "orquestrador",
        "auditor": "orquestrador",
    }
    return mapping.get(str(agent_slug or ""), "orquestrador")


def _agent_profit_answer(agent_slug: str, metrics: dict[str, Any], proposal: str) -> str:
    spend_today = _to_float(metrics.get("spend_today"))
    revenue_today = _to_float(metrics.get("revenue_today"))
    roi_today = _agent_roi_pct(metrics.get("roi_today"))
    spend_7d = _to_float(metrics.get("spend_7d"))
    revenue_7d = _to_float(metrics.get("revenue_7d"))
    roi_7d = _agent_roi_pct(metrics.get("roi_7d"))
    pmr = _agent_percent(metrics.get("pmr"))
    profit_today = revenue_today - spend_today
    profit_7d = revenue_7d - spend_7d
    if agent_slug in {"governanca-dados", "auditor"}:
        return "Protege o lucro evitando decisao automatica com Ads, GAM ou sistema divergentes."
    if agent_slug in {"placements", "roi"} and (profit_today < 0 or profit_7d < 0):
        return f"Protege lucro reduzindo desperdicio: lucro hoje R${profit_today:.2f}, lucro 7d R${profit_7d:.2f}, ROI hoje {roi_today:.2f}%, ROI 7d {roi_7d:.2f}%."
    if agent_slug == "orcamento" and roi_today > 0 and roi_7d > 0:
        return f"Pode aumentar lucro escalando campanha positiva com ROI hoje {roi_today:.2f}% e ROI 7d {roi_7d:.2f}%."
    if agent_slug in {"floor-price", "monetizacao"} and pmr >= 80:
        return f"Pode aumentar receita GAM mantendo entrega saudavel: PMR {pmr:.2f}%."
    return "Nao demonstrou claramente como protege ou aumenta o lucro."


def _agent_decision_package(agent_slug: str, metrics: dict[str, Any], proposal: str, project: str, attr_id: str, evidence: str = "") -> dict[str, Any]:
    spend_today = _to_float(metrics.get("spend_today"))
    revenue_today = _to_float(metrics.get("revenue_today"))
    roi_today = _agent_roi_pct(metrics.get("roi_today"))
    spend_7d = _to_float(metrics.get("spend_7d"))
    revenue_7d = _to_float(metrics.get("revenue_7d"))
    roi_7d = _agent_roi_pct(metrics.get("roi_7d"))
    pmr = _agent_percent(metrics.get("pmr"))
    target_cpa = _to_float(metrics.get("target_cpa"))
    avg_cpc = _to_float(metrics.get("avg_cpc"))
    profit_today = revenue_today - spend_today
    profit_7d = revenue_7d - spend_7d
    has_today = spend_today > 0 or revenue_today > 0 or roi_today != 0
    has_7d = spend_7d > 0 or revenue_7d > 0 or roi_7d != 0
    action_text = str(proposal or "").strip()
    missing: list[str] = []
    if not project:
        missing.append("projeto")
    if not attr_id:
        missing.append("attr_id")
    if not has_today:
        missing.append("dados de hoje")
    if not has_7d:
        missing.append("dados de 7 dias")
    action_l = action_text.lower()
    executable_verbs = ["pausar", "reduzir", "aumentar", "excluir", "negativar", "ajustar", "alterar", "bloquear", "liberar", "manter"]
    diagnostic_only = any(word in action_l for word in ["conferir", "validar", "analisar", "analise", "diagnostico", "revisar"]) and not any(word in action_l for word in executable_verbs)
    if not action_text or len(action_text) < 35:
        missing.append("acao concreta")
    if diagnostic_only:
        missing.append("decisao executavel; diagnostico deve ficar com o responsavel")
    if agent_slug not in {"governanca-dados", "auditor"} and spend_today + spend_7d <= 0:
        missing.append("investimento relevante")
    if agent_slug in {"governanca-dados", "auditor"}:
        missing.append("decisao de negocio; alerta de dados nao deve ir para Luciano sem acao executavel")
    summary = (
        f"Hoje: inv R${spend_today:.2f}, rec R${revenue_today:.2f}, lucro R${profit_today:.2f}, ROI {roi_today:.2f}%. "
        f"7d: inv R${spend_7d:.2f}, rec R${revenue_7d:.2f}, lucro R${profit_7d:.2f}, ROI {roi_7d:.2f}%. "
        f"PMR {pmr:.2f}%, CPA alvo R${target_cpa:.2f}, CPC R${avg_cpc:.2f}."
    )
    decision_question = "O que exatamente Luciano deve aprovar/reprovar?"
    if agent_slug in {"governanca-dados", "auditor"}:
        decision_question = "Nao enviar para Luciano ainda; responsavel deve reconciliar dados e trazer decisao executavel."
    elif agent_slug in {"roi", "placements"}:
        decision_question = "Aprovar diagnostico operacional antes de escalar ou manter gasto?"
    elif agent_slug in {"floor-price", "monetizacao"}:
        decision_question = "Aprovar revisao de floor somente se bloco tiver volume, PMR e receita suficientes?"
    elif agent_slug == "orcamento":
        decision_question = "Aprovar aumento de orcamento somente se lucro e ROI sustentarem escala?"
    is_complete = not missing
    return {
        "is_complete": is_complete,
        "missing": missing,
        "summary": summary,
        "decision_question": decision_question,
        "minimum_required": ["projeto", "attr_id", "dados de hoje", "dados de 7 dias", "impacto financeiro", "risco", "acao concreta"],
    }


def _agent_score_improvement(agent_slug: str, severity: str, metrics: dict[str, Any], proposal: str, project: str, attr_id: str) -> dict[str, Any]:
    spend_today = _to_float(metrics.get("spend_today"))
    revenue_today = _to_float(metrics.get("revenue_today"))
    spend_7d = _to_float(metrics.get("spend_7d"))
    revenue_7d = _to_float(metrics.get("revenue_7d"))
    roi_today = _agent_roi_pct(metrics.get("roi_today"))
    roi_7d = _agent_roi_pct(metrics.get("roi_7d"))
    pmr = _agent_percent(metrics.get("pmr"))
    profit_answer = _agent_profit_answer(agent_slug, metrics, proposal)
    decision_package = _agent_decision_package(agent_slug, metrics, proposal, project, attr_id)
    has_profit_answer = not profit_answer.startswith("Nao demonstrou")
    impact = 0
    if has_profit_answer and (spend_today >= 10 or spend_7d >= 30) and (roi_today < 0 or roi_7d < 0 or revenue_today > 0 or revenue_7d > 0):
        impact = 40
    elif has_profit_answer and (roi_today > 0 or roi_7d > 0 or pmr >= 80):
        impact = 30
    elif has_profit_answer:
        impact = 15
    has_today = spend_today > 0 or revenue_today > 0 or roi_today != 0
    has_7d = spend_7d > 0 or revenue_7d > 0 or roi_7d != 0
    evidence = 25 if has_today and has_7d else (15 if has_today or has_7d else 5 if metrics else 0)
    risk = 20
    proposal_l = str(proposal or "").lower()
    if any(word in proposal_l for word in ["aumentar", "escala", "orcamento", "cpa", "cpc", "floor"]):
        risk = 10
    if metrics.get("learning_locked") or (spend_today <= 0 and spend_7d <= 0 and agent_slug not in {"governanca-dados", "auditor"}):
        risk = 0
    if agent_slug in {"placements", "governanca-dados", "auditor"} and has_profit_answer:
        risk = max(risk, 20)
    clarity = 15 if (project and attr_id and proposal and len(str(proposal)) >= 20) else (8 if proposal else 0)
    total = int(impact + evidence + risk + clarity)
    if not has_profit_answer:
        total = min(total, 49)
    if not decision_package["is_complete"]:
        total = min(total, 49)
    if total >= 70:
        decision, reason, status, points = "approve", "score >= 70; enviar para Luciano.", "pending", 1
    elif total >= 50:
        decision, reason, status, points = "observe", "score 50-69; manter em observacao por falta de prova suficiente.", "observing", -1
    else:
        decision, reason, status, points = "reject", "score < 50; reprovar antes de chegar em Luciano.", "rejected", -2
    return {
        "profit_answer": profit_answer,
        "score_financial_impact": int(impact),
        "score_evidence": int(evidence),
        "score_risk": int(risk),
        "score_clarity": int(clarity),
        "score_total": total,
        "responsible_owner": _agent_responsible_owner(agent_slug),
        "responsible_decision": decision,
        "responsible_reason": reason if decision_package["is_complete"] else "informacoes insuficientes para Luciano decidir: " + ", ".join(decision_package["missing"]),
        "decision_package": decision_package,
        "status": status,
        "score_points": points,
    }


def _agent_record_score_event(conn: sqlite3.Connection, instance: str, agent_slug: str, suggestion_key: str, points: int, reason: str, now_text: str, payload: dict[str, Any], score_role: str = "author", decision_type: str = "") -> None:
    event_key = _agent_key(instance, agent_slug, suggestion_key, reason, score_role, decision_type, now_text[:10])
    conn.execute("INSERT OR IGNORE INTO agent_score_events (event_key, instance, agent_slug, suggestion_key, points, reason, created_at, payload_json, score_role, decision_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (event_key, instance, agent_slug, suggestion_key, points, reason, now_text, json.dumps(payload, ensure_ascii=False, sort_keys=True), score_role, decision_type))


def _agent_maybe_create_study_task(conn: sqlite3.Connection, instance: str, agent_slug: str, now_text: str) -> bool:
    rows = conn.execute("SELECT points, reason, suggestion_key FROM agent_score_events WHERE instance=? AND agent_slug=? ORDER BY datetime(created_at) DESC LIMIT 3", (instance, agent_slug)).fetchall()
    if len(rows) < 3 or any(int(row["points"] or 0) > -1 for row in rows):
        return False
    task_key = _agent_key(instance, agent_slug, "mandatory-study", now_text[:10])
    payload = {"recent_reasons": [str(row["reason"] or "") for row in rows], "recent_suggestions": [str(row["suggestion_key"] or "") for row in rows]}
    conn.execute('''
        INSERT OR IGNORE INTO agent_study_tasks (
            task_key, instance, agent_slug, status, title, error_committed, missing_evidence,
            new_rule_needed, apply_example, avoid_example, created_at, payload_json
        ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task_key, instance, agent_slug,
            "Modo estudo obrigatorio por 3 sugestoes fracas seguidas",
            "O agente sugeriu melhoria sem provar claramente protecao/aumento de lucro ou sem score suficiente.",
            "Faltou pacote minimo para Luciano decidir: dados hoje + 7 dias, impacto financeiro, risco, acao concreta e pergunta clara de aprovacao.",
            "Toda sugestao deve responder: como isso protege ou aumenta o lucro? Depois pontuar impacto, evidencia, risco e clareza.",
            "Aplicar quando houver ROI negativo com gasto relevante, PMR saudavel com volume ou divergencia que muda decisao.",
            "Nao aplicar para cosmetica, campanha nova sem dados, aumento de gasto sem receita comprovada ou floor sem volume.",
            now_text, json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ))
    return True


def _agent_recent_improvement_exists(conn: sqlite3.Connection, instance: str, agent_slug: str, project: str, attr_id: str, title: str, current_day: date) -> bool:
    start_day = (current_day - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    row = conn.execute(
        """
        SELECT 1 FROM agent_improvement_suggestions
        WHERE instance=? AND agent_slug=? AND project=? AND attr_id=? AND title=?
          AND status IN ('pending','approved','applied')
          AND created_at >= ?
        LIMIT 1
        """,
        (instance, agent_slug, project, attr_id, title, start_day),
    ).fetchone()
    return bool(row)


def _agent_compact_evidence(metrics: dict[str, Any]) -> str:
    parts: list[str] = []
    for label, key, money in [
        ("inv hoje", "spend_today", True),
        ("rec hoje", "revenue_today", True),
        ("roi hoje", "roi_today", False),
        ("inv 7d", "spend_7d", True),
        ("rec 7d", "revenue_7d", True),
        ("roi 7d", "roi_7d", False),
        ("PMR", "pmr", False),
        ("CPA alvo", "target_cpa", True),
        ("CPC", "avg_cpc", True),
    ]:
        if key not in metrics:
            continue
        value = _to_float(metrics.get(key))
        if money:
            parts.append(f"{label}: R${value:.2f}")
        elif "roi" in key:
            parts.append(f"{label}: {value:.2f}%")
        else:
            parts.append(f"{label}: {value:.2f}")
    return " | ".join(parts[:8])


def _agent_build_daily_improvements(runtime: dict[str, Any], conn: sqlite3.Connection, instance: str, current_day: date, now_text: str, limit_per_agent: int = 3) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM agent_observations
        WHERE instance=?
          AND severity IN ('CRITICAL','HIGH','WARN','OPPORTUNITY')
          AND date(created_at) >= date(?)
          AND status NOT IN ('resolved','dismissed')
        ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'WARN' THEN 3 ELSE 4 END, id DESC
        LIMIT 500
        """,
        (instance, (current_day - timedelta(days=3)).isoformat()),
    ).fetchall()
    created: list[dict[str, Any]] = []
    count_by_agent: dict[str, int] = {}
    for row in rows:
        agent_slug = str(row["agent_slug"] or "")
        if count_by_agent.get(agent_slug, 0) >= limit_per_agent:
            continue
        project = str(row["project"] or "")
        attr_id = str(row["attr_id"] or "")
        obs_title = str(row["title"] or "Melhoria operacional")
        title = f"Melhoria diaria: {obs_title}"
        if _agent_recent_improvement_exists(conn, instance, agent_slug, project, attr_id, title, current_day):
            continue
        try:
            metrics = json.loads(row["metrics_json"] or "{}")
        except Exception:
            metrics = {}
        suggestion_key = _agent_key(instance, current_day.isoformat(), agent_slug, project, attr_id, title)
        severity = str(row["severity"] or "INFO")
        recommendation = str(row["recommendation"] or "Revisar no painel de agentes antes de autorizar.")
        evidence = _agent_compact_evidence(metrics) or str(row["details"] or "Sem metrica compacta registrada.")[:240]
        score = _agent_score_improvement(agent_slug, severity, metrics, recommendation, project, attr_id)
        decision_package = score.get("decision_package") or {}
        package_summary = str(decision_package.get("summary") or "")
        package_question = str(decision_package.get("decision_question") or "")
        package_missing = ", ".join(decision_package.get("missing") or [])
        decision_evidence = " | ".join(part for part in [package_summary, f"Decisao: {package_question}" if package_question else "", f"Falta: {package_missing}" if package_missing else ""] if part)
        item = {
            "suggestion_key": suggestion_key,
            "instance": instance,
            "agent_slug": agent_slug,
            "project": project,
            "attr_id": attr_id,
            "area": _agent_improvement_area(agent_slug),
            "title": title,
            "problem": str(row["details"] or obs_title)[:500],
            "evidence": (decision_evidence or evidence)[:500],
            "proposal": recommendation[:500],
            "expected_impact": _agent_improvement_impact(agent_slug),
            "risk": _agent_improvement_risk(agent_slug, severity),
            "effort": "medio" if severity in {"CRITICAL", "HIGH"} else "baixo",
            "priority": severity,
            "source_scope": "internal_db,map,agent_history,profit_gate",
            "status": score["status"],
            "profit_answer": score["profit_answer"],
            "score_financial_impact": score["score_financial_impact"],
            "score_evidence": score["score_evidence"],
            "score_risk": score["score_risk"],
            "score_clarity": score["score_clarity"],
            "score_total": score["score_total"],
            "responsible_owner": score["responsible_owner"],
            "responsible_decision": score["responsible_decision"],
            "responsible_reason": score["responsible_reason"],
            "created_at": now_text,
            "sent_at": "",
            "decision_at": "",
            "result_json": json.dumps({"source_observation_key": str(row["observation_key"] or ""), "profit_gate": score, "decision_package": score.get("decision_package")}, ensure_ascii=False, sort_keys=True),
        }
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_improvement_suggestions (
                suggestion_key, instance, agent_slug, project, attr_id, area, title, problem, evidence,
                proposal, expected_impact, risk, effort, priority, source_scope, status, profit_answer,
                score_financial_impact, score_evidence, score_risk, score_clarity, score_total,
                responsible_owner, responsible_decision, responsible_reason, created_at, sent_at,
                decision_at, result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(item[key] for key in [
                "suggestion_key", "instance", "agent_slug", "project", "attr_id", "area", "title", "problem", "evidence",
                "proposal", "expected_impact", "risk", "effort", "priority", "source_scope", "status", "profit_answer",
                "score_financial_impact", "score_evidence", "score_risk", "score_clarity", "score_total",
                "responsible_owner", "responsible_decision", "responsible_reason", "created_at", "sent_at",
                "decision_at", "result_json",
            ]),
        )
        if item["responsible_decision"] == "approve":
            _agent_record_score_event(conn, instance, agent_slug, suggestion_key, 1, "passou pelo responsavel", now_text, item, "author", "responsible_passed")
        elif item["responsible_decision"] == "reject":
            _agent_record_score_event(conn, instance, agent_slug, suggestion_key, -1, "responsavel bloqueou ideia ruim", now_text, item, "author", "responsible_blocked")
            _agent_record_score_event(conn, instance, item["responsible_owner"], suggestion_key, 1, "bloqueou ideia ruim corretamente", now_text, item, "responsible", "responsible_blocked")
        elif item["responsible_decision"] == "observe":
            _agent_record_score_event(conn, instance, agent_slug, suggestion_key, -1, "ficou em observacao por falta de dados", now_text, item, "author", "responsible_observed")
        _agent_maybe_create_study_task(conn, instance, agent_slug, now_text)
        if item["status"] == "pending":
            created.append(item)
            count_by_agent[agent_slug] = count_by_agent.get(agent_slug, 0) + 1
    return created


def _agent_queue_improvement_digest(runtime: dict[str, Any], instance: str, suggestions: list[dict[str, Any]], now_text: str) -> dict[str, Any]:
    if not suggestions:
        return {"queued": 0, "reason": "no_new_suggestions"}
    lines = [
        f"Melhorias diarias dos agentes - {instance}",
        "Canal: grupo particular de autorizacao dos agentes.",
        "Status: aguardando autorizacao humana. Nada foi aplicado automaticamente.",
        "",
    ]
    for index, item in enumerate(suggestions[:18], start=1):
        short_id = str(item.get("suggestion_key") or "")[:8]
        lines.extend([
            f"{index}. [{short_id}] {item.get('agent_slug')} | {item.get('priority')}",
            f"Projeto: {item.get('project') or '-'} | Attr: {item.get('attr_id') or '-'}",
            f"Problema: {item.get('title')}",
            f"Evidencia: {item.get('evidence') or '-'}",
            f"Proposta: {item.get('proposal') or '-'}",
            f"Aprovar: /aprovar_melhoria {short_id} | Rejeitar: /rejeitar_melhoria {short_id}",
            "",
        ])
    lines.append("Aprovado = entra na fila de melhoria segura. Rejeitado = nao vira padrao.")
    event = {
        "event_id": f"daily-agent-improvements-{instance}-{datetime.now(APP_TZ).strftime('%Y%m%d%H%M%S')}",
        "agent_slug": "kaizen-agents",
        "title": "3 melhorias diarias por agente aguardando autorizacao",
        "message": "\n".join(lines)[:3500],
        "priority": "INFO",
        "project_id": "",
        "created_at": now_text,
    }
    try:
        queued = queue_war_room_events(runtime["db_path"], runtime["config"], events=[event], channel_key="agent-improvements")
        return {"queued": len(queued), "channel_key": "agent-improvements", "event_id": event["event_id"]}
    except Exception as exc:
        return {"queued": 0, "channel_key": "agent-improvements", "error": str(exc)}

def run_phase1_agent_observer(runtime: dict[str, Any], instance: str, today: date | None = None) -> dict[str, Any]:
    current_day = today or app_today()
    started = datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S")
    run_key = _agent_key(instance, current_day.isoformat(), started)
    lock_path = _agent_memory_db_path(runtime).with_suffix(".lock")
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, started.encode("utf-8"))
        os.close(lock_fd)
    except FileExistsError:
        try:
            age = datetime.now().timestamp() - lock_path.stat().st_mtime
        except Exception:
            age = 0
        if age < 600:
            return {"ok": True, "run_key": run_key, "skipped": True, "reason": "agent_cycle_already_running"}
        try:
            lock_path.unlink()
        except Exception:
            pass
    db_path = ensure_agent_memory_store(runtime)
    project_rows = load_project_kpis(runtime, current_day, operational=True)
    projects = [str(row.get("Projeto") or row.get("project") or "") for row in project_rows]
    projects = [project for project in projects if project]
    created = 0
    scanned_campaigns = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        for project in projects:
            try:
                campaign_rows = load_project_campaign_rows(runtime, project, current_day, operational=True)
            except Exception as exc:
                campaign_rows = []
                row = {
                    "observation_key": _agent_key(instance, "governanca-dados", project, "load-error", current_day.isoformat()),
                    "run_key": run_key,
                    "instance": instance,
                    "agent_slug": "governanca-dados",
                    "severity": "CRITICAL",
                    "entity_type": "project",
                    "entity_id": project,
                    "project": project,
                    "attr_id": "",
                    "title": "Falha ao carregar campanhas do projeto",
                    "details": str(exc),
                    "metrics_json": "{}",
                    "recommendation": "Corrigir erro antes de liberar automacao deste projeto.",
                    "status": "observed",
                    "created_at": started,
                }
                if _agent_record_observation(conn, row):
                    created += 1
            scanned_campaigns += len(campaign_rows)
            for observation in _agent_campaign_observations(instance, run_key, started, project, campaign_rows, runtime, current_day):
                if _agent_record_observation(conn, observation):
                    created += 1
        for observation in _agent_operational_change_observations(runtime, instance, run_key, started, current_day):
            if _agent_record_observation(conn, observation):
                created += 1
        review_summary = _agent_review_alert_feedback(runtime, conn, current_day, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        outcome_summary = _agent_evaluate_action_outcomes(runtime, conn, instance, current_day, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        new_learnings = _agent_consolidate_learnings(conn, instance, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        phase_readiness = _agent_phase_readiness(conn)
        responsible_study_tasks = _agent_create_responsible_accuracy_study_tasks(conn, instance, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        improvement_suggestions = _agent_build_daily_improvements(runtime, conn, instance, current_day, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"), limit_per_agent=3)
        improvement_delivery = _agent_queue_improvement_digest(runtime, instance, improvement_suggestions, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        summary = {"projects": len(projects), "campaigns": scanned_campaigns, "new_observations": created, "new_learnings": new_learnings, "responsible_study_tasks": responsible_study_tasks, "daily_improvements_created": len(improvement_suggestions), "daily_improvements_telegram_queued": int(improvement_delivery.get("queued") or 0), "daily_improvements_delivery": improvement_delivery, "phase": 2, **review_summary, **outcome_summary, "phase_readiness": phase_readiness}
        conn.execute(
            "INSERT OR REPLACE INTO agent_runs (run_key, instance, started_at, finished_at, status, summary_json) VALUES (?, ?, ?, ?, ?, ?)",
            (run_key, instance, started, datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"), "ok", json.dumps(summary, ensure_ascii=False, sort_keys=True)),
        )
        conn.commit()
    try:
        lock_path.unlink()
    except Exception:
        pass
    return {"ok": True, "run_key": run_key, "memory_db": str(db_path), **summary}


def load_phase1_agent_dashboard(runtime: dict[str, Any], limit: int = 80) -> dict[str, Any]:
    db_path = _agent_memory_db_path(runtime)
    if not db_path.exists():
        db_path = ensure_agent_memory_store(runtime)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        runs = [dict(row) for row in conn.execute("SELECT * FROM agent_runs ORDER BY id DESC LIMIT 10").fetchall()]
        observations = [dict(row) for row in conn.execute("SELECT * FROM agent_observations ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
        by_agent = [dict(row) for row in conn.execute("SELECT agent_slug, severity, COUNT(*) AS total FROM agent_observations GROUP BY agent_slug, severity ORDER BY total DESC").fetchall()]
        learnings = [dict(row) for row in conn.execute("SELECT * FROM agent_learnings ORDER BY datetime(updated_at) DESC, id DESC LIMIT ?", (limit,)).fetchall()]
        outcomes = [dict(row) for row in conn.execute("SELECT * FROM agent_action_outcomes ORDER BY datetime(updated_at) DESC LIMIT ?", (limit,)).fetchall()]
        improvements = [dict(row) for row in conn.execute("SELECT * FROM agent_improvement_suggestions ORDER BY datetime(created_at) DESC LIMIT ?", (limit,)).fetchall()]
        score_events = [dict(row) for row in conn.execute("SELECT * FROM agent_score_events ORDER BY datetime(created_at) DESC LIMIT ?", (limit,)).fetchall()]
        study_tasks = [dict(row) for row in conn.execute("SELECT * FROM agent_study_tasks ORDER BY datetime(created_at) DESC LIMIT ?", (limit,)).fetchall()]
        responsible_accuracy = _agent_responsible_accuracy_rows(conn)
        phase_readiness = _agent_phase_readiness(conn)
    quality_os = load_quality_os_dashboard(runtime)
    return {"memory_db": str(db_path), "runs": runs, "observations": observations, "by_agent": by_agent, "learnings": learnings, "outcomes": outcomes, "improvements": improvements, "score_events": score_events, "study_tasks": study_tasks, "responsible_accuracy": responsible_accuracy, "phase_readiness": phase_readiness, "quality_os": quality_os}
