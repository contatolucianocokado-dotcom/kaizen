# Graphify update - Wiz safe guard agents

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

Implemented safe Wiz-owned guard agents and timers after separating Wiz from Turnos and Nexus.

Added scripts:
- scripts/ensure_agent_authority.py
- scripts/run_system_guard_agents.py adapted to Wiz port 8010 and wiz-roas-web.service
- scripts/maintenance_prune_runtime_tables.py
- scripts/validate_google_ads_platform_spend.py in audit mode
- scripts/audit_fix_google_ads_tracking_templates.py in audit mode
- scripts/campaign_type_data_agents.py copied for later controlled use
- scripts/validate_quality_os_rules.py

Added timers:
- wiz-agent-authority.timer
- wiz-system-guard-agents.timer
- wiz-db-maintenance.timer
- wiz-google-ads-spend-audit.timer
- wiz-google-ads-tracking-template-audit.timer
- wiz-quality-os-rules.timer

Safety policy:
- Google Ads validators run without --fix by default.
- Financial or external account changes stay gated by Quality OS, evidence, and human approval.
