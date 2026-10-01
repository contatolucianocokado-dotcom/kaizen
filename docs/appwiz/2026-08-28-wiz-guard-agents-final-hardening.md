# Graphify update - Wiz guard agents final hardening

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

Finalized safe guard agent installation.

Hardening:
- Google Ads audit scripts omit login_customer_id when no valid 10-digit MCC is configured.
- Spend validator records blocked configuration/data states without failing the systemd timer.
- System guard creates missing event/health/learning tables defensively.
- Agent cycle service now has timeout protection.

Validation:
- Quality OS rules validator returns OK.
- Wiz timers are enabled and active.
- Wiz scripts and systemd units do not depend on app.turnos.xyz or /opt/paid-acquisition-nexus.
