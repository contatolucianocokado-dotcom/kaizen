# Graphify update - Wiz instance separated from Turnos and Nexus

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

Changed operational boundaries so the Wiz instance runs under Wiz-owned systemd units and the Wiz virtualenv.

Changes:
- wiz-roas-web.service now has an override that runs `.venv/bin/python -m uvicorn`, forcing the Wiz venv instead of the inherited uvicorn launcher.
- Added wiz-agent-cycle.service and wiz-agent-cycle.timer.
- Disabled julio-agent-cycle.timer.
- Copied push/GAM reconciliation into the Wiz app scripts directory.
- Added wiz-push-reconcile.service and wiz-push-reconcile.timer.
- Disabled julio-push-reconcile.timer.
- Disabled nexus-performance-guard.timer and nexus-agent-cycle.timer to stop cross-instance Nexus automation from touching or blocking Wiz.
- Updated Quality OS service inventory from julio-agent-cycle to wiz-agent-cycle and added wiz-push-reconcile.

Boundary rule:
- Wiz active services must not point to /var/www/app.turnos.xyz or /opt/paid-acquisition-nexus.
