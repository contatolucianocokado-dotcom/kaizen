# Graphify update - app.wiz Quality OS portable agent model

Date: 2026-08-28
App: app.wizseoservicesaustralia.com
Root: /var/www/app.wizseoservicesaustralia.com/public_html

Implemented the portable agent operating model in app.wiz.

Nodes added:
- quality_os.py
- quality-operating-system.json
- quality-agents/agent-operating-system.md
- quality-agents/agent-scorecards.json
- quality-agents/mandatory-agent-header.md
- quality-runs/error-database.json
- quality-runs/risk-database.json
- operational-db/tables/*.json
- API route /api/agents/quality-os
- Agents dashboard quality OS panel

Relationships:
- webapp.py exposes the quality OS dashboard API and UI panel.
- webapp_service.py initializes and loads the quality OS status during runtime.
- quality_os.py owns the canonical agent catalog, publication gate, scorecards, risk/error databases, and operational table scaffolding.

Validation:
- python compile passed for quality_os.py, webapp_service.py, and webapp.py.
- Runtime initialization returned ok=True with no missing structural files.
