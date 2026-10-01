# Graphify update - hide home alert cards

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

Changed the dashboard rendering to keep the global agent command center summary while removing the individual alert card grid from the home hero area.

User intent:
- Remove the visible ROI, campaign learning, and data governance alert blocks from the home page.

Implementation:
- webapp.py now returns only command_center from the global agent alert renderer.
- Agent observations and internal memory were preserved.
