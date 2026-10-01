# Graphify update - app.wiz existing agents integrated into Quality OS

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

The Quality OS was adjusted to recognize the agents that already existed in app.wiz.

Existing sources integrated:
- agent_backlog.py: backlog and conceptual Nexus agents.
- campaign_agent_governance.py: operational campaign governance profiles.
- systemd wiz services and timers: sync, GAM, floor, OAuth health, campaign status, and orchestration jobs.

Behavior added:
- quality_os.py now builds an existing_operational_agents inventory.
- operational-db/tables/agents.json is populated by merging existing rows with discovered agents.
- quality-operating-system.json and quality-agents/agent-scorecards.json include the integrated existing agents.
- /agents shows existing integrated count and total integrated agent count.

Rule:
- Existing agents are not replaced. They are brought under the same score, evidence, risk, and publication gate discipline.
