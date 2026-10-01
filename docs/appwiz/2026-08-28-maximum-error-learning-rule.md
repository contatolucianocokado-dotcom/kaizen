# Graphify update - Maximum error learning rule

Date: 2026-08-28
App: app.wizseoservicesaustralia.com

Implemented the maximum error-learning rule as executable infrastructure.

Rules enforced:
- Every real error must be registered.
- Every predictive risk must be registered before action.
- Every agent must read open errors and risks before acting.
- Errors become learning memory for the responsible agent and shared memory for all agents.
- Builder agents can only deliver after independent auditor approval with score >= 95.
- Audit score below target creates mandatory rework.

Added:
- quality_error_registry.py
- scripts/sync_quality_error_registry.py
- wiz-quality-error-registry.service
- wiz-quality-error-registry.timer
- FastAPI middleware exception registration
- system guard integration with the central registry
