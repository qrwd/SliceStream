# Agent Automation Protocol v1.0.0

Protocol ID: `agent_automation_protocol_v1`  
Version: `1.0.0`  
Status: experimental

## Purpose
This protocol gates intelligent automation capabilities (auto bidding, recovery trigger, reaper trigger, mode auto/hybrid switch) behind explicit user consent.

## Capability boundaries
Granted scopes after acceptance:
- `smart_auto_bidding`
- `smart_recovery_ops`
- `smart_reaper_ops`
- `smart_mode_auto`

Not granted:
- private key custody
- silent signing
- irreversible chain submission without explicit user signature

## Risk disclosure
- Experimental feature; no guarantee of profitability/success/continuity.
- User remains responsible for final signature and asset actions.
- Maintainers do not provide return guarantees or uninterrupted availability commitments.

## Pre-use requirements
Before using any gated scope, user must:
1. Confirm this protocol version.
2. Provide wallet address context.
3. (Optional) provide signature reference for external audit trail.

## Revocation
Protocol acceptance is revocable through API:
- `POST /internal/protocol/agreements/agent_automation_protocol_v1/revoke`

## Auditability
Acceptance/revocation emits market audit records:
- `protocol_accepted ...`
- `protocol_revoked ...`

## Legal note
This protocol text is an engineering-level risk boundary and requires future legal/protocol review before production-grade legal interpretation.
