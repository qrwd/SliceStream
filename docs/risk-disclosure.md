# SliceStream Risk Disclosure (Engineering Draft)

Status: draft for protocol/legal review.

## Scope
Applies to intelligent automation, recovery/reaper operations, pricing automation, and Fiber-related settlement workflows.

## Core disclosures
1. Experimental system: behavior may change, fail, timeout, or be unavailable.
2. No guarantee: no guarantee of profit, success rate, uptime, or uninterrupted operation.
3. User responsibility: final signatures, transaction approvals, and asset consequences remain with user.
4. Boundary of responsibility: maintainers provide software/protocol tooling and risk transparency, not custody or guaranteed outcomes.

## Permission and consent requirements
- High-impact automation features require explicit protocol acceptance.
- Permissions must be scoped, viewable, and revocable.
- Unaccepted protocol calls should be rejected by server-side gate checks.

## Fiber-specific caution
- Distinguish simulation vs real signing vs real on-chain execution.
- Verify network context (testnet/mainnet) before real actions.
- Missing or incompatible network prerequisites must block dangerous paths.

## Pending items
- Legal wording review.
- Jurisdiction-specific compliance review.
- UX copy consistency review across desktop/web interfaces.
