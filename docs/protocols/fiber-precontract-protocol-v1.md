# Fiber Pre-Contract Protocol v1.0.0

Protocol ID: `fiber_precontract_protocol_v1`  
Version: `1.0.0`  
Status: partial implementation (preflight + gating framework)

## Purpose
Gate Fiber-related high-risk actions before real execution.

## Mandatory checks
1. Protocol accepted (`fiber_preflight` scope).
2. Requested network must match configured runtime network.
3. For `execution_mode=real`:
   - `fiber_real_execution` scope required.
   - network must be recognized (`testnet|mainnet`).
   - if mainnet, `mainnet_ready` must be true.

## Explicit mode separation
- `simulate`: preview/simulation path.
- `real`: requires stricter checks and explicit user responsibility.

## Risk boundary
- Non-custodial tool; user signs/approves final chain actions.
- No guarantee of execution success or economic outcome.

## Current engineering scope
- Implemented: protocol record + API gate + preflight endpoint framework.
- Not yet fully implemented: complete on-chain contract-creation flow integration.
