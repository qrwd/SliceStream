# SliceStream Internal Preview Changelog

## 0.1.0-beta.2

- hardened Fiber fail-closed guard usage across preflight and core worker settlement stages.
- expanded replay-path guard checks to block unsafe create/submit/record actions.
- improved desktop UI clarity for endpoint/gate status and user-facing blocked reasons.
- tightened desktop packaging script preflight checks and metadata hints.
- made market persistence fallback explicit and observable (fail-closed by default; compatibility fallback requires opt-in env flag).
- tightened Fiber taxonomy guard by rejecting high-risk chain actions in simulate mode unless explicitly allowed.

> Internal preview only. Not a public stable release.
