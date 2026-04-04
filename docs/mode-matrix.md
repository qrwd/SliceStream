# SliceStream Mode Matrix

| Capability | manual | auto | hybrid |
|---|---|---|---|
| Proposal generation | Manual only | Automatic | Automatic |
| Match accept | Manual only | Automatic | Manual only |
| Recovery orchestrator auto-resume | Manual trigger | Automatic tick + manual trigger | Manual trigger |
| Retry/mark-final ops | Manual | Manual + optional automation | Manual |
| Recommended invalidated behavior | Block until reconfirm | Block until reconfirm | Block until reconfirm |

Notes:
- `auto_pause_reason/auto_pause_until` temporarily suppress automatic actions in auto mode.
- mode switch pushes in-flight attempts into recoverable or failed buckets with audit markers.
