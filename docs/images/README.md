# Dashboard visual references (text-first)

To keep the repository text-only and review-friendly, binary screenshot assets are not stored in git.

## UI views covered
- Home / KPI / risk overview
- Trade Terminal
- Profile / Settings + Gate Summary

## Re-generate screenshots locally (optional)
1. Start dashboard helper:
   ```bash
   SLICESTREAM_ENABLE_WEB_HELPER=1 cargo run -p dashboard
   ```
2. Use your browser automation tool to capture:
   - `http://127.0.0.1:4003` (home)
   - click `Trade Terminal`
   - click `Profile / Settings`

This keeps source control free of binary blobs while preserving reproducible capture steps.
