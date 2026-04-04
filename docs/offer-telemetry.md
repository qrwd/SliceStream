# Offer Telemetry and Confidence Model

This document defines how provider offers should be interpreted in SliceStream when comparing decentralized compute offers.

## Core metadata expected on offer details
- `hardware_vendor`
- `hardware_model`
- `gpu_count`
- `vram_gib`
- `system_ram_gib`
- `memory_bandwidth_gbps`
- `interconnect` (`pcie`/`nvlink`/other)
- `benchmark_suite_version`
- `measured_perf_value`
- `measured_perf_unit`
- `perf_sample_count`
- `perf_window_secs`
- `perf_confidence`
- `telemetry_source`
- `evidence_refs`

## Confidence semantics
- `high_confidence`: recent benchmark + sufficient samples + evidence refs present.
- `medium_confidence`: partial sampling or stale benchmark window.
- `low_confidence`: sparse data or missing evidence.
- `unavailable`: no reliable telemetry source.

## UI requirements
- Never show estimated values as definitive measured truth.
- Render confidence labels and data source near every performance metric.
- Provide hash/evidence reference links for verifiability.

## Recommended price explanation
Recommended pricing should include:
- benchmark basis
- sample window basis
- confidence score impact
- risk adjustments (dispute/breach history when available)


## Stage 5 telemetry trust model hardening

- Telemetry source is normalized into `measured|estimated|unavailable|mock` classes.
- `confidence_label` is computed from confidence score + source class (mock/unavailable always `unverified`).
- Offer telemetry snapshots are canonical-signed and verifiable; mock/estimated telemetry is explicitly not treated as verified measurements.
