# Implementation Status (2026-04-08)

## Completed in this iteration

- Phase 0 foundations are present:
  - system constitution
  - memory policy
  - task/evidence JSON schemas
- Phase 1 baseline scaffold implemented:
  - reproducible Python project structure
  - local run storage (`data/runs`, `data/cache`, `data/memory`)
  - SQLite memory DB (`data/memory/memory.db`) with run + artifact index
  - memory memo search (`memory-search`) over indexed runs
  - semantic sparse-vector search (`memory-semantic-search`) over indexed runs
  - hybrid retrieval (`memory-hybrid-search`) and bulk backfill (`memory-reindex-all`)
  - health checks (`jarvis health`)
  - one-command bootstrap scripts (`scripts/bootstrap.ps1`, `scripts/bootstrap.sh`)
  - smoke scripts (`scripts/smoke.ps1`, `scripts/smoke.sh`)
- Phase 3 seed implemented:
  - orchestrator loop (validate -> cache check -> compute -> evidence -> persist)
  - replay command (`jarvis replay --run-id`)
  - memory CLI commands (`memory-query`, `memory-get`, `memory-index`)
  - deterministic cache key according to memory policy fields
  - first real markets engine: CSV-backed MA crossover backtest
  - Truth Layer v1: claim-evidence validator with automatic block on unsupported user claims
  - Truth Layer v1.1: richer evidence ref operators (`contains`, `regex`, metric equality/existence)
  - Research Layer v1: source collection + hashed source snapshots + per-run manifest
  - Structured source extraction: JSON pretty-print + CSV/TSV tabular preview
  - Source provenance metadata (filesystem/http retrieval details) in research manifest
  - Research ref expansion for directories and glob patterns
  - Research source deduplication (SHA-based) for repeated refs
  - Execution policy v1: timeout + retry control with execution manifest
  - Run trace v1: per-stage timeline persisted per run
  - Run inspect command: consolidated diagnostics view with trace timings + execution/research/truth overview
  - Run compare command: cross-run diff for status/hashes/metrics/artifacts
  - Run audit commands: `audit-run` and `audit-all` for file/hash/schema integrity validation
  - Run history listing command: `runs-list` with status/domain/text filters
  - Run history aggregate command: `runs-stats` with status/domain success-rate summary
  - Queue runner v1: submit/list/get/work with retry-at-queue-level
  - Queue stats API/CLI (`queue-stats`) with retry/dead-failed counters

## Current known external blockers on this PC

- Docker CLI is not currently available in PATH.

Python runtime execution and tests are now passing via local venv.

## Next implementation targets

1. Add real domain engines:
   - materials: ASE
   - chemistry: RDKit
   - physics/CFD: OpenFOAM adapter
   - markets: backtesting adapter
2. Expand retrieval connectors (APIs, PDFs, structured web extraction) with stronger provenance metadata.
3. Add claim-to-evidence validator (truth layer gate).
4. Add sandbox resource policies and job queue runner.
5. Add vector memory retrieval APIs and semantic indexing pipeline.
