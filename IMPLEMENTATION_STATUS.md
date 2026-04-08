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

## Current known external blockers on this PC

- Docker CLI is not currently available in PATH.

Python runtime execution and tests are now passing via local venv.

## Next implementation targets

1. Add real domain engines:
   - materials: ASE
   - chemistry: RDKit
   - physics/CFD: OpenFOAM adapter
   - markets: backtesting adapter
2. Add retrieval + source tracking layer (internet/local docs) with provenance records.
3. Add claim-to-evidence validator (truth layer gate).
4. Add sandbox resource policies and job queue runner.
5. Add vector memory retrieval APIs and semantic indexing pipeline.
