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
  - Run report command: persistent JSON+Markdown run report generation
  - Run audit commands: `audit-run` and `audit-all` for file/hash/schema integrity validation
  - Run history listing command: `runs-list` with status/domain/text filters
  - Run history aggregate command: `runs-stats` with status/domain success-rate summary
  - Legacy run migration command: `runs-migrate-legacy`
  - Cache tooling: `cache-verify` and `cache-rebuild`
  - Doctor command: consolidated diagnostic snapshot (`health` + cache + queue + run audit summary)
  - Doctor stale-running detection (`queue_stale_running`) and warning surfacing
  - Doctor orphan-result detection (`queue_orphan_results`) and warning surfacing
  - Doctor autofix mode: `doctor --fix` for legacy run backfill + cache rebuild + stale-running recovery + failed queue requeue
  - Doctor optional queue cleanup: `doctor --fix --queue-prune ...`
  - Doctor optional orphan-result cleanup: `doctor --fix --queue-clean-results ...`
  - Batch run command: execute multiple task JSON files with continue/stop-on-error modes
  - Task validation commands: `task-validate` and `task-validate-dir`
  - Run export command: ZIP bundle generation for run portability (`export-run`)
  - Run import command: restore exported ZIP runs with optional memory/cache linking (`import-run`)
  - Batch run-import command: `import-runs-dir`
  - Queue runner v1: submit/list/get/work with retry-at-queue-level
  - Queue stats API/CLI (`queue-stats`) with retry/dead-failed counters
  - Queue recovery command: `queue-requeue-failed`
  - Queue stale-running inspect command: `queue-stale-running`
  - Queue stale-running recovery command: `queue-recover-running`
  - Queue prune command: `queue-prune` (supports `--dry-run` preview)
  - Queue orphan-result cleanup command: `queue-clean-results` (supports `--dry-run`)
  - Queue cancellation command: `queue-cancel`
  - Queue worker unlimited drain mode: `queue-work --max-jobs 0`
  - Queue daemon worker mode: `queue-work-daemon` with poll interval + idle-stop controls
  - Memory index audit command: `memory-audit`
  - Memory stale-index cleanup command: `memory-clean` (supports `--dry-run`)
  - Doctor memory-index stale-reference warning + autofix cleanup integration

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
