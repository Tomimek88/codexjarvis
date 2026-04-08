# CodexJarvis (Scaffold v0.1)

Local-first, evidence-first foundation for your JARVIS agentic framework.

Implementation snapshot: see `IMPLEMENTATION_STATUS.md`.

This repository now contains a working baseline for:
- strict no-guessing task contract validation
- deterministic cache key computation
- run artifact persistence
- evidence bundle creation and validation
- truth-layer claim/evidence validation (unsupported user claims are blocked)
- research/source tracking with local files + optional URLs (JSON/CSV extraction support)
- queue runner for asynchronous task processing
- replay from stored runs
- SQLite memory index + memo/semantic search for prior runs
- bootstrap scripts for local setup

## What Is Implemented Right Now

Phase-aligned foundation:
- Phase 0: governance docs + JSON contracts (already present)
- Phase 1: local runtime layout, deterministic run store, health checks, one-command bootstrap
- Phase 3 seed: orchestrator loop (validate -> cache lookup -> compute -> evidence -> store)

Current simulation engines are deterministic placeholders by domain. They are designed to be replaced in later phases with ASE/RDKit/OpenFOAM/backtesting engines.
`markets` now includes a CSV-backed baseline backtest engine (`markets_csv_backtest_v1`) for deterministic local runs.

## Project Layout

```
contracts/                  # JSON schemas (task + evidence)
src/jarvis/
  cli.py                    # CLI entrypoint
  contracts.py              # Runtime contract validation
  hashing.py                # Hashing and cache key
  memory_db.py              # SQLite long-term run index
  execution.py              # timeout/retry execution policy
  queue_db.py               # async job queue store
  orchestrator.py           # Planner/executor baseline
  research.py               # Source collection + tracking
  run_store.py              # Persistent run and cache index storage
  simulator.py              # Deterministic placeholder domain engines
scripts/
  bootstrap.ps1             # Windows setup
  bootstrap.sh              # Linux/macOS setup
examples/tasks/
  generic_sum_task.json     # Smoke task
  generic_research_task.json
  generic_timeout_task.json
  generic_truth_block_task.json
  markets_backtest_task.json
examples/data/markets/
  demo_prices.csv           # Sample close-price series for markets backtest
data/
  runs/                     # Per-run artifacts
  cache/                    # cache_index.json
  memory/                   # SQLite memory DB + future vector memory
```

## Quick Start (Windows PowerShell)

1. Install Python 3.10+.
2. Run:

```powershell
Set-Location C:\Users\Tomino\Desktop\codexjarvis
.\scripts\bootstrap.ps1
```

3. Run a dry-run task:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis dry-run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\generic_sum_task.json
```

4. Run real compute:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\generic_sum_task.json
```

Smoke test shortcut:

```powershell
.\scripts\smoke.ps1
```

Markets backtest example:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\markets_backtest_task.json
```

Truth-layer block example:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\generic_truth_block_task.json
```

This should return `status=blocked_by_truth_layer` and message `Nevim, musim to nasimulovat.`.

Research/source tracking example:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\generic_research_task.json
```

This run stores:
- `data/runs/<run_id>/research/sources_manifest.json`
- `data/runs/<run_id>/research/src_001.txt` (and more sources if provided)

Execution timeout example:

```powershell
$env:PYTHONPATH='C:\Users\Tomino\Desktop\codexjarvis\src'
.\.venv\Scripts\python.exe -m jarvis --root C:\Users\Tomino\Desktop\codexjarvis run --task-file C:\Users\Tomino\Desktop\codexjarvis\examples\tasks\generic_timeout_task.json
```

## Docker Option

If Docker Desktop is available:

```powershell
Set-Location C:\Users\Tomino\Desktop\codexjarvis
docker compose up --build
```

For a sample compute run:

```powershell
docker compose run --rm jarvis python -m jarvis run --root /app --task-file /app/examples/tasks/generic_sum_task.json
```

## CLI Commands

```bash
jarvis --root <project_root> health
jarvis --root <project_root> dry-run --task-file <task.json>
jarvis --root <project_root> run --task-file <task.json>
jarvis --root <project_root> replay --run-id <run_id>
jarvis --root <project_root> trace --run-id <run_id>
jarvis --root <project_root> memory-query --limit 20 [--domain generic] [--status SUCCESS] [--contains text]
jarvis --root <project_root> memory-search --query "<text>" [--limit 10] [--domain generic] [--status SUCCESS]
jarvis --root <project_root> memory-semantic-search --query "<text>" [--limit 10] [--domain generic] [--status SUCCESS] [--min-score 0.0]
jarvis --root <project_root> memory-hybrid-search --query "<text>" [--limit 10] [--lexical-weight 0.4] [--semantic-weight 0.6] [--min-combined-score 0.0]
jarvis --root <project_root> memory-get --run-id <run_id>
jarvis --root <project_root> memory-index --run-id <run_id>
jarvis --root <project_root> memory-reindex-all [--limit 0] [--include-failed]
jarvis --root <project_root> queue-submit --task-file <task.json> [--dry-run] [--max-attempts 1]
jarvis --root <project_root> queue-list [--status QUEUED] [--limit 20]
jarvis --root <project_root> queue-get --job-id <job_id>
jarvis --root <project_root> queue-stats
jarvis --root <project_root> queue-work-once [--worker-id worker-1]
jarvis --root <project_root> queue-work [--max-jobs 10] [--worker-id worker-1]
```

## Memory Layer (Current)

- Every successful real run is automatically indexed into `data/memory/memory.db`.
- Indexed data includes core hashes, metrics, summary/evidence paths, and artifact hashes.
- `memory-query` is the fast operator-facing lookup for replay/reuse decisions.
- `memory-search` returns ranked runs by memo/objective token match.
- `memory-semantic-search` returns cosine-ranked runs from local sparse vectors.
- `memory-hybrid-search` combines lexical + semantic ranking into one score.
- `memory-reindex-all` backfills memory DB entries from existing `data/runs/*`.
- Obsidian can still be used as human notes, but this SQLite DB is the source of truth for deterministic runtime memory.

## Truth Layer (Current)

- Auto metric claims are created from computed metrics and must resolve to evidence refs (`metrics.<key>`).
- User claims can be supplied in task parameters under `claims`.
- Supported advanced evidence refs now include:
  - `metrics.exists:<key>`
  - `metrics.value_eq:<key>=<value>`
  - `logs.stdout.contains:<text>` / `logs.stderr.contains:<text>`
  - `logs.stdout.regex:<pattern>` / `logs.stderr.regex:<pattern>`
  - `artifacts.path_contains:<fragment>` / `artifacts.path_regex:<pattern>`
- If any user claim lacks resolvable evidence refs, the run output is blocked with:
  - `status: blocked_by_truth_layer`
  - `message: Nevim, musim to nasimulovat.`

## Research Layer (Current)

- Add `parameters.research_refs` in task JSON (local paths, `local://...`, or URLs).
- Research artifacts are hash-tracked and attached to the run evidence.
- Structured local sources are normalized for reasoning:
  - `.json` -> pretty JSON text
  - `.csv` / `.tsv` -> tabular preview (header + sample rows)
- Every source manifest entry now includes `provenance` metadata
  (`retrieval_method`, resolved path or final URL, timestamps, size/status where available).
- URL fetch is best-effort and may fail if network is unavailable; failures are recorded in research manifest.

## Execution + Trace (Current)

- Add `parameters.execution_policy` to control execution:
  - `timeout_sec`
  - `max_retries`
  - `retry_delay_sec`
- Every run stores:
  - `data/runs/<run_id>/execution_manifest.json`
  - `data/runs/<run_id>/trace.json`
- You can inspect these via:
  - `jarvis --root <project_root> trace --run-id <run_id>`

## Queue Runner (Current)

- Queue state is stored in `data/queue/queue.db`.
- Job results are stored in `data/queue/results/<job_id>.json`.
- `queue-stats` provides aggregate status counts and retry/dead-failed indicators.
- Submit now, execute later pattern:
  1. `queue-submit`
  2. `queue-work-once` or `queue-work`
  3. `queue-get` / `queue-list`

## Evidence-First Guarantee in This Scaffold

When a run executes, the engine stores:
- `data/runs/<run_id>/meta.json`
- `data/runs/<run_id>/input_manifest.json`
- `data/runs/<run_id>/params.json`
- `data/runs/<run_id>/stdout.log`
- `data/runs/<run_id>/stderr.log`
- `data/runs/<run_id>/results/result.json`
- `data/runs/<run_id>/summary.json`
- `data/runs/<run_id>/evidence_bundle.json`

## Next Recommended Steps

1. Add real simulation adapters (ASE, RDKit, OpenFOAM, backtesting engine).
2. Add internet retrieval module with source tracking and claim-evidence mapping.
3. Add sandbox/process isolation and resource limits for simulation jobs.
4. Add vector DB memory and semantic retrieval on top of SQLite run index.
5. Add observability traces and replay dashboard.
