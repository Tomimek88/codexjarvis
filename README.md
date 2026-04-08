# CodexJarvis (Scaffold v0.1)

Local-first, evidence-first foundation for your JARVIS agentic framework.

Implementation snapshot: see `IMPLEMENTATION_STATUS.md`.

This repository now contains a working baseline for:
- strict no-guessing task contract validation
- deterministic cache key computation
- run artifact persistence
- evidence bundle creation and validation
- replay from stored runs
- bootstrap scripts for local setup

## What Is Implemented Right Now

Phase-aligned foundation:
- Phase 0: governance docs + JSON contracts (already present)
- Phase 1: local runtime layout, deterministic run store, health checks, one-command bootstrap
- Phase 3 seed: orchestrator loop (validate -> cache lookup -> compute -> evidence -> store)

Current simulation engines are deterministic placeholders by domain. They are designed to be replaced in later phases with ASE/RDKit/OpenFOAM/backtesting engines.

## Project Layout

```
contracts/                  # JSON schemas (task + evidence)
src/jarvis/
  cli.py                    # CLI entrypoint
  contracts.py              # Runtime contract validation
  hashing.py                # Hashing and cache key
  orchestrator.py           # Planner/executor baseline
  run_store.py              # Persistent run and cache index storage
  simulator.py              # Deterministic placeholder domain engines
scripts/
  bootstrap.ps1             # Windows setup
  bootstrap.sh              # Linux/macOS setup
examples/tasks/
  generic_sum_task.json     # Smoke task
data/
  runs/                     # Per-run artifacts
  cache/                    # cache_index.json
  memory/                   # Reserved for long-term/vector memory metadata
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
```

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
4. Add vector DB memory and run memo indexing.
5. Add observability traces and replay dashboard.
