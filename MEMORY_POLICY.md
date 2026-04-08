# Simulation Memory Policy (v1)

## Goal

Never recompute the same simulation unnecessarily, while preserving correctness.

## Memory Layers

1. Working memory (short-term)
   - Active task context
   - Temporary intermediate outputs
2. Long-term memory (persistent)
   - Prior runs metadata
   - Raw simulation artifacts
   - User preferences and accepted assumptions
3. Vector memory (semantic retrieval)
   - Notes, docs, reports, interpretations
   - Indexed with metadata filters

## Simulation Cache Key

Each simulation run must have a deterministic cache key:

`cache_key = hash(domain + objective + inputs_hash + params_hash + code_hash + env_hash + seed)`

Where:
- `inputs_hash` = hash of all input files/data slices
- `params_hash` = sorted simulation params
- `code_hash` = executed script/package state
- `env_hash` = container image + core dependency versions
- `seed` = explicit random seed (if stochastic)

## Reuse Rule

Reuse prior result only if:

1. cache_key matches exactly
2. artifacts are present and readable
3. run status is SUCCESS
4. result is not expired by TTL policy (if domain requires freshness)

If any condition fails -> run a new simulation.

## Freshness / TTL

- Physics/materials chemistry runs: usually reusable unless inputs change.
- Market/data-dependent runs: must validate data currency window before reuse.
- User can force re-run with `force=true`.

## Mandatory Stored Artifacts

For every successful run store:

- `runs/<run_id>/meta.json`
- `runs/<run_id>/input_manifest.json`
- `runs/<run_id>/params.json`
- `runs/<run_id>/stdout.log`
- `runs/<run_id>/stderr.log`
- `runs/<run_id>/results/` (raw outputs)
- `runs/<run_id>/summary.json`

## Link to Knowledge Memory

After simulation:
1. Persist raw artifacts in run store.
2. Write concise simulation memo (scope, method, key metrics, caveats).
3. Index memo + metadata into vector DB for semantic retrieval.
4. Never index raw outputs without linking run_id and hashes.

