# JARVIS Agentic Framework - Master Plan (v1)

## North Star

Build a local-first open-source agentic system that:
- can research with internet access
- never guesses
- computes/simulates when certainty is insufficient
- remembers prior simulations and reuses them safely

## Phase 0 - Governance and Contracts

Objective:
- Define hard rules, evidence schema, and memory behavior.

Deliverables:
- `SYSTEM_CONSTITUTION.md`
- `MEMORY_POLICY.md`
- task/result schema for evidence-first outputs

Exit criteria:
- No result can be returned without required evidence fields.

## Phase 1 - Local Infrastructure Baseline

Objective:
- Prepare deterministic local runtime (LLM + sandbox + storage).

Deliverables:
- containerized runtime
- persistent volumes for memory and run artifacts
- health checks and resource limits

Exit criteria:
- Reproducible environment bootstrap from one command.

## Phase 2 - Model Selection and Routing

Objective:
- Select best open-source models per task type (planner/coder/reasoner/verifier).

Deliverables:
- benchmark suite
- routing policy and fallback chain
- tool-calling compatibility tests

Exit criteria:
- Stable routing with measured quality/latency/cost profile.

## Phase 3 - Orchestrator Core

Objective:
- Implement planner-executor-verifier loop with function/tool calling.

Deliverables:
- orchestration state machine
- tool registry with typed interfaces
- guardrails for uncertainty and loop control

Exit criteria:
- End-to-end tool execution across representative tasks.

## Phase 4 - Long-Term Memory (RAG + Run Store)

Objective:
- Persist both semantic knowledge and exact simulation artifacts.

Deliverables:
- vector DB schema + metadata filters
- run artifact store + cache index
- memory retrieval APIs

Exit criteria:
- Identical simulation request can be served from cache with proof.

## Phase 5 - Simulation Sandbox Framework

Objective:
- Safe, isolated execution for Python/C++ simulation workloads.

Deliverables:
- domain-specific container images
- job queue + timeout/retry controls
- deterministic run manifests (hashes, seeds, env info)

Exit criteria:
- Every run produces replay-ready evidence bundle.

## Phase 6 - Domain Engines v1

Objective:
- Integrate first simulation engines.

Deliverables:
- materials: ASE workflows
- chemistry: RDKit workflows
- physics/CFD: OpenFOAM workflow
- markets/data: backtesting workflow

Exit criteria:
- Each domain completes one validated end-to-end scenario.

## Phase 7 - Truth Layer (Anti-Hallucination)

Objective:
- Enforce "claim must map to evidence".

Deliverables:
- claim-evidence validator
- confidence policy
- automatic fallback phrase when evidence is insufficient

Exit criteria:
- Unsupported claims are blocked automatically.

## Phase 8 - Multi-Agent Delegation

Objective:
- Add specialist sub-agents with explicit responsibilities.

Deliverables:
- planner/coder/verifier/scientist agent roles
- conflict-safe shared state
- delegation policy with measurable quality checks

Exit criteria:
- Complex tasks solved via parallel agent collaboration with traceability.

## Phase 9 - Observability and Audit

Objective:
- Full tracing, metrics, logs, replay, and provenance.

Deliverables:
- run dashboards
- per-task trace viewer
- audit exports

Exit criteria:
- Any output can be traced and replayed end-to-end.

## Phase 10 - Security Hardening

Objective:
- Harden runtime and tool execution boundaries.

Deliverables:
- rootless containers and syscall restrictions
- policy-as-code for allowed tools/actions
- penetration tests for prompt and tool abuse

Exit criteria:
- Security test suite passes with documented residual risk.

## Phase 11 - MVP and Release

Objective:
- Ship first practical JARVIS release.

Deliverables:
- 3 validated showcase pipelines (materials, markets, mixed)
- operator docs
- reproducibility and incident playbooks

Exit criteria:
- User can request, run, replay, and reuse simulations without manual debugging.

