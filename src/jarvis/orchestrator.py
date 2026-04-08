from __future__ import annotations

import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import FALLBACK_NO_GUESS
from .contracts import ValidationError, load_json_file, validate_evidence_bundle, validate_task_request
from .execution import build_execution_policy, execute_with_policy
from .hashing import compute_cache_key, compute_code_hash, sha256_object
from .memory_db import MemoryStore
from .research import collect_research_artifacts
from .run_store import RunStore
from .simulator import execute_domain_simulation
from .truth_layer import (
    build_metric_claims,
    has_unsupported_user_claims,
    normalize_user_claims,
    validate_claims,
)


class JarvisEngine:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.contracts_dir = self.project_root / "contracts"
        self.store = RunStore(project_root)
        self.store.ensure_layout()
        self.memory = MemoryStore(project_root)
        self.memory.ensure_schema()

    def health(self) -> dict[str, Any]:
        contracts_present = {
            "task_request_schema": (self.contracts_dir / "task_request.schema.json").exists(),
            "evidence_bundle_schema": (self.contracts_dir / "evidence_bundle.schema.json").exists(),
        }
        writable = {
            "data_runs": _is_writable(self.store.runs_dir),
            "data_cache": _is_writable(self.store.cache_dir),
            "data_memory": _is_writable(self.store.memory_dir),
        }
        return {
            "status": "ok" if all(contracts_present.values()) and all(writable.values()) else "degraded",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "project_root": str(self.project_root),
            "contracts_present": contracts_present,
            "storage_writable": writable,
            "memory_db_path": str(self.memory.db_path),
            "memory_db_exists": self.memory.db_path.exists(),
            "toolchain": {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            },
        }

    def dry_run(self, task_file: Path) -> dict[str, Any]:
        task = load_json_file(task_file)
        return self.run(task, dry_run=True)

    def run(self, task: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
        validate_task_request(task)
        run_trace: dict[str, Any] = {
            "run_mode": "dry_run" if dry_run else "run",
            "events": [],
        }
        self._trace_event(run_trace, "task_validated")

        if not task["requires_computation"]:
            self._trace_event(run_trace, "blocked_no_computation")
            return {
                "task_id": task["task_id"],
                "status": "blocked_no_computation",
                "message": FALLBACK_NO_GUESS,
                "run_trace": run_trace,
            }

        input_hash = sha256_object(task.get("input_refs", []))
        params_hash = sha256_object(task.get("parameters", {}))
        code_hash = compute_code_hash(self.project_root / "src" / "jarvis")
        env_hash = sha256_object(
            {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
        )
        seed = task.get("parameters", {}).get("seed", 42)

        cache_key = compute_cache_key(
            domain=task["domain"],
            objective=task["objective"],
            input_hash=input_hash,
            params_hash=params_hash,
            code_hash=code_hash,
            env_hash=env_hash,
            seed=seed,
        )
        self._trace_event(
            run_trace,
            "hashes_computed",
            {
                "cache_key": cache_key,
                "domain": task["domain"],
            },
        )

        force_rerun = bool(task.get("force_rerun", False))
        if not dry_run:
            self._trace_event(run_trace, "cache_lookup_started")
            cached_run_id = self.store.get_cached_run_id(cache_key)
            if cached_run_id and not force_rerun and self.store.run_exists(cached_run_id):
                cached_bundle = self.store.load_evidence(cached_run_id)
                validate_evidence_bundle(cached_bundle)
                is_dry_run_bundle = bool(cached_bundle.get("metrics", {}).get("dry_run", False))
                if cached_bundle["status"] == "SUCCESS" and not is_dry_run_bundle:
                    if self.memory.get_run(cached_run_id) is None:
                        self.index_run(cached_run_id)
                    claim_validation = self._validate_task_claims(task, cached_bundle)
                    research_bundle = self._load_run_research_bundle(cached_run_id)
                    execution_manifest = self._load_run_execution_manifest(cached_run_id)
                    cached_trace = self._load_run_trace(cached_run_id)
                    self._trace_event(
                        run_trace,
                        "cache_hit",
                        {"run_id": cached_run_id},
                    )
                    effective_trace = cached_trace if cached_trace.get("events") else run_trace
                    if has_unsupported_user_claims(claim_validation):
                        return {
                            "task_id": task["task_id"],
                            "status": "blocked_by_truth_layer",
                            "message": FALLBACK_NO_GUESS,
                            "cache_key": cache_key,
                            "run_id": cached_run_id,
                            "evidence_bundle": cached_bundle,
                            "claim_validation": claim_validation,
                            "research_bundle": research_bundle,
                            "execution_manifest": execution_manifest,
                            "run_trace": effective_trace,
                        }
                    return {
                        "task_id": task["task_id"],
                        "status": "cache_hit",
                        "cache_key": cache_key,
                        "run_id": cached_run_id,
                        "evidence_bundle": cached_bundle,
                        "claim_validation": claim_validation,
                        "research_bundle": research_bundle,
                        "execution_manifest": execution_manifest,
                        "run_trace": effective_trace,
                    }

        run_id = self.store.new_run_id()
        run_timestamp = datetime.now(timezone.utc).isoformat()
        run_trace["run_id"] = run_id
        self._trace_event(
            run_trace,
            "run_allocated",
            {"run_id": run_id},
        )
        (
            research_bundle,
            research_json_files,
            research_text_files,
            research_artifact_candidates,
        ) = collect_research_artifacts(
            task=task,
            project_root=self.project_root,
            run_id=run_id,
        )
        self._trace_event(
            run_trace,
            "research_collected",
            {
                "source_count": research_bundle.get("source_count", 0),
                "error_count": len(research_bundle.get("errors", [])),
            },
        )

        if dry_run:
            result_payload = {
                "domain": task["domain"],
                "objective": task["objective"],
                "result": {"dry_run": True},
                "metrics": {"dry_run": True},
                "research": {
                    "source_count": research_bundle.get("source_count", 0),
                    "error_count": len(research_bundle.get("errors", [])),
                },
            }
            summary_payload = {
                "headline": f"Dry run succeeded for task {task['task_id']}",
                "key_metrics": {"dry_run": True},
                "caveats": [
                    "No domain engine was executed in dry run mode.",
                    f"Research sources collected: {research_bundle.get('source_count', 0)}",
                ],
            }
            stdout_text = "Dry run completed.\n"
            stderr_text = ""
            status = "SUCCESS"
            execution_manifest = {
                "policy": {
                    "mode": "dry_run",
                    "timeout_sec": None,
                    "max_retries": 0,
                    "retry_delay_sec": 0.0,
                },
                "attempts": [{"attempt": 1, "status": "SUCCESS", "duration_sec": 0.0, "error": ""}],
                "final_status": "SUCCESS",
            }
            self._trace_event(run_trace, "simulation_skipped_dry_run")
        else:
            policy = build_execution_policy(task)
            self._trace_event(
                run_trace,
                "simulation_started",
                {
                    "timeout_sec": policy.timeout_sec,
                    "max_retries": policy.max_retries,
                },
            )
            execution_out = execute_with_policy(
                execute_domain_simulation,
                task=task,
                project_root=self.project_root,
                policy=policy,
            )
            execution_manifest = execution_out["execution_manifest"]
            if execution_out["ok"]:
                result_payload, summary_payload, stdout_text, stderr_text = execution_out["result"]
                status = "SUCCESS"
                self._trace_event(
                    run_trace,
                    "simulation_finished",
                    {"status": "SUCCESS", "attempts": len(execution_manifest.get("attempts", []))},
                )
            else:
                result_payload = {"error": execution_out.get("error", "Execution failed.")}
                summary_payload = {
                    "headline": f"Run failed for task {task['task_id']}",
                    "key_metrics": {},
                    "caveats": ["See execution_manifest for details."],
                }
                stdout_text = ""
                stderr_text = f"{execution_out.get('error', 'Execution failed.')}\n"
                status = "FAILED"
                self._trace_event(
                    run_trace,
                    "simulation_finished",
                    {"status": "FAILED", "error": execution_out.get("error", "")},
                )

        result_payload["research"] = {
            "source_count": research_bundle.get("source_count", 0),
            "error_count": len(research_bundle.get("errors", [])),
        }

        meta = {
            "run_id": run_id,
            "task_id": task["task_id"],
            "domain": task["domain"],
            "objective": task["objective"],
            "timestamp_utc": run_timestamp,
            "status": status,
            "cache_key": cache_key,
            "research_source_count": research_bundle.get("source_count", 0),
        }
        input_manifest = {
            "input_refs": task.get("input_refs", []),
            "input_hash": input_hash,
        }
        params = task.get("parameters", {})

        placeholder_bundle = {
            "run_id": run_id,
            "timestamp_utc": run_timestamp,
            "status": status,
            "domain": task["domain"],
            "input_hash": input_hash,
            "params_hash": params_hash,
            "code_hash": code_hash,
            "env_hash": env_hash,
            "seed": seed,
            "artifacts": [{"path": "pending", "sha256": "0" * 64, "kind": "raw"}],
            "logs": {"stdout": stdout_text, "stderr": stderr_text},
            "metrics": result_payload.get("metrics", {}),
            "notes": "Pre-artifact placeholder. Final bundle written after artifact hashing.",
        }
        extra_json_files = dict(research_json_files)
        extra_json_files["execution_manifest.json"] = execution_manifest
        extra_json_files["trace.json"] = run_trace
        extra_artifact_candidates = list(research_artifact_candidates)
        extra_artifact_candidates.append(("execution_manifest.json", "report"))
        extra_artifact_candidates.append(("trace.json", "report"))

        self._trace_event(run_trace, "placeholder_persist_started")
        placeholder_artifacts = self.store.save_run_files(
            run_id=run_id,
            meta=meta,
            input_manifest=input_manifest,
            params=params,
            stdout_text=stdout_text,
            stderr_text=stderr_text,
            result_payload=result_payload,
            summary_payload=summary_payload,
            evidence_bundle=placeholder_bundle,
            extra_json_files=extra_json_files,
            extra_text_files=research_text_files,
            extra_artifact_candidates=extra_artifact_candidates,
        )
        self._trace_event(
            run_trace,
            "placeholder_persist_finished",
            {"artifact_count": len(placeholder_artifacts)},
        )

        provisional_final_bundle = {
            "run_id": run_id,
            "timestamp_utc": run_timestamp,
            "status": status,
            "domain": task["domain"],
            "input_hash": input_hash,
            "params_hash": params_hash,
            "code_hash": code_hash,
            "env_hash": env_hash,
            "seed": seed,
            "artifacts": [{"path": "pending", "sha256": "0" * 64, "kind": "raw"}],
            "logs": {"stdout": stdout_text, "stderr": stderr_text},
            "metrics": result_payload.get("metrics", {}),
            "notes": "Validated evidence bundle.",
        }
        provisional_artifacts = self.store.save_run_files(
            run_id=run_id,
            meta=meta,
            input_manifest=input_manifest,
            params=params,
            stdout_text=stdout_text,
            stderr_text=stderr_text,
            result_payload=result_payload,
            summary_payload=summary_payload,
            evidence_bundle=provisional_final_bundle,
            extra_json_files=extra_json_files,
            extra_text_files=research_text_files,
            extra_artifact_candidates=extra_artifact_candidates,
        )

        final_bundle = {
            "run_id": run_id,
            "timestamp_utc": run_timestamp,
            "status": status,
            "domain": task["domain"],
            "input_hash": input_hash,
            "params_hash": params_hash,
            "code_hash": code_hash,
            "env_hash": env_hash,
            "seed": seed,
            "artifacts": provisional_artifacts,
            "logs": {"stdout": stdout_text, "stderr": stderr_text},
            "metrics": result_payload.get("metrics", {}),
            "notes": "Validated evidence bundle.",
        }
        validate_evidence_bundle(final_bundle)

        claim_validation = self._validate_task_claims(task, final_bundle)
        blocked_by_truth_layer = has_unsupported_user_claims(claim_validation)

        self.store.save_run_files(
            run_id=run_id,
            meta=meta,
            input_manifest=input_manifest,
            params=params,
            stdout_text=stdout_text,
            stderr_text=stderr_text,
            result_payload=result_payload,
            summary_payload=summary_payload,
            evidence_bundle=final_bundle,
            extra_json_files=extra_json_files,
            extra_text_files=research_text_files,
            extra_artifact_candidates=extra_artifact_candidates,
        )

        if status == "SUCCESS" and not dry_run:
            self.store.set_cache_entry(cache_key, run_id)
            self.memory.upsert_run(
                run_id=run_id,
                task_id=task["task_id"],
                domain=task["domain"],
                objective=task["objective"],
                cache_key=cache_key,
                timestamp_utc=run_timestamp,
                status=status,
                input_hash=input_hash,
                params_hash=params_hash,
                code_hash=code_hash,
                env_hash=env_hash,
                seed=seed,
                summary_path=f"data/runs/{run_id}/summary.json",
                evidence_path=f"data/runs/{run_id}/evidence_bundle.json",
                metrics=final_bundle.get("metrics", {}),
                artifacts=final_bundle["artifacts"],
            )

        if blocked_by_truth_layer:
            return {
                "task_id": task["task_id"],
                "status": "blocked_by_truth_layer",
                "message": FALLBACK_NO_GUESS,
                "cache_key": cache_key,
                "run_id": run_id,
                "evidence_bundle": final_bundle,
                "claim_validation": claim_validation,
                "research_bundle": research_bundle,
                "execution_manifest": execution_manifest,
                "run_trace": run_trace,
            }

        return {
            "task_id": task["task_id"],
            "status": "completed" if status == "SUCCESS" else "failed",
            "cache_key": cache_key,
            "run_id": run_id,
            "evidence_bundle": final_bundle,
            "claim_validation": claim_validation,
            "research_bundle": research_bundle,
            "execution_manifest": execution_manifest,
            "run_trace": run_trace,
        }

    def run_from_file(self, task_file: Path, *, dry_run: bool = False) -> dict[str, Any]:
        task = load_json_file(task_file)
        return self.run(task, dry_run=dry_run)

    def replay(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")
        evidence = self.store.load_evidence(run_id)
        validate_evidence_bundle(evidence)
        return {"status": "ok", "run_id": run_id, "evidence_bundle": evidence}

    def trace(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")
        return {
            "status": "ok",
            "run_id": run_id,
            "trace": self._load_run_trace(run_id),
            "execution_manifest": self._load_run_execution_manifest(run_id),
            "research_bundle": self._load_run_research_bundle(run_id),
        }

    def memory_query(
        self,
        *,
        limit: int = 20,
        domain: str | None = None,
        status: str | None = None,
        contains: str | None = None,
    ) -> dict[str, Any]:
        runs = self.memory.query_runs(limit=limit, domain=domain, status=status, contains=contains)
        return {"status": "ok", "count": len(runs), "runs": runs}

    def memory_get(self, run_id: str) -> dict[str, Any]:
        run = self.memory.get_run(run_id)
        if run is None:
            raise ValidationError(f"Run '{run_id}' not found in memory DB.")
        return {"status": "ok", "run": run}

    def index_run(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")

        meta = load_json_file(run_dir / "meta.json")
        evidence = load_json_file(run_dir / "evidence_bundle.json")
        validate_evidence_bundle(evidence)

        self.memory.upsert_run(
            run_id=run_id,
            task_id=meta.get("task_id", ""),
            domain=meta.get("domain", evidence["domain"]),
            objective=meta.get("objective", ""),
            cache_key=meta.get("cache_key", ""),
            timestamp_utc=meta.get("timestamp_utc", evidence["timestamp_utc"]),
            status=evidence["status"],
            input_hash=evidence["input_hash"],
            params_hash=evidence["params_hash"],
            code_hash=evidence["code_hash"],
            env_hash=evidence["env_hash"],
            seed=evidence["seed"],
            summary_path=f"data/runs/{run_id}/summary.json",
            evidence_path=f"data/runs/{run_id}/evidence_bundle.json",
            metrics=evidence.get("metrics", {}),
            artifacts=evidence["artifacts"],
        )
        return {"status": "ok", "run_id": run_id, "indexed": True}

    def _validate_task_claims(
        self,
        task: dict[str, Any],
        evidence_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        auto_claims = build_metric_claims(evidence_bundle.get("metrics", {}))
        user_claims = normalize_user_claims(task.get("parameters", {}).get("claims"))
        return validate_claims(claims=auto_claims + user_claims, evidence_bundle=evidence_bundle)

    def _load_run_research_bundle(self, run_id: str) -> dict[str, Any]:
        manifest_path = self.store.run_path(run_id) / "research" / "sources_manifest.json"
        if not manifest_path.exists():
            return {"source_count": 0, "sources": [], "errors": []}
        return load_json_file(manifest_path)

    def _load_run_execution_manifest(self, run_id: str) -> dict[str, Any]:
        path = self.store.run_path(run_id) / "execution_manifest.json"
        if not path.exists():
            return {
                "policy": {
                    "timeout_sec": None,
                    "max_retries": 0,
                    "retry_delay_sec": 0.0,
                },
                "attempts": [],
                "final_status": "UNKNOWN",
            }
        return load_json_file(path)

    def _load_run_trace(self, run_id: str) -> dict[str, Any]:
        path = self.store.run_path(run_id) / "trace.json"
        if not path.exists():
            return {"run_id": run_id, "run_mode": "unknown", "events": []}
        return load_json_file(path)

    @staticmethod
    def _trace_event(trace: dict[str, Any], stage: str, details: dict[str, Any] | None = None) -> None:
        trace.setdefault("events", []).append(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "details": details or {},
            }
        )


def _is_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False
