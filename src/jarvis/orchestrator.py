from __future__ import annotations

import json
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .constants import FALLBACK_NO_GUESS
from .contracts import ValidationError, load_json_file, validate_evidence_bundle, validate_task_request
from .execution import build_execution_policy, execute_with_policy
from .hashing import compute_cache_key, compute_code_hash, sha256_file, sha256_object
from .memory_db import MemoryStore
from .queue_db import QueueStore
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
        self.queue = QueueStore(project_root)
        self.queue.ensure_schema()

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
            "queue_db_path": str(self.queue.db_path),
            "queue_db_exists": self.queue.db_path.exists(),
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
                memo_text=self._compose_memo(
                    task=task,
                    summary_payload=summary_payload,
                    evidence_bundle=final_bundle,
                    research_bundle=research_bundle,
                ),
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

    def inspect(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")

        meta = self._load_run_meta(run_id)
        params = self._load_run_params(run_id)
        summary = self._load_run_summary(run_id)
        evidence = self.store.load_evidence(run_id)
        validate_evidence_bundle(evidence)
        trace = self._load_run_trace(run_id)
        execution_manifest = self._load_run_execution_manifest(run_id)
        research_bundle = self._load_run_research_bundle(run_id)
        claim_validation = self._validate_task_claims({"parameters": params}, evidence)

        trace_overview = self._summarize_trace(trace)
        attempts = execution_manifest.get("attempts", [])
        if not isinstance(attempts, list):
            attempts = []

        execution_overview = {
            "final_status": str(execution_manifest.get("final_status", "UNKNOWN")),
            "attempt_count": len(attempts),
            "success_attempts": sum(1 for a in attempts if str(a.get("status", "")).upper() == "SUCCESS"),
            "failed_attempts": sum(1 for a in attempts if str(a.get("status", "")).upper() != "SUCCESS"),
            "total_attempt_duration_sec": round(
                sum(float(a.get("duration_sec", 0.0) or 0.0) for a in attempts),
                6,
            ),
            "policy": execution_manifest.get("policy", {}),
        }

        sources = research_bundle.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        research_overview = {
            "source_count": int(research_bundle.get("source_count", 0)),
            "error_count": len(research_bundle.get("errors", [])),
            "deduplicated_count": int(research_bundle.get("deduplicated_count", 0)),
            "ok_count": sum(1 for s in sources if str(s.get("status", "")).upper() == "OK"),
            "duplicate_count": sum(1 for s in sources if str(s.get("status", "")).upper() == "DUPLICATE"),
            "failed_count": sum(1 for s in sources if str(s.get("status", "")).upper() == "FAILED"),
        }

        artifacts = evidence.get("artifacts", [])
        if not isinstance(artifacts, list):
            artifacts = []
        artifact_kind_counts: dict[str, int] = {}
        for artifact in artifacts:
            kind = str(artifact.get("kind", "unknown"))
            artifact_kind_counts[kind] = artifact_kind_counts.get(kind, 0) + 1
        evidence_overview = {
            "status": str(evidence.get("status", "")),
            "artifact_count": len(artifacts),
            "artifact_kind_counts": artifact_kind_counts,
            "metric_keys": sorted(evidence.get("metrics", {}).keys()) if isinstance(evidence.get("metrics", {}), dict) else [],
        }

        truth_overview = {
            "all_supported": bool(claim_validation.get("all_supported", False)),
            "supported_count": int(claim_validation.get("supported_count", 0)),
            "unsupported_count": int(claim_validation.get("unsupported_count", 0)),
            "blocked_user_claims": bool(has_unsupported_user_claims(claim_validation)),
        }

        return {
            "status": "ok",
            "run_id": run_id,
            "meta": meta,
            "summary": summary,
            "evidence_overview": evidence_overview,
            "trace_overview": trace_overview,
            "execution_overview": execution_overview,
            "research_overview": research_overview,
            "truth_overview": truth_overview,
        }

    def compare_runs(self, run_a: str, run_b: str) -> dict[str, Any]:
        path_a = self.store.run_path(run_a)
        path_b = self.store.run_path(run_b)
        if not path_a.exists():
            raise ValidationError(f"Run '{run_a}' does not exist.")
        if not path_b.exists():
            raise ValidationError(f"Run '{run_b}' does not exist.")

        evidence_a = self.store.load_evidence(run_a)
        evidence_b = self.store.load_evidence(run_b)
        validate_evidence_bundle(evidence_a)
        validate_evidence_bundle(evidence_b)

        meta_a = self._load_run_meta(run_a)
        meta_b = self._load_run_meta(run_b)

        metrics_a = evidence_a.get("metrics", {})
        if not isinstance(metrics_a, dict):
            metrics_a = {}
        metrics_b = evidence_b.get("metrics", {})
        if not isinstance(metrics_b, dict):
            metrics_b = {}

        metric_diff: dict[str, dict[str, Any]] = {}
        all_metric_keys = sorted(set(metrics_a.keys()) | set(metrics_b.keys()))
        for key in all_metric_keys:
            a_val = metrics_a.get(key)
            b_val = metrics_b.get(key)
            delta: float | None = None
            if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                delta = float(b_val) - float(a_val)
            metric_diff[str(key)] = {
                "run_a": a_val,
                "run_b": b_val,
                "delta": round(delta, 6) if delta is not None else None,
                "changed": a_val != b_val,
            }

        artifacts_a = evidence_a.get("artifacts", [])
        artifacts_b = evidence_b.get("artifacts", [])
        if not isinstance(artifacts_a, list):
            artifacts_a = []
        if not isinstance(artifacts_b, list):
            artifacts_b = []
        by_path_a = {
            _normalize_artifact_path(str(item.get("path", "")), run_a): str(item.get("sha256", ""))
            for item in artifacts_a
            if isinstance(item, dict) and str(item.get("path", ""))
        }
        by_path_b = {
            _normalize_artifact_path(str(item.get("path", "")), run_b): str(item.get("sha256", ""))
            for item in artifacts_b
            if isinstance(item, dict) and str(item.get("path", ""))
        }
        all_artifact_paths = sorted(set(by_path_a.keys()) | set(by_path_b.keys()))
        artifacts_only_in_a: list[str] = []
        artifacts_only_in_b: list[str] = []
        artifacts_changed: list[dict[str, str]] = []
        for artifact_path in all_artifact_paths:
            sha_a = by_path_a.get(artifact_path)
            sha_b = by_path_b.get(artifact_path)
            if sha_a and not sha_b:
                artifacts_only_in_a.append(artifact_path)
            elif sha_b and not sha_a:
                artifacts_only_in_b.append(artifact_path)
            elif sha_a != sha_b:
                artifacts_changed.append(
                    {
                        "path": artifact_path,
                        "sha_a": sha_a or "",
                        "sha_b": sha_b or "",
                    }
                )

        hash_fields = ["input_hash", "params_hash", "code_hash", "env_hash"]
        hash_comparison = {
            field: {
                "run_a": evidence_a.get(field),
                "run_b": evidence_b.get(field),
                "equal": evidence_a.get(field) == evidence_b.get(field),
            }
            for field in hash_fields
        }

        return {
            "status": "ok",
            "run_a": run_a,
            "run_b": run_b,
            "meta": {
                "run_a": meta_a,
                "run_b": meta_b,
            },
            "status_comparison": {
                "run_a": evidence_a.get("status"),
                "run_b": evidence_b.get("status"),
                "equal": evidence_a.get("status") == evidence_b.get("status"),
            },
            "hash_comparison": hash_comparison,
            "metric_diff": metric_diff,
            "artifact_diff": {
                "only_in_run_a": artifacts_only_in_a,
                "only_in_run_b": artifacts_only_in_b,
                "changed_sha": artifacts_changed,
            },
        }

    def audit_run(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")

        issues: list[dict[str, Any]] = []
        required_files = [
            "meta.json",
            "input_manifest.json",
            "params.json",
            "summary.json",
            "evidence_bundle.json",
            "results/result.json",
            "stdout.log",
            "stderr.log",
            "execution_manifest.json",
            "trace.json",
        ]
        for rel in required_files:
            path = run_dir / rel
            if not path.exists():
                issues.append(
                    {
                        "code": "missing_required_file",
                        "path": f"data/runs/{run_id}/{rel}",
                        "message": "Required run file is missing.",
                    }
                )

        evidence: dict[str, Any] | None = None
        try:
            evidence = self.store.load_evidence(run_id)
            validate_evidence_bundle(evidence)
        except Exception as exc:
            issues.append(
                {
                    "code": "invalid_evidence_bundle",
                    "path": f"data/runs/{run_id}/evidence_bundle.json",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
            evidence = None

        checked_artifacts = 0
        hash_mismatches = 0
        missing_artifacts = 0
        if isinstance(evidence, dict):
            if str(evidence.get("run_id", "")) != run_id:
                issues.append(
                    {
                        "code": "run_id_mismatch",
                        "path": f"data/runs/{run_id}/evidence_bundle.json",
                        "message": "Evidence run_id does not match directory run_id.",
                    }
                )

            artifacts = evidence.get("artifacts", [])
            if not isinstance(artifacts, list):
                issues.append(
                    {
                        "code": "invalid_artifacts_field",
                        "path": f"data/runs/{run_id}/evidence_bundle.json",
                        "message": "Field 'artifacts' is not a list.",
                    }
                )
                artifacts = []

            for idx, artifact in enumerate(artifacts):
                if not isinstance(artifact, dict):
                    issues.append(
                        {
                            "code": "invalid_artifact_entry",
                            "path": f"data/runs/{run_id}/evidence_bundle.json",
                            "message": f"Artifact entry at index {idx} is not an object.",
                        }
                    )
                    continue

                rel_path = str(artifact.get("path", "")).strip()
                expected_sha = str(artifact.get("sha256", "")).strip()
                if not rel_path or not expected_sha:
                    issues.append(
                        {
                            "code": "invalid_artifact_entry",
                            "path": f"data/runs/{run_id}/evidence_bundle.json",
                            "message": f"Artifact entry at index {idx} is missing path or sha256.",
                        }
                    )
                    continue

                artifact_abs = (self.project_root / rel_path).resolve()
                if not _is_within_root(artifact_abs, self.project_root.resolve()):
                    issues.append(
                        {
                            "code": "artifact_path_outside_root",
                            "path": rel_path,
                            "message": "Artifact path resolves outside project root.",
                        }
                    )
                    continue

                checked_artifacts += 1
                if not artifact_abs.exists():
                    missing_artifacts += 1
                    issues.append(
                        {
                            "code": "missing_artifact_file",
                            "path": rel_path,
                            "message": "Artifact file does not exist on disk.",
                        }
                    )
                    continue

                actual_sha = sha256_file(artifact_abs)
                if actual_sha != expected_sha:
                    hash_mismatches += 1
                    issues.append(
                        {
                            "code": "artifact_hash_mismatch",
                            "path": rel_path,
                            "message": "Artifact SHA256 does not match evidence bundle.",
                            "expected_sha256": expected_sha,
                            "actual_sha256": actual_sha,
                        }
                    )

        passed = len(issues) == 0
        return {
            "status": "ok",
            "run_id": run_id,
            "passed": passed,
            "issue_count": len(issues),
            "checked_artifact_count": checked_artifacts,
            "hash_mismatch_count": hash_mismatches,
            "missing_artifact_count": missing_artifacts,
            "issues": issues,
        }

    def audit_all(
        self,
        *,
        limit: int = 50,
        include_passed: bool = False,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        safe_limit = max(1, min(int(limit), 2000))
        run_dirs = run_dirs[:safe_limit]

        passed_count = 0
        failed_count = 0
        error_count = 0
        reports: list[dict[str, Any]] = []
        for run_dir in run_dirs:
            run_id = run_dir.name
            try:
                report = self.audit_run(run_id)
            except Exception as exc:
                error_count += 1
                report = {
                    "status": "error",
                    "run_id": run_id,
                    "passed": False,
                    "issue_count": 1,
                    "issues": [
                        {
                            "code": "audit_exception",
                            "path": f"data/runs/{run_id}",
                            "message": f"{type(exc).__name__}: {exc}",
                        }
                    ],
                }

            if bool(report.get("passed", False)):
                passed_count += 1
                if include_passed:
                    reports.append(report)
            else:
                failed_count += 1
                reports.append(report)

        return {
            "status": "ok",
            "scanned_count": len(run_dirs),
            "passed_count": passed_count,
            "failed_count": failed_count,
            "error_count": error_count,
            "reports": reports,
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

    def memory_search(
        self,
        *,
        query: str,
        limit: int = 10,
        domain: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        rows = self.memory.search_runs(
            query=query,
            limit=limit,
            domain=domain,
            status=status,
        )
        return {"status": "ok", "count": len(rows), "results": rows}

    def memory_semantic_search(
        self,
        *,
        query: str,
        limit: int = 10,
        domain: str | None = None,
        status: str | None = None,
        min_score: float = 0.0,
    ) -> dict[str, Any]:
        rows = self.memory.semantic_search_runs(
            query=query,
            limit=limit,
            domain=domain,
            status=status,
            min_score=min_score,
        )
        return {"status": "ok", "count": len(rows), "results": rows}

    def memory_hybrid_search(
        self,
        *,
        query: str,
        limit: int = 10,
        domain: str | None = None,
        status: str | None = None,
        lexical_weight: float = 0.4,
        semantic_weight: float = 0.6,
        min_combined_score: float = 0.0,
    ) -> dict[str, Any]:
        rows = self.memory.hybrid_search_runs(
            query=query,
            limit=limit,
            domain=domain,
            status=status,
            lexical_weight=lexical_weight,
            semantic_weight=semantic_weight,
            min_combined_score=min_combined_score,
        )
        return {"status": "ok", "count": len(rows), "results": rows}

    def memory_reindex_all(
        self,
        *,
        limit: int = 0,
        include_failed: bool = False,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        self.memory.ensure_schema()

        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        if limit > 0:
            run_dirs = run_dirs[:limit]

        indexed_run_ids: list[str] = []
        errors: list[dict[str, str]] = []
        skipped_missing = 0
        skipped_status = 0

        for run_dir in run_dirs:
            run_id = run_dir.name
            evidence_path = run_dir / "evidence_bundle.json"
            meta_path = run_dir / "meta.json"
            if not evidence_path.exists() or not meta_path.exists():
                skipped_missing += 1
                continue

            try:
                evidence = load_json_file(evidence_path)
            except Exception as exc:
                errors.append({"run_id": run_id, "error": f"{type(exc).__name__}: {exc}"})
                continue

            run_status = str(evidence.get("status", ""))
            if not include_failed and run_status != "SUCCESS":
                skipped_status += 1
                continue

            try:
                self.index_run(run_id)
                indexed_run_ids.append(run_id)
            except Exception as exc:
                errors.append({"run_id": run_id, "error": f"{type(exc).__name__}: {exc}"})

        return {
            "status": "ok",
            "total_candidates": len(run_dirs),
            "indexed_count": len(indexed_run_ids),
            "skipped_missing_count": skipped_missing,
            "skipped_status_count": skipped_status,
            "error_count": len(errors),
            "indexed_run_ids": indexed_run_ids,
            "errors": errors[:20],
        }

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
            memo_text=self._compose_memo(
                task={
                    "task_id": meta.get("task_id", ""),
                    "objective": meta.get("objective", ""),
                    "domain": meta.get("domain", evidence["domain"]),
                },
                summary_payload=self._load_run_summary(run_id),
                evidence_bundle=evidence,
                research_bundle=self._load_run_research_bundle(run_id),
            ),
        )
        return {"status": "ok", "run_id": run_id, "indexed": True}

    def queue_submit_from_file(
        self,
        task_file: Path,
        *,
        dry_run: bool = False,
        max_attempts: int = 1,
    ) -> dict[str, Any]:
        task = load_json_file(task_file)
        validate_task_request(task)
        return self.queue_submit(task, dry_run=dry_run, max_attempts=max_attempts)

    def queue_submit(
        self,
        task: dict[str, Any],
        *,
        dry_run: bool = False,
        max_attempts: int = 1,
    ) -> dict[str, Any]:
        validate_task_request(task)
        mode = "dry_run" if dry_run else "run"
        record = self.queue.submit_job(
            task=task,
            mode=mode,
            max_attempts=max(1, min(int(max_attempts), 20)),
        )
        return {"status": "queued", "job": record}

    def queue_list(self, *, limit: int = 20, status: str | None = None) -> dict[str, Any]:
        rows = self.queue.list_jobs(limit=limit, status=status)
        return {"status": "ok", "count": len(rows), "jobs": rows}

    def queue_get(self, job_id: str) -> dict[str, Any]:
        return {"status": "ok", "job": self.queue.get_job(job_id)}

    def queue_stats(self) -> dict[str, Any]:
        return {"status": "ok", "stats": self.queue.stats()}

    def queue_work_once(self, *, worker_id: str | None = None) -> dict[str, Any]:
        wid = worker_id or f"worker_{uuid4().hex[:8]}"
        job = self.queue.claim_next_job(wid)
        if job is None:
            return {"status": "idle", "message": "No queued jobs.", "worker_id": wid}

        mode = str(job.get("mode", "run"))
        dry_run = mode == "dry_run"
        task = job.get("task")
        if not isinstance(task, dict):
            task = json.loads(job.get("task_json", "{}"))

        try:
            result = self.run(task, dry_run=dry_run)
            run_status = str(result.get("status", ""))
            run_id = result.get("run_id")
            if run_status == "failed":
                error = self._extract_run_error(result)
                updated = self.queue.fail_job(job_id=job["job_id"], error=error, result_payload=result)
                return {
                    "status": "job_failed",
                    "job": updated,
                    "result": result,
                    "requeued": updated.get("status") == "QUEUED",
                }

            updated = self.queue.complete_job(
                job_id=job["job_id"],
                run_id=str(run_id) if run_id else "",
                result_payload=result,
            )
            return {"status": "job_completed", "job": updated, "result": result}
        except Exception as exc:  # pragma: no cover
            fallback = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
            updated = self.queue.fail_job(
                job_id=job["job_id"],
                error=f"{type(exc).__name__}: {exc}",
                result_payload=fallback,
            )
            return {
                "status": "job_failed",
                "job": updated,
                "result": fallback,
                "requeued": updated.get("status") == "QUEUED",
            }

    def queue_work(self, *, max_jobs: int = 10, worker_id: str | None = None) -> dict[str, Any]:
        max_jobs = max(1, min(int(max_jobs), 100))
        wid = worker_id or f"worker_{uuid4().hex[:8]}"
        processed = 0
        outputs: list[dict[str, Any]] = []
        while processed < max_jobs:
            out = self.queue_work_once(worker_id=wid)
            outputs.append(out)
            if out.get("status") == "idle":
                break
            processed += 1
        return {
            "status": "ok",
            "worker_id": wid,
            "processed": processed,
            "results": outputs,
        }

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

    def _load_run_summary(self, run_id: str) -> dict[str, Any]:
        path = self.store.run_path(run_id) / "summary.json"
        if not path.exists():
            return {"headline": "", "key_metrics": {}, "caveats": []}
        return load_json_file(path)

    def _load_run_meta(self, run_id: str) -> dict[str, Any]:
        path = self.store.run_path(run_id) / "meta.json"
        if not path.exists():
            return {}
        return load_json_file(path)

    def _load_run_params(self, run_id: str) -> dict[str, Any]:
        path = self.store.run_path(run_id) / "params.json"
        if not path.exists():
            return {}
        payload = load_json_file(path)
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _trace_event(trace: dict[str, Any], stage: str, details: dict[str, Any] | None = None) -> None:
        trace.setdefault("events", []).append(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "details": details or {},
            }
        )

    @staticmethod
    def _extract_run_error(result: dict[str, Any]) -> str:
        evidence = result.get("evidence_bundle", {})
        if isinstance(evidence, dict):
            logs = evidence.get("logs", {})
            if isinstance(logs, dict):
                err = str(logs.get("stderr", "")).strip()
                if err:
                    return err
        err_fallback = str(result.get("error", "")).strip()
        return err_fallback or "Run failed."

    @staticmethod
    def _compose_memo(
        *,
        task: dict[str, Any],
        summary_payload: dict[str, Any],
        evidence_bundle: dict[str, Any],
        research_bundle: dict[str, Any],
    ) -> str:
        task_id = str(task.get("task_id", ""))
        domain = str(task.get("domain", ""))
        objective = str(task.get("objective", ""))
        headline = str(summary_payload.get("headline", ""))

        key_metrics = summary_payload.get("key_metrics", {})
        if not isinstance(key_metrics, dict):
            key_metrics = {}
        metrics = evidence_bundle.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        caveats = summary_payload.get("caveats", [])
        if not isinstance(caveats, list):
            caveats = []

        metric_pairs = ", ".join(f"{k}={metrics[k]}" for k in sorted(metrics.keys()))
        key_metric_pairs = ", ".join(f"{k}={key_metrics[k]}" for k in sorted(key_metrics.keys()))
        caveat_text = " | ".join(str(item) for item in caveats if isinstance(item, str))
        source_count = int(research_bundle.get("source_count", 0))

        parts = [
            f"task_id={task_id}",
            f"domain={domain}",
            f"objective={objective}",
            f"headline={headline}",
            f"metrics={metric_pairs}",
            f"key_metrics={key_metric_pairs}",
            f"research_sources={source_count}",
            f"caveats={caveat_text}",
        ]
        return "; ".join(part for part in parts if part)

    @staticmethod
    def _summarize_trace(trace: dict[str, Any]) -> dict[str, Any]:
        events = trace.get("events", [])
        if not isinstance(events, list):
            events = []
        stages = [str(item.get("stage", "")) for item in events if isinstance(item, dict)]
        parsed: list[tuple[datetime, dict[str, Any]]] = []
        for item in events:
            if not isinstance(item, dict):
                continue
            stamp = item.get("timestamp_utc")
            if not isinstance(stamp, str):
                continue
            dt = _parse_iso_utc(stamp)
            if dt is None:
                continue
            parsed.append((dt, item))

        transition_durations: list[dict[str, Any]] = []
        stage_totals: dict[str, float] = {}
        for idx in range(len(parsed) - 1):
            dt1, ev1 = parsed[idx]
            dt2, ev2 = parsed[idx + 1]
            delta = max(0.0, (dt2 - dt1).total_seconds())
            from_stage = str(ev1.get("stage", ""))
            to_stage = str(ev2.get("stage", ""))
            transition_durations.append(
                {
                    "from_stage": from_stage,
                    "to_stage": to_stage,
                    "duration_sec": round(delta, 6),
                }
            )
            stage_totals[from_stage] = stage_totals.get(from_stage, 0.0) + delta

        total_span_sec = 0.0
        if len(parsed) >= 2:
            total_span_sec = max(0.0, (parsed[-1][0] - parsed[0][0]).total_seconds())

        sorted_stage_totals = sorted(stage_totals.items(), key=lambda x: (-x[1], x[0]))
        slowest_stages = [
            {"stage": stage, "duration_sec": round(duration, 6)}
            for stage, duration in sorted_stage_totals[:5]
        ]
        return {
            "event_count": len(events),
            "stages": stages,
            "total_span_sec": round(total_span_sec, 6),
            "transitions": transition_durations,
            "slowest_stages": slowest_stages,
        }


def _is_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _parse_iso_utc(value: str) -> datetime | None:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        out = datetime.fromisoformat(text)
    except ValueError:
        return None
    if out.tzinfo is None:
        return out.replace(tzinfo=timezone.utc)
    return out


def _normalize_artifact_path(path: str, run_id: str) -> str:
    marker = f"data/runs/{run_id}/"
    norm = path.replace("\\", "/")
    if norm.startswith(marker):
        return norm[len(marker) :]
    return norm


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
