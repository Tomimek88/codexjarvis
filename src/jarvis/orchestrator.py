from __future__ import annotations

import json
import platform
import shutil
import time
import zipfile
from datetime import datetime, timezone
from html import escape
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

    def run_quick(
        self,
        *,
        objective: str,
        domain: str = "generic",
        parameters: dict[str, Any] | None = None,
        task_id: str | None = None,
        force_rerun: bool = False,
        acceptance_criteria: list[str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        task = self._build_quick_task(
            objective=objective,
            domain=domain,
            parameters=parameters,
            task_id=task_id,
            force_rerun=force_rerun,
            acceptance_criteria=acceptance_criteria,
        )
        return self.run(task, dry_run=dry_run)

    def mission(
        self,
        *,
        objective: str,
        domain: str = "generic",
        parameters: dict[str, Any] | None = None,
        task_id: str | None = None,
        force_rerun: bool = False,
        acceptance_criteria: list[str] | None = None,
        dry_run: bool = False,
        generate_report: bool = True,
        generate_dashboard: bool = True,
        dashboard_limit: int = 50,
    ) -> dict[str, Any]:
        run_out = self.run_quick(
            objective=objective,
            domain=domain,
            parameters=parameters,
            task_id=task_id,
            force_rerun=force_rerun,
            acceptance_criteria=acceptance_criteria,
            dry_run=dry_run,
        )
        run_status = str(run_out.get("status", ""))
        run_id = str(run_out.get("run_id", "") or "")
        cache_key = str(run_out.get("cache_key", "") or "")
        warnings: list[str] = []

        report_payload: dict[str, Any] | None = None
        if generate_report and run_id:
            try:
                report_payload = self.report_run(run_id)
            except Exception as exc:
                report_payload = {
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
                warnings.append("report_generation_failed")

        dashboard_payload: dict[str, Any] | None = None
        if generate_dashboard:
            try:
                dashboard_payload = self.runs_dashboard(
                    limit=dashboard_limit,
                    domain=domain,
                    include_failed=True,
                )
            except Exception as exc:
                dashboard_payload = {
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
                warnings.append("dashboard_generation_failed")

        return {
            "status": "ok",
            "mission_status": run_status,
            "task_id": str(run_out.get("task_id", "")),
            "run_id": run_id,
            "cache_key": cache_key,
            "report": report_payload,
            "dashboard": dashboard_payload,
            "warnings": warnings,
        }

    def task_validate(self, task_file: Path) -> dict[str, Any]:
        path = task_file.resolve()
        task = load_json_file(path)
        validate_task_request(task)
        return {
            "status": "ok",
            "task_file": _as_project_relative(path, self.project_root),
            "valid": True,
            "task_id": str(task.get("task_id", "")),
            "domain": str(task.get("domain", "")),
        }

    def task_validate_dir(
        self,
        tasks_dir: Path,
        *,
        pattern: str = "*.json",
        recursive: bool = True,
        max_tasks: int = 0,
        stop_on_error: bool = False,
    ) -> dict[str, Any]:
        directory = tasks_dir.resolve()
        if not directory.exists() or not directory.is_dir():
            raise ValidationError(f"tasks_dir '{directory}' does not exist or is not a directory.")

        safe_pattern = pattern.strip() if isinstance(pattern, str) and pattern.strip() else "*.json"
        discovered = list(directory.rglob(safe_pattern)) if recursive else list(directory.glob(safe_pattern))
        files = sorted([path for path in discovered if path.is_file()], key=lambda p: str(p).lower())
        discovered_count = len(files)
        if max_tasks > 0:
            files = files[: max(1, min(int(max_tasks), 10000))]

        results: list[dict[str, Any]] = []
        valid_count = 0
        invalid_count = 0
        stopped_early = False

        for path in files:
            rel_path = _as_project_relative(path, self.project_root)
            try:
                task = load_json_file(path)
                validate_task_request(task)
                valid_count += 1
                results.append(
                    {
                        "task_file": rel_path,
                        "valid": True,
                        "task_id": str(task.get("task_id", "")),
                        "domain": str(task.get("domain", "")),
                        "error": "",
                    }
                )
            except Exception as exc:
                invalid_count += 1
                results.append(
                    {
                        "task_file": rel_path,
                        "valid": False,
                        "task_id": "",
                        "domain": "",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                if stop_on_error:
                    stopped_early = True
                    break

        processed_count = len(results)
        selected_count = len(files)
        remaining_count = max(0, selected_count - processed_count)
        return {
            "status": "ok",
            "discovered_count": discovered_count,
            "selected_count": selected_count,
            "processed_count": processed_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "stopped_early": stopped_early,
            "remaining_count": remaining_count,
            "pattern": safe_pattern,
            "recursive": bool(recursive),
            "results": results,
        }

    def batch_run(
        self,
        tasks_dir: Path,
        *,
        pattern: str = "*.json",
        dry_run: bool = False,
        max_tasks: int = 0,
        recursive: bool = True,
        continue_on_error: bool = True,
    ) -> dict[str, Any]:
        directory = tasks_dir.resolve()
        if not directory.exists() or not directory.is_dir():
            raise ValidationError(f"tasks_dir '{directory}' does not exist or is not a directory.")

        safe_pattern = pattern.strip() if isinstance(pattern, str) and pattern.strip() else "*.json"
        discovered = list(directory.rglob(safe_pattern)) if recursive else list(directory.glob(safe_pattern))
        files = sorted([path for path in discovered if path.is_file()], key=lambda p: str(p).lower())
        discovered_count = len(files)

        if max_tasks > 0:
            files = files[: max(1, min(int(max_tasks), 10000))]
        selected_count = len(files)

        results: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0
        processed = 0
        stopped_early = False

        for task_path in files:
            t0 = datetime.now(timezone.utc)
            rel_path = _as_project_relative(task_path, self.project_root)

            try:
                payload = self.run_from_file(task_path, dry_run=dry_run)
                run_status = str(payload.get("status", ""))
                ok = run_status not in {"failed", "error"}
                if ok:
                    succeeded += 1
                else:
                    failed += 1
                result_item = {
                    "task_file": str(rel_path),
                    "ok": ok,
                    "status": run_status,
                    "run_id": str(payload.get("run_id", "")),
                    "cache_key": str(payload.get("cache_key", "")),
                    "duration_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 6),
                }
            except Exception as exc:
                failed += 1
                result_item = {
                    "task_file": str(rel_path),
                    "ok": False,
                    "status": "error",
                    "run_id": "",
                    "cache_key": "",
                    "duration_sec": round((datetime.now(timezone.utc) - t0).total_seconds(), 6),
                    "error": f"{type(exc).__name__}: {exc}",
                }

            processed += 1
            results.append(result_item)

            if (not result_item["ok"]) and (not continue_on_error):
                stopped_early = True
                break

        remaining_count = max(0, selected_count - processed)
        return {
            "status": "ok",
            "discovered_count": discovered_count,
            "selected_count": selected_count,
            "processed_count": processed,
            "succeeded_count": succeeded,
            "failed_count": failed,
            "stopped_early": stopped_early,
            "remaining_count": remaining_count,
            "dry_run": bool(dry_run),
            "pattern": safe_pattern,
            "recursive": bool(recursive),
            "results": results,
        }

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

    def report_run(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")

        inspect_payload = self.inspect(run_id)
        evidence = self.store.load_evidence(run_id)
        metrics = evidence.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}

        reports_dir = run_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = reports_dir / f"run_report_{stamp}.json"
        md_path = reports_dir / f"run_report_{stamp}.md"

        report_json = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "inspect": inspect_payload,
            "metrics": metrics,
        }
        with json_path.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(report_json, f, indent=2, ensure_ascii=True)
            f.write("\n")

        md = self._build_markdown_run_report(
            run_id=run_id,
            inspect_payload=inspect_payload,
            metrics=metrics,
        )
        with md_path.open("w", encoding="utf-8", newline="\n") as f:
            f.write(md)

        return {
            "status": "ok",
            "run_id": run_id,
            "report_json_path": str(json_path.relative_to(self.project_root).as_posix()),
            "report_md_path": str(md_path.relative_to(self.project_root).as_posix()),
        }

    def runs_dashboard(
        self,
        *,
        limit: int = 100,
        domain: str | None = None,
        include_failed: bool = True,
        output_file: Path | None = None,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        safe_limit = max(1, min(int(limit), 2000))
        status_filter = None if include_failed else "SUCCESS"

        listed = self.runs_list(limit=safe_limit, status=status_filter, domain=domain, contains=None)
        rows = listed.get("runs", [])
        if not isinstance(rows, list):
            rows = []
        stats = self.runs_stats(limit=safe_limit, domain=domain)
        generated_at = datetime.now(timezone.utc).isoformat()

        root_resolved = self.project_root.resolve()
        if output_file is not None:
            dashboard_path = output_file.resolve()
            if not _is_within_root(dashboard_path, root_resolved):
                raise ValidationError("output_file must be within project root.")
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            dashboard_path = (self.project_root / "data" / "reports" / f"runs_dashboard_{stamp}.html").resolve()
        dashboard_path.parent.mkdir(parents=True, exist_ok=True)

        html = self._build_runs_dashboard_html(
            generated_at_utc=generated_at,
            rows=rows,
            stats=stats,
            scope={
                "limit": safe_limit,
                "domain": (domain or "").strip().lower(),
                "include_failed": bool(include_failed),
            },
        )
        with dashboard_path.open("w", encoding="utf-8", newline="\n") as f:
            f.write(html)

        return {
            "status": "ok",
            "generated_at_utc": generated_at,
            "dashboard_path": _as_project_relative(dashboard_path, self.project_root),
            "run_count": len(rows),
            "scope": {
                "limit": safe_limit,
                "domain": (domain or "").strip().lower(),
                "include_failed": bool(include_failed),
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

    def runs_list(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        domain: str | None = None,
        contains: str | None = None,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        safe_limit = max(1, min(int(limit), 500))
        rows: list[dict[str, Any]] = []
        status_filter = status.strip().upper() if isinstance(status, str) and status.strip() else None
        domain_filter = domain.strip().lower() if isinstance(domain, str) and domain.strip() else None
        contains_filter = contains.strip().lower() if isinstance(contains, str) and contains.strip() else None

        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        for run_dir in run_dirs:
            run_id = run_dir.name
            meta_path = run_dir / "meta.json"
            evidence_path = run_dir / "evidence_bundle.json"
            if not meta_path.exists() or not evidence_path.exists():
                continue

            try:
                meta = load_json_file(meta_path)
                evidence = load_json_file(evidence_path)
            except Exception:
                continue

            row_status = str(evidence.get("status", meta.get("status", ""))).upper()
            row_domain = str(meta.get("domain", evidence.get("domain", ""))).lower()
            task_id = str(meta.get("task_id", ""))
            objective = str(meta.get("objective", ""))

            if status_filter and row_status != status_filter:
                continue
            if domain_filter and row_domain != domain_filter:
                continue
            if contains_filter:
                hay = " ".join([run_id, task_id, objective, row_domain]).lower()
                if contains_filter not in hay:
                    continue

            row = {
                "run_id": run_id,
                "task_id": task_id,
                "domain": row_domain,
                "objective": objective,
                "status": row_status,
                "timestamp_utc": str(meta.get("timestamp_utc", evidence.get("timestamp_utc", ""))),
                "cache_key": str(meta.get("cache_key", "")),
                "research_source_count": int(meta.get("research_source_count", 0) or 0),
            }
            rows.append(row)
            if len(rows) >= safe_limit:
                break

        return {
            "status": "ok",
            "count": len(rows),
            "runs": rows,
        }

    def runs_stats(
        self,
        *,
        limit: int = 0,
        domain: str | None = None,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        domain_filter = domain.strip().lower() if isinstance(domain, str) and domain.strip() else None
        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        if limit > 0:
            safe_limit = max(1, min(int(limit), 5000))
            run_dirs = run_dirs[:safe_limit]

        counts_by_status: dict[str, int] = {}
        counts_by_domain: dict[str, int] = {}
        latest_timestamp = ""
        earliest_timestamp = ""
        total_scanned = 0

        for run_dir in run_dirs:
            meta_path = run_dir / "meta.json"
            evidence_path = run_dir / "evidence_bundle.json"
            if not meta_path.exists() or not evidence_path.exists():
                continue

            try:
                meta = load_json_file(meta_path)
                evidence = load_json_file(evidence_path)
            except Exception:
                continue

            row_domain = str(meta.get("domain", evidence.get("domain", ""))).lower()
            if domain_filter and row_domain != domain_filter:
                continue

            row_status = str(evidence.get("status", meta.get("status", ""))).upper()
            timestamp = str(meta.get("timestamp_utc", evidence.get("timestamp_utc", "")))
            total_scanned += 1

            counts_by_status[row_status] = counts_by_status.get(row_status, 0) + 1
            counts_by_domain[row_domain] = counts_by_domain.get(row_domain, 0) + 1

            if timestamp:
                if not latest_timestamp or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                if not earliest_timestamp or timestamp < earliest_timestamp:
                    earliest_timestamp = timestamp

        success_count = counts_by_status.get("SUCCESS", 0)
        failed_count = counts_by_status.get("FAILED", 0)
        finished = success_count + failed_count
        success_rate_finished = (float(success_count) / float(finished)) if finished > 0 else 0.0

        return {
            "status": "ok",
            "total_runs": total_scanned,
            "counts_by_status": counts_by_status,
            "counts_by_domain": counts_by_domain,
            "success_count": success_count,
            "failed_count": failed_count,
            "success_rate_finished": round(success_rate_finished, 6),
            "latest_timestamp_utc": latest_timestamp,
            "earliest_timestamp_utc": earliest_timestamp,
            "scope": {
                "limit": limit,
                "domain": domain_filter or "",
            },
        }

    def runs_migrate_legacy(
        self,
        *,
        limit: int = 0,
        write_execution_manifest: bool = True,
        write_trace: bool = True,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        if limit > 0:
            run_dirs = run_dirs[: max(1, min(int(limit), 100000))]

        migrated = 0
        touched_execution = 0
        touched_trace = 0
        skipped = 0
        errors: list[dict[str, str]] = []

        for run_dir in run_dirs:
            run_id = run_dir.name
            meta_path = run_dir / "meta.json"
            evidence_path = run_dir / "evidence_bundle.json"
            if not meta_path.exists() or not evidence_path.exists():
                skipped += 1
                continue

            try:
                meta = load_json_file(meta_path)
                evidence = load_json_file(evidence_path)
                validate_evidence_bundle(evidence)
            except Exception as exc:
                skipped += 1
                errors.append({"run_id": run_id, "error": f"{type(exc).__name__}: {exc}"})
                continue

            did_touch = False
            execution_path = run_dir / "execution_manifest.json"
            if write_execution_manifest and not execution_path.exists():
                default_execution = _build_legacy_execution_manifest(meta=meta, evidence=evidence)
                _write_json_file(execution_path, default_execution)
                touched_execution += 1
                did_touch = True

            trace_path = run_dir / "trace.json"
            if write_trace and not trace_path.exists():
                default_trace = _build_legacy_trace(run_id=run_id, meta=meta, evidence=evidence)
                _write_json_file(trace_path, default_trace)
                touched_trace += 1
                did_touch = True

            if did_touch:
                migrated += 1

        return {
            "status": "ok",
            "scanned_count": len(run_dirs),
            "migrated_runs": migrated,
            "execution_manifest_written": touched_execution,
            "trace_written": touched_trace,
            "skipped_count": skipped,
            "error_count": len(errors),
            "errors": errors[:100],
        }

    def cache_verify(self, *, limit: int = 0) -> dict[str, Any]:
        self.store.ensure_layout()
        index = self.store.load_cache_index()
        entries = index.get("entries", {})
        if not isinstance(entries, dict):
            return {
                "status": "ok",
                "valid_count": 0,
                "invalid_count": 1,
                "issue_count": 1,
                "issues": [
                    {
                        "code": "invalid_cache_index",
                        "message": "cache_index.entries is not an object",
                    }
                ],
            }

        items = list(entries.items())
        if limit > 0:
            items = items[: max(1, min(int(limit), 50000))]

        valid_count = 0
        invalid_count = 0
        issues: list[dict[str, Any]] = []

        for cache_key, payload in items:
            if not isinstance(cache_key, str) or len(cache_key.strip()) == 0:
                invalid_count += 1
                issues.append(
                    {
                        "code": "invalid_cache_key",
                        "cache_key": str(cache_key),
                        "message": "Cache key is missing or not a valid string.",
                    }
                )
                continue

            if not isinstance(payload, dict):
                invalid_count += 1
                issues.append(
                    {
                        "code": "invalid_cache_entry_payload",
                        "cache_key": cache_key,
                        "message": "Cache entry payload is not an object.",
                    }
                )
                continue

            run_id = payload.get("run_id")
            if not isinstance(run_id, str) or len(run_id.strip()) == 0:
                invalid_count += 1
                issues.append(
                    {
                        "code": "missing_run_id",
                        "cache_key": cache_key,
                        "message": "Cache entry has missing or invalid run_id.",
                    }
                )
                continue

            run_dir = self.store.run_path(run_id)
            if not run_dir.exists():
                invalid_count += 1
                issues.append(
                    {
                        "code": "missing_run_dir",
                        "cache_key": cache_key,
                        "run_id": run_id,
                        "message": "Referenced run directory does not exist.",
                    }
                )
                continue

            meta_path = run_dir / "meta.json"
            if not meta_path.exists():
                invalid_count += 1
                issues.append(
                    {
                        "code": "missing_meta",
                        "cache_key": cache_key,
                        "run_id": run_id,
                        "message": "Referenced run has no meta.json.",
                    }
                )
                continue

            try:
                meta = load_json_file(meta_path)
            except Exception as exc:
                invalid_count += 1
                issues.append(
                    {
                        "code": "meta_load_failed",
                        "cache_key": cache_key,
                        "run_id": run_id,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue

            meta_cache_key = str(meta.get("cache_key", ""))
            if meta_cache_key != cache_key:
                invalid_count += 1
                issues.append(
                    {
                        "code": "cache_key_mismatch",
                        "cache_key": cache_key,
                        "run_id": run_id,
                        "meta_cache_key": meta_cache_key,
                        "message": "Cache entry key differs from run meta cache_key.",
                    }
                )
                continue

            valid_count += 1

        return {
            "status": "ok",
            "scanned_count": len(items),
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "issue_count": len(issues),
            "issues": issues[:200],
        }

    def cache_rebuild(
        self,
        *,
        limit: int = 0,
        include_failed: bool = False,
    ) -> dict[str, Any]:
        self.store.ensure_layout()
        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        if limit > 0:
            run_dirs = run_dirs[: max(1, min(int(limit), 50000))]

        entries: dict[str, dict[str, str]] = {}
        processed = 0
        skipped = 0
        duplicates = 0
        for run_dir in run_dirs:
            meta_path = run_dir / "meta.json"
            evidence_path = run_dir / "evidence_bundle.json"
            if not meta_path.exists() or not evidence_path.exists():
                skipped += 1
                continue

            try:
                meta = load_json_file(meta_path)
                evidence = load_json_file(evidence_path)
            except Exception:
                skipped += 1
                continue

            run_status = str(evidence.get("status", meta.get("status", ""))).upper()
            if not include_failed and run_status != "SUCCESS":
                skipped += 1
                continue

            cache_key = str(meta.get("cache_key", "")).strip()
            run_id = str(meta.get("run_id", run_dir.name)).strip()
            if not cache_key or not run_id:
                skipped += 1
                continue

            processed += 1
            if cache_key in entries:
                duplicates += 1
            entries[cache_key] = {
                "run_id": run_id,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            }

        payload = {"entries": entries}
        self.store.save_cache_index(payload)
        return {
            "status": "ok",
            "scanned_runs": len(run_dirs),
            "processed_runs": processed,
            "skipped_runs": skipped,
            "duplicate_cache_keys": duplicates,
            "rebuilt_entry_count": len(entries),
        }

    def doctor(
        self,
        *,
        fix: bool = False,
        queue_prune: bool = False,
        queue_prune_limit: int = 200,
        queue_prune_older_than_sec: int = 86400,
        queue_prune_delete_results: bool = False,
        queue_clean_results: bool = False,
        queue_clean_results_limit: int = 0,
        memory_clean: bool = False,
        memory_clean_limit: int = 0,
    ) -> dict[str, Any]:
        snapshot_before = self._collect_doctor_snapshot()
        warnings_before = self._build_doctor_warnings(snapshot_before)
        fix_actions: list[dict[str, Any]] = []

        if fix:
            migrated = self.runs_migrate_legacy(limit=0, write_execution_manifest=True, write_trace=True)
            fix_actions.append(
                {
                    "action": "runs_migrate_legacy",
                    "result": {
                        "migrated_runs": int(migrated.get("migrated_runs", 0)),
                        "execution_manifest_written": int(migrated.get("execution_manifest_written", 0)),
                        "trace_written": int(migrated.get("trace_written", 0)),
                        "error_count": int(migrated.get("error_count", 0)),
                    },
                }
            )
            if "run_integrity_failures_present" in warnings_before:
                repaired = self._repair_runtime_artifact_hashes(limit=0)
                fix_actions.append(
                    {
                        "action": "repair_runtime_artifact_hashes",
                        "result": {
                            "scanned_runs": int(repaired.get("scanned_runs", 0)),
                            "touched_runs": int(repaired.get("touched_runs", 0)),
                            "updated_artifacts": int(repaired.get("updated_artifacts", 0)),
                            "error_count": int(repaired.get("error_count", 0)),
                        },
                    }
                )

            if int(snapshot_before["cache_verify"].get("invalid_count", 0)) > 0:
                rebuilt = self.cache_rebuild(limit=0, include_failed=False)
                fix_actions.append(
                    {
                        "action": "cache_rebuild",
                        "result": {
                            "processed_runs": int(rebuilt.get("processed_runs", 0)),
                            "skipped_runs": int(rebuilt.get("skipped_runs", 0)),
                            "rebuilt_entry_count": int(rebuilt.get("rebuilt_entry_count", 0)),
                            "duplicate_cache_keys": int(rebuilt.get("duplicate_cache_keys", 0)),
                        },
                    }
                )

            stale_before = snapshot_before.get("queue_stale_running", {})
            stale_count_before = int(stale_before.get("stale_count", 0))
            stale_age_sec = int(stale_before.get("max_age_sec", 600))
            if stale_count_before > 0:
                recovered_running = self.queue_recover_running(
                    limit=0,
                    max_age_sec=stale_age_sec,
                    force_requeue=False,
                    reset_attempts=False,
                )
                fix_actions.append(
                    {
                        "action": "queue_recover_running",
                        "result": {
                            "stale_count": int(recovered_running.get("stale_count", 0)),
                            "recovered_count": int(recovered_running.get("recovered_count", 0)),
                            "marked_failed_count": int(recovered_running.get("marked_failed_count", 0)),
                            "max_age_sec": int(recovered_running.get("max_age_sec", 0)),
                        },
                    }
                )

            current_queue = self.queue_stats()
            current_queue_stats = current_queue.get("stats", {}) if isinstance(current_queue, dict) else {}
            dead_failed_count = int(current_queue_stats.get("dead_failed_count", 0))
            if dead_failed_count > 0:
                requeued = self.queue_requeue_failed(limit=dead_failed_count, reset_attempts=True)
                fix_actions.append(
                    {
                        "action": "queue_requeue_failed",
                        "result": {
                            "requeued_count": int(requeued.get("requeued_count", 0)),
                            "requested_limit": int(requeued.get("requested_limit", 0)),
                        },
                    }
                )
            if queue_prune:
                pruned = self.queue_prune(
                    limit=queue_prune_limit,
                    statuses=["SUCCESS", "FAILED", "CANCELLED"],
                    older_than_sec=queue_prune_older_than_sec,
                    delete_results=queue_prune_delete_results,
                    dry_run=False,
                )
                fix_actions.append(
                    {
                        "action": "queue_prune",
                        "result": {
                            "pruned_count": int(pruned.get("pruned_count", 0)),
                            "matched_count": int(pruned.get("matched_count", 0)),
                            "requested_limit": int(pruned.get("requested_limit", 0)),
                            "older_than_sec": int(pruned.get("older_than_sec", 0)),
                            "result_files_deleted": int(pruned.get("result_files_deleted", 0)),
                        },
                    }
                )
            if queue_clean_results:
                cleaned = self.queue_clean_results(limit=queue_clean_results_limit, dry_run=False)
                fix_actions.append(
                    {
                        "action": "queue_clean_results",
                        "result": {
                            "requested_limit": int(cleaned.get("requested_limit", 0)),
                            "orphan_count": int(cleaned.get("orphan_count", 0)),
                            "deleted_count": int(cleaned.get("deleted_count", 0)),
                        },
                    }
                )
            if memory_clean:
                cleaned_memory = self.memory_clean(limit=memory_clean_limit, dry_run=False)
                fix_actions.append(
                    {
                        "action": "memory_clean",
                        "result": {
                            "requested_limit": int(cleaned_memory.get("requested_limit", 0)),
                            "stale_count": int(cleaned_memory.get("stale_count", 0)),
                            "deleted_count": int(cleaned_memory.get("deleted_count", 0)),
                        },
                    }
                )
            else:
                stale_memory = snapshot_before.get("memory_audit", {})
                stale_memory_count = int(stale_memory.get("stale_count", 0))
                if stale_memory_count > 0:
                    cleaned_memory = self.memory_clean(limit=memory_clean_limit, dry_run=False)
                    fix_actions.append(
                        {
                            "action": "memory_clean",
                            "result": {
                                "requested_limit": int(cleaned_memory.get("requested_limit", 0)),
                                "stale_count": int(cleaned_memory.get("stale_count", 0)),
                                "deleted_count": int(cleaned_memory.get("deleted_count", 0)),
                            },
                        }
                    )

        snapshot = self._collect_doctor_snapshot()
        warnings = self._build_doctor_warnings(snapshot)
        overall = "ok" if len(warnings) == 0 else "warning"

        payload = {
            "status": "ok",
            "overall": overall,
            "warning_count": len(warnings),
            "warnings": warnings,
            "health": snapshot["health"],
            "cache_verify": snapshot["cache_verify"],
            "queue_stats": snapshot["queue_stats"],
            "queue_stale_running": snapshot["queue_stale_running"],
            "queue_orphan_results": snapshot["queue_orphan_results"],
            "memory_audit": snapshot["memory_audit"],
            "runs_stats": snapshot["runs_stats"],
            "audit_summary": snapshot["audit_summary"],
        }
        if fix:
            payload["fix_requested"] = True
            payload["fix_actions"] = fix_actions
            payload["pre_fix_warning_count"] = len(warnings_before)
            payload["pre_fix_warnings"] = warnings_before
            payload["post_fix_warning_count"] = len(warnings)
        return payload

    def _collect_doctor_snapshot(self) -> dict[str, Any]:
        health = self.health()
        cache = self.cache_verify(limit=200)
        queue_payload = self.queue_stats()
        queue_stats = queue_payload.get("stats", {}) if isinstance(queue_payload, dict) else {}
        queue_stale_running = self.queue_stale_running(limit=200, max_age_sec=600)
        queue_orphan_results = self.queue_orphan_results(limit=500)
        memory_audit = self.memory_audit(limit=500)
        runs = self.runs_stats(limit=200)
        audit = self.audit_all(limit=50, include_passed=False)
        return {
            "health": health,
            "cache_verify": cache,
            "queue_stats": queue_stats,
            "queue_stale_running": queue_stale_running,
            "queue_orphan_results": queue_orphan_results,
            "memory_audit": memory_audit,
            "runs_stats": runs,
            "audit_summary": {
                "scanned_count": int(audit.get("scanned_count", 0)),
                "failed_count": int(audit.get("failed_count", 0)),
                "error_count": int(audit.get("error_count", 0)),
            },
        }

    def _repair_runtime_artifact_hashes(self, *, limit: int = 0) -> dict[str, Any]:
        self.store.ensure_layout()
        run_dirs = [path for path in self.store.runs_dir.iterdir() if path.is_dir()]
        run_dirs = sorted(run_dirs, key=lambda path: path.name, reverse=True)
        if limit > 0:
            run_dirs = run_dirs[: max(1, min(int(limit), 100000))]

        runtime_suffixes = ("/execution_manifest.json", "/trace.json")
        touched_runs = 0
        updated_artifacts = 0
        errors: list[dict[str, str]] = []
        root_resolved = self.project_root.resolve()

        for run_dir in run_dirs:
            run_id = run_dir.name
            evidence_path = run_dir / "evidence_bundle.json"
            if not evidence_path.exists():
                continue

            try:
                evidence = load_json_file(evidence_path)
            except Exception as exc:
                errors.append({"run_id": run_id, "error": f"{type(exc).__name__}: {exc}"})
                continue

            artifacts = evidence.get("artifacts", [])
            if not isinstance(artifacts, list):
                continue

            run_changed = False
            for artifact in artifacts:
                if not isinstance(artifact, dict):
                    continue
                rel_path = str(artifact.get("path", "")).replace("\\", "/")
                if not any(rel_path.endswith(suffix) for suffix in runtime_suffixes):
                    continue

                artifact_abs = (self.project_root / rel_path).resolve()
                if not _is_within_root(artifact_abs, root_resolved) or not artifact_abs.exists():
                    continue

                expected = str(artifact.get("sha256", ""))
                actual = sha256_file(artifact_abs)
                if expected != actual:
                    artifact["sha256"] = actual
                    run_changed = True
                    updated_artifacts += 1

            if run_changed:
                validate_evidence_bundle(evidence)
                _write_json_file(evidence_path, evidence)
                touched_runs += 1

        return {
            "status": "ok",
            "scanned_runs": len(run_dirs),
            "touched_runs": touched_runs,
            "updated_artifacts": updated_artifacts,
            "error_count": len(errors),
            "errors": errors[:100],
        }

    @staticmethod
    def _build_doctor_warnings(snapshot: dict[str, Any]) -> list[str]:
        warnings: list[str] = []
        health = snapshot.get("health", {})
        cache = snapshot.get("cache_verify", {})
        queue_stats = snapshot.get("queue_stats", {})
        queue_stale_running = snapshot.get("queue_stale_running", {})
        queue_orphan_results = snapshot.get("queue_orphan_results", {})
        memory_audit = snapshot.get("memory_audit", {})
        audit_summary = snapshot.get("audit_summary", {})
        if str(health.get("status", "")).lower() != "ok":
            warnings.append("runtime_health_degraded")
        if int(cache.get("invalid_count", 0)) > 0:
            warnings.append("cache_invalid_entries_present")
        if int(queue_stale_running.get("stale_count", 0)) > 0:
            warnings.append("queue_stale_running_jobs_present")
        if int(queue_orphan_results.get("orphan_count", 0)) > 0:
            warnings.append("queue_orphan_result_files_present")
        if int(memory_audit.get("stale_count", 0)) > 0:
            warnings.append("memory_stale_run_refs_present")
        if int(queue_stats.get("dead_failed_count", 0)) > 0:
            warnings.append("queue_dead_failed_jobs_present")
        if int(audit_summary.get("failed_count", 0)) > 0:
            warnings.append("run_integrity_failures_present")
        return warnings

    def export_run(self, run_id: str) -> dict[str, Any]:
        run_dir = self.store.run_path(run_id)
        if not run_dir.exists():
            raise ValidationError(f"Run '{run_id}' does not exist.")

        exports_dir = self.project_root / "data" / "exports"
        exports_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        zip_path = exports_dir / f"{run_id}_{stamp}.zip"

        files_exported = 0
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(run_dir.rglob("*"), key=lambda p: str(p).lower()):
                if not path.is_file():
                    continue
                arcname = str(path.relative_to(self.project_root).as_posix())
                zf.write(path, arcname=arcname)
                files_exported += 1

        return {
            "status": "ok",
            "run_id": run_id,
            "zip_path": str(zip_path.relative_to(self.project_root).as_posix()),
            "files_exported": files_exported,
            "size_bytes": int(zip_path.stat().st_size),
        }

    def import_run(
        self,
        zip_file: Path,
        *,
        index_memory: bool = True,
        link_cache: bool = True,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        zip_path = zip_file.resolve()
        if not zip_path.exists() or not zip_path.is_file():
            raise ValidationError(f"zip_file '{zip_path}' does not exist or is not a file.")

        with zipfile.ZipFile(zip_path, mode="r") as zf:
            members = [info.filename.replace("\\", "/") for info in zf.infolist() if not info.is_dir()]
            run_ids = _extract_run_ids_from_members(members)
            if len(run_ids) != 1:
                raise ValidationError(
                    f"ZIP must contain exactly one run under data/runs/<run_id>/, got {len(run_ids)}."
                )
            run_id = sorted(run_ids)[0]
            prefix = f"data/runs/{run_id}/"
            run_members = [member for member in members if member.startswith(prefix)]
            if len(run_members) == 0:
                raise ValidationError(f"No files found under '{prefix}' in ZIP.")

            run_dir = self.store.run_path(run_id)
            if run_dir.exists():
                if not overwrite:
                    raise ValidationError(
                        f"Run '{run_id}' already exists. Use overwrite=true to replace it."
                    )
                shutil.rmtree(run_dir)

            files_written = 0
            run_root = run_dir.resolve()
            for member in run_members:
                rel = member[len(prefix) :]
                if not rel:
                    continue
                rel_path = Path(rel)
                if any(part == ".." for part in rel_path.parts):
                    raise ValidationError(f"Unsafe ZIP member path: {member}")

                target = (run_dir / rel_path).resolve()
                if not _is_within_root(target, run_root):
                    raise ValidationError(f"ZIP member escapes run directory: {member}")

                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("wb") as f:
                    f.write(zf.read(member))
                files_written += 1

        evidence = load_json_file(run_dir / "evidence_bundle.json")
        validate_evidence_bundle(evidence)
        meta = load_json_file(run_dir / "meta.json")

        memory_indexed = False
        if index_memory:
            self.index_run(run_id)
            memory_indexed = True

        cache_linked = False
        if link_cache and str(evidence.get("status", "")).upper() == "SUCCESS":
            cache_key = str(meta.get("cache_key", "")).strip()
            if cache_key:
                self.store.set_cache_entry(cache_key, run_id)
                cache_linked = True

        return {
            "status": "ok",
            "run_id": run_id,
            "zip_path": str(zip_path),
            "files_imported": files_written,
            "memory_indexed": memory_indexed,
            "cache_linked": cache_linked,
            "overwrite": bool(overwrite),
        }

    def import_runs_dir(
        self,
        zips_dir: Path,
        *,
        pattern: str = "*.zip",
        recursive: bool = True,
        max_files: int = 0,
        continue_on_error: bool = True,
        index_memory: bool = True,
        link_cache: bool = True,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        directory = zips_dir.resolve()
        if not directory.exists() or not directory.is_dir():
            raise ValidationError(f"zips_dir '{directory}' does not exist or is not a directory.")

        safe_pattern = pattern.strip() if isinstance(pattern, str) and pattern.strip() else "*.zip"
        discovered = list(directory.rglob(safe_pattern)) if recursive else list(directory.glob(safe_pattern))
        files = sorted([path for path in discovered if path.is_file()], key=lambda p: str(p).lower())
        discovered_count = len(files)
        if max_files > 0:
            files = files[: max(1, min(int(max_files), 100000))]
        selected_count = len(files)

        results: list[dict[str, Any]] = []
        imported_count = 0
        failed_count = 0
        stopped_early = False

        for zip_path in files:
            rel_path = _as_project_relative(zip_path, self.project_root)
            try:
                out = self.import_run(
                    zip_path,
                    index_memory=index_memory,
                    link_cache=link_cache,
                    overwrite=overwrite,
                )
                imported_count += 1
                results.append(
                    {
                        "zip_file": rel_path,
                        "ok": True,
                        "run_id": str(out.get("run_id", "")),
                        "files_imported": int(out.get("files_imported", 0)),
                        "error": "",
                    }
                )
            except Exception as exc:
                failed_count += 1
                results.append(
                    {
                        "zip_file": rel_path,
                        "ok": False,
                        "run_id": "",
                        "files_imported": 0,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                if not continue_on_error:
                    stopped_early = True
                    break

        processed_count = len(results)
        remaining_count = max(0, selected_count - processed_count)
        return {
            "status": "ok",
            "discovered_count": discovered_count,
            "selected_count": selected_count,
            "processed_count": processed_count,
            "imported_count": imported_count,
            "failed_count": failed_count,
            "stopped_early": stopped_early,
            "remaining_count": remaining_count,
            "pattern": safe_pattern,
            "recursive": bool(recursive),
            "results": results,
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

    def memory_audit(self, *, limit: int = 0) -> dict[str, Any]:
        out = self.memory.audit_index(limit=limit)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "total_indexed_runs": out.get("total_indexed_runs", 0),
            "scanned_count": out.get("scanned_count", 0),
            "stale_count": out.get("stale_count", 0),
            "runs": out.get("runs", []),
        }

    def memory_clean(self, *, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
        out = self.memory.clean_stale_runs(limit=limit, dry_run=dry_run)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "dry_run": bool(out.get("dry_run", False)),
            "total_indexed_runs": out.get("total_indexed_runs", 0),
            "scanned_count": out.get("scanned_count", 0),
            "stale_count": out.get("stale_count", 0),
            "would_delete_count": out.get("would_delete_count", 0),
            "deleted_count": out.get("deleted_count", 0),
            "runs": out.get("runs", []),
        }

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

    def queue_submit_quick(
        self,
        *,
        objective: str,
        domain: str = "generic",
        parameters: dict[str, Any] | None = None,
        task_id: str | None = None,
        force_rerun: bool = False,
        acceptance_criteria: list[str] | None = None,
        dry_run: bool = False,
        max_attempts: int = 1,
    ) -> dict[str, Any]:
        task = self._build_quick_task(
            objective=objective,
            domain=domain,
            parameters=parameters,
            task_id=task_id,
            force_rerun=force_rerun,
            acceptance_criteria=acceptance_criteria,
        )
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

    def queue_requeue_failed(self, *, limit: int = 20, reset_attempts: bool = True) -> dict[str, Any]:
        out = self.queue.requeue_failed(limit=limit, reset_attempts=reset_attempts)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "requeued_count": out.get("requeued_count", 0),
            "jobs": out.get("jobs", []),
        }

    def queue_recover_running(
        self,
        *,
        limit: int = 20,
        max_age_sec: int = 300,
        force_requeue: bool = False,
        reset_attempts: bool = False,
    ) -> dict[str, Any]:
        out = self.queue.recover_stale_running(
            limit=limit,
            max_age_sec=max_age_sec,
            force_requeue=force_requeue,
            reset_attempts=reset_attempts,
        )
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "max_age_sec": out.get("max_age_sec", 0),
            "scanned_running_count": out.get("scanned_running_count", 0),
            "stale_count": out.get("stale_count", 0),
            "recovered_count": out.get("recovered_count", 0),
            "marked_failed_count": out.get("marked_failed_count", 0),
            "jobs": out.get("jobs", []),
        }

    def queue_stale_running(self, *, limit: int = 20, max_age_sec: int = 300) -> dict[str, Any]:
        out = self.queue.stale_running(limit=limit, max_age_sec=max_age_sec)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "max_age_sec": out.get("max_age_sec", 0),
            "scanned_running_count": out.get("scanned_running_count", 0),
            "stale_count": out.get("stale_count", 0),
            "jobs": out.get("jobs", []),
        }

    def queue_prune(
        self,
        *,
        limit: int = 100,
        statuses: list[str] | None = None,
        older_than_sec: int = 0,
        delete_results: bool = True,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        out = self.queue.prune_jobs(
            limit=limit,
            statuses=statuses,
            older_than_sec=older_than_sec,
            delete_results=delete_results,
            dry_run=dry_run,
        )
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "statuses": out.get("statuses", []),
            "older_than_sec": out.get("older_than_sec", 0),
            "dry_run": bool(out.get("dry_run", False)),
            "would_prune_count": out.get("would_prune_count", 0),
            "matched_count": out.get("matched_count", 0),
            "pruned_count": out.get("pruned_count", 0),
            "result_files_deleted": out.get("result_files_deleted", 0),
            "result_files_would_delete": out.get("result_files_would_delete", 0),
            "result_files_missing": out.get("result_files_missing", 0),
            "jobs": out.get("jobs", []),
        }

    def queue_clean_results(self, *, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
        out = self.queue.clean_orphan_results(limit=limit, dry_run=dry_run)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "dry_run": bool(out.get("dry_run", False)),
            "scanned_count": out.get("scanned_count", 0),
            "orphan_count": out.get("orphan_count", 0),
            "deleted_count": out.get("deleted_count", 0),
            "files": out.get("files", []),
        }

    def queue_orphan_results(self, *, limit: int = 0) -> dict[str, Any]:
        out = self.queue.orphan_results(limit=limit)
        return {
            "status": "ok",
            "requested_limit": out.get("requested_limit", 0),
            "scanned_count": out.get("scanned_count", 0),
            "orphan_count": out.get("orphan_count", 0),
            "files": out.get("files", []),
        }

    def queue_cancel(self, job_id: str, *, reason: str = "") -> dict[str, Any]:
        job = self.queue.cancel_job(job_id, reason=reason)
        return {"status": "ok", "job": job}

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
        requested_max_jobs = int(max_jobs)
        unlimited_mode = requested_max_jobs <= 0
        safe_max_jobs = 10000 if unlimited_mode else max(1, min(requested_max_jobs, 100))
        wid = worker_id or f"worker_{uuid4().hex[:8]}"
        processed = 0
        outputs: list[dict[str, Any]] = []
        stop_reason = "max_jobs_limit"
        while processed < safe_max_jobs:
            out = self.queue_work_once(worker_id=wid)
            outputs.append(out)
            if out.get("status") == "idle":
                stop_reason = "idle"
                break
            processed += 1
        if stop_reason != "idle" and unlimited_mode:
            stop_reason = "safety_limit"
        return {
            "status": "ok",
            "worker_id": wid,
            "requested_max_jobs": requested_max_jobs,
            "effective_max_jobs": safe_max_jobs,
            "unlimited_mode": unlimited_mode,
            "processed": processed,
            "stop_reason": stop_reason,
            "results": outputs,
        }

    def queue_work_daemon(
        self,
        *,
        max_cycles: int = 0,
        poll_interval_sec: float = 2.0,
        max_jobs_per_cycle: int = 10,
        idle_stop_after: int = 0,
        worker_id: str | None = None,
        include_cycle_results: bool = False,
    ) -> dict[str, Any]:
        requested_max_cycles = int(max_cycles)
        unlimited_cycles = requested_max_cycles <= 0
        effective_max_cycles = (
            50000 if unlimited_cycles else max(1, min(requested_max_cycles, 100000))
        )
        safe_poll_interval = max(0.0, min(float(poll_interval_sec), 60.0))
        safe_max_jobs_per_cycle = max(0, min(int(max_jobs_per_cycle), 10000))
        safe_idle_stop_after = max(0, min(int(idle_stop_after), 100000))
        wid = worker_id or f"worker_{uuid4().hex[:8]}"

        cycles_run = 0
        processed_total = 0
        idle_cycles = 0
        stop_reason = "max_cycles_limit"
        interrupted = False
        cycle_summaries: list[dict[str, Any]] = []
        cycle_results: list[dict[str, Any]] = []

        try:
            while cycles_run < effective_max_cycles:
                out = self.queue_work(max_jobs=safe_max_jobs_per_cycle, worker_id=wid)
                cycles_run += 1
                cycle_processed = max(0, int(out.get("processed", 0)))
                processed_total += cycle_processed
                if cycle_processed == 0:
                    idle_cycles += 1
                else:
                    idle_cycles = 0

                cycle_summaries.append(
                    {
                        "cycle": cycles_run,
                        "processed": cycle_processed,
                        "stop_reason": str(out.get("stop_reason", "")),
                        "worker_id": str(out.get("worker_id", wid)),
                    }
                )
                if include_cycle_results:
                    cycle_results.append(out)

                if safe_idle_stop_after > 0 and idle_cycles >= safe_idle_stop_after:
                    stop_reason = "idle_stop_after"
                    break

                if cycles_run >= effective_max_cycles:
                    stop_reason = "safety_limit" if unlimited_cycles else "max_cycles_limit"
                    break

                if safe_poll_interval > 0.0:
                    time.sleep(safe_poll_interval)
        except KeyboardInterrupt:  # pragma: no cover
            interrupted = True
            stop_reason = "interrupted"

        payload = {
            "status": "ok",
            "worker_id": wid,
            "requested_max_cycles": requested_max_cycles,
            "effective_max_cycles": effective_max_cycles,
            "unlimited_cycles": unlimited_cycles,
            "poll_interval_sec": safe_poll_interval,
            "max_jobs_per_cycle": safe_max_jobs_per_cycle,
            "idle_stop_after": safe_idle_stop_after,
            "cycles_run": cycles_run,
            "processed_total": processed_total,
            "idle_cycles_at_end": idle_cycles,
            "stop_reason": stop_reason,
            "interrupted": interrupted,
            "cycle_summaries": cycle_summaries[:1000],
        }
        if include_cycle_results:
            payload["cycle_results"] = cycle_results
        return payload

    @staticmethod
    def _build_quick_task(
        *,
        objective: str,
        domain: str,
        parameters: dict[str, Any] | None,
        task_id: str | None,
        force_rerun: bool,
        acceptance_criteria: list[str] | None,
    ) -> dict[str, Any]:
        if parameters is None:
            params: dict[str, Any] = {}
        elif isinstance(parameters, dict):
            params = dict(parameters)
        else:
            raise ValidationError("Quick task parameters must be a JSON object.")

        criteria = [str(item).strip() for item in (acceptance_criteria or []) if str(item).strip()]
        quick_task_id = (
            task_id
            or f"task_quick_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid4().hex[:8]}"
        ).strip()
        return {
            "task_id": quick_task_id,
            "objective": str(objective).strip(),
            "domain": str(domain).strip(),
            "requires_computation": True,
            "allow_internet_research": True,
            "strict_no_guessing": True,
            "force_rerun": bool(force_rerun),
            "parameters": params,
            "acceptance_criteria": criteria,
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
    def _build_markdown_run_report(
        *,
        run_id: str,
        inspect_payload: dict[str, Any],
        metrics: dict[str, Any],
    ) -> str:
        meta = inspect_payload.get("meta", {})
        summary = inspect_payload.get("summary", {})
        evidence_overview = inspect_payload.get("evidence_overview", {})
        trace_overview = inspect_payload.get("trace_overview", {})
        execution_overview = inspect_payload.get("execution_overview", {})
        research_overview = inspect_payload.get("research_overview", {})
        truth_overview = inspect_payload.get("truth_overview", {})

        lines: list[str] = []
        lines.append(f"# Run Report: {run_id}")
        lines.append("")
        lines.append("## Meta")
        lines.append(f"- task_id: {meta.get('task_id', '')}")
        lines.append(f"- domain: {meta.get('domain', '')}")
        lines.append(f"- status: {meta.get('status', '')}")
        lines.append(f"- timestamp_utc: {meta.get('timestamp_utc', '')}")
        lines.append(f"- objective: {meta.get('objective', '')}")
        lines.append("")

        lines.append("## Summary")
        lines.append(f"- headline: {summary.get('headline', '')}")
        caveats = summary.get("caveats", [])
        if isinstance(caveats, list) and len(caveats) > 0:
            lines.append("- caveats:")
            for item in caveats[:8]:
                lines.append(f"  - {item}")
        else:
            lines.append("- caveats: none")
        lines.append("")

        lines.append("## Metrics")
        if len(metrics) == 0:
            lines.append("- none")
        else:
            for key in sorted(metrics.keys()):
                lines.append(f"- {key}: {metrics[key]}")
        lines.append("")

        lines.append("## Evidence")
        lines.append(f"- artifact_count: {evidence_overview.get('artifact_count', 0)}")
        lines.append(f"- metric_keys: {', '.join(evidence_overview.get('metric_keys', []))}")
        lines.append("")

        lines.append("## Trace")
        lines.append(f"- event_count: {trace_overview.get('event_count', 0)}")
        lines.append(f"- total_span_sec: {trace_overview.get('total_span_sec', 0.0)}")
        slowest = trace_overview.get("slowest_stages", [])
        if isinstance(slowest, list) and len(slowest) > 0:
            lines.append("- slowest_stages:")
            for item in slowest[:5]:
                stage = item.get("stage", "")
                duration = item.get("duration_sec", 0.0)
                lines.append(f"  - {stage}: {duration}s")
        lines.append("")

        lines.append("## Execution")
        lines.append(f"- final_status: {execution_overview.get('final_status', 'UNKNOWN')}")
        lines.append(f"- attempt_count: {execution_overview.get('attempt_count', 0)}")
        lines.append(f"- total_attempt_duration_sec: {execution_overview.get('total_attempt_duration_sec', 0.0)}")
        lines.append("")

        lines.append("## Research")
        lines.append(f"- source_count: {research_overview.get('source_count', 0)}")
        lines.append(f"- deduplicated_count: {research_overview.get('deduplicated_count', 0)}")
        lines.append(f"- error_count: {research_overview.get('error_count', 0)}")
        lines.append("")

        lines.append("## Truth")
        lines.append(f"- all_supported: {truth_overview.get('all_supported', False)}")
        lines.append(f"- unsupported_count: {truth_overview.get('unsupported_count', 0)}")
        lines.append(f"- blocked_user_claims: {truth_overview.get('blocked_user_claims', False)}")
        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _build_runs_dashboard_html(
        *,
        generated_at_utc: str,
        rows: list[dict[str, Any]],
        stats: dict[str, Any],
        scope: dict[str, Any],
    ) -> str:
        status_counts = stats.get("counts_by_status", {})
        if not isinstance(status_counts, dict):
            status_counts = {}
        domain_counts = stats.get("counts_by_domain", {})
        if not isinstance(domain_counts, dict):
            domain_counts = {}

        status_bits = " | ".join(
            f"{escape(str(key))}: {int(value)}"
            for key, value in sorted(status_counts.items(), key=lambda kv: str(kv[0]))
        ) or "none"
        domain_bits = " | ".join(
            f"{escape(str(key))}: {int(value)}"
            for key, value in sorted(domain_counts.items(), key=lambda kv: str(kv[0]))
        ) or "none"

        table_rows: list[str] = []
        for row in rows:
            run_id = str(row.get("run_id", ""))
            task_id = str(row.get("task_id", ""))
            domain = str(row.get("domain", ""))
            status = str(row.get("status", ""))
            timestamp = str(row.get("timestamp_utc", ""))
            objective = str(row.get("objective", ""))
            source_count = int(row.get("research_source_count", 0) or 0)

            run_dir = f"data/runs/{run_id}/"
            evidence = f"data/runs/{run_id}/evidence_bundle.json"
            summary = f"data/runs/{run_id}/summary.json"
            trace = f"data/runs/{run_id}/trace.json"

            links = (
                f"<a href=\"{escape(run_dir)}\">run_dir</a> "
                f"<a href=\"{escape(evidence)}\">evidence</a> "
                f"<a href=\"{escape(summary)}\">summary</a> "
                f"<a href=\"{escape(trace)}\">trace</a>"
            )
            table_rows.append(
                "<tr>"
                f"<td>{escape(run_id)}</td>"
                f"<td>{escape(task_id)}</td>"
                f"<td>{escape(status)}</td>"
                f"<td>{escape(domain)}</td>"
                f"<td>{escape(timestamp)}</td>"
                f"<td>{escape(objective)}</td>"
                f"<td>{source_count}</td>"
                f"<td>{links}</td>"
                "</tr>"
            )

        rows_html = "\n".join(table_rows) if len(table_rows) > 0 else (
            "<tr><td colspan=\"8\">No runs found for this scope.</td></tr>"
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CodexJarvis Runs Dashboard</title>
  <style>
    body {{
      font-family: "Segoe UI", Tahoma, sans-serif;
      margin: 20px;
      color: #1c1c1c;
      background: linear-gradient(135deg, #f7fafc, #edf2f7);
    }}
    .card {{
      background: #ffffff;
      border: 1px solid #d9e2ec;
      border-radius: 10px;
      padding: 14px 16px;
      margin-bottom: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #ffffff;
    }}
    th, td {{
      border: 1px solid #e2e8f0;
      text-align: left;
      padding: 8px;
      vertical-align: top;
      font-size: 13px;
    }}
    th {{
      background: #f1f5f9;
      position: sticky;
      top: 0;
    }}
    code {{
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      padding: 2px 6px;
    }}
    a {{
      margin-right: 6px;
    }}
  </style>
</head>
<body>
  <h1>CodexJarvis Runs Dashboard</h1>
  <div class="card">
    <div><strong>Generated:</strong> <code>{escape(generated_at_utc)}</code></div>
    <div><strong>Scope:</strong> limit=<code>{int(scope.get("limit", 0))}</code> domain=<code>{escape(str(scope.get("domain", "")))}</code> include_failed=<code>{bool(scope.get("include_failed", True))}</code></div>
    <div><strong>Total in table:</strong> <code>{len(rows)}</code></div>
    <div><strong>Status counts:</strong> {status_bits}</div>
    <div><strong>Domain counts:</strong> {domain_bits}</div>
  </div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>run_id</th>
          <th>task_id</th>
          <th>status</th>
          <th>domain</th>
          <th>timestamp_utc</th>
          <th>objective</th>
          <th>research_sources</th>
          <th>links</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</body>
</html>
"""

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


def _as_project_relative(path: Path, project_root: Path) -> str:
    abs_path = path.resolve()
    try:
        return str(abs_path.relative_to(project_root.resolve()).as_posix())
    except ValueError:
        return str(abs_path)


def _extract_run_ids_from_members(members: list[str]) -> set[str]:
    run_ids: set[str] = set()
    for member in members:
        parts = member.split("/")
        if len(parts) < 4:
            continue
        if parts[0] != "data" or parts[1] != "runs":
            continue
        run_id = parts[2].strip()
        if run_id:
            run_ids.add(run_id)
    return run_ids


def _build_legacy_execution_manifest(*, meta: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    status = str(evidence.get("status", "")).upper()
    stderr_text = str(evidence.get("logs", {}).get("stderr", "")) if isinstance(evidence.get("logs", {}), dict) else ""
    if status == "SUCCESS":
        attempt_status = "SUCCESS"
        final_status = "SUCCESS"
        error_text = ""
    else:
        attempt_status = "FAILED"
        final_status = "FAILED"
        error_text = stderr_text.strip() or "Legacy run imported without execution manifest."

    is_dry = bool(evidence.get("metrics", {}).get("dry_run", False)) if isinstance(evidence.get("metrics", {}), dict) else False
    return {
        "policy": {
            "mode": "dry_run" if is_dry else "legacy",
            "timeout_sec": None,
            "max_retries": 0,
            "retry_delay_sec": 0.0,
        },
        "attempts": [
            {
                "attempt": 1,
                "status": attempt_status,
                "duration_sec": 0.0,
                "error": error_text,
            }
        ],
        "final_status": final_status,
        "legacy_migration": {
            "migrated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_timestamp_utc": str(meta.get("timestamp_utc", evidence.get("timestamp_utc", ""))),
        },
    }


def _build_legacy_trace(*, run_id: str, meta: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    timestamp = str(meta.get("timestamp_utc", evidence.get("timestamp_utc", datetime.now(timezone.utc).isoformat())))
    run_status = str(evidence.get("status", "UNKNOWN"))
    return {
        "run_id": run_id,
        "run_mode": "legacy",
        "events": [
            {
                "timestamp_utc": timestamp,
                "stage": "legacy_run_rehydrated",
                "details": {
                    "status": run_status,
                    "message": "Trace reconstructed during legacy migration.",
                },
            }
        ],
    }


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)
        f.write("\n")
