from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass
class ExecutionPolicy:
    timeout_sec: int = 60
    max_retries: int = 0
    retry_delay_sec: float = 0.0


def build_execution_policy(task: dict[str, Any]) -> ExecutionPolicy:
    params = task.get("parameters", {})
    raw = params.get("execution_policy", {})
    if not isinstance(raw, dict):
        raw = {}
    timeout_sec = _coerce_int(raw.get("timeout_sec", 60), default=60, min_value=1, max_value=3600)
    max_retries = _coerce_int(raw.get("max_retries", 0), default=0, min_value=0, max_value=5)
    retry_delay_sec = _coerce_float(
        raw.get("retry_delay_sec", 0.0),
        default=0.0,
        min_value=0.0,
        max_value=30.0,
    )
    return ExecutionPolicy(
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        retry_delay_sec=retry_delay_sec,
    )


def execute_with_policy(
    fn: Callable[..., tuple[dict[str, Any], dict[str, Any], str, str]],
    *,
    task: dict[str, Any],
    project_root: Path,
    policy: ExecutionPolicy,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    total_attempts = policy.max_retries + 1

    for attempt_no in range(1, total_attempts + 1):
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fn, task, project_root=project_root)
            try:
                result = future.result(timeout=policy.timeout_sec)
                duration_sec = time.perf_counter() - start
                attempts.append(
                    {
                        "attempt": attempt_no,
                        "status": "SUCCESS",
                        "duration_sec": round(duration_sec, 6),
                        "error": "",
                    }
                )
                return {
                    "ok": True,
                    "result": result,
                    "execution_manifest": {
                        "policy": {
                            "timeout_sec": policy.timeout_sec,
                            "max_retries": policy.max_retries,
                            "retry_delay_sec": policy.retry_delay_sec,
                        },
                        "attempts": attempts,
                        "final_status": "SUCCESS",
                    },
                }
            except FuturesTimeoutError:
                duration_sec = time.perf_counter() - start
                attempts.append(
                    {
                        "attempt": attempt_no,
                        "status": "TIMEOUT",
                        "duration_sec": round(duration_sec, 6),
                        "error": f"Execution exceeded timeout_sec={policy.timeout_sec}",
                    }
                )
            except Exception as exc:  # pragma: no cover
                duration_sec = time.perf_counter() - start
                attempts.append(
                    {
                        "attempt": attempt_no,
                        "status": "FAILED",
                        "duration_sec": round(duration_sec, 6),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        if attempt_no < total_attempts and policy.retry_delay_sec > 0:
            time.sleep(policy.retry_delay_sec)

    last_error = attempts[-1]["error"] if attempts else "Unknown execution failure."
    return {
        "ok": False,
        "error": last_error,
        "execution_manifest": {
            "policy": {
                "timeout_sec": policy.timeout_sec,
                "max_retries": policy.max_retries,
                "retry_delay_sec": policy.retry_delay_sec,
            },
            "attempts": attempts,
            "final_status": "FAILED",
        },
    }


def _coerce_int(value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(min_value, min(max_value, out))


def _coerce_float(value: Any, *, default: float, min_value: float, max_value: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        out = default
    return max(min_value, min(max_value, out))
