from __future__ import annotations

import time
import unittest
from pathlib import Path

from jarvis.execution import ExecutionPolicy, execute_with_policy


def _ok_fn(task: dict, *, project_root: Path):
    _ = task, project_root
    return (
        {"metrics": {"x": 1}},
        {"headline": "ok", "key_metrics": {}, "caveats": []},
        "ok\n",
        "",
    )


class _FailThenOk:
    def __init__(self):
        self.calls = 0

    def __call__(self, task: dict, *, project_root: Path):
        _ = task, project_root
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("first attempt fails")
        return (
            {"metrics": {"x": 2}},
            {"headline": "ok", "key_metrics": {}, "caveats": []},
            "ok\n",
            "",
        )


def _slow_fn(task: dict, *, project_root: Path):
    _ = task, project_root
    time.sleep(0.15)
    return (
        {"metrics": {"x": 3}},
        {"headline": "ok", "key_metrics": {}, "caveats": []},
        "ok\n",
        "",
    )


class ExecutionPolicyTests(unittest.TestCase):
    def test_success_first_attempt(self) -> None:
        out = execute_with_policy(
            _ok_fn,
            task={},
            project_root=Path("."),
            policy=ExecutionPolicy(timeout_sec=2, max_retries=0, retry_delay_sec=0.0),
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["execution_manifest"]["final_status"], "SUCCESS")
        self.assertEqual(len(out["execution_manifest"]["attempts"]), 1)

    def test_retry_then_success(self) -> None:
        fn = _FailThenOk()
        out = execute_with_policy(
            fn,
            task={},
            project_root=Path("."),
            policy=ExecutionPolicy(timeout_sec=2, max_retries=1, retry_delay_sec=0.0),
        )
        self.assertTrue(out["ok"])
        self.assertEqual(len(out["execution_manifest"]["attempts"]), 2)
        self.assertEqual(out["execution_manifest"]["attempts"][0]["status"], "FAILED")
        self.assertEqual(out["execution_manifest"]["attempts"][1]["status"], "SUCCESS")

    def test_timeout_failure(self) -> None:
        out = execute_with_policy(
            _slow_fn,
            task={},
            project_root=Path("."),
            policy=ExecutionPolicy(timeout_sec=1, max_retries=0, retry_delay_sec=0.0),
        )
        self.assertTrue(out["ok"])

        out_timeout = execute_with_policy(
            _slow_fn,
            task={},
            project_root=Path("."),
            policy=ExecutionPolicy(timeout_sec=0, max_retries=0, retry_delay_sec=0.0),
        )
        self.assertFalse(out_timeout["ok"])
        self.assertEqual(out_timeout["execution_manifest"]["final_status"], "FAILED")
        self.assertEqual(out_timeout["execution_manifest"]["attempts"][0]["status"], "TIMEOUT")


if __name__ == "__main__":
    unittest.main()
