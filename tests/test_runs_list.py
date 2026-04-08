from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class RunsListTests(unittest.TestCase):
    def test_runs_list_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            success_task = {
                "task_id": "task-runs-0001",
                "objective": "successful run for runs-list",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "force_rerun": True,
                "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
            }
            fail_task = {
                "task_id": "task-runs-0002",
                "objective": "failed run for runs-list timeout",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "force_rerun": True,
                "parameters": {
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "seed": 42,
                    "simulate_delay_sec": 1.2,
                    "execution_policy": {
                        "timeout_sec": 1,
                        "max_retries": 0,
                        "retry_delay_sec": 0.0,
                    },
                },
            }

            out_ok = engine.run(success_task, dry_run=False)
            out_fail = engine.run(fail_task, dry_run=False)
            self.assertEqual(out_ok["status"], "completed")
            self.assertEqual(out_fail["status"], "failed")

            all_runs = engine.runs_list(limit=10)
            self.assertEqual(all_runs["status"], "ok")
            self.assertGreaterEqual(all_runs["count"], 2)

            success_only = engine.runs_list(limit=10, status="SUCCESS")
            ids_success = [row["run_id"] for row in success_only["runs"]]
            self.assertIn(out_ok["run_id"], ids_success)
            self.assertNotIn(out_fail["run_id"], ids_success)

            failed_only = engine.runs_list(limit=10, status="FAILED")
            ids_failed = [row["run_id"] for row in failed_only["runs"]]
            self.assertIn(out_fail["run_id"], ids_failed)
            self.assertNotIn(out_ok["run_id"], ids_failed)

            contains_filtered = engine.runs_list(limit=10, contains="timeout")
            ids_contains = [row["run_id"] for row in contains_filtered["runs"]]
            self.assertIn(out_fail["run_id"], ids_contains)
            self.assertNotIn(out_ok["run_id"], ids_contains)


if __name__ == "__main__":
    unittest.main()
