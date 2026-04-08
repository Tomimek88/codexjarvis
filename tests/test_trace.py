from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class TraceTests(unittest.TestCase):
    def test_run_trace_and_execution_manifest_are_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = {
                "task_id": "task-trace-0001",
                "objective": "trace smoke",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "seed": 42,
                    "execution_policy": {
                        "timeout_sec": 10,
                        "max_retries": 0,
                        "retry_delay_sec": 0
                    }
                },
            }
            out = engine.run(task, dry_run=False)
            run_id = out["run_id"]
            self.assertIn("run_trace", out)
            self.assertIn("execution_manifest", out)
            self.assertTrue(len(out["run_trace"]["events"]) > 0)
            self.assertEqual(out["execution_manifest"]["final_status"], "SUCCESS")

            trace_view = engine.trace(run_id)
            self.assertEqual(trace_view["status"], "ok")
            self.assertEqual(trace_view["execution_manifest"]["final_status"], "SUCCESS")
            self.assertTrue(len(trace_view["trace"]["events"]) > 0)

            run_dir = root / "data" / "runs" / run_id
            self.assertTrue((run_dir / "trace.json").exists())
            self.assertTrue((run_dir / "execution_manifest.json").exists())

            inspect = engine.inspect(run_id)
            self.assertEqual(inspect["status"], "ok")
            self.assertEqual(inspect["run_id"], run_id)
            self.assertIn("trace_overview", inspect)
            self.assertIn("execution_overview", inspect)
            self.assertIn("research_overview", inspect)
            self.assertIn("truth_overview", inspect)
            self.assertGreaterEqual(inspect["trace_overview"]["event_count"], 1)
            self.assertGreaterEqual(inspect["trace_overview"]["total_span_sec"], 0.0)
            self.assertEqual(inspect["execution_overview"]["final_status"], "SUCCESS")

    def test_compare_runs_reports_metric_and_hash_differences(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            task_a = {
                "task_id": "task-compare-0001a",
                "objective": "compare run a",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "force_rerun": True,
                "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
            }
            task_b = {
                "task_id": "task-compare-0001b",
                "objective": "compare run b",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "force_rerun": True,
                "parameters": {"a": 2, "b": 4, "c": 8, "seed": 42},
            }

            out_a = engine.run(task_a, dry_run=False)
            out_b = engine.run(task_b, dry_run=False)
            cmp = engine.compare_runs(out_a["run_id"], out_b["run_id"])

            self.assertEqual(cmp["status"], "ok")
            self.assertFalse(cmp["hash_comparison"]["params_hash"]["equal"])
            self.assertTrue(cmp["hash_comparison"]["code_hash"]["equal"])
            self.assertIn("weighted_sum", cmp["metric_diff"])
            self.assertTrue(cmp["metric_diff"]["weighted_sum"]["changed"])
            self.assertEqual(cmp["artifact_diff"]["only_in_run_a"], [])
            self.assertEqual(cmp["artifact_diff"]["only_in_run_b"], [])
            self.assertTrue(any(item["path"] == "results/result.json" for item in cmp["artifact_diff"]["changed_sha"]))


if __name__ == "__main__":
    unittest.main()
