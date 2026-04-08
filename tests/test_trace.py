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


if __name__ == "__main__":
    unittest.main()
