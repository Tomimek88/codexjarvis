from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.contracts import ValidationError
from jarvis.orchestrator import JarvisEngine


class QuickTaskTests(unittest.TestCase):
    def test_run_quick_completes_with_generated_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run_quick(
                objective="Quick generic run objective.",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
            )
            self.assertEqual(out["status"], "completed")
            self.assertTrue(str(out["task_id"]).startswith("task_quick_"))
            self.assertTrue(bool(out.get("run_id")))

    def test_run_quick_rejects_non_object_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            with self.assertRaises(ValidationError):
                engine.run_quick(
                    objective="Quick invalid parameters",
                    domain="generic",
                    parameters=["not", "an", "object"],  # type: ignore[arg-type]
                )

    def test_queue_submit_quick_and_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            queued = engine.queue_submit_quick(
                objective="Quick queued run objective.",
                domain="generic",
                parameters={"a": 2, "b": 3, "c": 4, "seed": 42},
                max_attempts=1,
            )
            self.assertEqual(queued["status"], "queued")
            self.assertEqual(str(queued["job"]["status"]), "QUEUED")

            processed = engine.queue_work_once(worker_id="worker-quick")
            self.assertEqual(processed["status"], "job_completed")
            self.assertEqual(str(processed["job"]["status"]), "SUCCESS")
            self.assertTrue(bool(processed["result"].get("run_id")))


if __name__ == "__main__":
    unittest.main()
