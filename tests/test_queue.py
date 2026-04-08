from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _base_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": "queue test objective",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "parameters": {
            "a": 1,
            "b": 2,
            "c": 3,
            "seed": 42,
        },
    }


class QueueTests(unittest.TestCase):
    def test_queue_submit_and_process_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_base_task("task-q-0001"), dry_run=False, max_attempts=1)
            self.assertEqual(submitted["status"], "queued")
            job_id = submitted["job"]["job_id"]

            listed = engine.queue_list(limit=10)
            self.assertEqual(listed["count"], 1)
            self.assertEqual(listed["jobs"][0]["status"], "QUEUED")

            out = engine.queue_work_once(worker_id="worker-test")
            self.assertEqual(out["status"], "job_completed")
            self.assertEqual(out["job"]["status"], "SUCCESS")
            self.assertTrue(bool(out["job"]["run_id"]))

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "SUCCESS")

            idle = engine.queue_work_once(worker_id="worker-test")
            self.assertEqual(idle["status"], "idle")

    def test_queue_retry_then_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = _base_task("task-q-0002")
            task["parameters"]["simulate_delay_sec"] = 1.2
            task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }

            submitted = engine.queue_submit(task, dry_run=False, max_attempts=2)
            job_id = submitted["job"]["job_id"]

            first = engine.queue_work_once(worker_id="worker-test")
            self.assertEqual(first["status"], "job_failed")
            self.assertTrue(first["requeued"])
            self.assertEqual(first["job"]["status"], "QUEUED")

            second = engine.queue_work_once(worker_id="worker-test")
            self.assertEqual(second["status"], "job_failed")
            self.assertFalse(second["requeued"])
            self.assertEqual(second["job"]["status"], "FAILED")

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "FAILED")


if __name__ == "__main__":
    unittest.main()
