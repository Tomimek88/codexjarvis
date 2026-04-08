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

    def test_queue_stats_for_success_and_failed_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            engine.queue_submit(_base_task("task-q-0003"), dry_run=False, max_attempts=1)
            task_fail = _base_task("task-q-0004")
            task_fail["parameters"]["simulate_delay_sec"] = 1.2
            task_fail["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            engine.queue_submit(task_fail, dry_run=False, max_attempts=1)

            engine.queue_work(max_jobs=10, worker_id="worker-stats")

            stats = engine.queue_stats()
            self.assertEqual(stats["status"], "ok")
            counts = stats["stats"]["status_counts"]
            self.assertEqual(stats["stats"]["total_jobs"], 2)
            self.assertEqual(counts["SUCCESS"], 1)
            self.assertEqual(counts["FAILED"], 1)
            self.assertEqual(stats["stats"]["retry_queued_count"], 0)
            self.assertEqual(stats["stats"]["dead_failed_count"], 1)

    def test_queue_stats_tracks_retry_queued(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = _base_task("task-q-0005")
            task["parameters"]["simulate_delay_sec"] = 1.2
            task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            engine.queue_submit(task, dry_run=False, max_attempts=2)

            first = engine.queue_work_once(worker_id="worker-stats")
            self.assertEqual(first["status"], "job_failed")
            self.assertTrue(first["requeued"])

            stats = engine.queue_stats()
            self.assertEqual(stats["status"], "ok")
            counts = stats["stats"]["status_counts"]
            self.assertEqual(counts["QUEUED"], 1)
            self.assertEqual(stats["stats"]["retry_queued_count"], 1)

    def test_queue_requeue_failed_resets_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = _base_task("task-q-0006")
            task["parameters"]["simulate_delay_sec"] = 1.2
            task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            submitted = engine.queue_submit(task, dry_run=False, max_attempts=1)
            job_id = submitted["job"]["job_id"]

            failed = engine.queue_work_once(worker_id="worker-requeue")
            self.assertEqual(failed["status"], "job_failed")
            self.assertFalse(failed["requeued"])
            self.assertEqual(failed["job"]["status"], "FAILED")

            requeued = engine.queue_requeue_failed(limit=10, reset_attempts=True)
            self.assertEqual(requeued["status"], "ok")
            self.assertGreaterEqual(requeued["requeued_count"], 1)
            self.assertTrue(any(job["job_id"] == job_id for job in requeued["jobs"]))

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "QUEUED")
            self.assertEqual(int(fetched["job"]["attempts"]), 0)


if __name__ == "__main__":
    unittest.main()
