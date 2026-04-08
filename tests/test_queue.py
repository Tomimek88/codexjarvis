from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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

    def test_queue_cancel_sets_cancelled_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_base_task("task-q-0007"), dry_run=False, max_attempts=1)
            job_id = submitted["job"]["job_id"]

            cancelled = engine.queue_cancel(job_id, reason="manual stop")
            self.assertEqual(cancelled["status"], "ok")
            self.assertEqual(cancelled["job"]["status"], "CANCELLED")
            self.assertEqual(cancelled["job"]["last_error"], "manual stop")

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "CANCELLED")

    def test_queue_recover_running_requeues_stale_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_base_task("task-q-0008"), dry_run=False, max_attempts=2)
            job_id = submitted["job"]["job_id"]
            claimed = engine.queue.claim_next_job("worker-recover")
            self.assertIsNotNone(claimed)
            self.assertEqual(str(claimed["status"]), "RUNNING")

            stale_started = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            con = engine.queue._connect()
            try:
                con.execute(
                    "UPDATE jobs SET started_at_utc = ? WHERE job_id = ?",
                    (stale_started, job_id),
                )
                con.commit()
            finally:
                con.close()

            recovered = engine.queue_recover_running(limit=10, max_age_sec=60)
            self.assertEqual(recovered["status"], "ok")
            self.assertEqual(int(recovered["stale_count"]), 1)
            self.assertEqual(int(recovered["recovered_count"]), 1)
            self.assertEqual(int(recovered["marked_failed_count"]), 0)

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "QUEUED")
            self.assertEqual(int(fetched["job"]["attempts"]), 1)

    def test_queue_recover_running_marks_failed_when_attempts_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_base_task("task-q-0009"), dry_run=False, max_attempts=1)
            job_id = submitted["job"]["job_id"]
            claimed = engine.queue.claim_next_job("worker-recover")
            self.assertIsNotNone(claimed)
            self.assertEqual(str(claimed["status"]), "RUNNING")

            stale_started = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            con = engine.queue._connect()
            try:
                con.execute(
                    "UPDATE jobs SET started_at_utc = ? WHERE job_id = ?",
                    (stale_started, job_id),
                )
                con.commit()
            finally:
                con.close()

            recovered = engine.queue_recover_running(limit=10, max_age_sec=60)
            self.assertEqual(recovered["status"], "ok")
            self.assertEqual(int(recovered["stale_count"]), 1)
            self.assertEqual(int(recovered["recovered_count"]), 0)
            self.assertEqual(int(recovered["marked_failed_count"]), 1)

            fetched = engine.queue_get(job_id)
            self.assertEqual(fetched["job"]["status"], "FAILED")

    def test_queue_work_zero_max_jobs_processes_until_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            engine.queue_submit(_base_task("task-q-0010"), dry_run=False, max_attempts=1)
            engine.queue_submit(_base_task("task-q-0011"), dry_run=False, max_attempts=1)

            out = engine.queue_work(max_jobs=0, worker_id="worker-unlimited")
            self.assertEqual(out["status"], "ok")
            self.assertTrue(bool(out["unlimited_mode"]))
            self.assertEqual(out["requested_max_jobs"], 0)
            self.assertEqual(out["stop_reason"], "idle")
            self.assertGreaterEqual(int(out["processed"]), 2)

            stats = engine.queue_stats()
            counts = stats["stats"]["status_counts"]
            self.assertEqual(counts.get("QUEUED", 0), 0)
            self.assertEqual(counts.get("SUCCESS", 0), 2)

    def test_queue_stale_running_lists_old_running_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_base_task("task-q-0012"), dry_run=False, max_attempts=2)
            job_id = submitted["job"]["job_id"]
            claimed = engine.queue.claim_next_job("worker-stale-view")
            self.assertIsNotNone(claimed)
            self.assertEqual(str(claimed["status"]), "RUNNING")

            stale_started = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            con = engine.queue._connect()
            try:
                con.execute(
                    "UPDATE jobs SET started_at_utc = ? WHERE job_id = ?",
                    (stale_started, job_id),
                )
                con.commit()
            finally:
                con.close()

            stale = engine.queue_stale_running(limit=20, max_age_sec=60)
            self.assertEqual(stale["status"], "ok")
            self.assertEqual(int(stale["stale_count"]), 1)
            self.assertEqual(int(stale["scanned_running_count"]), 1)
            self.assertEqual(str(stale["jobs"][0]["job_id"]), job_id)

    def test_queue_prune_removes_finished_jobs_and_result_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            success_sub = engine.queue_submit(_base_task("task-q-0013"), dry_run=False, max_attempts=1)
            success_id = success_sub["job"]["job_id"]
            success_out = engine.queue_work_once(worker_id="worker-prune")
            self.assertEqual(success_out["status"], "job_completed")

            fail_task = _base_task("task-q-0014")
            fail_task["parameters"]["simulate_delay_sec"] = 1.2
            fail_task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            fail_sub = engine.queue_submit(fail_task, dry_run=False, max_attempts=1)
            fail_id = fail_sub["job"]["job_id"]
            fail_out = engine.queue_work_once(worker_id="worker-prune")
            self.assertEqual(fail_out["status"], "job_failed")
            self.assertFalse(fail_out["requeued"])

            cancel_sub = engine.queue_submit(_base_task("task-q-0015"), dry_run=False, max_attempts=1)
            cancel_id = cancel_sub["job"]["job_id"]
            engine.queue_cancel(cancel_id, reason="cleanup test")

            success_job = engine.queue_get(success_id)["job"]
            fail_job = engine.queue_get(fail_id)["job"]
            success_result = str(success_job.get("result_path", "")).strip()
            fail_result = str(fail_job.get("result_path", "")).strip()
            self.assertTrue((root / success_result).exists())
            self.assertTrue((root / fail_result).exists())

            pruned = engine.queue_prune(limit=10, statuses=["SUCCESS", "FAILED", "CANCELLED"], older_than_sec=0)
            self.assertEqual(pruned["status"], "ok")
            self.assertEqual(int(pruned["pruned_count"]), 3)
            self.assertGreaterEqual(int(pruned["result_files_deleted"]), 2)

            self.assertFalse((root / success_result).exists())
            self.assertFalse((root / fail_result).exists())
            with self.assertRaises(ValueError):
                engine.queue_get(success_id)
            with self.assertRaises(ValueError):
                engine.queue_get(fail_id)
            with self.assertRaises(ValueError):
                engine.queue_get(cancel_id)

    def test_queue_prune_dry_run_does_not_delete_jobs_or_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            success_sub = engine.queue_submit(_base_task("task-q-0016"), dry_run=False, max_attempts=1)
            success_id = success_sub["job"]["job_id"]
            success_out = engine.queue_work_once(worker_id="worker-prune-dry")
            self.assertEqual(success_out["status"], "job_completed")

            success_job = engine.queue_get(success_id)["job"]
            success_result = str(success_job.get("result_path", "")).strip()
            self.assertTrue((root / success_result).exists())

            preview = engine.queue_prune(
                limit=10,
                statuses=["SUCCESS"],
                older_than_sec=0,
                dry_run=True,
            )
            self.assertEqual(preview["status"], "ok")
            self.assertTrue(bool(preview["dry_run"]))
            self.assertEqual(int(preview["would_prune_count"]), 1)
            self.assertEqual(int(preview["pruned_count"]), 0)
            self.assertGreaterEqual(int(preview["result_files_would_delete"]), 1)

            fetched = engine.queue_get(success_id)["job"]
            self.assertEqual(str(fetched["status"]), "SUCCESS")
            self.assertTrue((root / success_result).exists())

    def test_queue_clean_results_removes_orphan_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            sub = engine.queue_submit(_base_task("task-q-0017"), dry_run=False, max_attempts=1)
            job_id = sub["job"]["job_id"]
            out = engine.queue_work_once(worker_id="worker-clean-results")
            self.assertEqual(out["status"], "job_completed")
            job = engine.queue_get(job_id)["job"]
            result_rel = str(job.get("result_path", "")).strip()
            result_abs = root / result_rel
            self.assertTrue(result_abs.exists())

            pruned = engine.queue_prune(
                limit=10,
                statuses=["SUCCESS"],
                older_than_sec=0,
                delete_results=False,
            )
            self.assertEqual(pruned["status"], "ok")
            self.assertEqual(int(pruned["pruned_count"]), 1)
            self.assertTrue(result_abs.exists())
            with self.assertRaises(ValueError):
                engine.queue_get(job_id)

            preview = engine.queue_clean_results(dry_run=True)
            self.assertEqual(preview["status"], "ok")
            self.assertTrue(bool(preview["dry_run"]))
            self.assertGreaterEqual(int(preview["orphan_count"]), 1)
            self.assertEqual(int(preview["deleted_count"]), 0)
            self.assertTrue(result_abs.exists())

            cleaned = engine.queue_clean_results(dry_run=False)
            self.assertEqual(cleaned["status"], "ok")
            self.assertGreaterEqual(int(cleaned["orphan_count"]), 1)
            self.assertGreaterEqual(int(cleaned["deleted_count"]), 1)
            self.assertFalse(result_abs.exists())

    def test_queue_work_daemon_processes_and_stops_on_idle_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            engine.queue_submit(_base_task("task-q-0018"), dry_run=False, max_attempts=1)

            out = engine.queue_work_daemon(
                max_cycles=10,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=1,
                idle_stop_after=2,
                worker_id="worker-daemon",
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(str(out["stop_reason"]), "idle_stop_after")
            self.assertEqual(int(out["idle_cycles_at_end"]), 2)
            self.assertGreaterEqual(int(out["processed_total"]), 1)
            self.assertEqual(int(out["cycles_run"]), 3)

            stats = engine.queue_stats()
            counts = stats["stats"]["status_counts"]
            self.assertEqual(counts.get("QUEUED", 0), 0)
            self.assertEqual(counts.get("SUCCESS", 0), 1)

    def test_queue_work_daemon_respects_max_cycles_without_idle_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            out = engine.queue_work_daemon(
                max_cycles=3,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=1,
                idle_stop_after=0,
                worker_id="worker-daemon-limit",
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(str(out["stop_reason"]), "max_cycles_limit")
            self.assertEqual(int(out["cycles_run"]), 3)
            self.assertEqual(int(out["processed_total"]), 0)
            self.assertEqual(int(out["idle_cycles_at_end"]), 3)


if __name__ == "__main__":
    unittest.main()
