from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jarvis.contracts import ValidationError
from jarvis.orchestrator import JarvisEngine


def _task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"doctor objective {task_id}",
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
        },
    }


class DoctorTests(unittest.TestCase):
    def test_doctor_ok_for_clean_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-doctor-0001"), dry_run=False)
            self.assertEqual(out["status"], "completed")

            doctor = engine.doctor()
            self.assertEqual(doctor["status"], "ok")
            self.assertIn(doctor["overall"], {"ok", "warning"})
            self.assertIn("health", doctor)
            self.assertIn("cache_verify", doctor)
            self.assertIn("queue_stats", doctor)
            self.assertIn("queue_stale_running", doctor)
            self.assertIn("queue_orphan_results", doctor)
            self.assertIn("memory_audit", doctor)
            self.assertIn("runs_stats", doctor)
            self.assertIn("audit_summary", doctor)

    def test_doctor_warns_when_integrity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-doctor-0002"), dry_run=False)
            run_id = out["run_id"]

            evidence = out["evidence_bundle"]
            result_rel = next(item["path"] for item in evidence["artifacts"] if item["path"].endswith("/results/result.json"))
            (root / result_rel).write_text("tampered\n", encoding="utf-8")

            doctor = engine.doctor()
            self.assertEqual(doctor["status"], "ok")
            self.assertEqual(doctor["overall"], "warning")
            self.assertTrue("run_integrity_failures_present" in doctor["warnings"])
            self.assertGreaterEqual(doctor["audit_summary"]["failed_count"], 1)
            self.assertTrue(bool(run_id))

    def test_doctor_fix_repairs_common_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-doctor-0003"), dry_run=False)
            run_id = out["run_id"]

            execution_manifest_path = root / "data" / "runs" / run_id / "execution_manifest.json"
            execution_manifest_path.unlink(missing_ok=True)

            engine.store.save_cache_index(
                {
                    "entries": {
                        "bad-cache-key": {
                            "run_id": "run_missing",
                            "updated_at_utc": "2026-04-08T00:00:00+00:00",
                        }
                    }
                }
            )

            failed_task = _task("task-doctor-0004")
            failed_task["parameters"]["simulate_delay_sec"] = 1.2
            failed_task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            engine.queue_submit(failed_task, dry_run=False, max_attempts=1)
            failed_job = engine.queue_work_once(worker_id="worker-doctor-fix")
            self.assertEqual(failed_job["status"], "job_failed")
            self.assertFalse(failed_job["requeued"])
            self.assertEqual(failed_job["job"]["status"], "FAILED")

            stale_task = _task("task-doctor-0005")
            stale_submitted = engine.queue_submit(stale_task, dry_run=False, max_attempts=2)
            stale_job_id = stale_submitted["job"]["job_id"]
            claimed = engine.queue.claim_next_job("worker-stale")
            self.assertIsNotNone(claimed)
            stale_started = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            con = engine.queue._connect()
            try:
                con.execute(
                    "UPDATE jobs SET started_at_utc = ? WHERE job_id = ?",
                    (stale_started, stale_job_id),
                )
                con.commit()
            finally:
                con.close()

            before = engine.doctor()
            self.assertEqual(before["overall"], "warning")
            self.assertIn("run_integrity_failures_present", before["warnings"])
            self.assertIn("cache_invalid_entries_present", before["warnings"])
            self.assertIn("queue_dead_failed_jobs_present", before["warnings"])
            self.assertIn("queue_stale_running_jobs_present", before["warnings"])
            self.assertGreaterEqual(int(before["queue_stale_running"]["stale_count"]), 1)

            fixed = engine.doctor(fix=True)
            self.assertTrue(bool(fixed.get("fix_requested")))
            self.assertEqual(fixed["cache_verify"]["invalid_count"], 0)
            self.assertEqual(fixed["queue_stats"]["dead_failed_count"], 0)
            self.assertEqual(int(fixed["queue_stale_running"]["stale_count"]), 0)
            self.assertEqual(fixed["audit_summary"]["failed_count"], 0)
            self.assertGreaterEqual(int(fixed.get("pre_fix_warning_count", 0)), 4)
            self.assertNotIn("cache_invalid_entries_present", fixed["warnings"])
            self.assertNotIn("queue_dead_failed_jobs_present", fixed["warnings"])
            self.assertNotIn("queue_stale_running_jobs_present", fixed["warnings"])
            self.assertNotIn("run_integrity_failures_present", fixed["warnings"])
            actions = [str(item.get("action", "")) for item in fixed.get("fix_actions", [])]
            self.assertIn("runs_migrate_legacy", actions)
            self.assertIn("repair_runtime_artifact_hashes", actions)
            self.assertIn("queue_recover_running", actions)
            self.assertIn("cache_rebuild", actions)
            self.assertIn("queue_requeue_failed", actions)

    def test_doctor_fix_optionally_prunes_finished_queue_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            submitted = engine.queue_submit(_task("task-doctor-prune-01"), dry_run=False, max_attempts=1)
            job_id = submitted["job"]["job_id"]
            worked = engine.queue_work_once(worker_id="worker-doctor-prune")
            self.assertEqual(worked["status"], "job_completed")

            doctor = engine.doctor(
                fix=True,
                queue_prune=True,
                queue_prune_limit=10,
                queue_prune_older_than_sec=0,
                queue_prune_delete_results=False,
            )
            actions = [item for item in doctor.get("fix_actions", []) if item.get("action") == "queue_prune"]
            self.assertTrue(len(actions) >= 1)
            self.assertGreaterEqual(int(actions[0].get("result", {}).get("pruned_count", 0)), 1)
            with self.assertRaises(ValueError):
                engine.queue_get(job_id)

    def test_doctor_detects_and_cleans_orphan_queue_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            orphan_file = root / "data" / "queue" / "results" / "orphan_manual.json"
            orphan_file.parent.mkdir(parents=True, exist_ok=True)
            orphan_file.write_text("{\"status\":\"orphan\"}\n", encoding="utf-8")

            before = engine.doctor()
            self.assertIn("queue_orphan_result_files_present", before["warnings"])
            self.assertGreaterEqual(int(before["queue_orphan_results"]["orphan_count"]), 1)
            self.assertTrue(orphan_file.exists())

            fixed = engine.doctor(
                fix=True,
                queue_clean_results=True,
                queue_clean_results_limit=0,
            )
            self.assertNotIn("queue_orphan_result_files_present", fixed["warnings"])
            self.assertEqual(int(fixed["queue_orphan_results"]["orphan_count"]), 0)
            actions = [item for item in fixed.get("fix_actions", []) if item.get("action") == "queue_clean_results"]
            self.assertTrue(len(actions) >= 1)
            self.assertGreaterEqual(int(actions[0].get("result", {}).get("deleted_count", 0)), 1)
            self.assertFalse(orphan_file.exists())

    def test_doctor_detects_and_cleans_stale_memory_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-doctor-memory-0001"), dry_run=False)
            self.assertEqual(out["status"], "completed")
            run_id = out["run_id"]

            run_dir = root / "data" / "runs" / run_id
            shutil.rmtree(run_dir)

            before = engine.doctor()
            self.assertIn("memory_stale_run_refs_present", before["warnings"])
            self.assertGreaterEqual(int(before["memory_audit"]["stale_count"]), 1)

            fixed = engine.doctor(fix=True)
            self.assertNotIn("memory_stale_run_refs_present", fixed["warnings"])
            self.assertEqual(int(fixed["memory_audit"]["stale_count"]), 0)
            actions = [item for item in fixed.get("fix_actions", []) if item.get("action") == "memory_clean"]
            self.assertTrue(len(actions) >= 1)
            self.assertGreaterEqual(int(actions[0].get("result", {}).get("deleted_count", 0)), 1)

            with self.assertRaises(ValidationError):
                engine.memory_get(run_id)


if __name__ == "__main__":
    unittest.main()
