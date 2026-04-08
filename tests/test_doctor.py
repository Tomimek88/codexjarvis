from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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

            before = engine.doctor()
            self.assertEqual(before["overall"], "warning")
            self.assertIn("run_integrity_failures_present", before["warnings"])
            self.assertIn("cache_invalid_entries_present", before["warnings"])
            self.assertIn("queue_dead_failed_jobs_present", before["warnings"])

            fixed = engine.doctor(fix=True)
            self.assertTrue(bool(fixed.get("fix_requested")))
            self.assertEqual(fixed["cache_verify"]["invalid_count"], 0)
            self.assertEqual(fixed["queue_stats"]["dead_failed_count"], 0)
            self.assertEqual(fixed["audit_summary"]["failed_count"], 0)
            self.assertGreaterEqual(int(fixed.get("pre_fix_warning_count", 0)), 3)
            self.assertNotIn("cache_invalid_entries_present", fixed["warnings"])
            self.assertNotIn("queue_dead_failed_jobs_present", fixed["warnings"])
            self.assertNotIn("run_integrity_failures_present", fixed["warnings"])
            actions = [str(item.get("action", "")) for item in fixed.get("fix_actions", [])]
            self.assertIn("runs_migrate_legacy", actions)
            self.assertIn("repair_runtime_artifact_hashes", actions)
            self.assertIn("cache_rebuild", actions)
            self.assertIn("queue_requeue_failed", actions)


if __name__ == "__main__":
    unittest.main()
