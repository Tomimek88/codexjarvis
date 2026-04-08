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


if __name__ == "__main__":
    unittest.main()
