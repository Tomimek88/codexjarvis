from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class MissionGetTests(unittest.TestCase):
    def test_mission_get_for_queued_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            queued = engine.mission_queue(
                objective="Mission get queued",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=False,
                generate_report=False,
                generate_dashboard=False,
            )
            job_id = str(queued["job_id"])

            out = engine.mission_get(
                job_id=job_id,
                generate_report=True,
                generate_dashboard=False,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "queued")
            self.assertEqual(out["job_status"], "QUEUED")
            self.assertEqual(out["run_id"], "")
            self.assertIn("report_skipped_no_run_id", out["warnings"])
            self.assertIsNone(out["dashboard"])

    def test_mission_get_for_completed_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            completed = engine.mission_queue(
                objective="Mission get completed",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=True,
                poll_interval_sec=0.0,
                max_cycles=10,
                idle_stop_after=1,
                generate_report=False,
                generate_dashboard=False,
            )
            job_id = str(completed["job_id"])

            out = engine.mission_get(
                job_id=job_id,
                generate_report=True,
                generate_dashboard=True,
                dashboard_limit=20,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "completed")
            self.assertEqual(out["job_status"], "SUCCESS")
            self.assertTrue(bool(out["run_id"]))

            report = out["report"]
            assert isinstance(report, dict)
            self.assertEqual(report["status"], "ok")
            report_md = root / str(report["report_md_path"])
            self.assertTrue(report_md.exists())

            dashboard = out["dashboard"]
            assert isinstance(dashboard, dict)
            self.assertEqual(dashboard["status"], "ok")
            dashboard_file = root / str(dashboard["dashboard_path"])
            self.assertTrue(dashboard_file.exists())

    def test_mission_get_missing_job_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            with self.assertRaises(ValueError):
                engine.mission_get(job_id="job_missing")


if __name__ == "__main__":
    unittest.main()
