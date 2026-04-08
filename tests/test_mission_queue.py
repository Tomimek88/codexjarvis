from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class MissionQueueTests(unittest.TestCase):
    def test_mission_queue_submit_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission_queue(
                objective="Mission queue submit only",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=False,
                generate_report=True,
                generate_dashboard=False,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "queued")
            self.assertEqual(out["job_status"], "QUEUED")
            self.assertTrue(bool(out["job_id"]))
            self.assertEqual(out["report"], None)
            self.assertEqual(out["dashboard"], None)
            self.assertIn("report_skipped_queue_not_processed", out["warnings"])

    def test_mission_queue_process_now_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission_queue(
                objective="Mission queue success",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=True,
                max_cycles=10,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=2,
                idle_stop_after=1,
                generate_report=True,
                generate_dashboard=True,
                dashboard_limit=20,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "completed")
            self.assertEqual(out["job_status"], "SUCCESS")
            self.assertTrue(bool(out["run_id"]))

            daemon = out["daemon"]
            assert isinstance(daemon, dict)
            self.assertEqual(daemon["status"], "ok")

            report = out["report"]
            assert isinstance(report, dict)
            self.assertEqual(report["status"], "ok")
            report_md = root / str(report["report_md_path"])
            self.assertTrue(report_md.exists())

            dashboard = out["dashboard"]
            assert isinstance(dashboard, dict)
            self.assertEqual(dashboard["status"], "ok")
            dashboard_path = root / str(dashboard["dashboard_path"])
            self.assertTrue(dashboard_path.exists())

    def test_mission_queue_process_now_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission_queue(
                objective="Mission queue failed",
                domain="generic",
                parameters={
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "seed": 42,
                    "simulate_delay_sec": 1.2,
                    "execution_policy": {
                        "timeout_sec": 1,
                        "max_retries": 0,
                        "retry_delay_sec": 0.0,
                    },
                },
                max_attempts=1,
                process_now=True,
                max_cycles=10,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=2,
                idle_stop_after=1,
                generate_report=True,
                generate_dashboard=False,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "failed")
            self.assertEqual(out["job_status"], "FAILED")
            self.assertTrue(bool(out["run_id"]))

            report = out["report"]
            assert isinstance(report, dict)
            self.assertEqual(report["status"], "ok")
            report_json = root / str(report["report_json_path"])
            self.assertTrue(report_json.exists())
            self.assertEqual(out["dashboard"], None)


if __name__ == "__main__":
    unittest.main()
