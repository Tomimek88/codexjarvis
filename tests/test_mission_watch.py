from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class MissionWatchTests(unittest.TestCase):
    def test_mission_list_filters_and_queue_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            completed = engine.mission_queue(
                objective="Mission list completed alpha",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=True,
                max_cycles=10,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=2,
                idle_stop_after=1,
                generate_report=False,
                generate_dashboard=False,
            )
            queued = engine.mission_queue(
                objective="Mission list queued beta",
                domain="generic",
                parameters={"a": 2, "b": 3, "c": 4, "seed": 42},
                process_now=False,
                generate_report=False,
                generate_dashboard=False,
            )

            all_out = engine.mission_list(limit=20, include_queue_result=True)
            self.assertEqual(all_out["status"], "ok")
            self.assertGreaterEqual(all_out["count"], 2)
            self.assertEqual(all_out["requested_limit"], 20)
            self.assertGreaterEqual(all_out["queue_count"], 2)

            by_job = {row["job_id"]: row for row in all_out["missions"]}
            completed_row = by_job[str(completed["job_id"])]
            queued_row = by_job[str(queued["job_id"])]

            self.assertEqual(completed_row["job_status"], "SUCCESS")
            self.assertEqual(completed_row["mission_status"], "completed")
            self.assertIsInstance(completed_row["queue_result"], dict)
            self.assertEqual(queued_row["job_status"], "QUEUED")
            self.assertEqual(queued_row["mission_status"], "queued")
            self.assertIsNone(queued_row["queue_result"])

            contains_out = engine.mission_list(limit=20, contains="queued beta")
            ids_contains = [row["job_id"] for row in contains_out["missions"]]
            self.assertIn(str(queued["job_id"]), ids_contains)
            self.assertNotIn(str(completed["job_id"]), ids_contains)

            success_out = engine.mission_list(limit=20, status="success")
            ids_success = [row["job_id"] for row in success_out["missions"]]
            self.assertIn(str(completed["job_id"]), ids_success)
            self.assertNotIn(str(queued["job_id"]), ids_success)

    def test_mission_watch_timeout_for_queued_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            queued = engine.mission_queue(
                objective="Mission watch timeout",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                process_now=False,
                generate_report=False,
                generate_dashboard=False,
            )

            out = engine.mission_watch(
                job_id=str(queued["job_id"]),
                timeout_sec=1,
                poll_interval_sec=0.01,
                generate_report=False,
                generate_dashboard=False,
                include_updates=True,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["watch_status"], "timeout")
            self.assertEqual(out["job_id"], str(queued["job_id"]))
            mission = out["mission"]
            assert isinstance(mission, dict)
            self.assertEqual(mission["mission_status"], "queued")
            self.assertEqual(mission["job_status"], "QUEUED")
            updates = out["updates"]
            assert isinstance(updates, list)
            self.assertGreaterEqual(len(updates), 1)

    def test_mission_watch_completed_job_with_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            completed = engine.mission_queue(
                objective="Mission watch completed",
                domain="generic",
                parameters={"a": 3, "b": 4, "c": 5, "seed": 42},
                process_now=True,
                max_cycles=10,
                poll_interval_sec=0.0,
                max_jobs_per_cycle=2,
                idle_stop_after=1,
                generate_report=False,
                generate_dashboard=False,
            )

            out = engine.mission_watch(
                job_id=str(completed["job_id"]),
                timeout_sec=5,
                poll_interval_sec=0.01,
                generate_report=True,
                generate_dashboard=True,
                dashboard_limit=20,
                include_updates=True,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["watch_status"], "completed")
            mission = out["mission"]
            assert isinstance(mission, dict)
            self.assertEqual(mission["status"], "ok")
            self.assertEqual(mission["mission_status"], "completed")
            self.assertEqual(mission["job_status"], "SUCCESS")
            self.assertTrue(bool(mission["run_id"]))

            report = mission["report"]
            assert isinstance(report, dict)
            self.assertEqual(report["status"], "ok")
            report_md = root / str(report["report_md_path"])
            self.assertTrue(report_md.exists())

            dashboard = mission["dashboard"]
            assert isinstance(dashboard, dict)
            self.assertEqual(dashboard["status"], "ok")
            dashboard_file = root / str(dashboard["dashboard_path"])
            self.assertTrue(dashboard_file.exists())

            updates = out["updates"]
            assert isinstance(updates, list)
            self.assertGreaterEqual(len(updates), 1)


if __name__ == "__main__":
    unittest.main()
