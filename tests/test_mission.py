from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class MissionTests(unittest.TestCase):
    def test_mission_generates_run_report_and_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission(
                objective="Mission success objective",
                domain="generic",
                parameters={"a": 1, "b": 2, "c": 3, "seed": 42},
                dry_run=False,
                generate_report=True,
                generate_dashboard=True,
                dashboard_limit=20,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "completed")
            self.assertTrue(bool(out["run_id"]))
            self.assertEqual(out["warnings"], [])

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

    def test_mission_can_skip_report_and_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission(
                objective="Mission with disabled outputs",
                domain="generic",
                parameters={"a": 2, "b": 3, "c": 4, "seed": 42},
                generate_report=False,
                generate_dashboard=False,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "completed")
            self.assertIsNone(out["report"])
            self.assertIsNone(out["dashboard"])

    def test_mission_failed_run_still_returns_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.mission(
                objective="Mission fail objective",
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
                dry_run=False,
                generate_report=True,
                generate_dashboard=False,
            )
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["mission_status"], "failed")
            self.assertTrue(bool(out["run_id"]))

            report = out["report"]
            assert isinstance(report, dict)
            self.assertEqual(report["status"], "ok")
            report_json = root / str(report["report_json_path"])
            self.assertTrue(report_json.exists())
            self.assertIsNone(out["dashboard"])


if __name__ == "__main__":
    unittest.main()
