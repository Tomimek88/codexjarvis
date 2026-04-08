from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _task(task_id: str, *, timeout_fail: bool = False) -> dict:
    task = {
        "task_id": task_id,
        "objective": f"dashboard objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }
    if timeout_fail:
        task["parameters"]["simulate_delay_sec"] = 1.2
        task["parameters"]["execution_policy"] = {
            "timeout_sec": 1,
            "max_retries": 0,
            "retry_delay_sec": 0.0,
        }
    return task


class RunsDashboardTests(unittest.TestCase):
    def test_runs_dashboard_generates_html_for_recent_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            ok = engine.run(_task("task-dash-0001"), dry_run=False)
            fail = engine.run(_task("task-dash-0002", timeout_fail=True), dry_run=False)
            self.assertEqual(ok["status"], "completed")
            self.assertEqual(fail["status"], "failed")

            out = engine.runs_dashboard(limit=20, include_failed=True)
            self.assertEqual(out["status"], "ok")
            self.assertGreaterEqual(int(out["run_count"]), 2)

            html_path = root / str(out["dashboard_path"])
            self.assertTrue(html_path.exists())
            html = html_path.read_text(encoding="utf-8")
            self.assertIn("CodexJarvis Runs Dashboard", html)
            self.assertIn(ok["run_id"], html)
            self.assertIn(fail["run_id"], html)

    def test_runs_dashboard_success_only_and_custom_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            ok = engine.run(_task("task-dash-0101"), dry_run=False)
            fail = engine.run(_task("task-dash-0102", timeout_fail=True), dry_run=False)
            self.assertEqual(ok["status"], "completed")
            self.assertEqual(fail["status"], "failed")

            output_file = root / "data" / "reports" / "dashboard_success_only.html"
            out = engine.runs_dashboard(
                limit=20,
                include_failed=False,
                output_file=output_file,
            )
            self.assertEqual(out["status"], "ok")
            self.assertTrue(output_file.exists())
            self.assertEqual(str(out["dashboard_path"]), "data/reports/dashboard_success_only.html")

            html = output_file.read_text(encoding="utf-8")
            self.assertIn(ok["run_id"], html)
            self.assertNotIn(fail["run_id"], html)


if __name__ == "__main__":
    unittest.main()
