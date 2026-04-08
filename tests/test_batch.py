from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _valid_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"batch objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


def _invalid_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"batch invalid objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": False,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


class BatchRunTests(unittest.TestCase):
    def test_batch_run_continue_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            tasks_dir = root / "batch_tasks"
            tasks_dir.mkdir(parents=True, exist_ok=True)

            (tasks_dir / "001_valid.json").write_text(json.dumps(_valid_task("task-batch-0001")), encoding="utf-8")
            (tasks_dir / "002_invalid.json").write_text(
                json.dumps(_invalid_task("task-batch-0002")), encoding="utf-8"
            )
            (tasks_dir / "003_valid.json").write_text(json.dumps(_valid_task("task-batch-0003")), encoding="utf-8")

            out = engine.batch_run(tasks_dir, continue_on_error=True)
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["discovered_count"], 3)
            self.assertEqual(out["processed_count"], 3)
            self.assertEqual(out["succeeded_count"], 2)
            self.assertEqual(out["failed_count"], 1)
            self.assertFalse(out["stopped_early"])
            self.assertEqual(out["remaining_count"], 0)

    def test_batch_run_stop_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            tasks_dir = root / "batch_tasks"
            tasks_dir.mkdir(parents=True, exist_ok=True)

            (tasks_dir / "001_valid.json").write_text(json.dumps(_valid_task("task-batch-0101")), encoding="utf-8")
            (tasks_dir / "002_invalid.json").write_text(
                json.dumps(_invalid_task("task-batch-0102")), encoding="utf-8"
            )
            (tasks_dir / "003_valid.json").write_text(json.dumps(_valid_task("task-batch-0103")), encoding="utf-8")

            out = engine.batch_run(tasks_dir, continue_on_error=False)
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["selected_count"], 3)
            self.assertEqual(out["processed_count"], 2)
            self.assertEqual(out["succeeded_count"], 1)
            self.assertEqual(out["failed_count"], 1)
            self.assertTrue(out["stopped_early"])
            self.assertEqual(out["remaining_count"], 1)


if __name__ == "__main__":
    unittest.main()
