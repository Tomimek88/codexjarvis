from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jarvis.contracts import ValidationError
from jarvis.orchestrator import JarvisEngine


def _valid_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"validate objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


def _invalid_task(task_id: str) -> dict:
    data = _valid_task(task_id)
    data["strict_no_guessing"] = False
    return data


class TaskValidateTests(unittest.TestCase):
    def test_task_validate_single(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task_path = root / "task_valid.json"
            task_path.write_text(json.dumps(_valid_task("task-val-0001")), encoding="utf-8")

            out = engine.task_validate(task_path)
            self.assertEqual(out["status"], "ok")
            self.assertTrue(out["valid"])
            self.assertEqual(out["task_id"], "task-val-0001")

    def test_task_validate_single_invalid_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task_path = root / "task_invalid.json"
            task_path.write_text(json.dumps(_invalid_task("task-val-0002")), encoding="utf-8")

            with self.assertRaises(ValidationError):
                engine.task_validate(task_path)

    def test_task_validate_dir_mixed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            tasks_dir = root / "tasks"
            tasks_dir.mkdir(parents=True, exist_ok=True)

            (tasks_dir / "001_valid.json").write_text(
                json.dumps(_valid_task("task-val-0101")),
                encoding="utf-8",
            )
            (tasks_dir / "002_invalid.json").write_text(
                json.dumps(_invalid_task("task-val-0102")),
                encoding="utf-8",
            )
            (tasks_dir / "003_valid.json").write_text(
                json.dumps(_valid_task("task-val-0103")),
                encoding="utf-8",
            )

            out = engine.task_validate_dir(tasks_dir)
            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["selected_count"], 3)
            self.assertEqual(out["processed_count"], 3)
            self.assertEqual(out["valid_count"], 2)
            self.assertEqual(out["invalid_count"], 1)
            self.assertFalse(out["stopped_early"])

            out_stop = engine.task_validate_dir(tasks_dir, stop_on_error=True)
            self.assertEqual(out_stop["status"], "ok")
            self.assertEqual(out_stop["processed_count"], 2)
            self.assertEqual(out_stop["valid_count"], 1)
            self.assertEqual(out_stop["invalid_count"], 1)
            self.assertTrue(out_stop["stopped_early"])
            self.assertEqual(out_stop["remaining_count"], 1)


if __name__ == "__main__":
    unittest.main()
