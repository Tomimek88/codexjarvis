from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.contracts import ValidationError
from jarvis.orchestrator import JarvisEngine


def _task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"import objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


class ImportTests(unittest.TestCase):
    def test_import_run_from_export_zip(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            src_root = Path(src_tmp)
            dst_root = Path(dst_tmp)

            src_engine = JarvisEngine(src_root)
            out = src_engine.run(_task("task-import-0001"), dry_run=False)
            run_id = out["run_id"]
            exported = src_engine.export_run(run_id)
            zip_abs = src_root / exported["zip_path"]
            self.assertTrue(zip_abs.exists())

            dst_engine = JarvisEngine(dst_root)
            imported = dst_engine.import_run(zip_abs)
            self.assertEqual(imported["status"], "ok")
            self.assertEqual(imported["run_id"], run_id)
            self.assertTrue(imported["memory_indexed"])

            replay = dst_engine.replay(run_id)
            self.assertEqual(replay["status"], "ok")
            mem = dst_engine.memory_get(run_id)
            self.assertEqual(mem["status"], "ok")

    def test_import_run_existing_requires_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            src_root = Path(src_tmp)
            dst_root = Path(dst_tmp)

            src_engine = JarvisEngine(src_root)
            out = src_engine.run(_task("task-import-0002"), dry_run=False)
            run_id = out["run_id"]
            exported = src_engine.export_run(run_id)
            zip_abs = src_root / exported["zip_path"]

            dst_engine = JarvisEngine(dst_root)
            first = dst_engine.import_run(zip_abs)
            self.assertEqual(first["status"], "ok")
            self.assertEqual(first["run_id"], run_id)

            with self.assertRaises(ValidationError):
                dst_engine.import_run(zip_abs)

            second = dst_engine.import_run(zip_abs, overwrite=True)
            self.assertEqual(second["status"], "ok")
            self.assertTrue(second["overwrite"])


if __name__ == "__main__":
    unittest.main()
