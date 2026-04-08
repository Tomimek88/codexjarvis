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

    def test_import_runs_dir_multiple_archives(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            src_root = Path(src_tmp)
            dst_root = Path(dst_tmp)

            src_engine = JarvisEngine(src_root)
            out1 = src_engine.run(_task("task-import-0101"), dry_run=False)
            out2 = src_engine.run(_task("task-import-0102"), dry_run=False)
            zip1 = src_root / src_engine.export_run(out1["run_id"])["zip_path"]
            zip2 = src_root / src_engine.export_run(out2["run_id"])["zip_path"]

            bundle_dir = src_root / "bundle"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "a.zip").write_bytes(zip1.read_bytes())
            (bundle_dir / "b.zip").write_bytes(zip2.read_bytes())

            dst_engine = JarvisEngine(dst_root)
            imported = dst_engine.import_runs_dir(bundle_dir)
            self.assertEqual(imported["status"], "ok")
            self.assertEqual(imported["selected_count"], 2)
            self.assertEqual(imported["imported_count"], 2)
            self.assertEqual(imported["failed_count"], 0)

            self.assertEqual(dst_engine.replay(out1["run_id"])["status"], "ok")
            self.assertEqual(dst_engine.replay(out2["run_id"])["status"], "ok")

    def test_import_runs_dir_stop_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            src_root = Path(src_tmp)
            dst_root = Path(dst_tmp)
            src_engine = JarvisEngine(src_root)
            out = src_engine.run(_task("task-import-0201"), dry_run=False)
            good_zip = src_root / src_engine.export_run(out["run_id"])["zip_path"]

            bundle_dir = src_root / "bundle"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "001_bad.zip").write_text("not a zip", encoding="utf-8")
            (bundle_dir / "002_good.zip").write_bytes(good_zip.read_bytes())

            dst_engine = JarvisEngine(dst_root)
            imported = dst_engine.import_runs_dir(bundle_dir, continue_on_error=False)
            self.assertEqual(imported["status"], "ok")
            self.assertEqual(imported["processed_count"], 1)
            self.assertEqual(imported["imported_count"], 0)
            self.assertEqual(imported["failed_count"], 1)
            self.assertTrue(imported["stopped_early"])


if __name__ == "__main__":
    unittest.main()
