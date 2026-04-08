from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path

from jarvis.contracts import ValidationError
from jarvis.orchestrator import JarvisEngine


def _task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"export objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


class ExportTests(unittest.TestCase):
    def test_export_run_creates_zip_with_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-export-0001"), dry_run=False)
            run_id = out["run_id"]

            exported = engine.export_run(run_id)
            self.assertEqual(exported["status"], "ok")
            self.assertGreater(exported["files_exported"], 0)
            self.assertGreater(exported["size_bytes"], 0)

            zip_path = root / exported["zip_path"]
            self.assertTrue(zip_path.exists())
            with zipfile.ZipFile(zip_path, mode="r") as zf:
                names = zf.namelist()
                self.assertTrue(any(name.endswith("/evidence_bundle.json") for name in names))
                self.assertTrue(any(name.endswith("/summary.json") for name in names))

    def test_export_run_missing_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            with self.assertRaises(ValidationError):
                engine.export_run("run_missing_123")


if __name__ == "__main__":
    unittest.main()
