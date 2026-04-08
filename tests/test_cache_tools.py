from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _task(task_id: str, *, a: int, b: int, c: int) -> dict:
    return {
        "task_id": task_id,
        "objective": f"cache tool objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {
            "a": a,
            "b": b,
            "c": c,
            "seed": 42,
        },
    }


class CacheToolsTests(unittest.TestCase):
    def test_cache_verify_passes_for_valid_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-cache-0001", a=1, b=2, c=3), dry_run=False)
            self.assertEqual(out["status"], "completed")

            verify = engine.cache_verify()
            self.assertEqual(verify["status"], "ok")
            self.assertGreaterEqual(verify["valid_count"], 1)
            self.assertEqual(verify["invalid_count"], 0)

    def test_cache_verify_detects_missing_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-cache-0002", a=2, b=3, c=4), dry_run=False)
            self.assertEqual(out["status"], "completed")

            index = engine.store.load_cache_index()
            entries = index.get("entries", {})
            cache_key = next(iter(entries.keys()))
            entries[cache_key]["run_id"] = "run_missing_dir_123"
            engine.store.save_cache_index(index)

            verify = engine.cache_verify()
            self.assertEqual(verify["status"], "ok")
            self.assertGreaterEqual(verify["invalid_count"], 1)
            self.assertTrue(any(issue["code"] == "missing_run_dir" for issue in verify["issues"]))

    def test_cache_rebuild_recreates_index_from_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)

            out_success = engine.run(_task("task-cache-0003", a=3, b=4, c=5), dry_run=False)
            self.assertEqual(out_success["status"], "completed")

            failed_task = _task("task-cache-0004", a=1, b=2, c=3)
            failed_task["parameters"]["simulate_delay_sec"] = 1.2
            failed_task["parameters"]["execution_policy"] = {
                "timeout_sec": 1,
                "max_retries": 0,
                "retry_delay_sec": 0.0,
            }
            out_failed = engine.run(failed_task, dry_run=False)
            self.assertEqual(out_failed["status"], "failed")

            engine.store.save_cache_index({"entries": {}})
            rebuild = engine.cache_rebuild()
            self.assertEqual(rebuild["status"], "ok")
            self.assertGreaterEqual(rebuild["processed_runs"], 1)
            self.assertEqual(rebuild["rebuilt_entry_count"], 1)

            verify = engine.cache_verify()
            self.assertEqual(verify["status"], "ok")
            self.assertEqual(verify["invalid_count"], 0)


if __name__ == "__main__":
    unittest.main()
