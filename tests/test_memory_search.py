from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


class MemorySearchTests(unittest.TestCase):
    def test_memory_search_finds_run_by_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = {
                "task_id": "task-ms-0001",
                "objective": "Analyze generic weighted sum for baseline verification.",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
            }
            out = engine.run(task, dry_run=False)
            self.assertEqual(out["status"], "completed")

            search = engine.memory_search(query="weighted baseline generic", limit=5)
            self.assertEqual(search["status"], "ok")
            self.assertGreaterEqual(search["count"], 1)
            run_ids = [row["run_id"] for row in search["results"]]
            self.assertIn(out["run_id"], run_ids)

    def test_memory_semantic_search_finds_similar_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            task = {
                "task_id": "task-ms-0002",
                "objective": "Compute deterministic momentum baseline with weighted signal.",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {"a": 2, "b": 5, "c": 7, "seed": 42},
            }
            out = engine.run(task, dry_run=False)
            self.assertEqual(out["status"], "completed")

            semantic = engine.memory_semantic_search(
                query="market weighted momentum strategy",
                limit=5,
                min_score=0.05,
            )
            self.assertEqual(semantic["status"], "ok")
            self.assertGreaterEqual(semantic["count"], 1)
            run_ids = [row["run_id"] for row in semantic["results"]]
            self.assertIn(out["run_id"], run_ids)


if __name__ == "__main__":
    unittest.main()
