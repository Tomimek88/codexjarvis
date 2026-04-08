from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.memory_db import MemoryStore


class MemoryStoreTests(unittest.TestCase):
    def test_upsert_query_and_get(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = MemoryStore(root)
            store.ensure_schema()

            store.upsert_run(
                run_id="run_abc12345",
                task_id="task-0001",
                domain="generic",
                objective="test objective",
                cache_key="x" * 64,
                timestamp_utc="2026-04-08T00:00:00+00:00",
                status="SUCCESS",
                input_hash="a" * 64,
                params_hash="b" * 64,
                code_hash="c" * 64,
                env_hash="d" * 64,
                seed=42,
                summary_path="data/runs/run_abc12345/summary.json",
                evidence_path="data/runs/run_abc12345/evidence_bundle.json",
                metrics={"score": 1.23},
                artifacts=[
                    {
                        "path": "data/runs/run_abc12345/results/result.json",
                        "sha256": "e" * 64,
                        "kind": "raw",
                    }
                ],
            )

            rows = store.query_runs(limit=10, domain="generic")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run_abc12345")
            self.assertEqual(rows[0]["metrics"]["score"], 1.23)

            run = store.get_run("run_abc12345")
            assert run is not None
            self.assertEqual(run["run_id"], "run_abc12345")
            self.assertEqual(len(run["artifacts"]), 1)


if __name__ == "__main__":
    unittest.main()
