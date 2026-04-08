from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "objective": f"migrate objective {task_id}",
        "domain": "generic",
        "requires_computation": True,
        "allow_internet_research": True,
        "strict_no_guessing": True,
        "force_rerun": True,
        "parameters": {"a": 1, "b": 2, "c": 3, "seed": 42},
    }


class LegacyMigrateTests(unittest.TestCase):
    def test_runs_migrate_legacy_backfills_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-migrate-0001"), dry_run=False)
            run_id = out["run_id"]

            run_dir = root / "data" / "runs" / run_id
            (run_dir / "execution_manifest.json").unlink(missing_ok=True)
            (run_dir / "trace.json").unlink(missing_ok=True)

            evidence_path = run_dir / "evidence_bundle.json"
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
            artifacts = evidence.get("artifacts", [])
            if isinstance(artifacts, list):
                evidence["artifacts"] = [
                    item
                    for item in artifacts
                    if not str(item.get("path", "")).endswith("/execution_manifest.json")
                    and not str(item.get("path", "")).endswith("/trace.json")
                ]
            evidence_path.write_text(json.dumps(evidence, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

            before = engine.audit_run(run_id)
            self.assertFalse(before["passed"])
            self.assertTrue(any(issue["code"] == "missing_required_file" for issue in before["issues"]))

            migrated = engine.runs_migrate_legacy(limit=10)
            self.assertEqual(migrated["status"], "ok")
            self.assertGreaterEqual(migrated["migrated_runs"], 1)
            self.assertTrue((run_dir / "execution_manifest.json").exists())
            self.assertTrue((run_dir / "trace.json").exists())

            after = engine.audit_run(run_id)
            self.assertTrue(after["passed"])
            self.assertEqual(after["issue_count"], 0)


if __name__ == "__main__":
    unittest.main()
