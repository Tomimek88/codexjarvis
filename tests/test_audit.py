from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine


def _task(task_id: str, *, a: int, b: int, c: int) -> dict:
    return {
        "task_id": task_id,
        "objective": f"audit objective {task_id}",
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


class AuditTests(unittest.TestCase):
    def test_audit_run_passes_for_fresh_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-audit-0001", a=1, b=2, c=3), dry_run=False)

            audit = engine.audit_run(out["run_id"])
            self.assertEqual(audit["status"], "ok")
            self.assertTrue(audit["passed"])
            self.assertEqual(audit["issue_count"], 0)

    def test_audit_run_detects_artifact_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out = engine.run(_task("task-audit-0002", a=2, b=3, c=4), dry_run=False)
            run_id = out["run_id"]

            evidence = out["evidence_bundle"]
            artifacts = evidence["artifacts"]
            result_rel = next(item["path"] for item in artifacts if item["path"].endswith("/results/result.json"))
            tampered = root / result_rel
            tampered.write_text('{"tampered":true}\n', encoding="utf-8")

            audit = engine.audit_run(run_id)
            self.assertEqual(audit["status"], "ok")
            self.assertFalse(audit["passed"])
            self.assertGreaterEqual(audit["hash_mismatch_count"], 1)
            self.assertTrue(any(issue["code"] == "artifact_hash_mismatch" for issue in audit["issues"]))

    def test_audit_all_aggregates_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = JarvisEngine(root)
            out_ok = engine.run(_task("task-audit-0003", a=3, b=4, c=5), dry_run=False)
            out_bad = engine.run(_task("task-audit-0004", a=7, b=8, c=9), dry_run=False)

            evidence = out_bad["evidence_bundle"]
            result_rel = next(
                item["path"] for item in evidence["artifacts"] if item["path"].endswith("/results/result.json")
            )
            (root / result_rel).write_text("tampered-payload\n", encoding="utf-8")

            all_audit = engine.audit_all(limit=20)
            self.assertEqual(all_audit["status"], "ok")
            self.assertGreaterEqual(all_audit["scanned_count"], 2)
            self.assertGreaterEqual(all_audit["failed_count"], 1)
            failed_ids = [report["run_id"] for report in all_audit["reports"]]
            self.assertIn(out_bad["run_id"], failed_ids)

            all_with_passed = engine.audit_all(limit=20, include_passed=True)
            self.assertEqual(all_with_passed["status"], "ok")
            returned_ids = [report["run_id"] for report in all_with_passed["reports"]]
            self.assertIn(out_ok["run_id"], returned_ids)
            self.assertIn(out_bad["run_id"], returned_ids)


if __name__ == "__main__":
    unittest.main()
