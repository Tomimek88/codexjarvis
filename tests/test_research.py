from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.orchestrator import JarvisEngine
from jarvis.research import collect_research_artifacts


class ResearchTests(unittest.TestCase):
    def test_collect_local_research_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            doc_path = root / "docs" / "note.txt"
            doc_path.parent.mkdir(parents=True, exist_ok=True)
            doc_path.write_text("hello research source\n", encoding="utf-8")

            task = {
                "task_id": "task-r-0001",
                "objective": "collect local sources",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {"research_refs": ["docs/note.txt"]},
            }
            bundle, extra_json, extra_text, artifacts = collect_research_artifacts(
                task=task,
                project_root=root,
                run_id="run_abc12345",
            )

            self.assertEqual(bundle["source_count"], 1)
            self.assertEqual(bundle["deduplicated_count"], 0)
            self.assertEqual(len(bundle["errors"]), 0)
            self.assertIn("research/sources_manifest.json", extra_json)
            self.assertIn("research/src_001.txt", extra_text)
            self.assertIn(("research/src_001.txt", "raw"), artifacts)
            source = bundle["sources"][0]
            self.assertEqual(source["provenance"]["retrieval_method"], "filesystem")
            self.assertEqual(Path(source["provenance"]["resolved_path"]).name, "note.txt")

    def test_collect_structured_sources_with_extraction_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            docs = root / "docs"
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "sample.json").write_text('{"alpha": 1, "beta": [2, 3]}', encoding="utf-8")
            (docs / "sample.csv").write_text("a,b\n10,20\n30,40\n", encoding="utf-8")

            task = {
                "task_id": "task-r-0001b",
                "objective": "collect structured local sources",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {"research_refs": ["docs/sample.json", "docs/sample.csv"]},
            }
            bundle, _, extra_text, _ = collect_research_artifacts(
                task=task,
                project_root=root,
                run_id="run_struct_001",
            )

            self.assertEqual(bundle["source_count"], 2)
            self.assertEqual(bundle["deduplicated_count"], 0)
            modes = [item["extraction_mode"] for item in bundle["sources"]]
            self.assertIn("json_pretty", modes)
            self.assertIn("tabular_preview", modes)
            self.assertIn('"alpha": 1', extra_text["research/src_001.txt"])
            self.assertIn("table_preview:", extra_text["research/src_002.txt"])
            for item in bundle["sources"]:
                self.assertTrue(isinstance(item.get("provenance", {}), dict))
                self.assertTrue(bool(item["provenance"].get("fetched_at_utc", "")))

    def test_collect_directory_and_glob_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            docs = root / "docs"
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "a.txt").write_text("A", encoding="utf-8")
            (docs / "b.md").write_text("B", encoding="utf-8")
            (docs / "c.json").write_text('{"c": 1}', encoding="utf-8")

            task = {
                "task_id": "task-r-0001c",
                "objective": "collect directory and glob refs",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {
                    "research_refs": [
                        "docs",
                        "glob://docs/*.md",
                    ],
                    "research_max_files": 10,
                },
            }
            bundle, _, extra_text, _ = collect_research_artifacts(
                task=task,
                project_root=root,
                run_id="run_struct_002",
            )

            self.assertGreaterEqual(bundle["source_count"], 3)
            self.assertGreaterEqual(bundle["deduplicated_count"], 1)
            uris = [item["uri"] for item in bundle["sources"]]
            self.assertIn("docs/a.txt", uris)
            self.assertIn("docs/b.md", uris)
            self.assertIn("docs/c.json", uris)
            self.assertTrue(any(text.strip() == "B" for text in extra_text.values()))
            self.assertTrue(any(item["status"] == "DUPLICATE" for item in bundle["sources"]))

    def test_orchestrator_persists_research_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "docs" / "plan.txt"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("phase steps and assumptions", encoding="utf-8")

            engine = JarvisEngine(root)
            task = {
                "task_id": "task-r-0002",
                "objective": "run with research artifacts",
                "domain": "generic",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "seed": 42,
                    "research_refs": ["docs/plan.txt"],
                },
            }
            out = engine.run(task, dry_run=False)
            artifacts = out["evidence_bundle"]["artifacts"]
            paths = [item["path"] for item in artifacts]
            self.assertTrue(any(path.endswith("/research/sources_manifest.json") for path in paths))
            self.assertTrue(any(path.endswith("/research/src_001.txt") for path in paths))
            self.assertEqual(out["research_bundle"]["source_count"], 1)


if __name__ == "__main__":
    unittest.main()
