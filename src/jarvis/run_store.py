from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .hashing import sha256_file


class RunStore:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.data_dir = project_root / "data"
        self.runs_dir = self.data_dir / "runs"
        self.cache_dir = self.data_dir / "cache"
        self.memory_dir = self.data_dir / "memory"
        self.cache_index_path = self.cache_dir / "cache_index.json"

    def ensure_layout(self) -> None:
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        if not self.cache_index_path.exists():
            self._write_json(self.cache_index_path, {"entries": {}})

    def new_run_id(self) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"run_{stamp}_{uuid4().hex[:8]}"

    def run_path(self, run_id: str) -> Path:
        return self.runs_dir / run_id

    def load_cache_index(self) -> dict[str, Any]:
        if not self.cache_index_path.exists():
            return {"entries": {}}
        with self.cache_index_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def save_cache_index(self, payload: dict[str, Any]) -> None:
        self._write_json(self.cache_index_path, payload)

    def get_cached_run_id(self, cache_key: str) -> str | None:
        index = self.load_cache_index()
        entry = index.get("entries", {}).get(cache_key)
        if not isinstance(entry, dict):
            return None
        run_id = entry.get("run_id")
        return run_id if isinstance(run_id, str) else None

    def set_cache_entry(self, cache_key: str, run_id: str) -> None:
        index = self.load_cache_index()
        entries = index.setdefault("entries", {})
        entries[cache_key] = {
            "run_id": run_id,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        self.save_cache_index(index)

    def save_run_files(
        self,
        *,
        run_id: str,
        meta: dict[str, Any],
        input_manifest: dict[str, Any],
        params: dict[str, Any],
        stdout_text: str,
        stderr_text: str,
        result_payload: dict[str, Any],
        summary_payload: dict[str, Any],
        evidence_bundle: dict[str, Any],
    ) -> list[dict[str, str]]:
        run_dir = self.run_path(run_id)
        results_dir = run_dir / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        files = {
            "meta.json": meta,
            "input_manifest.json": input_manifest,
            "params.json": params,
            "summary.json": summary_payload,
            "evidence_bundle.json": evidence_bundle,
        }
        for filename, payload in files.items():
            self._write_json(run_dir / filename, payload)

        self._write_text(run_dir / "stdout.log", stdout_text)
        self._write_text(run_dir / "stderr.log", stderr_text)
        self._write_json(results_dir / "result.json", result_payload)

        artifact_candidates = [
            (results_dir / "result.json", "raw"),
            (run_dir / "summary.json", "report"),
        ]

        artifacts: list[dict[str, str]] = []
        for file_path, kind in artifact_candidates:
            artifacts.append(
                {
                    "path": str(file_path.relative_to(self.project_root).as_posix()),
                    "sha256": sha256_file(file_path),
                    "kind": kind,
                }
            )
        return artifacts

    def load_evidence(self, run_id: str) -> dict[str, Any]:
        evidence_path = self.run_path(run_id) / "evidence_bundle.json"
        with evidence_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def run_exists(self, run_id: str) -> bool:
        return self.run_path(run_id).exists()

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(payload, f, indent=2, ensure_ascii=True)
            f.write("\n")

    @staticmethod
    def _write_text(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as f:
            f.write(text)
