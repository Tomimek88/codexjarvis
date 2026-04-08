from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class MemoryStore:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.db_path = project_root / "data" / "memory" / "memory.db"

    def ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = self._connect()
        try:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs(
                  run_id TEXT PRIMARY KEY,
                  task_id TEXT NOT NULL,
                  domain TEXT NOT NULL,
                  objective TEXT NOT NULL,
                  cache_key TEXT NOT NULL,
                  timestamp_utc TEXT NOT NULL,
                  status TEXT NOT NULL,
                  input_hash TEXT NOT NULL,
                  params_hash TEXT NOT NULL,
                  code_hash TEXT NOT NULL,
                  env_hash TEXT NOT NULL,
                  seed TEXT,
                  summary_path TEXT NOT NULL,
                  evidence_path TEXT NOT NULL,
                  metrics_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_domain ON runs(domain);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE INDEX IF NOT EXISTS idx_runs_timestamp ON runs(timestamp_utc);
                CREATE INDEX IF NOT EXISTS idx_runs_cache_key ON runs(cache_key);

                CREATE TABLE IF NOT EXISTS artifacts(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  path TEXT NOT NULL,
                  sha256 TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
                """
            )
            con.commit()
        finally:
            con.close()

    def upsert_run(
        self,
        *,
        run_id: str,
        task_id: str,
        domain: str,
        objective: str,
        cache_key: str,
        timestamp_utc: str,
        status: str,
        input_hash: str,
        params_hash: str,
        code_hash: str,
        env_hash: str,
        seed: int | str | None,
        summary_path: str,
        evidence_path: str,
        metrics: dict[str, Any],
        artifacts: list[dict[str, str]],
    ) -> None:
        self.ensure_schema()
        seed_text = str(seed) if seed is not None else ""
        metrics_json = json.dumps(metrics, sort_keys=True, ensure_ascii=True)

        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO runs(
                  run_id, task_id, domain, objective, cache_key, timestamp_utc, status,
                  input_hash, params_hash, code_hash, env_hash, seed,
                  summary_path, evidence_path, metrics_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                  task_id=excluded.task_id,
                  domain=excluded.domain,
                  objective=excluded.objective,
                  cache_key=excluded.cache_key,
                  timestamp_utc=excluded.timestamp_utc,
                  status=excluded.status,
                  input_hash=excluded.input_hash,
                  params_hash=excluded.params_hash,
                  code_hash=excluded.code_hash,
                  env_hash=excluded.env_hash,
                  seed=excluded.seed,
                  summary_path=excluded.summary_path,
                  evidence_path=excluded.evidence_path,
                  metrics_json=excluded.metrics_json
                """,
                (
                    run_id,
                    task_id,
                    domain,
                    objective,
                    cache_key,
                    timestamp_utc,
                    status,
                    input_hash,
                    params_hash,
                    code_hash,
                    env_hash,
                    seed_text,
                    summary_path,
                    evidence_path,
                    metrics_json,
                ),
            )
            con.execute("DELETE FROM artifacts WHERE run_id = ?", (run_id,))
            con.executemany(
                """
                INSERT INTO artifacts(run_id, path, sha256, kind)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        artifact["path"],
                        artifact["sha256"],
                        artifact["kind"],
                    )
                    for artifact in artifacts
                ],
            )
            con.commit()
        finally:
            con.close()

    def query_runs(
        self,
        *,
        limit: int = 20,
        domain: str | None = None,
        status: str | None = None,
        contains: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        clauses: list[str] = []
        params: list[Any] = []

        if domain:
            clauses.append("domain = ?")
            params.append(domain)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if contains:
            clauses.append("(objective LIKE ? OR task_id LIKE ?)")
            like = f"%{contains}%"
            params.extend([like, like])

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            "SELECT run_id, task_id, domain, objective, cache_key, timestamp_utc, status, "
            "input_hash, params_hash, code_hash, env_hash, seed, summary_path, evidence_path, metrics_json "
            f"FROM runs {where_sql} ORDER BY timestamp_utc DESC LIMIT ?"
        )
        params.append(max(1, min(limit, 100)))

        con = self._connect()
        try:
            rows = con.execute(sql, params).fetchall()
        finally:
            con.close()

        output: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)
            record["metrics"] = json.loads(record.pop("metrics_json"))
            output.append(record)
        return output

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        con = self._connect()
        try:
            row = con.execute(
                """
                SELECT run_id, task_id, domain, objective, cache_key, timestamp_utc, status,
                       input_hash, params_hash, code_hash, env_hash, seed, summary_path,
                       evidence_path, metrics_json
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            artifacts = con.execute(
                """
                SELECT path, sha256, kind
                FROM artifacts
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
        finally:
            con.close()

        record = dict(row)
        record["metrics"] = json.loads(record.pop("metrics_json"))
        record["artifacts"] = [dict(item) for item in artifacts]
        return record

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        return con
