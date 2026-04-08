from __future__ import annotations

import json
import re
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

                CREATE TABLE IF NOT EXISTS run_memos(
                  run_id TEXT PRIMARY KEY,
                  memo_text TEXT NOT NULL,
                  updated_at_utc TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS run_vectors(
                  run_id TEXT PRIMARY KEY,
                  vector_json TEXT NOT NULL,
                  norm REAL NOT NULL,
                  updated_at_utc TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
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
        memo_text: str | None = None,
    ) -> None:
        self.ensure_schema()
        seed_text = str(seed) if seed is not None else ""
        metrics_json = json.dumps(metrics, sort_keys=True, ensure_ascii=True)
        effective_memo = (memo_text or "").strip()
        if not effective_memo:
            metric_bits = ", ".join(f"{k}={metrics[k]}" for k in sorted(metrics.keys()))
            effective_memo = (
                f"task_id={task_id}; domain={domain}; objective={objective}; "
                f"status={status}; metrics={metric_bits}"
            ).strip()

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

            con.execute(
                """
                INSERT INTO run_memos(run_id, memo_text, updated_at_utc)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(run_id) DO UPDATE SET
                  memo_text=excluded.memo_text,
                  updated_at_utc=excluded.updated_at_utc
                """,
                (run_id, effective_memo),
            )

            vector_text = " ".join([task_id, domain, objective, effective_memo])
            sparse_vector = _build_sparse_vector(vector_text)
            con.execute(
                """
                INSERT INTO run_vectors(run_id, vector_json, norm, updated_at_utc)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(run_id) DO UPDATE SET
                  vector_json=excluded.vector_json,
                  norm=excluded.norm,
                  updated_at_utc=excluded.updated_at_utc
                """,
                (
                    run_id,
                    json.dumps(sparse_vector, sort_keys=True, ensure_ascii=True),
                    _l2_norm(sparse_vector),
                ),
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
        if where_sql:
            where_sql = where_sql.replace("domain = ?", "r.domain = ?").replace("status = ?", "r.status = ?")
            where_sql = where_sql.replace("objective LIKE ?", "r.objective LIKE ?")
            where_sql = where_sql.replace("task_id LIKE ?", "r.task_id LIKE ?")
        sql = (
            "SELECT r.run_id, r.task_id, r.domain, r.objective, r.cache_key, r.timestamp_utc, r.status, "
            "r.input_hash, r.params_hash, r.code_hash, r.env_hash, r.seed, r.summary_path, r.evidence_path, "
            "r.metrics_json, COALESCE(m.memo_text, '') AS memo_text "
            "FROM runs r LEFT JOIN run_memos m ON m.run_id = r.run_id "
            f"{where_sql} ORDER BY r.timestamp_utc DESC LIMIT ?"
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
            record["memo_text"] = str(record.pop("memo_text", ""))
            output.append(record)
        return output

    def search_runs(
        self,
        *,
        query: str,
        limit: int = 10,
        domain: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        text = query.strip().lower()
        tokens = _tokenize(text)
        if len(tokens) == 0:
            return []

        clauses: list[str] = []
        params: list[Any] = []
        if domain:
            clauses.append("r.domain = ?")
            params.append(domain)
        if status:
            clauses.append("r.status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = f"""
            SELECT r.run_id, r.task_id, r.domain, r.objective, r.cache_key, r.timestamp_utc, r.status,
                   r.input_hash, r.params_hash, r.code_hash, r.env_hash, r.seed,
                   r.summary_path, r.evidence_path, r.metrics_json,
                   COALESCE(m.memo_text, '') AS memo_text
            FROM runs r
            LEFT JOIN run_memos m ON m.run_id = r.run_id
            {where_sql}
            ORDER BY r.timestamp_utc DESC
            LIMIT 500
        """

        con = self._connect()
        try:
            rows = con.execute(sql, params).fetchall()
        finally:
            con.close()

        phrase = " ".join(tokens)
        scored: list[dict[str, Any]] = []
        for row in rows:
            rec = dict(row)
            haystack = " ".join(
                [
                    rec.get("objective", ""),
                    rec.get("task_id", ""),
                    rec.get("domain", ""),
                    rec.get("memo_text", ""),
                ]
            ).lower()
            score = 0.0
            for token in tokens:
                score += haystack.count(token)
            if phrase and phrase in haystack:
                score += 2.0
            if score <= 0:
                continue

            metrics = json.loads(rec.pop("metrics_json"))
            memo = rec.pop("memo_text")
            if not memo:
                memo = (
                    f"task_id={rec.get('task_id', '')}; "
                    f"domain={rec.get('domain', '')}; "
                    f"objective={rec.get('objective', '')}"
                )
            rec["metrics"] = metrics
            rec["memo_preview"] = memo[:400]
            rec["score"] = round(score, 6)
            scored.append(rec)

        scored = sorted(scored, key=lambda item: (-item["score"], item["timestamp_utc"]))
        return scored[: max(1, min(limit, 100))]

    def semantic_search_runs(
        self,
        *,
        query: str,
        limit: int = 10,
        domain: str | None = None,
        status: str | None = None,
        min_score: float = 0.0,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        query_vector = _build_sparse_vector(query)
        query_norm = _l2_norm(query_vector)
        if query_norm <= 0.0:
            return []

        clauses: list[str] = []
        params: list[Any] = []
        if domain:
            clauses.append("r.domain = ?")
            params.append(domain)
        if status:
            clauses.append("r.status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        sql = f"""
            SELECT r.run_id, r.task_id, r.domain, r.objective, r.cache_key, r.timestamp_utc, r.status,
                   r.input_hash, r.params_hash, r.code_hash, r.env_hash, r.seed,
                   r.summary_path, r.evidence_path, r.metrics_json,
                   COALESCE(m.memo_text, '') AS memo_text,
                   v.vector_json AS vector_json,
                   COALESCE(v.norm, 0.0) AS vector_norm
            FROM runs r
            LEFT JOIN run_memos m ON m.run_id = r.run_id
            LEFT JOIN run_vectors v ON v.run_id = r.run_id
            {where_sql}
            ORDER BY r.timestamp_utc DESC
            LIMIT 1000
        """

        con = self._connect()
        try:
            rows = con.execute(sql, params).fetchall()
        finally:
            con.close()

        scored: list[dict[str, Any]] = []
        threshold = max(0.0, float(min_score))
        for row in rows:
            rec = dict(row)
            vector_json = rec.pop("vector_json")
            memo_text = str(rec.pop("memo_text", ""))

            if vector_json:
                try:
                    doc_vector = {
                        str(k): float(v)
                        for k, v in json.loads(vector_json).items()
                    }
                except (ValueError, TypeError, json.JSONDecodeError):
                    doc_vector = {}
            else:
                doc_vector = {}

            if len(doc_vector) == 0:
                doc_vector = _build_sparse_vector(
                    " ".join(
                        [
                            str(rec.get("task_id", "")),
                            str(rec.get("domain", "")),
                            str(rec.get("objective", "")),
                            memo_text,
                        ]
                    )
                )

            doc_norm = float(rec.pop("vector_norm", 0.0) or 0.0)
            if doc_norm <= 0.0:
                doc_norm = _l2_norm(doc_vector)
            if doc_norm <= 0.0:
                continue

            score = _cosine_sparse(query_vector, query_norm, doc_vector, doc_norm)
            if score < threshold:
                continue

            metrics = json.loads(rec.pop("metrics_json"))
            if not memo_text:
                memo_text = (
                    f"task_id={rec.get('task_id', '')}; "
                    f"domain={rec.get('domain', '')}; "
                    f"objective={rec.get('objective', '')}"
                )
            rec["metrics"] = metrics
            rec["memo_preview"] = memo_text[:400]
            rec["semantic_score"] = round(score, 6)
            scored.append(rec)

        scored = sorted(scored, key=lambda item: (-item["semantic_score"], item["timestamp_utc"]))
        return scored[: max(1, min(limit, 100))]

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
        record["memo_text"] = self._get_memo_text(run_id)
        return record

    def _get_memo_text(self, run_id: str) -> str:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT memo_text FROM run_memos WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return ""
        return str(row["memo_text"])

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        return con


def _tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9_]+", text.lower()) if len(token) >= 2]


def _build_sparse_vector(text: str) -> dict[str, float]:
    vector: dict[str, float] = {}
    for token in _tokenize(text):
        vector[token] = vector.get(token, 0.0) + 1.0
    return vector


def _l2_norm(vector: dict[str, float]) -> float:
    return sum(value * value for value in vector.values()) ** 0.5


def _cosine_sparse(
    query_vector: dict[str, float],
    query_norm: float,
    doc_vector: dict[str, float],
    doc_norm: float,
) -> float:
    if query_norm <= 0.0 or doc_norm <= 0.0:
        return 0.0

    if len(query_vector) <= len(doc_vector):
        small, large = query_vector, doc_vector
    else:
        small, large = doc_vector, query_vector

    dot = 0.0
    for key, value in small.items():
        dot += value * large.get(key, 0.0)

    if dot <= 0.0:
        return 0.0
    return dot / (query_norm * doc_norm)
