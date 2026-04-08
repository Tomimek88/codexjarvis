from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


class QueueStore:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.queue_dir = project_root / "data" / "queue"
        self.results_dir = self.queue_dir / "results"
        self.db_path = self.queue_dir / "queue.db"

    def ensure_schema(self) -> None:
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        con = self._connect()
        try:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs(
                  job_id TEXT PRIMARY KEY,
                  task_id TEXT NOT NULL,
                  mode TEXT NOT NULL,
                  status TEXT NOT NULL,
                  created_at_utc TEXT NOT NULL,
                  started_at_utc TEXT,
                  finished_at_utc TEXT,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 1,
                  worker_id TEXT,
                  run_id TEXT,
                  result_path TEXT,
                  last_error TEXT,
                  task_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at_utc);
                CREATE INDEX IF NOT EXISTS idx_jobs_task_id ON jobs(task_id);
                """
            )
            con.commit()
        finally:
            con.close()

    def submit_job(
        self,
        *,
        task: dict[str, Any],
        mode: str,
        max_attempts: int,
    ) -> dict[str, Any]:
        self.ensure_schema()
        job_id = self._new_job_id()
        task_id = str(task.get("task_id", ""))
        now = _now_utc()
        task_json = json.dumps(task, sort_keys=True, ensure_ascii=True)

        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO jobs(
                  job_id, task_id, mode, status, created_at_utc, attempts, max_attempts, task_json
                ) VALUES (?, ?, ?, 'QUEUED', ?, 0, ?, ?)
                """,
                (job_id, task_id, mode, now, max_attempts, task_json),
            )
            con.commit()
        finally:
            con.close()
        return self.get_job(job_id)

    def claim_next_job(self, worker_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                """
                SELECT *
                FROM jobs
                WHERE status = 'QUEUED'
                ORDER BY created_at_utc ASC, job_id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                con.commit()
                return None

            job_id = row["job_id"]
            started = _now_utc()
            updated = con.execute(
                """
                UPDATE jobs
                SET status='RUNNING',
                    started_at_utc=?,
                    worker_id=?,
                    attempts=attempts + 1
                WHERE job_id=? AND status='QUEUED'
                """,
                (started, worker_id, job_id),
            )
            if updated.rowcount != 1:
                con.rollback()
                return None
            claimed = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            con.commit()
        finally:
            con.close()
        return dict(claimed) if claimed is not None else None

    def complete_job(
        self,
        *,
        job_id: str,
        run_id: str | None,
        result_payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.ensure_schema()
        result_path = self._write_result(job_id, result_payload)
        con = self._connect()
        try:
            con.execute(
                """
                UPDATE jobs
                SET status='SUCCESS',
                    finished_at_utc=?,
                    run_id=?,
                    result_path=?,
                    last_error=NULL
                WHERE job_id=?
                """,
                (_now_utc(), run_id or "", result_path, job_id),
            )
            con.commit()
        finally:
            con.close()
        return self.get_job(job_id)

    def fail_job(
        self,
        *,
        job_id: str,
        error: str,
        result_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        con = self._connect()
        try:
            row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise ValueError(f"Queue job '{job_id}' not found.")

            result_path = row["result_path"] or ""
            if result_payload is not None:
                result_path = self._write_result(job_id, result_payload)

            attempts = int(row["attempts"])
            max_attempts = int(row["max_attempts"])
            can_retry = attempts < max_attempts

            if can_retry:
                con.execute(
                    """
                    UPDATE jobs
                    SET status='QUEUED',
                        started_at_utc=NULL,
                        finished_at_utc=NULL,
                        worker_id='',
                        result_path=?,
                        last_error=?
                    WHERE job_id=?
                    """,
                    (result_path, error, job_id),
                )
            else:
                con.execute(
                    """
                    UPDATE jobs
                    SET status='FAILED',
                        finished_at_utc=?,
                        result_path=?,
                        last_error=?
                    WHERE job_id=?
                    """,
                    (_now_utc(), result_path, error, job_id),
                )
            con.commit()
        finally:
            con.close()
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        self.ensure_schema()
        con = self._connect()
        try:
            row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        finally:
            con.close()
        if row is None:
            raise ValueError(f"Queue job '{job_id}' not found.")
        record = dict(row)
        record["task"] = json.loads(record.pop("task_json"))
        return record

    def list_jobs(self, *, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
        self.ensure_schema()
        limit = max(1, min(limit, 200))
        con = self._connect()
        try:
            if status:
                rows = con.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE status = ?
                    ORDER BY created_at_utc DESC, job_id DESC
                    LIMIT ?
                    """,
                    (status, limit),
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT *
                    FROM jobs
                    ORDER BY created_at_utc DESC, job_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            con.close()

        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["task"] = json.loads(item.pop("task_json"))
            out.append(item)
        return out

    def stats(self) -> dict[str, Any]:
        self.ensure_schema()
        con = self._connect()
        try:
            grouped = con.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM jobs
                GROUP BY status
                """
            ).fetchall()
            totals = con.execute(
                """
                SELECT
                  COUNT(*) AS total_jobs,
                  COALESCE(AVG(attempts), 0.0) AS avg_attempts_all,
                  COALESCE(AVG(CASE WHEN status IN ('SUCCESS', 'FAILED') THEN attempts END), 0.0) AS avg_attempts_finished,
                  MIN(CASE WHEN status = 'QUEUED' THEN created_at_utc END) AS oldest_queued_at_utc,
                  MAX(created_at_utc) AS newest_created_at_utc
                FROM jobs
                """
            ).fetchone()
            retry_queued = con.execute(
                """
                SELECT COUNT(*) AS retry_queued_count
                FROM jobs
                WHERE status = 'QUEUED'
                  AND attempts > 0
                  AND attempts < max_attempts
                """
            ).fetchone()
            dead_failed = con.execute(
                """
                SELECT COUNT(*) AS dead_failed_count
                FROM jobs
                WHERE status = 'FAILED'
                  AND attempts >= max_attempts
                """
            ).fetchone()
        finally:
            con.close()

        status_counts = {
            "QUEUED": 0,
            "RUNNING": 0,
            "SUCCESS": 0,
            "FAILED": 0,
        }
        for row in grouped:
            status = str(row["status"])
            status_counts[status] = int(row["cnt"])

        return {
            "status_counts": status_counts,
            "total_jobs": int(totals["total_jobs"]),
            "avg_attempts_all": round(float(totals["avg_attempts_all"]), 6),
            "avg_attempts_finished": round(float(totals["avg_attempts_finished"]), 6),
            "oldest_queued_at_utc": totals["oldest_queued_at_utc"],
            "newest_created_at_utc": totals["newest_created_at_utc"],
            "retry_queued_count": int(retry_queued["retry_queued_count"]),
            "dead_failed_count": int(dead_failed["dead_failed_count"]),
        }

    def _write_result(self, job_id: str, payload: dict[str, Any]) -> str:
        self.results_dir.mkdir(parents=True, exist_ok=True)
        path = self.results_dir / f"{job_id}.json"
        with path.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(payload, f, indent=2, ensure_ascii=True)
            f.write("\n")
        return str(path.relative_to(self.project_root).as_posix())

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _new_job_id() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"job_{stamp}_{uuid4().hex[:8]}"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()
