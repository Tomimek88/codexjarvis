from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .contracts import ValidationError
from .orchestrator import JarvisEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CodexJarvis local runtime CLI")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Project root path (default: current working directory).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("health", help="Show runtime and storage health.")

    run_parser = subparsers.add_parser("run", help="Run task from task JSON file.")
    run_parser.add_argument("--task-file", type=Path, required=True)

    dry_parser = subparsers.add_parser(
        "dry-run",
        help="Run dry simulation and still produce an evidence bundle.",
    )
    dry_parser.add_argument("--task-file", type=Path, required=True)

    replay_parser = subparsers.add_parser("replay", help="Load evidence for an existing run.")
    replay_parser.add_argument("--run-id", type=str, required=True)

    trace_parser = subparsers.add_parser(
        "trace",
        help="Load run trace + execution manifest + research bundle.",
    )
    trace_parser.add_argument("--run-id", type=str, required=True)

    mem_query_parser = subparsers.add_parser(
        "memory-query",
        help="Query indexed run metadata from SQLite memory store.",
    )
    mem_query_parser.add_argument("--limit", type=int, default=20)
    mem_query_parser.add_argument("--domain", type=str, required=False)
    mem_query_parser.add_argument("--status", type=str, required=False)
    mem_query_parser.add_argument("--contains", type=str, required=False)

    mem_search_parser = subparsers.add_parser(
        "memory-search",
        help="Search run memos and objectives with token-based ranking.",
    )
    mem_search_parser.add_argument("--query", type=str, required=True)
    mem_search_parser.add_argument("--limit", type=int, default=10)
    mem_search_parser.add_argument("--domain", type=str, required=False)
    mem_search_parser.add_argument("--status", type=str, required=False)

    mem_semantic_parser = subparsers.add_parser(
        "memory-semantic-search",
        help="Search runs by semantic similarity over memo/objective sparse vectors.",
    )
    mem_semantic_parser.add_argument("--query", type=str, required=True)
    mem_semantic_parser.add_argument("--limit", type=int, default=10)
    mem_semantic_parser.add_argument("--domain", type=str, required=False)
    mem_semantic_parser.add_argument("--status", type=str, required=False)
    mem_semantic_parser.add_argument("--min-score", type=float, default=0.0)

    mem_hybrid_parser = subparsers.add_parser(
        "memory-hybrid-search",
        help="Hybrid search combining lexical and semantic memory ranking.",
    )
    mem_hybrid_parser.add_argument("--query", type=str, required=True)
    mem_hybrid_parser.add_argument("--limit", type=int, default=10)
    mem_hybrid_parser.add_argument("--domain", type=str, required=False)
    mem_hybrid_parser.add_argument("--status", type=str, required=False)
    mem_hybrid_parser.add_argument("--lexical-weight", type=float, default=0.4)
    mem_hybrid_parser.add_argument("--semantic-weight", type=float, default=0.6)
    mem_hybrid_parser.add_argument("--min-combined-score", type=float, default=0.0)

    mem_get_parser = subparsers.add_parser(
        "memory-get",
        help="Get one indexed run and its artifacts from SQLite memory store.",
    )
    mem_get_parser.add_argument("--run-id", type=str, required=True)

    mem_index_parser = subparsers.add_parser(
        "memory-index",
        help="Index an existing run from data/runs/<run_id> into SQLite memory store.",
    )
    mem_index_parser.add_argument("--run-id", type=str, required=True)

    mem_reindex_parser = subparsers.add_parser(
        "memory-reindex-all",
        help="Reindex existing run directories into SQLite memory store.",
    )
    mem_reindex_parser.add_argument("--limit", type=int, default=0)
    mem_reindex_parser.add_argument("--include-failed", action="store_true")

    queue_submit_parser = subparsers.add_parser(
        "queue-submit",
        help="Submit task file into queue for asynchronous processing.",
    )
    queue_submit_parser.add_argument("--task-file", type=Path, required=True)
    queue_submit_parser.add_argument("--dry-run", action="store_true")
    queue_submit_parser.add_argument("--max-attempts", type=int, default=1)

    queue_list_parser = subparsers.add_parser(
        "queue-list",
        help="List queue jobs.",
    )
    queue_list_parser.add_argument("--limit", type=int, default=20)
    queue_list_parser.add_argument("--status", type=str, required=False)

    queue_get_parser = subparsers.add_parser(
        "queue-get",
        help="Get one queue job by id.",
    )
    queue_get_parser.add_argument("--job-id", type=str, required=True)

    queue_work_once_parser = subparsers.add_parser(
        "queue-work-once",
        help="Process at most one queued job.",
    )
    queue_work_once_parser.add_argument("--worker-id", type=str, required=False)

    queue_work_parser = subparsers.add_parser(
        "queue-work",
        help="Process queued jobs in a loop.",
    )
    queue_work_parser.add_argument("--max-jobs", type=int, default=10)
    queue_work_parser.add_argument("--worker-id", type=str, required=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    engine = JarvisEngine(project_root=args.root.resolve())

    try:
        if args.command == "health":
            payload = engine.health()
        elif args.command == "run":
            payload = engine.run_from_file(args.task_file.resolve(), dry_run=False)
        elif args.command == "dry-run":
            payload = engine.run_from_file(args.task_file.resolve(), dry_run=True)
        elif args.command == "replay":
            payload = engine.replay(args.run_id)
        elif args.command == "trace":
            payload = engine.trace(args.run_id)
        elif args.command == "memory-query":
            payload = engine.memory_query(
                limit=args.limit,
                domain=args.domain,
                status=args.status,
                contains=args.contains,
            )
        elif args.command == "memory-search":
            payload = engine.memory_search(
                query=args.query,
                limit=args.limit,
                domain=args.domain,
                status=args.status,
            )
        elif args.command == "memory-semantic-search":
            payload = engine.memory_semantic_search(
                query=args.query,
                limit=args.limit,
                domain=args.domain,
                status=args.status,
                min_score=args.min_score,
            )
        elif args.command == "memory-hybrid-search":
            payload = engine.memory_hybrid_search(
                query=args.query,
                limit=args.limit,
                domain=args.domain,
                status=args.status,
                lexical_weight=args.lexical_weight,
                semantic_weight=args.semantic_weight,
                min_combined_score=args.min_combined_score,
            )
        elif args.command == "memory-get":
            payload = engine.memory_get(args.run_id)
        elif args.command == "memory-index":
            payload = engine.index_run(args.run_id)
        elif args.command == "memory-reindex-all":
            payload = engine.memory_reindex_all(
                limit=args.limit,
                include_failed=bool(args.include_failed),
            )
        elif args.command == "queue-submit":
            payload = engine.queue_submit_from_file(
                args.task_file.resolve(),
                dry_run=bool(args.dry_run),
                max_attempts=args.max_attempts,
            )
        elif args.command == "queue-list":
            payload = engine.queue_list(limit=args.limit, status=args.status)
        elif args.command == "queue-get":
            payload = engine.queue_get(args.job_id)
        elif args.command == "queue-work-once":
            payload = engine.queue_work_once(worker_id=args.worker_id)
        elif args.command == "queue-work":
            payload = engine.queue_work(max_jobs=args.max_jobs, worker_id=args.worker_id)
        else:  # pragma: no cover
            parser.error(f"Unsupported command: {args.command}")
            return 2
        _print_json(payload)
        return 0
    except (ValidationError, FileNotFoundError, json.JSONDecodeError) as exc:
        _print_json({"status": "error", "error": f"{type(exc).__name__}: {exc}"})
        return 1
    except Exception as exc:  # pragma: no cover
        _print_json({"status": "error", "error": f"UnhandledError: {exc}"})
        return 2


def _print_json(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, indent=2, ensure_ascii=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
