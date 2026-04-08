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
        elif args.command == "memory-get":
            payload = engine.memory_get(args.run_id)
        elif args.command == "memory-index":
            payload = engine.index_run(args.run_id)
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
