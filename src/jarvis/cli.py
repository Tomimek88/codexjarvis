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
    doctor_parser = subparsers.add_parser("doctor", help="Run consolidated system diagnostics.")
    doctor_parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt automatic fixes for common issues (legacy run files, cache index, failed queue jobs).",
    )
    doctor_parser.add_argument(
        "--queue-prune",
        action="store_true",
        help="With --fix, additionally prune finished queue jobs.",
    )
    doctor_parser.add_argument(
        "--queue-prune-limit",
        type=int,
        default=200,
        help="Max number of queue jobs to prune in one doctor fix pass.",
    )
    doctor_parser.add_argument(
        "--queue-prune-older-than-sec",
        type=int,
        default=86400,
        help="Only prune jobs older than this age (seconds) by finished_at_utc.",
    )
    doctor_parser.add_argument(
        "--queue-prune-delete-results",
        action="store_true",
        help="With --queue-prune, also delete queue result files referenced by pruned jobs.",
    )
    doctor_parser.add_argument(
        "--queue-clean-results",
        action="store_true",
        help="With --fix, clean orphan queue result files not referenced by jobs.",
    )
    doctor_parser.add_argument(
        "--queue-clean-results-limit",
        type=int,
        default=0,
        help="Max number of queue result files to inspect when cleaning orphans (0 = all).",
    )
    doctor_parser.add_argument(
        "--memory-clean",
        action="store_true",
        help="With --fix, remove stale memory index entries pointing to missing run files.",
    )
    doctor_parser.add_argument(
        "--memory-clean-limit",
        type=int,
        default=0,
        help="Max number of memory rows to inspect during memory cleanup (0 = all).",
    )

    run_parser = subparsers.add_parser("run", help="Run task from task JSON file.")
    run_parser.add_argument("--task-file", type=Path, required=True)

    run_quick_parser = subparsers.add_parser(
        "run-quick",
        help="Run one quick task without creating a task JSON file.",
    )
    run_quick_parser.add_argument("--objective", type=str, required=True)
    run_quick_parser.add_argument("--domain", type=str, default="generic")
    run_quick_parser.add_argument("--task-id", type=str, required=False)
    run_quick_parser.add_argument("--params-json", type=str, default="{}")
    run_quick_parser.add_argument("--param", action="append", dest="param_pairs", default=[])
    run_quick_parser.add_argument("--acceptance", action="append", dest="acceptance", default=[])
    run_quick_parser.add_argument("--force-rerun", action="store_true")
    run_quick_parser.add_argument("--dry-run", action="store_true")

    mission_parser = subparsers.add_parser(
        "mission",
        help="Run one mission end-to-end (quick run + optional report + optional dashboard).",
    )
    mission_parser.add_argument("--objective", type=str, required=True)
    mission_parser.add_argument("--domain", type=str, default="generic")
    mission_parser.add_argument("--task-id", type=str, required=False)
    mission_parser.add_argument("--params-json", type=str, default="{}")
    mission_parser.add_argument("--param", action="append", dest="param_pairs", default=[])
    mission_parser.add_argument("--acceptance", action="append", dest="acceptance", default=[])
    mission_parser.add_argument("--force-rerun", action="store_true")
    mission_parser.add_argument("--dry-run", action="store_true")
    mission_parser.add_argument("--no-report", action="store_true")
    mission_parser.add_argument("--no-dashboard", action="store_true")
    mission_parser.add_argument("--dashboard-limit", type=int, default=50)

    mission_queue_parser = subparsers.add_parser(
        "mission-queue",
        help="Submit one mission to queue and optionally process it immediately.",
    )
    mission_queue_parser.add_argument("--objective", type=str, required=True)
    mission_queue_parser.add_argument("--domain", type=str, default="generic")
    mission_queue_parser.add_argument("--task-id", type=str, required=False)
    mission_queue_parser.add_argument("--params-json", type=str, default="{}")
    mission_queue_parser.add_argument("--param", action="append", dest="param_pairs", default=[])
    mission_queue_parser.add_argument("--acceptance", action="append", dest="acceptance", default=[])
    mission_queue_parser.add_argument("--force-rerun", action="store_true")
    mission_queue_parser.add_argument("--dry-run", action="store_true")
    mission_queue_parser.add_argument("--max-attempts", type=int, default=1)
    mission_queue_parser.add_argument("--process-now", action="store_true")
    mission_queue_parser.add_argument("--worker-id", type=str, required=False)
    mission_queue_parser.add_argument("--max-cycles", type=int, default=20)
    mission_queue_parser.add_argument("--poll-interval-sec", type=float, default=1.0)
    mission_queue_parser.add_argument("--max-jobs-per-cycle", type=int, default=10)
    mission_queue_parser.add_argument("--idle-stop-after", type=int, default=1)
    mission_queue_parser.add_argument("--no-report", action="store_true")
    mission_queue_parser.add_argument("--no-dashboard", action="store_true")
    mission_queue_parser.add_argument("--dashboard-limit", type=int, default=50)

    mission_get_parser = subparsers.add_parser(
        "mission-get",
        help="Get mission status by queue job id and optionally generate report/dashboard.",
    )
    mission_get_parser.add_argument("--job-id", type=str, required=True)
    mission_get_parser.add_argument("--no-report", action="store_true")
    mission_get_parser.add_argument("--no-dashboard", action="store_true")
    mission_get_parser.add_argument("--dashboard-limit", type=int, default=50)
    mission_get_parser.add_argument("--dashboard-domain", type=str, required=False)

    batch_parser = subparsers.add_parser(
        "batch-run",
        help="Run multiple task JSON files from a directory.",
    )
    batch_parser.add_argument("--tasks-dir", type=Path, required=True)
    batch_parser.add_argument("--pattern", type=str, default="*.json")
    batch_parser.add_argument("--max-tasks", type=int, default=0)
    batch_parser.add_argument("--dry-run", action="store_true")
    batch_parser.add_argument("--non-recursive", action="store_true")
    batch_parser.add_argument("--stop-on-error", action="store_true")

    task_validate_parser = subparsers.add_parser(
        "task-validate",
        help="Validate one task JSON file against task contract.",
    )
    task_validate_parser.add_argument("--task-file", type=Path, required=True)

    task_validate_dir_parser = subparsers.add_parser(
        "task-validate-dir",
        help="Validate multiple task JSON files in a directory.",
    )
    task_validate_dir_parser.add_argument("--tasks-dir", type=Path, required=True)
    task_validate_dir_parser.add_argument("--pattern", type=str, default="*.json")
    task_validate_dir_parser.add_argument("--max-tasks", type=int, default=0)
    task_validate_dir_parser.add_argument("--non-recursive", action="store_true")
    task_validate_dir_parser.add_argument("--stop-on-error", action="store_true")

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

    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Show one consolidated diagnostics view for a run.",
    )
    inspect_parser.add_argument("--run-id", type=str, required=True)

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare two runs (status/hashes/metrics/artifacts).",
    )
    compare_parser.add_argument("--run-a", type=str, required=True)
    compare_parser.add_argument("--run-b", type=str, required=True)

    export_parser = subparsers.add_parser(
        "export-run",
        help="Export one run directory as a ZIP archive.",
    )
    export_parser.add_argument("--run-id", type=str, required=True)

    import_parser = subparsers.add_parser(
        "import-run",
        help="Import one run ZIP archive into local data/runs.",
    )
    import_parser.add_argument("--zip-file", type=Path, required=True)
    import_parser.add_argument("--skip-memory-index", action="store_true")
    import_parser.add_argument("--skip-cache-link", action="store_true")
    import_parser.add_argument("--overwrite", action="store_true")

    import_dir_parser = subparsers.add_parser(
        "import-runs-dir",
        help="Import multiple run ZIP archives from a directory.",
    )
    import_dir_parser.add_argument("--zips-dir", type=Path, required=True)
    import_dir_parser.add_argument("--pattern", type=str, default="*.zip")
    import_dir_parser.add_argument("--max-files", type=int, default=0)
    import_dir_parser.add_argument("--non-recursive", action="store_true")
    import_dir_parser.add_argument("--stop-on-error", action="store_true")
    import_dir_parser.add_argument("--skip-memory-index", action="store_true")
    import_dir_parser.add_argument("--skip-cache-link", action="store_true")
    import_dir_parser.add_argument("--overwrite", action="store_true")

    report_parser = subparsers.add_parser(
        "report",
        help="Generate run report files (JSON + Markdown).",
    )
    report_parser.add_argument("--run-id", type=str, required=True)

    audit_run_parser = subparsers.add_parser(
        "audit-run",
        help="Audit one run for file/hash integrity.",
    )
    audit_run_parser.add_argument("--run-id", type=str, required=True)

    audit_all_parser = subparsers.add_parser(
        "audit-all",
        help="Audit multiple runs and report integrity issues.",
    )
    audit_all_parser.add_argument("--limit", type=int, default=50)
    audit_all_parser.add_argument("--include-passed", action="store_true")

    runs_list_parser = subparsers.add_parser(
        "runs-list",
        help="List runs from data/runs with optional filters.",
    )
    runs_list_parser.add_argument("--limit", type=int, default=20)
    runs_list_parser.add_argument("--status", type=str, required=False)
    runs_list_parser.add_argument("--domain", type=str, required=False)
    runs_list_parser.add_argument("--contains", type=str, required=False)

    runs_stats_parser = subparsers.add_parser(
        "runs-stats",
        help="Show aggregate run history stats.",
    )
    runs_stats_parser.add_argument("--limit", type=int, default=0)
    runs_stats_parser.add_argument("--domain", type=str, required=False)

    runs_dashboard_parser = subparsers.add_parser(
        "runs-dashboard",
        help="Generate static HTML dashboard for recent runs.",
    )
    runs_dashboard_parser.add_argument("--limit", type=int, default=100)
    runs_dashboard_parser.add_argument("--domain", type=str, required=False)
    runs_dashboard_parser.add_argument("--success-only", action="store_true")
    runs_dashboard_parser.add_argument("--output-file", type=Path, required=False)

    runs_migrate_parser = subparsers.add_parser(
        "runs-migrate-legacy",
        help="Backfill missing legacy run files (execution_manifest/trace).",
    )
    runs_migrate_parser.add_argument("--limit", type=int, default=0)
    runs_migrate_parser.add_argument("--skip-execution-manifest", action="store_true")
    runs_migrate_parser.add_argument("--skip-trace", action="store_true")

    cache_verify_parser = subparsers.add_parser(
        "cache-verify",
        help="Verify cache index entries against run metadata.",
    )
    cache_verify_parser.add_argument("--limit", type=int, default=0)

    cache_rebuild_parser = subparsers.add_parser(
        "cache-rebuild",
        help="Rebuild cache index from existing run metadata.",
    )
    cache_rebuild_parser.add_argument("--limit", type=int, default=0)
    cache_rebuild_parser.add_argument("--include-failed", action="store_true")

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

    mem_audit_parser = subparsers.add_parser(
        "memory-audit",
        help="Audit memory index for stale entries referencing missing run files.",
    )
    mem_audit_parser.add_argument("--limit", type=int, default=0)

    mem_clean_parser = subparsers.add_parser(
        "memory-clean",
        help="Clean stale memory index entries referencing missing run files.",
    )
    mem_clean_parser.add_argument("--limit", type=int, default=0)
    mem_clean_parser.add_argument("--dry-run", action="store_true")

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

    queue_submit_quick_parser = subparsers.add_parser(
        "queue-submit-quick",
        help="Submit one quick task into queue without creating a task JSON file.",
    )
    queue_submit_quick_parser.add_argument("--objective", type=str, required=True)
    queue_submit_quick_parser.add_argument("--domain", type=str, default="generic")
    queue_submit_quick_parser.add_argument("--task-id", type=str, required=False)
    queue_submit_quick_parser.add_argument("--params-json", type=str, default="{}")
    queue_submit_quick_parser.add_argument("--param", action="append", dest="param_pairs", default=[])
    queue_submit_quick_parser.add_argument("--acceptance", action="append", dest="acceptance", default=[])
    queue_submit_quick_parser.add_argument("--force-rerun", action="store_true")
    queue_submit_quick_parser.add_argument("--dry-run", action="store_true")
    queue_submit_quick_parser.add_argument("--max-attempts", type=int, default=1)

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

    subparsers.add_parser(
        "queue-stats",
        help="Show queue aggregate stats and retry/dead-letter counts.",
    )

    queue_requeue_parser = subparsers.add_parser(
        "queue-requeue-failed",
        help="Move failed jobs back to QUEUED state.",
    )
    queue_requeue_parser.add_argument("--limit", type=int, default=20)
    queue_requeue_parser.add_argument("--keep-attempts", action="store_true")

    queue_recover_parser = subparsers.add_parser(
        "queue-recover-running",
        help="Recover stale RUNNING jobs back to QUEUED or FAILED state.",
    )
    queue_recover_parser.add_argument("--limit", type=int, default=20)
    queue_recover_parser.add_argument("--max-age-sec", type=int, default=300)
    queue_recover_parser.add_argument("--force-requeue", action="store_true")
    queue_recover_parser.add_argument("--reset-attempts", action="store_true")

    queue_stale_parser = subparsers.add_parser(
        "queue-stale-running",
        help="List stale RUNNING jobs without modifying queue state.",
    )
    queue_stale_parser.add_argument("--limit", type=int, default=20)
    queue_stale_parser.add_argument("--max-age-sec", type=int, default=300)

    queue_prune_parser = subparsers.add_parser(
        "queue-prune",
        help="Prune old finished queue jobs (SUCCESS/FAILED/CANCELLED).",
    )
    queue_prune_parser.add_argument("--limit", type=int, default=100)
    queue_prune_parser.add_argument("--status", action="append", dest="statuses")
    queue_prune_parser.add_argument("--older-than-sec", type=int, default=0)
    queue_prune_parser.add_argument("--keep-result-files", action="store_true")
    queue_prune_parser.add_argument("--dry-run", action="store_true")

    queue_clean_results_parser = subparsers.add_parser(
        "queue-clean-results",
        help="Delete orphan queue result files not referenced by any job.",
    )
    queue_clean_results_parser.add_argument("--limit", type=int, default=0)
    queue_clean_results_parser.add_argument("--dry-run", action="store_true")

    queue_cancel_parser = subparsers.add_parser(
        "queue-cancel",
        help="Cancel one queue job by id.",
    )
    queue_cancel_parser.add_argument("--job-id", type=str, required=True)
    queue_cancel_parser.add_argument("--reason", type=str, default="")

    queue_work_once_parser = subparsers.add_parser(
        "queue-work-once",
        help="Process at most one queued job.",
    )
    queue_work_once_parser.add_argument("--worker-id", type=str, required=False)

    queue_work_parser = subparsers.add_parser(
        "queue-work",
        help="Process queued jobs in a loop.",
    )
    queue_work_parser.add_argument(
        "--max-jobs",
        type=int,
        default=10,
        help="Max processed jobs in this call. Use 0 to process until idle (with internal safety cap).",
    )
    queue_work_parser.add_argument("--worker-id", type=str, required=False)

    queue_work_daemon_parser = subparsers.add_parser(
        "queue-work-daemon",
        help="Continuously poll and process queue jobs.",
    )
    queue_work_daemon_parser.add_argument("--max-cycles", type=int, default=0)
    queue_work_daemon_parser.add_argument("--poll-interval-sec", type=float, default=2.0)
    queue_work_daemon_parser.add_argument("--max-jobs-per-cycle", type=int, default=10)
    queue_work_daemon_parser.add_argument(
        "--idle-stop-after",
        type=int,
        default=0,
        help="Stop after N consecutive idle cycles (0 = keep polling until max-cycles/safety limit).",
    )
    queue_work_daemon_parser.add_argument("--worker-id", type=str, required=False)
    queue_work_daemon_parser.add_argument("--include-cycle-results", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    engine = JarvisEngine(project_root=args.root.resolve())

    try:
        if args.command == "health":
            payload = engine.health()
        elif args.command == "doctor":
            payload = engine.doctor(
                fix=bool(args.fix),
                queue_prune=bool(args.queue_prune),
                queue_prune_limit=args.queue_prune_limit,
                queue_prune_older_than_sec=args.queue_prune_older_than_sec,
                queue_prune_delete_results=bool(args.queue_prune_delete_results),
                queue_clean_results=bool(args.queue_clean_results),
                queue_clean_results_limit=args.queue_clean_results_limit,
                memory_clean=bool(args.memory_clean),
                memory_clean_limit=args.memory_clean_limit,
            )
        elif args.command == "run":
            payload = engine.run_from_file(args.task_file.resolve(), dry_run=False)
        elif args.command == "run-quick":
            params = _parse_json_object_arg(args.params_json, "--params-json")
            params.update(_parse_param_pairs(list(args.param_pairs or []), "--param"))
            payload = engine.run_quick(
                objective=args.objective,
                domain=args.domain,
                parameters=params,
                task_id=args.task_id,
                force_rerun=bool(args.force_rerun),
                acceptance_criteria=list(args.acceptance or []),
                dry_run=bool(args.dry_run),
            )
        elif args.command == "mission":
            params = _parse_json_object_arg(args.params_json, "--params-json")
            params.update(_parse_param_pairs(list(args.param_pairs or []), "--param"))
            payload = engine.mission(
                objective=args.objective,
                domain=args.domain,
                parameters=params,
                task_id=args.task_id,
                force_rerun=bool(args.force_rerun),
                acceptance_criteria=list(args.acceptance or []),
                dry_run=bool(args.dry_run),
                generate_report=not bool(args.no_report),
                generate_dashboard=not bool(args.no_dashboard),
                dashboard_limit=args.dashboard_limit,
            )
        elif args.command == "mission-queue":
            params = _parse_json_object_arg(args.params_json, "--params-json")
            params.update(_parse_param_pairs(list(args.param_pairs or []), "--param"))
            payload = engine.mission_queue(
                objective=args.objective,
                domain=args.domain,
                parameters=params,
                task_id=args.task_id,
                force_rerun=bool(args.force_rerun),
                acceptance_criteria=list(args.acceptance or []),
                dry_run=bool(args.dry_run),
                max_attempts=args.max_attempts,
                process_now=bool(args.process_now),
                worker_id=args.worker_id,
                max_cycles=args.max_cycles,
                poll_interval_sec=args.poll_interval_sec,
                max_jobs_per_cycle=args.max_jobs_per_cycle,
                idle_stop_after=args.idle_stop_after,
                generate_report=not bool(args.no_report),
                generate_dashboard=not bool(args.no_dashboard),
                dashboard_limit=args.dashboard_limit,
            )
        elif args.command == "mission-get":
            payload = engine.mission_get(
                job_id=args.job_id,
                generate_report=not bool(args.no_report),
                generate_dashboard=not bool(args.no_dashboard),
                dashboard_limit=args.dashboard_limit,
                dashboard_domain=args.dashboard_domain,
            )
        elif args.command == "batch-run":
            payload = engine.batch_run(
                args.tasks_dir.resolve(),
                pattern=args.pattern,
                dry_run=bool(args.dry_run),
                max_tasks=args.max_tasks,
                recursive=not bool(args.non_recursive),
                continue_on_error=not bool(args.stop_on_error),
            )
        elif args.command == "task-validate":
            payload = engine.task_validate(args.task_file.resolve())
        elif args.command == "task-validate-dir":
            payload = engine.task_validate_dir(
                args.tasks_dir.resolve(),
                pattern=args.pattern,
                recursive=not bool(args.non_recursive),
                max_tasks=args.max_tasks,
                stop_on_error=bool(args.stop_on_error),
            )
        elif args.command == "dry-run":
            payload = engine.run_from_file(args.task_file.resolve(), dry_run=True)
        elif args.command == "replay":
            payload = engine.replay(args.run_id)
        elif args.command == "trace":
            payload = engine.trace(args.run_id)
        elif args.command == "inspect":
            payload = engine.inspect(args.run_id)
        elif args.command == "compare":
            payload = engine.compare_runs(args.run_a, args.run_b)
        elif args.command == "export-run":
            payload = engine.export_run(args.run_id)
        elif args.command == "import-run":
            payload = engine.import_run(
                args.zip_file.resolve(),
                index_memory=not bool(args.skip_memory_index),
                link_cache=not bool(args.skip_cache_link),
                overwrite=bool(args.overwrite),
            )
        elif args.command == "import-runs-dir":
            payload = engine.import_runs_dir(
                args.zips_dir.resolve(),
                pattern=args.pattern,
                recursive=not bool(args.non_recursive),
                max_files=args.max_files,
                continue_on_error=not bool(args.stop_on_error),
                index_memory=not bool(args.skip_memory_index),
                link_cache=not bool(args.skip_cache_link),
                overwrite=bool(args.overwrite),
            )
        elif args.command == "report":
            payload = engine.report_run(args.run_id)
        elif args.command == "audit-run":
            payload = engine.audit_run(args.run_id)
        elif args.command == "audit-all":
            payload = engine.audit_all(limit=args.limit, include_passed=bool(args.include_passed))
        elif args.command == "runs-list":
            payload = engine.runs_list(
                limit=args.limit,
                status=args.status,
                domain=args.domain,
                contains=args.contains,
            )
        elif args.command == "runs-stats":
            payload = engine.runs_stats(limit=args.limit, domain=args.domain)
        elif args.command == "runs-dashboard":
            payload = engine.runs_dashboard(
                limit=args.limit,
                domain=args.domain,
                include_failed=not bool(args.success_only),
                output_file=args.output_file.resolve() if args.output_file else None,
            )
        elif args.command == "runs-migrate-legacy":
            payload = engine.runs_migrate_legacy(
                limit=args.limit,
                write_execution_manifest=not bool(args.skip_execution_manifest),
                write_trace=not bool(args.skip_trace),
            )
        elif args.command == "cache-verify":
            payload = engine.cache_verify(limit=args.limit)
        elif args.command == "cache-rebuild":
            payload = engine.cache_rebuild(limit=args.limit, include_failed=bool(args.include_failed))
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
        elif args.command == "memory-audit":
            payload = engine.memory_audit(limit=args.limit)
        elif args.command == "memory-clean":
            payload = engine.memory_clean(limit=args.limit, dry_run=bool(args.dry_run))
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
        elif args.command == "queue-submit-quick":
            params = _parse_json_object_arg(args.params_json, "--params-json")
            params.update(_parse_param_pairs(list(args.param_pairs or []), "--param"))
            payload = engine.queue_submit_quick(
                objective=args.objective,
                domain=args.domain,
                parameters=params,
                task_id=args.task_id,
                force_rerun=bool(args.force_rerun),
                acceptance_criteria=list(args.acceptance or []),
                dry_run=bool(args.dry_run),
                max_attempts=args.max_attempts,
            )
        elif args.command == "queue-list":
            payload = engine.queue_list(limit=args.limit, status=args.status)
        elif args.command == "queue-get":
            payload = engine.queue_get(args.job_id)
        elif args.command == "queue-stats":
            payload = engine.queue_stats()
        elif args.command == "queue-requeue-failed":
            payload = engine.queue_requeue_failed(
                limit=args.limit,
                reset_attempts=not bool(args.keep_attempts),
            )
        elif args.command == "queue-recover-running":
            payload = engine.queue_recover_running(
                limit=args.limit,
                max_age_sec=args.max_age_sec,
                force_requeue=bool(args.force_requeue),
                reset_attempts=bool(args.reset_attempts),
            )
        elif args.command == "queue-stale-running":
            payload = engine.queue_stale_running(
                limit=args.limit,
                max_age_sec=args.max_age_sec,
            )
        elif args.command == "queue-prune":
            payload = engine.queue_prune(
                limit=args.limit,
                statuses=args.statuses,
                older_than_sec=args.older_than_sec,
                delete_results=not bool(args.keep_result_files),
                dry_run=bool(args.dry_run),
            )
        elif args.command == "queue-clean-results":
            payload = engine.queue_clean_results(
                limit=args.limit,
                dry_run=bool(args.dry_run),
            )
        elif args.command == "queue-cancel":
            payload = engine.queue_cancel(args.job_id, reason=args.reason)
        elif args.command == "queue-work-once":
            payload = engine.queue_work_once(worker_id=args.worker_id)
        elif args.command == "queue-work":
            payload = engine.queue_work(max_jobs=args.max_jobs, worker_id=args.worker_id)
        elif args.command == "queue-work-daemon":
            payload = engine.queue_work_daemon(
                max_cycles=args.max_cycles,
                poll_interval_sec=args.poll_interval_sec,
                max_jobs_per_cycle=args.max_jobs_per_cycle,
                idle_stop_after=args.idle_stop_after,
                worker_id=args.worker_id,
                include_cycle_results=bool(args.include_cycle_results),
            )
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


def _parse_json_object_arg(value: str, label: str) -> dict[str, Any]:
    text = str(value).strip() if value is not None else "{}"
    if text == "":
        text = "{}"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        try:
            return _parse_relaxed_object_text(text, label=label)
        except ValidationError:
            raise ValidationError(f"{label} must be valid JSON object text: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValidationError(f"{label} must decode to a JSON object.")
    return parsed


def _parse_relaxed_object_text(text: str, *, label: str) -> dict[str, Any]:
    raw = text.strip()
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1].strip()
    if raw == "":
        return {}

    out: dict[str, Any] = {}
    parts = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    for part in parts:
        if ":" in part:
            key_raw, value_raw = part.split(":", 1)
        elif "=" in part:
            key_raw, value_raw = part.split("=", 1)
        else:
            raise ValidationError(f"{label} relaxed object segment must include ':' or '=': {part}")
        key = key_raw.strip().strip("\"'")
        if key == "":
            raise ValidationError(f"{label} contains an empty key.")
        out[key] = _parse_scalar_value(value_raw.strip())
    return out


def _parse_param_pairs(values: list[str], label: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for raw in values:
        text = str(raw).strip()
        if text == "":
            continue
        if "=" not in text:
            raise ValidationError(f"{label} entries must use key=value format: {text}")
        key_raw, value_raw = text.split("=", 1)
        key = key_raw.strip()
        if key == "":
            raise ValidationError(f"{label} key cannot be empty.")
        out[key] = _parse_scalar_value(value_raw.strip())
    return out


def _parse_scalar_value(text: str) -> Any:
    value = text.strip()
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    if value.startswith("\"") and value.endswith("\"") and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


if __name__ == "__main__":
    raise SystemExit(main())
