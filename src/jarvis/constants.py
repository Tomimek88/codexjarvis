from __future__ import annotations

FALLBACK_NO_GUESS = "Nevim, musim to nasimulovat."

DOMAINS = {"materials", "chemistry", "physics", "markets", "generic"}

TASK_REQUIRED_FIELDS = {
    "task_id",
    "objective",
    "domain",
    "requires_computation",
    "allow_internet_research",
    "strict_no_guessing",
}

TASK_OPTIONAL_FIELDS = {
    "force_rerun",
    "input_refs",
    "parameters",
    "acceptance_criteria",
}

EVIDENCE_REQUIRED_FIELDS = {
    "run_id",
    "timestamp_utc",
    "status",
    "domain",
    "input_hash",
    "params_hash",
    "code_hash",
    "env_hash",
    "seed",
    "artifacts",
    "logs",
    "metrics",
}

EVIDENCE_STATUSES = {"SUCCESS", "FAILED", "PARTIAL"}
