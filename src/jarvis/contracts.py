from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .constants import (
    DOMAINS,
    EVIDENCE_REQUIRED_FIELDS,
    EVIDENCE_STATUSES,
    TASK_OPTIONAL_FIELDS,
    TASK_REQUIRED_FIELDS,
)

HASH_RE = re.compile(r"^[a-fA-F0-9]{64}$")


class ValidationError(ValueError):
    """Raised when a task request or evidence bundle violates contract rules."""


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValidationError(f"Expected top-level JSON object in {path}.")
    return data


def _check_required_keys(data: dict[str, Any], required: set[str], label: str) -> None:
    missing = sorted(required - set(data.keys()))
    if missing:
        raise ValidationError(f"{label} is missing required keys: {', '.join(missing)}")


def _check_no_extra_keys(data: dict[str, Any], allowed: set[str], label: str) -> None:
    extras = sorted(set(data.keys()) - allowed)
    if extras:
        raise ValidationError(f"{label} has unsupported keys: {', '.join(extras)}")


def _check_hash(value: str, label: str) -> None:
    if not isinstance(value, str) or not HASH_RE.match(value):
        raise ValidationError(f"{label} must be a 64-char hex sha256 string.")


def validate_task_request(task: dict[str, Any]) -> None:
    if not isinstance(task, dict):
        raise ValidationError("Task request must be an object.")

    allowed = TASK_REQUIRED_FIELDS | TASK_OPTIONAL_FIELDS
    _check_required_keys(task, TASK_REQUIRED_FIELDS, "Task request")
    _check_no_extra_keys(task, allowed, "Task request")

    if not isinstance(task["task_id"], str) or len(task["task_id"]) < 8:
        raise ValidationError("task_id must be a string with min length 8.")

    if not isinstance(task["objective"], str) or len(task["objective"]) < 3:
        raise ValidationError("objective must be a string with min length 3.")

    domain = task["domain"]
    if domain not in DOMAINS:
        raise ValidationError(f"domain must be one of: {', '.join(sorted(DOMAINS))}")

    if not isinstance(task["requires_computation"], bool):
        raise ValidationError("requires_computation must be a boolean.")

    if task["allow_internet_research"] is not True:
        raise ValidationError("allow_internet_research must be true.")

    if task["strict_no_guessing"] is not True:
        raise ValidationError("strict_no_guessing must be true.")

    force_rerun = task.get("force_rerun", False)
    if not isinstance(force_rerun, bool):
        raise ValidationError("force_rerun must be a boolean when present.")

    input_refs = task.get("input_refs", [])
    if not isinstance(input_refs, list):
        raise ValidationError("input_refs must be an array.")
    for idx, item in enumerate(input_refs):
        if not isinstance(item, dict):
            raise ValidationError(f"input_refs[{idx}] must be an object.")
        required_keys = {"name", "uri"}
        _check_required_keys(item, required_keys, f"input_refs[{idx}]")
        _check_no_extra_keys(item, {"name", "uri", "hash"}, f"input_refs[{idx}]")
        if not isinstance(item["name"], str) or not item["name"]:
            raise ValidationError(f"input_refs[{idx}].name must be a non-empty string.")
        if not isinstance(item["uri"], str) or not item["uri"]:
            raise ValidationError(f"input_refs[{idx}].uri must be a non-empty string.")
        if "hash" in item:
            _check_hash(item["hash"], f"input_refs[{idx}].hash")

    params = task.get("parameters", {})
    if not isinstance(params, dict):
        raise ValidationError("parameters must be an object.")

    criteria = task.get("acceptance_criteria", [])
    if not isinstance(criteria, list) or not all(isinstance(x, str) for x in criteria):
        raise ValidationError("acceptance_criteria must be an array of strings.")


def validate_evidence_bundle(bundle: dict[str, Any]) -> None:
    if not isinstance(bundle, dict):
        raise ValidationError("Evidence bundle must be an object.")
    _check_required_keys(bundle, EVIDENCE_REQUIRED_FIELDS, "Evidence bundle")

    if not isinstance(bundle["run_id"], str) or len(bundle["run_id"]) < 8:
        raise ValidationError("run_id must be a string with min length 8.")

    if bundle["status"] not in EVIDENCE_STATUSES:
        raise ValidationError(
            f"status must be one of: {', '.join(sorted(EVIDENCE_STATUSES))}"
        )

    if bundle["domain"] not in DOMAINS:
        raise ValidationError(f"domain must be one of: {', '.join(sorted(DOMAINS))}")

    for key in ("input_hash", "params_hash", "code_hash", "env_hash"):
        _check_hash(bundle[key], key)

    artifacts = bundle["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) == 0:
        raise ValidationError("artifacts must be a non-empty array.")
    for idx, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            raise ValidationError(f"artifacts[{idx}] must be an object.")
        _check_required_keys(artifact, {"path", "sha256", "kind"}, f"artifacts[{idx}]")
        _check_no_extra_keys(
            artifact, {"path", "sha256", "kind"}, f"artifacts[{idx}]"
        )
        if not isinstance(artifact["path"], str) or not artifact["path"]:
            raise ValidationError(f"artifacts[{idx}].path must be a non-empty string.")
        _check_hash(artifact["sha256"], f"artifacts[{idx}].sha256")
        if artifact["kind"] not in {"raw", "plot", "table", "model", "report"}:
            raise ValidationError(
                f"artifacts[{idx}].kind must be one of raw/plot/table/model/report."
            )

    logs = bundle["logs"]
    if not isinstance(logs, dict):
        raise ValidationError("logs must be an object.")
    _check_required_keys(logs, {"stdout", "stderr"}, "logs")
    if not isinstance(logs["stdout"], str) or not isinstance(logs["stderr"], str):
        raise ValidationError("logs.stdout and logs.stderr must be strings.")

    metrics = bundle["metrics"]
    if not isinstance(metrics, dict):
        raise ValidationError("metrics must be an object.")
