from __future__ import annotations

import re
from typing import Any


def build_metric_claims(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for key in sorted(metrics.keys()):
        value = metrics[key]
        claims.append(
            {
                "claim_id": f"auto_metric_{key}",
                "text": f"{key}={value}",
                "evidence_refs": [f"metrics.{key}"],
                "source": "auto_metric",
            }
        )
    return claims


def normalize_user_claims(raw_claims: Any) -> list[dict[str, Any]]:
    if raw_claims is None:
        return []
    if not isinstance(raw_claims, list):
        return [
            {
                "claim_id": "user_0",
                "text": str(raw_claims),
                "evidence_refs": [],
                "source": "user",
            }
        ]

    normalized: list[dict[str, Any]] = []
    for idx, item in enumerate(raw_claims):
        claim_id = f"user_{idx}"
        if isinstance(item, str):
            normalized.append(
                {
                    "claim_id": claim_id,
                    "text": item,
                    "evidence_refs": [],
                    "source": "user",
                }
            )
            continue
        if isinstance(item, dict):
            text = item.get("text", "")
            refs = item.get("evidence_refs", [])
            normalized.append(
                {
                    "claim_id": str(item.get("claim_id", claim_id)),
                    "text": str(text),
                    "evidence_refs": refs if isinstance(refs, list) else [],
                    "source": "user",
                }
            )
            continue
        normalized.append(
            {
                "claim_id": claim_id,
                "text": str(item),
                "evidence_refs": [],
                "source": "user",
            }
        )
    return normalized


def validate_claims(
    *,
    claims: list[dict[str, Any]],
    evidence_bundle: dict[str, Any],
) -> dict[str, Any]:
    evaluated: list[dict[str, Any]] = []
    supported = 0

    for claim in claims:
        claim_id = str(claim.get("claim_id", "unknown"))
        text = str(claim.get("text", ""))
        source = str(claim.get("source", "unknown"))
        raw_refs = claim.get("evidence_refs", [])
        refs = [str(ref) for ref in raw_refs] if isinstance(raw_refs, list) else []
        unresolved: list[str] = []

        if not refs:
            unresolved.append("missing_evidence_refs")
        else:
            for ref in refs:
                if not _evidence_ref_exists(ref, evidence_bundle):
                    unresolved.append(ref)

        is_supported = len(unresolved) == 0
        if is_supported:
            supported += 1
        evaluated.append(
            {
                "claim_id": claim_id,
                "text": text,
                "source": source,
                "evidence_refs": refs,
                "status": "supported" if is_supported else "unsupported",
                "unresolved_refs": unresolved,
            }
        )

    unsupported = len(evaluated) - supported
    return {
        "all_supported": unsupported == 0,
        "supported_count": supported,
        "unsupported_count": unsupported,
        "claims": evaluated,
    }


def has_unsupported_user_claims(claim_validation: dict[str, Any]) -> bool:
    for claim in claim_validation.get("claims", []):
        if claim.get("source") == "user" and claim.get("status") != "supported":
            return True
    return False


def _evidence_ref_exists(ref: str, evidence_bundle: dict[str, Any]) -> bool:
    if ref.startswith("metrics.exists:"):
        key = ref.split(":", 1)[1]
        return key in evidence_bundle.get("metrics", {})

    if ref.startswith("metrics.value_eq:"):
        raw = ref.split(":", 1)[1]
        if "=" not in raw:
            return False
        key, expected = raw.split("=", 1)
        metrics = evidence_bundle.get("metrics", {})
        if key not in metrics:
            return False
        return str(metrics.get(key)) == expected

    if ref.startswith("metrics."):
        key = ref.split(".", 1)[1]
        return key in evidence_bundle.get("metrics", {})

    if ref == "logs.stdout":
        return bool(evidence_bundle.get("logs", {}).get("stdout", "") is not None)
    if ref == "logs.stderr":
        return bool(evidence_bundle.get("logs", {}).get("stderr", "") is not None)

    if ref.startswith("logs.stdout.contains:"):
        needle = ref.split(":", 1)[1]
        hay = str(evidence_bundle.get("logs", {}).get("stdout", ""))
        return needle.lower() in hay.lower()
    if ref.startswith("logs.stderr.contains:"):
        needle = ref.split(":", 1)[1]
        hay = str(evidence_bundle.get("logs", {}).get("stderr", ""))
        return needle.lower() in hay.lower()
    if ref.startswith("logs.stdout.regex:"):
        pattern = ref.split(":", 1)[1]
        return _matches_regex(pattern, str(evidence_bundle.get("logs", {}).get("stdout", "")))
    if ref.startswith("logs.stderr.regex:"):
        pattern = ref.split(":", 1)[1]
        return _matches_regex(pattern, str(evidence_bundle.get("logs", {}).get("stderr", "")))

    if ref.startswith("artifacts.kind:"):
        kind = ref.split(":", 1)[1]
        return any(a.get("kind") == kind for a in evidence_bundle.get("artifacts", []))

    if ref.startswith("artifacts.path:"):
        target = ref.split(":", 1)[1]
        return any(a.get("path") == target for a in evidence_bundle.get("artifacts", []))

    if ref.startswith("artifacts.path_contains:"):
        fragment = ref.split(":", 1)[1].lower()
        return any(fragment in str(a.get("path", "")).lower() for a in evidence_bundle.get("artifacts", []))

    if ref.startswith("artifacts.path_regex:"):
        pattern = ref.split(":", 1)[1]
        for artifact in evidence_bundle.get("artifacts", []):
            path = str(artifact.get("path", ""))
            if _matches_regex(pattern, path):
                return True
        return False

    return False


def _matches_regex(pattern: str, text: str) -> bool:
    try:
        return re.search(pattern, text) is not None
    except re.error:
        return False
