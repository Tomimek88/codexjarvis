from __future__ import annotations

import csv
import io
import json
import mimetypes
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .hashing import sha256_bytes


def collect_research_artifacts(
    *,
    task: dict[str, Any],
    project_root: Path,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, str], list[tuple[str, str]]]:
    params = task.get("parameters", {})
    refs = params.get("research_refs", [])
    if not isinstance(refs, list):
        refs = [refs]

    bundle: dict[str, Any] = {
        "task_id": task.get("task_id", ""),
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_count": 0,
        "sources": [],
        "errors": [],
    }
    extra_json_files: dict[str, dict[str, Any]] = {}
    extra_text_files: dict[str, str] = {}
    artifact_candidates: list[tuple[str, str]] = []

    if len(refs) == 0:
        return bundle, extra_json_files, extra_text_files, artifact_candidates

    allow_internet = bool(task.get("allow_internet_research", False))
    max_chars = _coerce_int(params.get("research_max_chars", 20000), default=20000, min_value=200, max_value=200000)

    for idx, item in enumerate(refs, start=1):
        uri, label = _extract_uri_and_label(item)
        source_id = f"src_{idx:03d}"
        source_rel_path = f"research/{source_id}.txt"
        source_record: dict[str, Any] = {
            "source_id": source_id,
            "uri": uri,
            "label": label,
            "status": "FAILED",
            "kind": "unknown",
            "mime_type": "",
            "extraction_mode": "",
            "bytes": 0,
            "sha256": "",
            "snapshot_path": f"data/runs/{run_id}/{source_rel_path}",
            "preview": "",
        }
        try:
            content_bytes, kind, mime_type = _load_source(
                uri=uri,
                project_root=project_root,
                allow_internet=allow_internet,
            )
            content_bytes = content_bytes[: max_chars * 4]
            content_text, extraction_mode = _extract_text_content(
                content_bytes=content_bytes,
                mime_type=mime_type,
                uri=uri,
                max_chars=max_chars,
            )

            source_record["status"] = "OK"
            source_record["kind"] = kind
            source_record["mime_type"] = mime_type
            source_record["extraction_mode"] = extraction_mode
            source_record["bytes"] = len(content_text.encode("utf-8"))
            source_record["sha256"] = sha256_bytes(content_text.encode("utf-8"))
            source_record["preview"] = content_text[:500]

            extra_text_files[source_rel_path] = content_text
            artifact_candidates.append((source_rel_path, "raw"))
        except Exception as exc:  # pragma: no cover
            bundle["errors"].append(
                {
                    "source_id": source_id,
                    "uri": uri,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            source_record["preview"] = ""

        bundle["sources"].append(source_record)

    bundle["source_count"] = len(bundle["sources"])
    extra_json_files["research/sources_manifest.json"] = bundle
    artifact_candidates.append(("research/sources_manifest.json", "report"))
    return bundle, extra_json_files, extra_text_files, artifact_candidates


def _extract_uri_and_label(item: Any) -> tuple[str, str]:
    if isinstance(item, str):
        return item.strip(), item.strip()
    if isinstance(item, dict):
        uri = str(item.get("uri", "")).strip()
        label = str(item.get("label", uri)).strip()
        if not uri:
            raise ValueError("research_refs entry must include non-empty uri.")
        return uri, label or uri
    raise ValueError("research_refs entries must be string or object.")


def _load_source(
    *,
    uri: str,
    project_root: Path,
    allow_internet: bool,
) -> tuple[bytes, str, str]:
    lowered = uri.lower()
    if lowered.startswith("local://"):
        raw_path = uri[len("local://") :]
        path = _resolve_local_path(raw_path, project_root)
        content = path.read_bytes()
        mime_type = mimetypes.guess_type(str(path))[0] or "text/plain"
        return content, "local_file", mime_type

    if lowered.startswith("http://") or lowered.startswith("https://"):
        if not allow_internet:
            raise ValueError("Internet research is disabled for this task.")
        req = urllib.request.Request(
            uri,
            headers={
                "User-Agent": "CodexJarvis/0.1 (+local research fetch)",
                "Accept": "text/plain,text/html,application/json;q=0.9,*/*;q=0.8",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                body = resp.read()
                mime_type = resp.headers.get_content_type() or "application/octet-stream"
            return body, "web_url", mime_type
        except urllib.error.URLError as exc:
            raise ValueError(f"Failed fetching URL '{uri}': {exc}") from exc

    path = _resolve_local_path(uri, project_root)
    content = path.read_bytes()
    mime_type = mimetypes.guess_type(str(path))[0] or "text/plain"
    return content, "local_file", mime_type


def _resolve_local_path(raw_path: str, project_root: Path) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"Local research source not found: {candidate}")
    if candidate.is_dir():
        raise ValueError(f"Local research source must be a file, got directory: {candidate}")
    return candidate


def _coerce_int(value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(min_value, min(max_value, out))


def _extract_text_content(
    *,
    content_bytes: bytes,
    mime_type: str,
    uri: str,
    max_chars: int,
) -> tuple[str, str]:
    lowered_uri = uri.lower()
    lowered_mime = mime_type.lower()

    if "json" in lowered_mime or lowered_uri.endswith(".json"):
        parsed = _extract_json_text(content_bytes)
        if parsed is not None:
            return _clip(parsed, max_chars), "json_pretty"

    if (
        "csv" in lowered_mime
        or lowered_uri.endswith(".csv")
        or lowered_uri.endswith(".tsv")
        or "tab-separated-values" in lowered_mime
    ):
        delimiter = "\t" if lowered_uri.endswith(".tsv") or "tab-separated-values" in lowered_mime else ","
        return _clip(_extract_tabular_preview(content_bytes, delimiter=delimiter), max_chars), "tabular_preview"

    decoded = content_bytes.decode("utf-8", errors="replace")
    if "html" in lowered_mime or lowered_uri.endswith(".html") or lowered_uri.endswith(".htm"):
        stripped = re.sub(r"<[^>]+>", " ", decoded)
        normalized = re.sub(r"\s+", " ", stripped).strip()
        return _clip(normalized, max_chars), "html_text"

    return _clip(decoded, max_chars), "plain_text"


def _extract_json_text(content_bytes: bytes) -> str | None:
    decoded = content_bytes.decode("utf-8", errors="replace")
    try:
        payload = json.loads(decoded)
    except json.JSONDecodeError:
        return None
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)


def _extract_tabular_preview(content_bytes: bytes, *, delimiter: str) -> str:
    decoded = content_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(decoded), delimiter=delimiter)
    rows = list(reader)
    if len(rows) == 0:
        return f"table_preview: empty (delimiter='{delimiter}')"

    header = rows[0]
    data_rows = rows[1:]
    sample_rows = data_rows[:10]
    lines: list[str] = [
        f"table_preview: delimiter='{delimiter}' rows={len(data_rows)} cols={len(header)}",
        f"header: {' | '.join(header)}",
    ]
    for idx, row in enumerate(sample_rows, start=1):
        lines.append(f"row_{idx:02d}: {' | '.join(row)}")
    return "\n".join(lines)


def _clip(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars]
