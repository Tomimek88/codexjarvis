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
    raw_refs = params.get("research_refs", [])
    if not isinstance(raw_refs, list):
        raw_refs = [raw_refs]
    max_files = _coerce_int(params.get("research_max_files", 50), default=50, min_value=1, max_value=500)
    refs = _expand_research_refs(raw_refs, project_root=project_root, max_files_per_ref=max_files)

    bundle: dict[str, Any] = {
        "task_id": task.get("task_id", ""),
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_count": 0,
        "deduplicated_count": 0,
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
    seen_sources_by_sha: dict[str, dict[str, str]] = {}

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
            "provenance": {},
        }
        try:
            content_bytes, kind, mime_type, provenance = _load_source(
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
            content_sha = sha256_bytes(content_text.encode("utf-8"))
            source_record["sha256"] = content_sha
            source_record["preview"] = content_text[:500]
            source_record["provenance"] = provenance

            if content_sha in seen_sources_by_sha:
                canonical = seen_sources_by_sha[content_sha]
                source_record["status"] = "DUPLICATE"
                source_record["duplicate_of_source_id"] = canonical["source_id"]
                source_record["snapshot_path"] = canonical["snapshot_path"]
                source_record["preview"] = f"Duplicate content of {canonical['source_id']}."
                bundle["deduplicated_count"] = int(bundle.get("deduplicated_count", 0)) + 1
            else:
                seen_sources_by_sha[content_sha] = {
                    "source_id": source_id,
                    "snapshot_path": source_record["snapshot_path"],
                }
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


def _expand_research_refs(
    refs: list[Any],
    *,
    project_root: Path,
    max_files_per_ref: int,
) -> list[Any]:
    expanded: list[Any] = []
    for item in refs:
        expanded.extend(
            _expand_single_ref(
                item=item,
                project_root=project_root,
                max_files=max_files_per_ref,
            )
        )
    return expanded


def _expand_single_ref(
    *,
    item: Any,
    project_root: Path,
    max_files: int,
) -> list[Any]:
    if isinstance(item, str):
        return _expand_uri(uri=item, label=item, project_root=project_root, max_files=max_files, options={})

    if isinstance(item, dict):
        uri = str(item.get("uri", "")).strip()
        label = str(item.get("label", uri)).strip()
        if not uri:
            raise ValueError("research_refs entry must include non-empty uri.")
        return _expand_uri(uri=uri, label=label or uri, project_root=project_root, max_files=max_files, options=item)

    raise ValueError("research_refs entries must be string or object.")


def _expand_uri(
    *,
    uri: str,
    label: str,
    project_root: Path,
    max_files: int,
    options: dict[str, Any],
) -> list[Any]:
    lowered = uri.lower()
    if lowered.startswith("http://") or lowered.startswith("https://") or lowered.startswith("local://"):
        return [{"uri": uri, "label": label}]

    explicit_glob = str(options.get("glob", "")).strip()
    if lowered.startswith("glob://") or explicit_glob:
        if lowered.startswith("glob://"):
            pattern = uri[len("glob://") :].strip()
            base_dir = project_root
        else:
            pattern = explicit_glob
            base_dir = _resolve_directory_base(str(options.get("uri", ".")), project_root)
        recursive = bool(options.get("recursive", True))
        max_for_item = _coerce_int(options.get("max_files", max_files), default=max_files, min_value=1, max_value=500)
        return _expand_glob(
            base_dir=base_dir,
            pattern=pattern,
            recursive=recursive,
            max_files=max_for_item,
            project_root=project_root,
            label_prefix=label,
        )

    candidate = Path(uri)
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    if candidate.exists() and candidate.is_dir():
        pattern = str(options.get("dir_glob", "**/*")).strip() or "**/*"
        recursive = bool(options.get("recursive", True))
        max_for_item = _coerce_int(options.get("max_files", max_files), default=max_files, min_value=1, max_value=500)
        return _expand_glob(
            base_dir=candidate,
            pattern=pattern,
            recursive=recursive,
            max_files=max_for_item,
            project_root=project_root,
            label_prefix=label,
        )

    return [{"uri": uri, "label": label}]


def _expand_glob(
    *,
    base_dir: Path,
    pattern: str,
    recursive: bool,
    max_files: int,
    project_root: Path,
    label_prefix: str,
) -> list[dict[str, str]]:
    if not base_dir.exists() or not base_dir.is_dir():
        return [{"uri": str(base_dir), "label": label_prefix}]

    if recursive:
        candidates = list(base_dir.rglob(pattern))
    else:
        candidates = list(base_dir.glob(pattern))
    files = sorted([path for path in candidates if path.is_file()], key=lambda p: str(p).lower())
    files = files[:max_files]

    out: list[dict[str, str]] = []
    for path in files:
        if path.is_absolute():
            try:
                uri = str(path.relative_to(project_root).as_posix())
            except ValueError:
                uri = str(path)
        else:
            uri = str(path.as_posix())
        out.append({"uri": uri, "label": f"{label_prefix}:{Path(uri).name}"})
    return out


def _resolve_directory_base(raw_path: str, project_root: Path) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    return candidate


def _load_source(
    *,
    uri: str,
    project_root: Path,
    allow_internet: bool,
) -> tuple[bytes, str, str, dict[str, Any]]:
    lowered = uri.lower()
    if lowered.startswith("local://"):
        raw_path = uri[len("local://") :]
        path = _resolve_local_path(raw_path, project_root)
        content = path.read_bytes()
        mime_type = mimetypes.guess_type(str(path))[0] or "text/plain"
        return content, "local_file", mime_type, _build_local_provenance(path)

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
                metadata = {
                    "retrieval_method": "http_get",
                    "status_code": int(resp.getcode() or 0),
                    "final_url": str(resp.geturl() or uri),
                    "content_type": mime_type,
                    "content_length_header": str(resp.headers.get("Content-Length", "")),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            return body, "web_url", mime_type, metadata
        except urllib.error.URLError as exc:
            raise ValueError(f"Failed fetching URL '{uri}': {exc}") from exc

    path = _resolve_local_path(uri, project_root)
    content = path.read_bytes()
    mime_type = mimetypes.guess_type(str(path))[0] or "text/plain"
    return content, "local_file", mime_type, _build_local_provenance(path)


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


def _build_local_provenance(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "retrieval_method": "filesystem",
        "resolved_path": str(path),
        "size_bytes_raw": int(stat.st_size),
        "modified_at_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }
