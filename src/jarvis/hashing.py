from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_object(data: Any) -> str:
    return sha256_text(canonical_json(data))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_cache_key(
    *,
    domain: str,
    objective: str,
    input_hash: str,
    params_hash: str,
    code_hash: str,
    env_hash: str,
    seed: int | str | None,
) -> str:
    cache_payload = {
        "domain": domain,
        "objective": objective,
        "input_hash": input_hash,
        "params_hash": params_hash,
        "code_hash": code_hash,
        "env_hash": env_hash,
        "seed": seed,
    }
    return sha256_object(cache_payload)


def compute_code_hash(code_dir: Path) -> str:
    py_files = sorted(code_dir.rglob("*.py"))
    payload: list[dict[str, str]] = []
    for path in py_files:
        payload.append({"path": str(path.as_posix()), "sha256": sha256_file(path)})
    return sha256_object(payload)
