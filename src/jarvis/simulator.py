from __future__ import annotations

import random
from typing import Any


def execute_domain_simulation(task: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    domain = task["domain"]
    objective = task["objective"]
    params = task.get("parameters", {})
    seed = params.get("seed", 42)
    rng = random.Random(str(seed))

    if domain == "markets":
        steps = _coerce_int(params.get("steps", 30), default=30, min_value=5, max_value=365)
        start_value = float(params.get("start_value", 100.0))
        drift = float(params.get("drift", 0.001))
        vol = float(params.get("vol", 0.01))
        series = [round(start_value, 6)]
        for _ in range(steps):
            shock = rng.uniform(-vol, vol)
            next_value = series[-1] * (1.0 + drift + shock)
            series.append(round(next_value, 6))
        result = {"series": series, "steps": steps}
        metrics = {
            "start_value": series[0],
            "end_value": series[-1],
            "return_pct": round(((series[-1] / series[0]) - 1.0) * 100.0, 6),
        }
    elif domain == "physics":
        mass = float(params.get("mass", 1.0))
        velocity = float(params.get("velocity", 3.0))
        result = {"mass": mass, "velocity": velocity}
        metrics = {"kinetic_energy_j": round(0.5 * mass * velocity * velocity, 6)}
    elif domain == "materials":
        lattice = float(params.get("lattice_constant", 3.6))
        stiffness = float(params.get("stiffness", 120.0))
        result = {"lattice_constant": lattice, "stiffness": stiffness}
        metrics = {"pseudo_binding_energy": round((stiffness / lattice) * 0.01, 6)}
    elif domain == "chemistry":
        atoms = int(params.get("atom_count", 12))
        temperature = float(params.get("temperature", 298.15))
        result = {"atom_count": atoms, "temperature": temperature}
        metrics = {"pseudo_reactivity_score": round((atoms / temperature) * 100.0, 6)}
    else:
        a = float(params.get("a", 1.0))
        b = float(params.get("b", 2.0))
        c = float(params.get("c", 3.0))
        result = {"a": a, "b": b, "c": c}
        metrics = {"weighted_sum": round((a * 0.5) + (b * 1.0) + (c * 1.5), 6)}

    result_payload = {
        "domain": domain,
        "objective": objective,
        "parameters": params,
        "result": result,
        "metrics": metrics,
    }
    stdout_text = (
        f"Simulation completed for domain={domain} objective={objective} seed={seed}\n"
    )
    stderr_text = ""
    summary = {
        "headline": f"Computed {domain} result for task {task['task_id']}",
        "key_metrics": metrics,
        "caveats": [
            "Current engine uses deterministic placeholder models.",
            "Replace with domain-native engines in later phases.",
        ],
    }
    return result_payload, summary, stdout_text, stderr_text


def _coerce_int(value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(min_value, min(max_value, out))
