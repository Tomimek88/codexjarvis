from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Any


def execute_domain_simulation(
    task: dict[str, Any],
    *,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    domain = task["domain"]
    objective = task["objective"]
    params = task.get("parameters", {})
    seed = params.get("seed", 42)
    rng = random.Random(str(seed))

    if domain == "markets":
        csv_rel_path = params.get("price_csv_path")
        if isinstance(csv_rel_path, str) and csv_rel_path.strip():
            csv_path = _resolve_input_path(project_root, csv_rel_path.strip())
            closes, dates = _load_price_series(csv_path)
            if len(closes) < 10:
                raise ValueError("markets CSV must contain at least 10 close prices.")
            short_window = _coerce_int(
                params.get("short_window", 5), default=5, min_value=2, max_value=60
            )
            long_window = _coerce_int(
                params.get("long_window", 20), default=20, min_value=3, max_value=240
            )
            if long_window <= short_window:
                long_window = short_window + 1

            strategy_returns, signals, equity_curve = _backtest_ma_crossover(
                closes=closes,
                short_window=short_window,
                long_window=long_window,
            )
            buy_hold_returns = _returns_from_prices(closes)
            buy_hold_equity = _equity_curve(buy_hold_returns)

            metrics = {
                "data_points": len(closes),
                "start_price": round(closes[0], 6),
                "end_price": round(closes[-1], 6),
                "buy_and_hold_return_pct": round((buy_hold_equity[-1] - 1.0) * 100.0, 6),
                "strategy_return_pct": round((equity_curve[-1] - 1.0) * 100.0, 6),
                "strategy_volatility_pct": round(_stdev(strategy_returns) * 100.0, 6),
                "strategy_max_drawdown_pct": round(_max_drawdown_pct(equity_curve), 6),
                "strategy_sharpe_like": round(_sharpe_like(strategy_returns), 6),
                "long_signals": int(sum(1 for x in signals if x == 1)),
                "flat_signals": int(sum(1 for x in signals if x == 0)),
            }
            result = {
                "engine": "markets_csv_backtest_v1",
                "price_csv_path": str(csv_path),
                "short_window": short_window,
                "long_window": long_window,
                "first_date": dates[0] if dates else None,
                "last_date": dates[-1] if dates else None,
                "sample_prices_tail": [round(x, 6) for x in closes[-10:]],
                "sample_equity_tail": [round(x, 8) for x in equity_curve[-10:]],
                "sample_signals_tail": signals[-10:],
            }
            stdout_text = (
                "Simulation completed for domain=markets "
                f"engine=markets_csv_backtest_v1 rows={len(closes)} seed={seed}\n"
            )
            stderr_text = ""
            summary = {
                "headline": f"Computed markets backtest result for task {task['task_id']}",
                "key_metrics": metrics,
                "caveats": [
                    "Simple moving-average crossover strategy for baseline verification.",
                    "No transaction costs/slippage modeled in v1.",
                ],
            }
            result_payload = {
                "domain": domain,
                "objective": objective,
                "parameters": params,
                "result": result,
                "metrics": metrics,
            }
            return result_payload, summary, stdout_text, stderr_text

        steps = _coerce_int(params.get("steps", 30), default=30, min_value=5, max_value=365)
        start_value = float(params.get("start_value", 100.0))
        drift = float(params.get("drift", 0.001))
        vol = float(params.get("vol", 0.01))
        series = [round(start_value, 6)]
        for _ in range(steps):
            shock = rng.uniform(-vol, vol)
            next_value = series[-1] * (1.0 + drift + shock)
            series.append(round(next_value, 6))
        result = {"engine": "markets_synthetic_v0", "series": series, "steps": steps}
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


def _resolve_input_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input CSV path does not exist: {path}")
    return path


def _load_price_series(csv_path: Path) -> tuple[list[float], list[str]]:
    closes: list[float] = []
    dates: list[str] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV file has no header.")
        close_key = _find_column(reader.fieldnames, ["close", "adj_close", "adj close"])
        date_key = _find_column(reader.fieldnames, ["date", "timestamp", "time"])
        if close_key is None:
            raise ValueError("CSV must include close price column (close/adj_close).")
        for row in reader:
            raw = row.get(close_key, "").strip()
            if not raw:
                continue
            closes.append(float(raw))
            if date_key:
                dates.append(row.get(date_key, "").strip())
            else:
                dates.append("")
    if len(closes) == 0:
        raise ValueError("CSV contains no valid close prices.")
    return closes, dates


def _find_column(columns: list[str], expected: list[str]) -> str | None:
    normalized = {c.strip().lower(): c for c in columns}
    for item in expected:
        if item in normalized:
            return normalized[item]
    return None


def _returns_from_prices(prices: list[float]) -> list[float]:
    returns: list[float] = []
    for idx in range(1, len(prices)):
        prev_price = prices[idx - 1]
        curr_price = prices[idx]
        if prev_price == 0:
            returns.append(0.0)
        else:
            returns.append((curr_price / prev_price) - 1.0)
    return returns


def _equity_curve(returns: list[float]) -> list[float]:
    equity = [1.0]
    for r in returns:
        equity.append(equity[-1] * (1.0 + r))
    return equity


def _backtest_ma_crossover(
    *,
    closes: list[float],
    short_window: int,
    long_window: int,
) -> tuple[list[float], list[int], list[float]]:
    base_returns = _returns_from_prices(closes)
    signals = [0 for _ in closes]
    for idx in range(long_window - 1, len(closes)):
        short_ma = sum(closes[idx - short_window + 1 : idx + 1]) / short_window
        long_ma = sum(closes[idx - long_window + 1 : idx + 1]) / long_window
        signals[idx] = 1 if short_ma > long_ma else 0

    strategy_returns: list[float] = []
    for day in range(1, len(closes)):
        prev_signal = signals[day - 1]
        strategy_returns.append(prev_signal * base_returns[day - 1])
    return strategy_returns, signals, _equity_curve(strategy_returns)


def _stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return var**0.5


def _max_drawdown_pct(equity_curve: list[float]) -> float:
    peak = equity_curve[0]
    worst = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (eq / peak) - 1.0
        if dd < worst:
            worst = dd
    return abs(worst) * 100.0


def _sharpe_like(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    avg = sum(returns) / len(returns)
    std = _stdev(returns)
    if std == 0:
        return 0.0
    # Daily-like scaling for rough comparability.
    return (avg / std) * (252**0.5)
