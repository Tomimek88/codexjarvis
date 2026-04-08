from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis.simulator import execute_domain_simulation


class MarketsEngineTests(unittest.TestCase):
    def test_csv_backtest_outputs_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "inputs"
            data_dir.mkdir(parents=True, exist_ok=True)
            csv_path = data_dir / "prices.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "date,close",
                        "2026-01-01,100",
                        "2026-01-02,101",
                        "2026-01-03,102",
                        "2026-01-04,103",
                        "2026-01-05,102",
                        "2026-01-06,104",
                        "2026-01-07,105",
                        "2026-01-08,106",
                        "2026-01-09,107",
                        "2026-01-10,108",
                        "2026-01-11,109",
                        "2026-01-12,110",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            task = {
                "task_id": "task-market-test",
                "objective": "Backtest sample prices",
                "domain": "markets",
                "requires_computation": True,
                "allow_internet_research": True,
                "strict_no_guessing": True,
                "parameters": {
                    "price_csv_path": "inputs/prices.csv",
                    "short_window": 3,
                    "long_window": 6,
                    "seed": 42,
                },
            }
            payload, summary, stdout_text, stderr_text = execute_domain_simulation(
                task, project_root=root
            )

            self.assertEqual(payload["result"]["engine"], "markets_csv_backtest_v1")
            self.assertEqual(payload["metrics"]["data_points"], 12)
            self.assertIn("strategy_return_pct", payload["metrics"])
            self.assertIn("buy_and_hold_return_pct", payload["metrics"])
            self.assertIn("strategy_max_drawdown_pct", payload["metrics"])
            self.assertTrue(stdout_text.startswith("Simulation completed for domain=markets"))
            self.assertEqual(stderr_text, "")
            self.assertIn("key_metrics", summary)


if __name__ == "__main__":
    unittest.main()
