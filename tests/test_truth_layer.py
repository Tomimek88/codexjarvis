from __future__ import annotations

import unittest

from jarvis.truth_layer import (
    build_metric_claims,
    has_unsupported_user_claims,
    normalize_user_claims,
    validate_claims,
)


class TruthLayerTests(unittest.TestCase):
    def test_metric_claims_are_supported(self) -> None:
        evidence = {
            "metrics": {"alpha": 1.23, "beta": 2.34},
            "logs": {"stdout": "", "stderr": ""},
            "artifacts": [],
        }
        claims = build_metric_claims(evidence["metrics"])
        validation = validate_claims(claims=claims, evidence_bundle=evidence)
        self.assertTrue(validation["all_supported"])
        self.assertEqual(validation["unsupported_count"], 0)

    def test_user_claim_without_refs_is_unsupported(self) -> None:
        evidence = {
            "metrics": {"alpha": 1.23},
            "logs": {"stdout": "", "stderr": ""},
            "artifacts": [],
        }
        user_claims = normalize_user_claims(["Alpha is definitely best."])
        validation = validate_claims(claims=user_claims, evidence_bundle=evidence)
        self.assertFalse(validation["all_supported"])
        self.assertEqual(validation["unsupported_count"], 1)
        self.assertTrue(has_unsupported_user_claims(validation))

    def test_user_claim_with_rich_refs_is_supported(self) -> None:
        evidence = {
            "metrics": {"alpha": 1.23},
            "logs": {
                "stdout": "Run completed in 0.12s with alpha=1.23",
                "stderr": "warning: none",
            },
            "artifacts": [
                {"path": "data/runs/run_x/results/result.json", "kind": "raw"},
                {"path": "data/runs/run_x/research/sources_manifest.json", "kind": "report"},
            ],
        }
        user_claims = normalize_user_claims(
            [
                {
                    "text": "Claim supported by metrics/logs/artifacts",
                    "evidence_refs": [
                        "metrics.exists:alpha",
                        "metrics.value_eq:alpha=1.23",
                        "logs.stdout.contains:completed",
                        "logs.stdout.regex:alpha=1\\.23",
                        "artifacts.path_contains:results/",
                        "artifacts.path_regex:research/.+\\.json",
                    ],
                }
            ]
        )
        validation = validate_claims(claims=user_claims, evidence_bundle=evidence)
        self.assertTrue(validation["all_supported"])
        self.assertEqual(validation["unsupported_count"], 0)

    def test_invalid_regex_ref_is_unsupported(self) -> None:
        evidence = {
            "metrics": {"alpha": 1.23},
            "logs": {"stdout": "ok", "stderr": ""},
            "artifacts": [],
        }
        user_claims = normalize_user_claims(
            [
                {
                    "text": "Bad regex should fail evidence lookup.",
                    "evidence_refs": ["logs.stdout.regex:*invalid["],
                }
            ]
        )
        validation = validate_claims(claims=user_claims, evidence_bundle=evidence)
        self.assertFalse(validation["all_supported"])
        self.assertEqual(validation["unsupported_count"], 1)


if __name__ == "__main__":
    unittest.main()
