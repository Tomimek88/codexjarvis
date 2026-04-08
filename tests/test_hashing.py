from __future__ import annotations

import unittest

from jarvis.hashing import compute_cache_key


class HashingTests(unittest.TestCase):
    def test_cache_key_is_deterministic(self) -> None:
        left = compute_cache_key(
            domain="generic",
            objective="x",
            input_hash="a" * 64,
            params_hash="b" * 64,
            code_hash="c" * 64,
            env_hash="d" * 64,
            seed=42,
        )
        right = compute_cache_key(
            domain="generic",
            objective="x",
            input_hash="a" * 64,
            params_hash="b" * 64,
            code_hash="c" * 64,
            env_hash="d" * 64,
            seed=42,
        )
        self.assertEqual(left, right)


if __name__ == "__main__":
    unittest.main()
