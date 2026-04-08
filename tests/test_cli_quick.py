from __future__ import annotations

import unittest

from jarvis.cli import _parse_json_object_arg, _parse_param_pairs
from jarvis.contracts import ValidationError


class CliQuickParsingTests(unittest.TestCase):
    def test_parse_json_object_arg_accepts_json(self) -> None:
        out = _parse_json_object_arg('{"a":1,"b":2,"flag":true}', "--params-json")
        self.assertEqual(out["a"], 1)
        self.assertEqual(out["b"], 2)
        self.assertTrue(bool(out["flag"]))

    def test_parse_json_object_arg_accepts_relaxed_format(self) -> None:
        out = _parse_json_object_arg("{a:1,b:2,c:true,name:'jarvis'}", "--params-json")
        self.assertEqual(out["a"], 1)
        self.assertEqual(out["b"], 2)
        self.assertTrue(bool(out["c"]))
        self.assertEqual(out["name"], "jarvis")

    def test_parse_param_pairs_parses_scalars(self) -> None:
        out = _parse_param_pairs(
            ["a=1", "b=2.5", "flag=true", "note=hello", "empty=null"],
            "--param",
        )
        self.assertEqual(out["a"], 1)
        self.assertEqual(out["b"], 2.5)
        self.assertTrue(bool(out["flag"]))
        self.assertEqual(out["note"], "hello")
        self.assertIsNone(out["empty"])

    def test_parse_param_pairs_rejects_invalid_pair(self) -> None:
        with self.assertRaises(ValidationError):
            _parse_param_pairs(["badpair"], "--param")


if __name__ == "__main__":
    unittest.main()
