from __future__ import annotations

import unittest

from jarvis.contracts import ValidationError, validate_task_request


class ContractTests(unittest.TestCase):
    def test_valid_task_passes(self) -> None:
        task = {
            "task_id": "task-1234",
            "objective": "run smoke task",
            "domain": "generic",
            "requires_computation": True,
            "allow_internet_research": True,
            "strict_no_guessing": True,
            "parameters": {"seed": 7},
        }
        validate_task_request(task)

    def test_invalid_no_guessing_fails(self) -> None:
        task = {
            "task_id": "task-1234",
            "objective": "run smoke task",
            "domain": "generic",
            "requires_computation": True,
            "allow_internet_research": True,
            "strict_no_guessing": False,
        }
        with self.assertRaises(ValidationError):
            validate_task_request(task)


if __name__ == "__main__":
    unittest.main()
