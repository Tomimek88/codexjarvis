#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "Virtual environment not found. Run scripts/bootstrap.sh first."
  exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}/src"
"${VENV_PYTHON}" -m jarvis --root "${PROJECT_ROOT}" health
"${VENV_PYTHON}" -m jarvis --root "${PROJECT_ROOT}" dry-run --task-file "${PROJECT_ROOT}/examples/tasks/generic_sum_task.json"
"${VENV_PYTHON}" -m jarvis --root "${PROJECT_ROOT}" run --task-file "${PROJECT_ROOT}/examples/tasks/generic_sum_task.json"
