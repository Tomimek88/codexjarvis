#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD="python"
else
  echo "Python 3.10+ not found. Install it and re-run scripts/bootstrap.sh."
  exit 1
fi

echo "Using Python command: ${PYTHON_CMD}"

if [[ ! -d ".venv" ]]; then
  "${PYTHON_CMD}" -m venv .venv
fi

VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"
export PYTHONPATH="${PROJECT_ROOT}/src"
"${VENV_PYTHON}" -m jarvis --root "${PROJECT_ROOT}" health

echo "Bootstrap complete."
echo "Run:"
echo "PYTHONPATH=${PROJECT_ROOT}/src ${VENV_PYTHON} -m jarvis --root ${PROJECT_ROOT} dry-run --task-file ${PROJECT_ROOT}/examples/tasks/generic_sum_task.json"
