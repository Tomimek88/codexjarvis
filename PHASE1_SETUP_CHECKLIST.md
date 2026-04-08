# Phase 1 Setup Checklist (local baseline)

## Required Inputs

1. GPU model and VRAM
2. CPU model and core/thread count
3. RAM size
4. Disk free space on workspace drive
5. Preferred runtime path:
   - Docker Desktop + WSL2
   - Native Python + uv/venv
   - Hybrid (recommended)

## Default Assumptions (if not provided)

- OS: Windows 11
- Internet: available
- Strict no-guessing policy: enabled
- Simulations cached and reused by deterministic key

## Phase 1 Acceptance Criteria

1. Environment bootstrap command works from clean machine state.
2. Health checks report model runtime + sandbox runtime + storage paths.
3. Artifact store path exists and is writable.
4. First dry-run creates a valid evidence bundle JSON.

