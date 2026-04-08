param(
  [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

function Resolve-Python {
  if (Get-Command python -ErrorAction SilentlyContinue) {
    & python --version *> $null
    if ($LASTEXITCODE -eq 0) {
      return "python"
    }
  }
  $fallbacks = @(
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe"
  )
  foreach ($candidate in $fallbacks) {
    if (Test-Path $candidate) {
      return $candidate
    }
  }
  throw "Python 3.10+ was not found. Install Python and rerun scripts/bootstrap.ps1."
}

$pythonCmd = Resolve-Python
Write-Host "Using Python command: $pythonCmd"

if (-not (Test-Path ".venv")) {
  & $pythonCmd -m venv .venv
}

$venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$env:PYTHONPATH = (Join-Path $ProjectRoot "src")
& $venvPython -m jarvis --root $ProjectRoot health
if ($LASTEXITCODE -ne 0) { throw "jarvis health failed during bootstrap." }

Write-Host "Bootstrap complete. Run:"
Write-Host "`$env:PYTHONPATH='$ProjectRoot\src'; $venvPython -m jarvis --root $ProjectRoot dry-run --task-file $ProjectRoot\examples\tasks\generic_sum_task.json"
