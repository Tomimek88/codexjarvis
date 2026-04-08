param(
  [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
  throw "Virtual environment not found. Run scripts/bootstrap.ps1 first."
}

$env:PYTHONPATH = (Join-Path $ProjectRoot "src")
& $venvPython -m jarvis --root $ProjectRoot health
if ($LASTEXITCODE -ne 0) { throw "jarvis health failed." }
& $venvPython -m jarvis --root $ProjectRoot dry-run --task-file "$ProjectRoot\examples\tasks\generic_sum_task.json"
if ($LASTEXITCODE -ne 0) { throw "jarvis dry-run failed." }
& $venvPython -m jarvis --root $ProjectRoot run --task-file "$ProjectRoot\examples\tasks\generic_sum_task.json"
if ($LASTEXITCODE -ne 0) { throw "jarvis run failed." }
