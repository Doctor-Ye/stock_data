param(
    [string]$PythonExe = (Join-Path $PSScriptRoot "..\tools\python312\runtime\python.exe")
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
& $PythonExe $scriptPath status
