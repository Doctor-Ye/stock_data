param(
    [string]$PythonExe = (Join-Path $PSScriptRoot "..\tools\python312\runtime\python.exe"),
    [string]$Ticker,
    [int]$Limit,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
$arguments = @($scriptPath, "daily-update")

if ($Ticker) {
    $arguments += @("--ticker", $Ticker)
}

if ($Limit) {
    $arguments += @("--limit", $Limit)
}

if ($Force) {
    $arguments += "--force"
}

& $PythonExe @arguments
