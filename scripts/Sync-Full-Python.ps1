param(
    [string]$PythonExe = (Join-Path $PSScriptRoot "..\tools\python312\runtime\python.exe"),
    [string]$Ticker,
    [int]$Limit,
    [switch]$Force,
    [switch]$Resume,
    [switch]$RefreshUniverse
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
$arguments = @($scriptPath, "full-sync")

if ($Ticker) {
    $arguments += @("--ticker", $Ticker)
}

if ($Limit) {
    $arguments += @("--limit", $Limit)
}

if ($Force) {
    $arguments += "--force"
}

if ($Resume) {
    $arguments += "--resume"
}

if ($RefreshUniverse) {
    $arguments += "--refresh-universe"
}

& $PythonExe @arguments
