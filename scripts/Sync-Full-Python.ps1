param(
    [string]$PythonExe = "C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe",
    [string]$Ticker,
    [int]$Limit,
    [switch]$Force
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

& $PythonExe @arguments
