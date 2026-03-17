param(
    [string]$PythonExe = (Join-Path $PSScriptRoot "..\tools\python312\runtime\python.exe"),
    [int]$BatchSize = 100,
    [int]$PollSeconds = 30,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
$fullSyncScript = Join-Path $PSScriptRoot "Sync-Full-Python.ps1"

function Get-SyncProgress {
    $json = & $PythonExe $scriptPath sync-progress
    return $json | ConvertFrom-Json
}

function Get-ActiveFullSyncCount {
    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -match 'sec_db\.py full-sync|Sync-Full-Python\.ps1'
    }
    return @($processes).Count
}

while ((Get-ActiveFullSyncCount) -gt 1) {
    Start-Sleep -Seconds $PollSeconds
}

$progress = Get-SyncProgress
while ($progress.remainingCompanies -gt 0) {
    $before = $progress.completedCompanies
    $arguments = @(
        "-ExecutionPolicy", "Bypass",
        "-File", $fullSyncScript,
        "-Resume",
        "-Limit", $BatchSize
    )
    if ($Force) {
        $arguments += "-Force"
    }

    & powershell @arguments

    $progress = Get-SyncProgress
    if ($progress.completedCompanies -le $before) {
        throw "Sync did not advance. Last processed ticker: $($progress.lastProcessedTicker)"
    }
}
