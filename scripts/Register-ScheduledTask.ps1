param(
    [string]$DailyTime = "08:00"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\src\StockDb.ps1")

Register-DailyTask -DailyTime $DailyTime
