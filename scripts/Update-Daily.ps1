$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\src\StockDb.ps1")

Invoke-DailyUpdate
