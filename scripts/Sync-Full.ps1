param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\src\StockDb.ps1")

Invoke-FullSync -Force:$Force
