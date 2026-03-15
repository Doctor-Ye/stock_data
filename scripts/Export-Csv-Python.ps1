param(
    [string]$PythonExe = "C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe",
    [Parameter(Mandatory = $true)]
    [string]$Table,
    [Parameter(Mandatory = $true)]
    [string]$Output,
    [string]$Where,
    [string]$OrderBy,
    [int]$Limit
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
$arguments = @($scriptPath, "export-csv", "--table", $Table, "--output", $Output)

if ($Where) {
    $arguments += @("--where", $Where)
}

if ($OrderBy) {
    $arguments += @("--order-by", $OrderBy)
}

if ($Limit) {
    $arguments += @("--limit", $Limit)
}

& $PythonExe @arguments
