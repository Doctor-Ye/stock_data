param(
    [string]$PythonExe = (Join-Path $PSScriptRoot "..\tools\python312\runtime\python.exe"),
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
& $PythonExe -m http.server $Port --directory (Join-Path $PSScriptRoot "..\\web")
