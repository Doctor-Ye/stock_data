param(
    [string]$PythonExe = "C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe",
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
& $PythonExe -m http.server $Port --directory (Join-Path $PSScriptRoot "..\\web")
