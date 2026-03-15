param(
    [string]$PythonExe = "C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe",
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"

$webRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\web")).Path

$process = Start-Process -FilePath $PythonExe `
    -ArgumentList "-m", "http.server", $Port, "--directory", $webRoot `
    -WorkingDirectory $webRoot `
    -PassThru

Start-Sleep -Seconds 2
Write-Output "Dashboard started (PID $($process.Id)): http://127.0.0.1:$Port/dashboard.html"
