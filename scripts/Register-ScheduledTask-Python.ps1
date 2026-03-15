param(
    [string]$PythonExe = "C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe",
    [string]$DailyTime = "08:00"
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "sec_db.py"
& $PythonExe $scriptPath register-task --daily-time $DailyTime --python-exe $PythonExe
