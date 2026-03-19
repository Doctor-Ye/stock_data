param(
    [string]$Source = ".\web",
    [string]$Destination = ".\docs"
)

$ErrorActionPreference = "Stop"

$sourcePath = (Resolve-Path $Source).Path
if (-not (Test-Path $sourcePath)) {
    throw "Source path not found: $Source"
}

if (-not (Test-Path $Destination)) {
    New-Item -ItemType Directory -Path $Destination | Out-Null
}

$destinationPath = (Resolve-Path $Destination).Path

robocopy $sourcePath $destinationPath /MIR /NFL /NDL /NJH /NJS /NP | Out-Null

if ($LASTEXITCODE -gt 7) {
    throw "robocopy failed with exit code $LASTEXITCODE"
}

New-Item -ItemType File -Path (Join-Path $destinationPath ".nojekyll") -Force | Out-Null

Write-Output "Synced web site to $destinationPath"
