$sourceDir = "\\192.168.15.1\HDLNoCGen\cosim_top"
$destinationRoot = "C:\HDLNoCGEN"
$destinationDir = Join-Path $destinationRoot "cosim_top"

if (-not (Test-Path -Path $sourceDir)) {
    Write-Error "Source directory not found: $sourceDir"
    exit 1
}

if (-not (Test-Path -Path $destinationRoot)) {
    New-Item -Path $destinationRoot -ItemType Directory -Force | Out-Null
}

Copy-Item -Path $sourceDir -Destination $destinationRoot -Recurse -Force
Write-Host "Copied cosim_top to $destinationDir"
