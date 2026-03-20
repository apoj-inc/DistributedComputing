$sourcePath = "\\192.168.15.1\HDLNoCGen\scr\dc_worker.exe"
$destinationDir = "C:\HDLNoCGEN"
$destinationPath = Join-Path $destinationDir "dc_worker.exe"

if (-not (Test-Path -Path $sourcePath)) {
    Write-Error "Source file not found: $sourcePath"
    exit 1
}

if (-not (Test-Path -Path $destinationDir)) {
    New-Item -Path $destinationDir -ItemType Directory -Force | Out-Null
}

Copy-Item -Path $sourcePath -Destination $destinationPath -Force
Write-Host "Copied dc_worker.exe to $destinationPath"
