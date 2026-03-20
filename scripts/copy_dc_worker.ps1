param(
    [string]$ShareUser = "",
    [string]$SharePassword = ""
)

if (-not $ShareUser) {
    $ShareUser = $env:HDLNOCGEN_SHARE_USER
}

if (-not $SharePassword) {
    $SharePassword = $env:HDLNOCGEN_SHARE_PASSWORD
}

$shareRoot = "\\192.168.15.1\HDLNoCGen"
$sourcePath = Join-Path $shareRoot "scr\dc_worker.exe"
$destinationDir = "C:\HDLNoCGEN"
$destinationPath = Join-Path $destinationDir "dc_worker.exe"

$ErrorActionPreference = "Stop"
$shareConnectedByScript = $false

if (-not (Test-Path -Path $destinationDir)) {
    New-Item -Path $destinationDir -ItemType Directory -Force | Out-Null
}

try {
    if ($ShareUser -and $SharePassword) {
        cmd.exe /c "net use $shareRoot /user:$ShareUser $SharePassword" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to authenticate to $shareRoot via net use."
        }
        $shareConnectedByScript = $true
    }

    Copy-Item -Path $sourcePath -Destination $destinationPath -Force
    Write-Host "Copied dc_worker.exe to $destinationPath"
}
catch [System.UnauthorizedAccessException] {
    Write-Error "Access denied to source path: $sourcePath"
    exit 1
}
catch [System.Management.Automation.ItemNotFoundException] {
    Write-Error "Source file not found: $sourcePath"
    exit 1
}
catch {
    Write-Error "Failed to copy $sourcePath to $destinationPath: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($shareConnectedByScript) {
        cmd.exe /c "net use $shareRoot /delete /y" | Out-Null
    }
}
