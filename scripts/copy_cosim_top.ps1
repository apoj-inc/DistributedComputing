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
$sourceDir = Join-Path $shareRoot "cosim_top"
$destinationRoot = "C:\HDLNoCGEN"
$destinationDir = Join-Path $destinationRoot "cosim_top"

$ErrorActionPreference = "Stop"
$shareConnectedByScript = $false

if (-not (Test-Path -Path $destinationRoot)) {
    New-Item -Path $destinationRoot -ItemType Directory -Force | Out-Null
}

try {
    if ($ShareUser -and $SharePassword) {
        cmd.exe /c "net use $shareRoot /user:$ShareUser $SharePassword" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to authenticate to $shareRoot via net use."
        }
        $shareConnectedByScript = $true
    }

    Copy-Item -Path $sourceDir -Destination $destinationRoot -Recurse -Force
    Write-Host "Copied cosim_top to $destinationDir"
}
catch [System.UnauthorizedAccessException] {
    Write-Error "Access denied to source path: $sourceDir"
    exit 1
}
catch [System.Management.Automation.ItemNotFoundException] {
    Write-Error "Source directory not found: $sourceDir"
    exit 1
}
catch {
    Write-Error "Failed to copy $sourceDir to $destinationDir: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($shareConnectedByScript) {
        cmd.exe /c "net use $shareRoot /delete /y" | Out-Null
    }
}
