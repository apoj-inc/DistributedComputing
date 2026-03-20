param(
    [string]$ShareUser = "",
    [string]$SharePassword = ""
)

function Log-Info {
    param([string]$Message)
    [Console]::Out.WriteLine($Message)
}

function Log-ErrorMessage {
    param([string]$Message)
    [Console]::Error.WriteLine($Message)
}

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
$ProgressPreference = "SilentlyContinue"
$shareConnectedByScript = $false

Log-Info "Starting copy of $sourceDir to $destinationDir"

if (-not (Test-Path -Path $destinationRoot)) {
    Log-Info "Creating destination directory $destinationRoot"
    New-Item -Path $destinationRoot -ItemType Directory -Force | Out-Null
}

try {
    if ($ShareUser -and $SharePassword) {
        Log-Info "Authenticating to $shareRoot as $ShareUser"
        $netUseOutput = cmd.exe /c "net use $shareRoot /user:$ShareUser $SharePassword" 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "net use failed: $($netUseOutput -join ' ')"
        }
        $shareConnectedByScript = $true
    }
    else {
        Log-Info "No share credentials provided; using current Windows session credentials"
    }

    Copy-Item -Path $sourceDir -Destination $destinationRoot -Recurse -Force

    if (-not (Test-Path -Path $destinationDir)) {
        throw "Copy finished without creating destination directory $destinationDir"
    }

    Log-Info "Copied cosim_top to $destinationDir"
}
catch [System.UnauthorizedAccessException] {
    Log-ErrorMessage "Access denied to source path: $sourceDir"
    exit 1
}
catch [System.Management.Automation.ItemNotFoundException] {
    Log-ErrorMessage "Source directory not found: $sourceDir"
    exit 1
}
catch {
    Log-ErrorMessage "Failed to copy $sourceDir to $destinationDir: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($shareConnectedByScript) {
        Log-Info "Disconnecting $shareRoot"
        cmd.exe /c "net use $shareRoot /delete /y" > $null 2>&1
    }
}
