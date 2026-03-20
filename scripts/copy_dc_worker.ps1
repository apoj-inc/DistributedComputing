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
$sourcePath = Join-Path $shareRoot "scr\dc_worker.exe"
$destinationDir = "C:\HDLNoCGEN"
$destinationPath = Join-Path $destinationDir "dc_worker.exe"

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$shareConnectedByScript = $false

Log-Info "Starting copy of $sourcePath to $destinationPath"

if (-not (Test-Path -Path $destinationDir)) {
    Log-Info "Creating destination directory $destinationDir"
    New-Item -Path $destinationDir -ItemType Directory -Force | Out-Null
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

    Copy-Item -Path $sourcePath -Destination $destinationPath -Force

    if (-not (Test-Path -Path $destinationPath)) {
        throw "Copy finished without creating destination file $destinationPath"
    }

    Log-Info "Copied dc_worker.exe to $destinationPath"
}
catch [System.UnauthorizedAccessException] {
    Log-ErrorMessage "Access denied to source path: $sourcePath"
    exit 1
}
catch [System.Management.Automation.ItemNotFoundException] {
    Log-ErrorMessage "Source file not found: $sourcePath"
    exit 1
}
catch {
    Log-ErrorMessage "Failed to copy $sourcePath to $destinationPath: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($shareConnectedByScript) {
        Log-Info "Disconnecting $shareRoot"
        cmd.exe /c "net use $shareRoot /delete /y" > $null 2>&1
    }
}
