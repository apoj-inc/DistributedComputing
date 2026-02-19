param(
    [Parameter(Mandatory = $true)]
    [string]$Id,

    [Parameter(Mandatory = $true)]
    [string]$ProjectDir,

    [Parameter(Mandatory = $true)]
    [string]$OutputDir,

    [Parameter(Mandatory = $true)]
    [string]$QuartusArgsLine,

    [string]$QuartusSh = "quartus_sh"
)

$ErrorActionPreference = "Stop"
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8NoBom
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

function Fail {
    param([string]$Message)
    Write-Error $Message
    exit 1
}

if (-not (Test-Path -LiteralPath $ProjectDir -PathType Container)) {
    Fail "ProjectDir not found: $ProjectDir"
}

if (-not (Test-Path -LiteralPath $OutputDir -PathType Container)) {
    New-Item -Path $OutputDir -ItemType Directory -Force | Out-Null
}

Push-Location -LiteralPath $ProjectDir
try {
    $cmdLine = "$QuartusSh $QuartusArgsLine"
    $cmdLineUtf8 = "chcp 65001>nul & $cmdLine"
    Write-Host "Running: $cmdLine"
    Write-Host "Working directory: $ProjectDir"

    $proc = Start-Process -FilePath "cmd.exe" `
        -ArgumentList @("/d", "/s", "/c", $cmdLineUtf8) `
        -NoNewWindow `
        -Wait `
        -PassThru

    if ($proc.ExitCode -ne 0) {
        Fail "quartus_sh finished with non-zero exit code: $($proc.ExitCode)"
    }
}
finally {
    Pop-Location
}

$dbExportDir = Join-Path -Path $ProjectDir -ChildPath "db_export"
if (-not (Test-Path -LiteralPath $dbExportDir -PathType Container)) {
    Fail "Directory db_export not found: $dbExportDir"
}

$zipName = "${Id}_db_export.zip"
$tempZip = Join-Path -Path $env:TEMP -ChildPath ("dc_" + [Guid]::NewGuid().ToString() + ".zip")
$finalZip = Join-Path -Path $OutputDir -ChildPath $zipName

if (Test-Path -LiteralPath $tempZip) {
    Remove-Item -LiteralPath $tempZip -Force
}
if (Test-Path -LiteralPath $finalZip) {
    Remove-Item -LiteralPath $finalZip -Force
}

Compress-Archive -Path $dbExportDir -DestinationPath $tempZip -Force
Copy-Item -LiteralPath $tempZip -Destination $finalZip -Force
Remove-Item -LiteralPath $tempZip -Force

Write-Host "Archive created: $finalZip"
exit 0
