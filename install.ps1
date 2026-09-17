$ErrorActionPreference = "Stop"

function Show-Usage {
    Write-Output "Usage: install.ps1 [PLUGIN]"
    Write-Output "Install Crabbot core or one official plugin from a release."
    Write-Output "Environment: CRABBOT_VERSION, CRABBOT_RELEASE_BASE,"
    Write-Output "             CRABBOT_HOME, CRABBOT_BIN."
}

function Stop-Install([string] $Message) {
    Write-Error "[ERROR] $Message"
    exit 2
}

function Write-Install([string] $Message) {
    Write-Output "[INFO] $Message"
}

if ($args.Count -eq 1 -and ($args[0] -eq "--help" -or $args[0] -eq "-h")) {
    Show-Usage
    exit 0
}
if ($args.Count -gt 1) {
    Show-Usage
    exit 2
}

$plugin = if ($args.Count -eq 1) { $args[0] } else { "core" }
$valid = @("core", "codex", "claude", "gemini", "ollama", "openrouter", "telegram", "discord", "whatsapp", "signal", "slack", "sqlite", "memory", "timer", "tools", "mcp", "whisper", "tui", "pi")
if ($valid -notcontains $plugin) { Stop-Install "Unknown plugin '$plugin'." }

$arch = switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    "X64" { "x86_64" }
    "Arm64" { "aarch64" }
    default { Stop-Install "Unsupported architecture." }
}

if ($env:CRABBOT_VERSION) {
    $version = $env:CRABBOT_VERSION
} else {
    $version = (Invoke-WebRequest -UseBasicParsing -TimeoutSec 120 https://raw.githubusercontent.com/airscripts/crabbot/main/VERSION).Content.Trim()
}
if ($version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$') {
    Stop-Install "Version '$version' is not valid."
}

$name = if ($plugin -eq "core") { "crabbot" } else { "crabbot-plugin-$plugin" }
$archive = "crabbot-$plugin-v$version-x86_64-pc-windows-msvc.zip"
if ($arch -eq "aarch64") { $archive = "crabbot-$plugin-v$version-aarch64-pc-windows-msvc.zip" }
$base = if ($env:CRABBOT_RELEASE_BASE) {
    $env:CRABBOT_RELEASE_BASE.TrimEnd('/')
} else {
    "https://github.com/airscripts/crabbot/releases/download/v$version"
}
if ($base -notmatch '^(https://|file://)') {
    Stop-Install "CRABBOT_RELEASE_BASE must use https:// or file://."
}
$tmp = Join-Path ([IO.Path]::GetTempPath()) ([IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    $archivePath = Join-Path $tmp $archive
    $checksumsPath = Join-Path $tmp "SHA256SUMS"
    Write-Install "Downloading $archive."
    $archiveUri = "$base/$archive"
    $checksumsUri = "$base/SHA256SUMS"
    if ($base.StartsWith("file://")) {
        Copy-Item ([Uri]$archiveUri).LocalPath $archivePath
        Copy-Item ([Uri]$checksumsUri).LocalPath $checksumsPath
    } else {
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 120 $archiveUri -OutFile $archivePath
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 120 $checksumsUri -OutFile $checksumsPath
    }

    $line = Get-Content $checksumsPath | Where-Object {
        $parts = $_ -split '\s+', 3
        $parts.Count -ge 2 -and $parts[1].TrimStart('*') -eq $archive
    } | Select-Object -First 1
    if (-not $line) { Stop-Install "Checksum is missing for '$archive'." }
    $expected = (($line -split '\s+', 3)[0]).ToLowerInvariant()
    $actual = (Get-FileHash $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $expected) { Stop-Install "Checksum verification failed for '$archive'." }

    if ($plugin -eq "core") {
        Expand-Archive $archivePath -DestinationPath $tmp
        $file = Get-ChildItem $tmp -Recurse -File -Filter "$name.exe" | Select-Object -First 1
        if (-not $file) { Stop-Install "The archive does not contain '$name.exe'." }
        $daemon = Get-ChildItem $tmp -Recurse -File -Filter "crabbot-daemon.exe" | Select-Object -First 1
        if (-not $daemon) { Stop-Install "The archive does not contain 'crabbot-daemon.exe'." }
        $dest = Join-Path $env:LOCALAPPDATA "Crabbot/bin"
        New-Item -ItemType Directory -Force -Path $dest | Out-Null
        $temporary = Join-Path $dest ".$name.exe.tmp"
        Copy-Item $file.FullName $temporary
        Move-Item -Force $temporary (Join-Path $dest "$name.exe")
        $daemonTemporary = Join-Path $dest ".crabbot-daemon.exe.tmp"
        Copy-Item $daemon.FullName $daemonTemporary
        Move-Item -Force $daemonTemporary (Join-Path $dest "crabbot-daemon.exe")
        Write-Install "Installed $name and crabbot-daemon in $dest."
    } else {
        if ($env:CRABBOT_HOME) {
            $config = $env:CRABBOT_HOME
        } elseif ($env:XDG_CONFIG_HOME) {
            $config = Join-Path $env:XDG_CONFIG_HOME "crabbot"
        } else {
            $config = Join-Path $env:USERPROFILE ".config/crabbot"
        }
        New-Item -ItemType Directory -Force -Path (Join-Path $config "plugins") | Out-Null
        $crabbot = if ($env:CRABBOT_BIN) {
            $env:CRABBOT_BIN
        } else {
            Join-Path $env:LOCALAPPDATA "Crabbot/bin/crabbot.exe"
        }
        if (-not (Test-Path -LiteralPath $crabbot -PathType Leaf)) {
            $command = Get-Command crabbot.exe -ErrorAction SilentlyContinue
            if ($command) { $crabbot = $command.Source }
        }
        if (-not (Test-Path -LiteralPath $crabbot -PathType Leaf)) {
            Stop-Install "Install the Crabbot core before installing an official plugin."
        }
        & $crabbot plugin install $plugin "$archiveUri#sha256=$expected" --yes
        if ($LASTEXITCODE -ne 0) { Stop-Install "The Crabbot plugin installation failed." }
        Write-Install "Installed plugin $plugin."
    }
} finally {
    if (Test-Path -LiteralPath $tmp) { Remove-Item -Recurse -Force $tmp }
}
