$ErrorActionPreference = "Stop"

function Show-Usage {
    Write-Output "Usage: install.ps1 [PLUGIN]"
    Write-Output "Install Krabb core or one official plugin from a release."
    Write-Output "Environment: KRABB_VERSION, KRABB_RELEASE_BASE,"
    Write-Output "             KRABB_HOME, KRABB_BIN."
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
$valid = @("core", "acp", "a2a", "ag-ui", "codex", "anthropic", "openai", "gemini", "ollama", "openrouter", "telegram", "discord", "whatsapp", "signal", "slack", "sqlite", "memory", "scheduler", "tools", "mcp", "whisper", "tui")
if ($valid -notcontains $plugin) { Stop-Install "Unknown plugin '$plugin'." }

$arch = switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    "X64" { "x86_64" }
    "Arm64" { "aarch64" }
    default { Stop-Install "Unsupported architecture." }
}

if ($env:KRABB_VERSION) {
    $version = $env:KRABB_VERSION
} else {
    $version = (Invoke-WebRequest -UseBasicParsing -TimeoutSec 120 https://raw.githubusercontent.com/krabblab/krabb/main/VERSION).Content.Trim()
}
if ($version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$') {
    Stop-Install "Version '$version' is not valid."
}

$name = if ($plugin -eq "core") { "krabb" } else { "krabb-plugin-$plugin" }
$archive = "krabb-$plugin-v$version-x86_64-pc-windows-msvc.zip"
if ($arch -eq "aarch64") { $archive = "krabb-$plugin-v$version-aarch64-pc-windows-msvc.zip" }
$base = if ($env:KRABB_RELEASE_BASE) {
    $env:KRABB_RELEASE_BASE.TrimEnd('/')
} else {
    "https://github.com/krabblab/krabb/releases/download/v$version"
}
if ($base -notmatch '^(https://|file://)') {
    Stop-Install "KRABB_RELEASE_BASE must use https:// or file://."
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
        $alias = Get-ChildItem $tmp -Recurse -File -Filter "krabb.exe" | Select-Object -First 1
        if (-not $alias) { Stop-Install "The archive does not contain 'krabb.exe'." }
        $daemon = Get-ChildItem $tmp -Recurse -File -Filter "krabb-daemon.exe" | Select-Object -First 1
        if (-not $daemon) { Stop-Install "The archive does not contain 'krabb-daemon.exe'." }
        $dest = Join-Path $env:LOCALAPPDATA "Krabb/bin"
        New-Item -ItemType Directory -Force -Path $dest | Out-Null
        $temporary = Join-Path $dest ".$name.exe.tmp"
        Copy-Item $file.FullName $temporary
        Move-Item -Force $temporary (Join-Path $dest "$name.exe")
        $aliasTemporary = Join-Path $dest ".krabb.exe.tmp"
        Copy-Item $alias.FullName $aliasTemporary
        Move-Item -Force $aliasTemporary (Join-Path $dest "krabb.exe")
        $daemonTemporary = Join-Path $dest ".krabb-daemon.exe.tmp"
        Copy-Item $daemon.FullName $daemonTemporary
        Move-Item -Force $daemonTemporary (Join-Path $dest "krabb-daemon.exe")
        Write-Install "Installed $name, krabb, and krabb-daemon in $dest."
    } else {
        if ($env:KRABB_HOME) {
            $config = $env:KRABB_HOME
        } elseif ($env:XDG_CONFIG_HOME) {
            $config = Join-Path $env:XDG_CONFIG_HOME "krabb"
        } else {
            $config = Join-Path $env:USERPROFILE ".config/krabb"
        }
        New-Item -ItemType Directory -Force -Path (Join-Path $config "plugins") | Out-Null
        $krabb = if ($env:KRABB_BIN) {
            $env:KRABB_BIN
        } else {
            Join-Path $env:LOCALAPPDATA "Krabb/bin/krabb.exe"
        }
        if (-not (Test-Path -LiteralPath $krabb -PathType Leaf)) {
            $command = Get-Command krabb.exe -ErrorAction SilentlyContinue
            if ($command) { $krabb = $command.Source }
        }
        if (-not (Test-Path -LiteralPath $krabb -PathType Leaf)) {
            Stop-Install "Install the Krabb core before installing an official plugin."
        }
        & $krabb plugin install $plugin "$archiveUri#sha256=$expected" --yes
        if ($LASTEXITCODE -ne 0) { Stop-Install "The Krabb plugin installation failed." }
        Write-Install "Installed plugin $plugin."
    }
} finally {
    if (Test-Path -LiteralPath $tmp) { Remove-Item -Recurse -Force $tmp }
}
