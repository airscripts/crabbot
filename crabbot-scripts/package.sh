#!/usr/bin/env bash
set -euo pipefail

plugins=(core codex claude gemini ollama openrouter telegram discord whatsapp signal slack sqlite memory timer tools mcp whisper tui pi)

usage() {
    printf '%s\n' 'Usage: crabbot-scripts/package.sh VERSION TARGET'
    printf '%s\n' 'Build and archive every Crabbot binary for a target triple.'
}

die() {
    printf '[ERROR] %s\n' "$*" >&2
    exit 2
}

info() {
    printf '[INFO] %s\n' "$*"
}

if (($# == 1)) && [[ "$1" == '--help' || "$1" == '-h' ]]; then
    usage
    exit 0
fi

if (($# != 2)); then
    usage >&2
    exit 2
fi

version=$1
target=$2
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] || \
    die "Version '$version' is not a valid release version."
[[ "$target" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || \
    die "Target '$target' is not a valid target triple."

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || {
    cd "$script_dir/.."
    pwd -P
})
repo_root=$(cd "$repo_root" && pwd -P)
[[ -f "$repo_root/Cargo.toml" ]] || die "Repository root '$repo_root' has no Cargo.toml."

for command in cargo mktemp tar; do
    command -v "$command" >/dev/null 2>&1 || die "The $command command is required."
done
if [[ "$target" == *-pc-windows-* ]]; then
    command -v zip >/dev/null 2>&1 || die 'The zip command is required for Windows archives.'
fi

release="$repo_root/target/$target/release"
stage=$(mktemp -d "$repo_root/.package.XXXXXX")
trap 'rm -rf "$stage"' EXIT

info "Building workspace for $target."
(cd "$repo_root" && cargo build --workspace --release --locked --target "$target")

for plugin in "${plugins[@]}"; do
    case "$plugin" in
        core)
            name=crabbot
            ;;
        pi)
            name=crabbot-agent-pi
            ;;
        *)
            name="crabbot-plugin-$plugin"
            ;;
    esac

    package_name="crabbot-$plugin-v$version-$target"
    package_dir="$stage/$package_name"
    binary="$release/$name"
    [[ -f "$binary" ]] || binary="$release/$name.exe"
    [[ -f "$binary" ]] || die "Built binary '$name' was not found for '$target'."

    mkdir -p "$package_dir/bin"
    cp "$binary" "$package_dir/bin/"
    if [[ "$plugin" == core ]]; then
        daemon="$release/crabbot-daemon"
        [[ -f "$daemon" ]] || daemon="$release/crabbot-daemon.exe"
        [[ -f "$daemon" ]] || die "Built binary 'crabbot-daemon' was not found for '$target'."
        cp "$daemon" "$package_dir/bin/"
    fi
    cp "$repo_root/LICENSE" "$repo_root/NOTICE" "$package_dir/"
    if [[ "$plugin" != core ]]; then
        manifest=$(find "$repo_root/crabbot-plugins" -type f -path "*/$plugin/crabbot-plugin.toml" -print -quit)
        [[ -f "$manifest" ]] || die "Plugin manifest is missing: $manifest."
        cp "$manifest" "$package_dir/"
    fi

    if [[ "$target" == *-pc-windows-* ]]; then
        archive="$repo_root/$package_name.zip"
        rm -f "$archive"
        (cd "$stage" && zip -qr "$archive" "$package_name")
    else
        archive="$repo_root/$package_name.tar.gz"
        rm -f "$archive"
        tar -C "$stage" -czf "$archive" "$package_name"
    fi

    info "Created archive: ${archive#"$repo_root/"}."
done
