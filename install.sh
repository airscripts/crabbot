#!/usr/bin/env sh
set -eu

usage() {
    printf '%s\n' 'Usage: install.sh [PLUGIN]'
    printf '%s\n' 'Install Crabbot core or one official plugin from a release.'
    printf '%s\n' \
        'Environment: CRABBOT_VERSION, CRABBOT_RELEASE_BASE,' \
        '             CRABBOT_PREFIX, CRABBOT_HOME, CRABBOT_BIN.'
}

die() {
    printf '[ERROR] %s\n' "$*" >&2
    exit 2
}

info() {
    printf '[INFO] %s\n' "$*"
}

if [ "$#" -eq 1 ] && { [ "$1" = '--help' ] || [ "$1" = '-h' ]; }; then
    usage
    exit 0
fi
[ "$#" -le 1 ] || {
    usage >&2
    exit 2
}

plugin=${1:-core}
case "$plugin" in
    core|codex|claude|gemini|ollama|openrouter|telegram|discord|whatsapp|signal|slack|sqlite|memory|timer|tools|mcp|whisper|tui|pi) ;;
    *) die "Unknown plugin '$plugin'." ;;
esac

for command in awk curl find install mkdir mktemp mv tar tr; do
    command -v "$command" >/dev/null 2>&1 || die "The $command command is required."
done

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in
    x86_64|amd64) target_arch=x86_64 ;;
    aarch64|arm64) target_arch=aarch64 ;;
    *) die "Unsupported architecture: '$arch'." ;;
esac
case "$os" in
    linux) target_os=unknown-linux-gnu; archive_format=tar.gz ;;
    darwin) target_os=apple-darwin; archive_format=tar.gz ;;
    *) die 'Use install.ps1 on Windows.' ;;
esac

version_file=''
tmp=''
cleanup() {
    [ -z "$tmp" ] || rm -rf "$tmp"
    [ -z "$version_file" ] || rm -f "$version_file"
}
trap cleanup EXIT

if [ -n "${CRABBOT_VERSION:-}" ]; then
    version=$CRABBOT_VERSION
else
    [ -n "${HOME:-}" ] || die 'HOME is not set; configure CRABBOT_VERSION or CRABBOT_PREFIX.'
    version_file=$(mktemp)
    curl --fail --silent --show-error --location --retry 3 \
        --connect-timeout 10 --max-time 120 \
        'https://raw.githubusercontent.com/airscripts/crabbot/main/VERSION' \
        -o "$version_file"
    version=$(tr -d '[:space:]' < "$version_file")
fi
if ! printf '%s\n' "$version" | awk \
    '$0 ~ /^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$/ { valid = 1 } END { exit !valid }'
then
    die "Version '$version' is not valid."
fi

if [ -n "${CRABBOT_PREFIX:-}" ]; then
    prefix=$CRABBOT_PREFIX
else
    [ -n "${HOME:-}" ] || die 'HOME is not set; configure CRABBOT_PREFIX.'
    prefix="$HOME/.local"
fi

target="$target_arch-$target_os"
name=crabbot
[ "$plugin" = core ] || name="crabbot-plugin-$plugin"
archive_name="crabbot-$plugin-v$version-$target.$archive_format"
base=${CRABBOT_RELEASE_BASE:-"https://github.com/airscripts/crabbot/releases/download/v$version"}
base=${base%/}
case "$base" in
    https://*|file://*) ;;
    *) die 'CRABBOT_RELEASE_BASE must use https:// or file://.' ;;
esac
tmp=$(mktemp -d)

info "Downloading $archive_name."
curl --fail --silent --show-error --location --retry 3 \
    --connect-timeout 10 --max-time 120 \
    "$base/$archive_name" -o "$tmp/$archive_name"
curl --fail --silent --show-error --location --retry 3 \
    --connect-timeout 10 --max-time 120 \
    "$base/SHA256SUMS" -o "$tmp/SHA256SUMS"

expected=$(awk -v archive="$archive_name" '$2 == archive || $2 == "*" archive { print $1; exit }' \
    "$tmp/SHA256SUMS")
[ -n "$expected" ] || die "Checksum is missing for '$archive_name'."

if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/$archive_name" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$tmp/$archive_name" | awk '{ print $1 }')
else
    die 'The sha256sum or shasum command is required.'
fi
[ "$actual" = "$expected" ] || die "Checksum verification failed for '$archive_name'."

if [ "$plugin" = core ]; then
    tar -xzf "$tmp/$archive_name" -C "$tmp"
    binary=$(find "$tmp" -type f -name "$name" -print -quit)
    [ -n "$binary" ] || die "Archive does not contain '$name'."
    daemon=$(find "$tmp" -type f -name "crabbot-daemon" -print -quit)
    [ -n "$daemon" ] || die "Archive does not contain 'crabbot-daemon'."
    mkdir -p "$prefix/bin"
    install -m 0755 "$binary" "$prefix/bin/.$name.tmp.$$"
    mv -f "$prefix/bin/.$name.tmp.$$" "$prefix/bin/$name"
    install -m 0755 "$daemon" "$prefix/bin/.crabbot-daemon.tmp.$$"
    mv -f "$prefix/bin/.crabbot-daemon.tmp.$$" "$prefix/bin/crabbot-daemon"
    info "Installed $name and crabbot-daemon in $prefix/bin."
else
    if [ -n "${CRABBOT_HOME:-}" ]; then
        config=$CRABBOT_HOME
    elif [ -n "${XDG_CONFIG_HOME:-}" ]; then
        config="$XDG_CONFIG_HOME/crabbot"
    else
        [ -n "${HOME:-}" ] || die 'HOME is not set; configure CRABBOT_HOME.'
        config="$HOME/.config/crabbot"
    fi
    mkdir -p "$config/plugins"
    crabbot=${CRABBOT_BIN:-"$prefix/bin/crabbot"}
    if [ ! -x "$crabbot" ]; then
        crabbot=$(command -v crabbot || true)
    fi
    [ -n "$crabbot" ] && [ -x "$crabbot" ] || \
        die 'Install the Crabbot core before installing an official plugin.'
    "$crabbot" plugin install "$plugin" "$base/$archive_name#sha256=$expected" --yes
    info "Installed plugin $plugin."
fi
