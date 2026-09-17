#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: crabbot-scripts/release.sh TAG'
    printf '%s\n' 'Validate a release tag and build the optimized workspace.'
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

if (($# != 1)); then
    usage >&2
    exit 2
fi

tag=$1
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] || \
    die "Tag '$tag' is not a valid release tag."

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || {
    cd "$script_dir/.."
    pwd -P
})
repo_root=$(cd "$repo_root" && pwd -P)
[[ -f "$repo_root/Cargo.toml" ]] || die "Repository root '$repo_root' has no Cargo.toml."
[[ -f "$repo_root/VERSION" ]] || die "VERSION is missing from '$repo_root'."
[[ -f "$repo_root/CHANGELOG.md" ]] || die "CHANGELOG.md is missing from '$repo_root'."
command -v cargo >/dev/null 2>&1 || die 'The cargo command is required.'

version=${tag#v}
expected=$(tr -d '[:space:]' < "$repo_root/VERSION")
[[ "$version" == "$expected" ]] || \
    die "Tag '$tag' does not match VERSION '$expected'."
grep -Fq "## [$version]" "$repo_root/CHANGELOG.md" || \
    die "CHANGELOG.md has no section for version '$version'."

info "Building release $tag."
(cd "$repo_root" && cargo build --workspace --release --locked)
info "Release $tag is ready."
