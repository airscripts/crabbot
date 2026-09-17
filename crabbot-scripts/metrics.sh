#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: crabbot-scripts/metrics.sh [--root PATH]'
    printf '%s\n' 'Report Rust, script, and test counts for a Crabbot checkout.'
}

die() {
    printf '[ERROR] %s\n' "$*" >&2
    exit 2
}

info() {
    printf '[INFO] %s\n' "$*"
}

root=''
while (($# > 0)); do
    case "$1" in
        --root)
            (($# >= 2)) || die "Option '--root' requires a path."
            root=$2
            shift 2
            ;;
        --help|-h)
            (($# == 1)) || die "Option '$1' cannot be combined with other arguments."
            usage
            exit 0
            ;;
        *)
            die "Unknown option '$1'. Use --help for usage."
            ;;
    esac
done

if [[ -z "$root" ]]; then
    script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
    root=$(cd "$script_dir/.." && pwd -P)
else
    root=$(cd "$root" 2>/dev/null && pwd -P) || die "Repository root '$root' is not accessible."
fi

[[ -f "$root/Cargo.toml" ]] || die "Repository root '$root' has no Cargo.toml."
for command in awk cat find rg wc; do
    command -v "$command" >/dev/null 2>&1 || die "The $command command is required."
done

rust_lines=$(find "$root" -type f -name '*.rs' \
        ! -path "$root/target/*" \
        ! -path "$root/.git/*" \
        ! -path "$root/.revloop/*" \
        -exec cat {} + | awk 'END { print NR + 0 }')
script_lines=$(find "$root/crabbot-scripts" -type f \( -name '*.sh' -o -name '*.ps1' \) \
    -exec cat {} + | awk 'END { print NR + 0 }')
for file in "$root/install.sh" "$root/install.ps1"; do
    if [[ -f "$file" ]]; then
        script_lines=$((script_lines + $(wc -l < "$file")))
    fi
done
tests=$(rg -o --glob '*.rs' --glob '!target/**' --glob '!.git/**' \
    --glob '!.revloop/**' \
    '#\[(tokio::test|test)\]' "$root" | wc -l)
total_lines=$((rust_lines + script_lines))

info "Rust lines: $rust_lines."
info "Script lines: $script_lines."
info "Total source lines: $total_lines."
info "Tests: $tests."
