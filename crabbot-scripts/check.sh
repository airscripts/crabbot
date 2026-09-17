#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: crabbot-scripts/check.sh ARCHIVE'
    printf '%s\n' 'Validate a Crabbot release archive without extracting it.'
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

archive=$1
[[ -f "$archive" ]] || die "Archive '$archive' does not exist."

case "$archive" in
    *.tar.gz)
        command -v tar >/dev/null 2>&1 || die 'The tar command is required.'
        info "Validating archive: $archive."
        tar -tzf "$archive" >/dev/null || die "Archive validation failed for '$archive'."
        ;;
    *.zip)
        command -v unzip >/dev/null 2>&1 || die 'The unzip command is required.'
        info "Validating archive: $archive."
        unzip -tqq "$archive" >/dev/null || die "Archive validation failed for '$archive'."
        ;;
    *)
        die "Unsupported archive format: '$archive'. Use .tar.gz or .zip."
        ;;
esac

info "Archive is valid: $archive."
