#!/usr/bin/env bash
set -euo pipefail

packages=(
    crabbot
    crabbot-core
    crabbot-file
    crabbot-runtime
    crabbot-daemon
    crabbot-plugin-claude
    crabbot-plugin-codex
    crabbot-plugin-gemini
    crabbot-plugin-ollama
    crabbot-plugin-openrouter
    crabbot-plugin-pi
    crabbot-plugin-mcp
    crabbot-plugin-memory
    crabbot-plugin-discord
    crabbot-plugin-signal
    crabbot-plugin-slack
    crabbot-plugin-telegram
    crabbot-plugin-whatsapp
    crabbot-plugin-sqlite
    crabbot-plugin-timer
    crabbot-plugin-tools
    crabbot-plugin-tui
    crabbot-plugin-whisper
)

platform_packages=(
    crabbot
    crabbot-core
    crabbot-file
    crabbot-runtime
    crabbot-daemon
)

coverage_profile=${1:-full}
package_threshold=${CRABBOT_COVERAGE_PACKAGE_MIN:-}
runtime_threshold=${CRABBOT_COVERAGE_RUNTIME_MIN:-}

case "$coverage_profile" in
    full)
        package_threshold=${package_threshold:-80}
        runtime_threshold=${runtime_threshold:-80}
        ;;
    platform)
        packages=("${platform_packages[@]}")
        package_threshold=${package_threshold:-60}
        runtime_threshold=${runtime_threshold:-40}
        ;;
    *)
        printf '[coverage] usage: %s [full|platform]\n' "$0" >&2
        exit 2
        ;;
esac

cargo_command=${CARGO:-cargo}

for threshold in "$package_threshold" "$runtime_threshold"; do
    if ! [[ "$threshold" =~ ^[0-9]+$ ]] || (( threshold > 100 )); then
        printf '[coverage] threshold must be an integer from 0 to 100: %s\n' "$threshold" >&2
        exit 2
    fi
done

printf '[coverage] package line threshold: %s%%\n' "$package_threshold"
printf '[coverage] runtime line threshold: %s%%\n' "$runtime_threshold"
printf '[coverage] profile: %s\n' "$coverage_profile"

failures=()

for package in "${packages[@]}"; do
    printf '[coverage] %s\n' "$package"

    threshold=$package_threshold
    if [[ "$package" == crabbot-runtime ]]; then
        threshold=$runtime_threshold
    fi

    if "$cargo_command" llvm-cov --no-fail-fast --fail-under-lines "$threshold" \
        --locked -p "$package"; then
        continue
    else
        status=$?
        failures+=("$package (exit $status)")
        printf '[coverage] %s failed; continuing with remaining packages.\n' "$package" >&2
    fi
done

if (( ${#failures[@]} > 0 )); then
    printf '\n[coverage] %s package(s) failed:\n' "${#failures[@]}" >&2

    for failure in "${failures[@]}"; do
        printf '[coverage]   - %s\n' "$failure" >&2
    done

    exit 1
fi

printf '[coverage] All package tests and coverage gates passed.\n'
