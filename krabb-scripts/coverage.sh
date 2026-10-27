#!/usr/bin/env bash
set -euo pipefail

packages=(
    krabb
    krabb-core
    krabb-date
    krabb-file
    krabb-process
    krabb-runtime
    krabb-daemon
    krabb-plugin-anthropic
    krabb-plugin-acp
    krabb-plugin-a2a
    krabb-plugin-ag-ui
    krabb-plugin-openai
    krabb-plugin-codex
    krabb-plugin-gemini
    krabb-plugin-ollama
    krabb-plugin-openrouter
    krabb-plugin-mcp
    krabb-plugin-memory
    krabb-plugin-discord
    krabb-plugin-signal
    krabb-plugin-slack
    krabb-plugin-telegram
    krabb-plugin-whatsapp
    krabb-plugin-sqlite
    krabb-plugin-scheduler
    krabb-plugin-tools
    krabb-plugin-browser
    krabb-plugin-computer
    krabb-plugin-tui
    krabb-plugin-whisper
)

platform_packages=(
    krabb
    krabb-core
    krabb-file
    krabb-runtime
    krabb-daemon
)

coverage_profile=${1:-full}
package_threshold=${KRABB_COVERAGE_PACKAGE_MIN:-}
runtime_threshold=${KRABB_COVERAGE_RUNTIME_MIN:-}

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
    if [[ "$package" == krabb-runtime ]]; then
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
