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

cargo_command=${CARGO:-cargo}

for package in "${packages[@]}"; do
    printf '[coverage] %s\n' "$package"
    "$cargo_command" llvm-cov --fail-under-lines 80 --locked -p "$package"
done
