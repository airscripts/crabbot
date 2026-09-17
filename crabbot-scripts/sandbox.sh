#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: crabbot-scripts/sandbox.sh PLUGIN'
    printf '%s\n' 'Run the tools plugin through a local Docker-compatible sandbox.'
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

plugin=$1
[[ -x "$plugin" ]] || die "Plugin executable '$plugin' does not exist."
command -v python3 >/dev/null 2>&1 || die 'The python3 command is required.'
runtime=${CRABBOT_SANDBOX_RUNTIME:-docker}
image=${CRABBOT_SANDBOX_IMAGE:-busybox:1.36.1}
command -v "$runtime" >/dev/null 2>&1 || die "The $runtime command is required."

root=$(mktemp -d)
input=$(mktemp)
output=$(mktemp)
error=$(mktemp)
cleanup() {
    [ -z "${pid:-}" ] || kill "$pid" 2>/dev/null || true
    rm -rf "$root" "$input" "$output" "$error"
}
trap cleanup EXIT

cat >"$input" <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"hello","params":{}}
{"jsonrpc":"2.0","id":2,"method":"shell","params":{"command":"touch sandbox-marker && ! touch /etc/crabbot-marker && ! wget -q -T 2 -O - http://example.com","approve":true}}
{"jsonrpc":"2.0","id":3,"method":"shutdown","params":{}}
EOF

info "Starting the tools plugin through $runtime."
CRABBOT_ROOT="$root" \
    CRABBOT_SHELL=on \
    CRABBOT_SANDBOX_RUNTIME="$runtime" \
    CRABBOT_SANDBOX_IMAGE="$image" \
    "$plugin" <"$input" >"$output" 2>"$error" &
pid=$!
deadline=$((SECONDS + 60))
while kill -0 "$pid" 2>/dev/null; do
    if ((SECONDS >= deadline)); then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        die 'The sandbox plugin smoke test exceeded its time limit.'
    fi
    sleep 1
done
if ! wait "$pid"; then
    printf '%s\n' '--- Plugin diagnostics ---' >&2
    sed -n '1,80p' "$error" >&2
    die 'The sandbox plugin smoke test failed.'
fi

python3 - "$output" "$root" <<'PY'
import json
import pathlib
import sys

output = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
responses = {}
for line in output.read_text(encoding="utf-8").splitlines():
    value = json.loads(line)
    if "id" in value:
        responses[value["id"]] = value

response = responses.get(2)
if response is None or "error" in response:
    raise SystemExit("The sandbox response is missing or failed.")
result = response.get("result", {})
if result.get("status") != 0 or not (root / "sandbox-marker").exists():
    raise SystemExit("The sandbox did not provide the expected confined shell.")
print("[INFO] Sandbox execution, workspace mounting, read-only root, and network isolation passed.")
PY
