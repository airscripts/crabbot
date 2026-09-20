#!/usr/bin/env bash
set -euo pipefail

die() {
    printf '[ERROR] %s\n' "$1" >&2
    exit 1
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
act_bin=${CRABBOT_ACT_BIN:-act}
act_image=${CRABBOT_ACT_IMAGE:-catthehacker/ubuntu:act-latest}
container_image=${CRABBOT_CI_IMAGE:-crabbot-ci:rust-1.89}

case "$(uname -m)" in
    aarch64|arm64)
        native_arch=linux/arm64
        native_target=aarch64-unknown-linux-gnu
        ;;

    x86_64|amd64)
        native_arch=linux/amd64
        native_target=x86_64-unknown-linux-gnu
        ;;

    *)
        die "Unsupported host architecture '$(uname -m)'."
        ;;
esac

act_arch=${CRABBOT_ACT_ARCH:-$native_arch}
act_jobs=${CRABBOT_CI_JOBS:-1}

case "$act_arch" in
    linux/arm64|linux/amd64) ;;
    *) die "Unsupported act architecture '$act_arch'." ;;
esac

ci_target=${CRABBOT_CI_TARGET:-$native_target}
cache_dir=${CRABBOT_CI_CACHE:-${repo_root}/crabbot-ci/.cache}
artifact_dir=${CRABBOT_CI_ARTIFACTS:-${repo_root}/crabbot-ci/.artifacts}
local_target_dir=${CRABBOT_CI_TARGET_DIR:-${repo_root}/crabbot-ci/.target}
workspace_dir=$(mktemp -d "${TMPDIR:-/tmp}/crabbot-ci.XXXXXX")

cleanup() {
    if [[ -d "$workspace_dir/target" ]] && command -v docker >/dev/null 2>&1; then
        docker run --rm -v "$workspace_dir:/workspace" "$container_image" \
            rm -rf /workspace/target >/dev/null 2>&1 || true
    fi

    rm -rf "$workspace_dir"
}

trap cleanup EXIT
mkdir -p "$cache_dir" "$artifact_dir" "$local_target_dir"

local_make() {
    CARGO_TARGET_DIR="$local_target_dir" make "$@"
}

printf '\n[INFO] Local Verify preflight\n'
local_make fmt clippy metrics

RUSTDOCFLAGS='-D warnings' CARGO_TARGET_DIR="$local_target_dir" \
    cargo doc --workspace --no-deps --locked

bash -n crabbot-scripts/*.sh install.sh

printf '\n[INFO] Local Test preflight\n'
local_make coverage
local_make test

printf '\n[INFO] Local Build preflight\n'
local_make build

command -v "$act_bin" >/dev/null 2>&1 || die "The act command is required after local preflight."
docker info >/dev/null 2>&1 || die "The Docker daemon is required after local preflight."

mkdir -p "$cache_dir" "$artifact_dir"

tar \
    --exclude=.git \
    --exclude=target \
    --exclude=crabbot-ci/.target \
    --exclude=crabbot-ci/.cache \
    --exclude=crabbot-ci/.artifacts \
    -cf - \
    -C "$repo_root" . \
    | tar -xf - -C "$workspace_dir"

git -C "$workspace_dir" init -q
git -C "$workspace_dir" config user.name "Crabbot"
git -C "$workspace_dir" config user.email "ci@localhost"
git -C "$workspace_dir" config commit.gpgsign false
git -C "$workspace_dir" add -A
git -C "$workspace_dir" commit -qm "chore: local ci snapshot"

if ! docker image inspect "$container_image" >/dev/null 2>&1; then
    printf '\n[INFO] Building %s\n' "$container_image"

    docker build \
        --build-arg "BASE_IMAGE=$act_image" \
        --tag "$container_image" \
        --file "$repo_root/crabbot-ci/Dockerfile" \
        "$repo_root/crabbot-ci"
fi

if ! docker image inspect "$act_image" >/dev/null 2>&1; then
    printf '\n[INFO] Pulling %s\n' "$act_image"
    docker pull --platform "$act_arch" "$act_image"
fi

act_common=(
    --platform "ubuntu-latest=${container_image}"
    --platform "ubuntu-24.04=${container_image}"
    --container-architecture "$act_arch"
    --pull=false
    --rm
    --concurrent-jobs "$act_jobs"
    --artifact-server-path "$artifact_dir"
    --env GITHUB_REPOSITORY=airscripts/crabbot
    --bind
)

run_workflow() {
    local label=$1
    shift

    printf '\n[INFO] %s\n' "$label"
    (
        cd "$workspace_dir"
        XDG_CACHE_HOME="$cache_dir" "$act_bin" "$@" "${act_common[@]}"
    )
}

run_workflow "Verify" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/verify.yml" \
    --input "container_image=$container_image" \
    --input local=true

run_workflow "Agentskill" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/agentskill.yml" \
    --input "container_image=$container_image" \
    --input local=true

run_workflow "Security" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/security.yml" \
    --input local=true

run_workflow "Test Agent" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/test.yml" \
    --job agent \
    --matrix "target:${ci_target}" \
    --input local=true

run_workflow "Test Sandbox" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/test.yml" \
    --job sandbox \
    --input local=true

run_workflow "Build Agent" workflow_dispatch \
    --workflows "$workspace_dir/.github/workflows/build.yml" \
    --job agent \
    --matrix "target:${ci_target}" \
    --input local=true

printf '\n[INFO] Local non-release CI passed.\n'
