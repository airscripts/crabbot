# Workflows

Use these workflows to set up, operate, and update a Krabb installation.

## Table Of Contents

- [First Run](#first-run)
- [Local Development](#local-development)
- [Background Service](#background-service)
- [Session Operations](#session-operations)
- [Updates](#updates)

## First Run

Install the core and at least one intelligence and messaging plugin using the
platform instructions in the [README](../README.md). Configure their required
credentials from [Providers](providers.md), then run:

```sh
krabb init
krabb plugin list
krabb doctor
krabb status
krabb-daemon
```

Configure one model and one channel before enabling tools. Keep the daemon in
the foreground until polling, replies, and acknowledgements work as expected.

## Local Development

```sh
cargo build -p krabb-plugin-telegram --locked
cargo install --path krabb --locked
cargo install --path krabb-daemon --locked
krabb plugin install telegram --link
krabb doctor
```

Before opening a pull request, check Rust formatting and repository block
spacing with `make fmt`. Apply both formatters with:

```sh
cargo fmt --all
make spacing
make fmt
```

The spacing formatter is idempotent and preserves the contents of Rust raw
strings. Keep its inserted blank lines between adjacent multiline statements.

Build only the plugins you plan to use. After changing a linked plugin, rebuild
its package and run `krabb plugin install <id> --link`; the host health-checks the
staged executable and loads it into a running daemon without a restart.
`krabb plugin update` previews available changes, then prompts to apply them
in a terminal. Use `--yes` to skip that prompt. It reloads active plugins in
place without restarting the daemon, and plugins that were inactive remain
inactive.

Continuous integration also checks the `x86_64-pc-windows-gnu` target from
Linux. The job installs MinGW for native dependencies before running the
locked workspace check; release archives continue to use the supported MSVC
targets.

Repository scripts keep their responsibilities separate and expose `--help`.
Use `krabb-scripts/metrics.sh` for Rust, script, and test counts; use
`krabb-scripts/check.sh ARCHIVE` to validate a release archive without
extracting it.

On a Linux host with Docker or Podman available, run
`krabb-scripts/sandbox.sh target/debug/krabb-plugin-tools` to verify the
networkless, read-only-root tools sandbox against a local image. The script
uses only an image already present on the host and does not pull one.

Use `make ci` for new or changed GitHub Actions workflows and for debugging
workflow behavior that depends on action ordering, inputs, runners, or
artifacts. It runs `krabb-ci/ci.sh`, which performs the local preflight and
then exercises the important Actions flow through `act`. For ordinary source
changes, use `make verify`, `make test`, and `make build` without the container
workflow.

## Background Service

```sh
krabb service install
krabb service start
krabb service status
```

Use `krabb service install --force` to replace an existing definition, and
`krabb service stop` before changing binaries or environment values. Use
`krabb service uninstall -y` to unregister the definition; pending worktree
cleanup is reported for the next daemon start.

## Session Operations

```sh
krabb session new telegram-123 --model gpt-4o-mini
krabb session list
krabb session show telegram-123
krabb session model telegram-123 gpt-4o-mini
krabb session cancel telegram-123
```

Session listings are bounded summaries. Use `session show` for one transcript,
and delete only idle sessions after reviewing their worktree state.

## Updates

Use `update = "check"` to observe source changes, `"prompt"` to require an
explicit update command, or `"auto"` for staged startup updates. Keep explicit
Git revisions pinned in production and review permission changes before
accepting an update.
