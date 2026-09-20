# Workflows

## First Run

Install the core and at least one intelligence and messaging plugin using the
platform instructions in the [README](../README.md). Configure their required
credentials from [Providers](providers.md), then run:

```sh
crabbot init
crabbot plugin list
crabbot doctor
crabbot status
crabbot-daemon
```

Configure one model and one channel before enabling tools. Keep the daemon in
the foreground until polling, replies, and acknowledgements work as expected.

## Local Development

```sh
cargo build -p crabbot-plugin-telegram --locked
cargo install --path crabbot --locked
cargo install --path crabbot-daemon --locked
crabbot plugin link telegram --yes
crabbot doctor
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
its package and run `crabbot plugin link <id> --yes`; the host health-checks the
staged executable and loads it into a running daemon without a restart.
`crabbot plugin update` similarly reloads active plugins in place; it does not
restart the daemon, and plugins that were inactive remain inactive.

Continuous integration also checks the `x86_64-pc-windows-gnu` target from
Linux. The job installs MinGW for native dependencies before running the
locked workspace check; release archives continue to use the supported MSVC
targets.

Repository scripts keep their responsibilities separate and expose `--help`.
Use `crabbot-scripts/metrics.sh` for Rust, script, and test counts; use
`crabbot-scripts/check.sh ARCHIVE` to validate a release archive without
extracting it.

On a Linux host with Docker or Podman available, run
`crabbot-scripts/sandbox.sh target/debug/crabbot-plugin-tools` to verify the
networkless, read-only-root tools sandbox against a local image. The script
uses only an image already present on the host and does not pull one.

Use `make ci` for new or changed GitHub Actions workflows and for debugging
workflow behavior that depends on action ordering, inputs, runners, or
artifacts. It runs `crabbot-ci/ci.sh`, which performs the local preflight and
then exercises the important Actions flow through `act`. For ordinary source
changes, use `make verify`, `make test`, and `make build` without the container
workflow.

## Background Service

```sh
crabbot service install
crabbot service start
crabbot service status
```

Use `crabbot service stop` before changing binaries or environment values.
Use `crabbot service remove` to unregister the definition; pending worktree
cleanup is reported for the next daemon start.

## Session Operations

```sh
crabbot session new telegram-123 --model gpt-4o-mini
crabbot session list
crabbot session show telegram-123
crabbot session model telegram-123 gpt-4o-mini
crabbot session cancel telegram-123
```

Session listings are bounded summaries. Use `session show` for one transcript,
and delete only idle sessions after reviewing their worktree state.

## Updates

Use `update = "check"` to observe source changes, `"prompt"` to require an
explicit update command, or `"auto"` for staged startup updates. Keep explicit
Git revisions pinned in production and review permission changes before
accepting an update.
