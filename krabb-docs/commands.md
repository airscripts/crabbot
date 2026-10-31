# Commands

Use this reference to find CLI, daemon, plugin, and terminal commands.

```text
krabb help
krabb --version
krabb init [--force] [-y|--yes] [--json]
krabb doctor [--fix] [--json]
krabb config get [<key>] [--json]
krabb config set <key=value> [--force] [--json]
krabb status [--json]
krabb version [--json]
krabb completion <bash|fish|powershell|zsh>
krabb plugin list [<page>] [--json]
krabb plugin install <id>... [--source <path-or-url>] [--revision <rev>] [--link] [-y|--yes] [--json]
krabb plugin update [-y|--yes] [--json]
krabb plugin uninstall <id>... [-y|--yes] [--force] [--json]
krabb memory <status|search|list|show|remember|edit|forget|audit|converge|learning|help> [arguments...]
krabb session new <id> [--model <name>] [--json]
krabb session list [<page>] [--json]
krabb session show <id> [--json]
krabb session fork <source> <target> [--json]
krabb session model <id> <model> [--json]
krabb session cancel <id> [--json]
krabb session delete <id> [-y|--yes] [--json]
krabb delivery list [<page>] [--json]
krabb delivery retry <id> [-y|--yes] [--json]
krabb delivery drop <id> [-y|--yes] [--json]
krabb export [PATH] [--path <PATH>] [--force] [--json]
krabb validate [--path <PATH>] [--json]
krabb import [--path <PATH>] [-y|--yes] [--force] [--json]
krabb service [install [--force]|uninstall [-y|--yes]|status|start|stop] [--json]
krabb tui [--session <id>] [--model <name>] [--plugin <id>]
krabb tui --once <prompt> [--session <id>] [--model <name>] [--plugin <id>]
krabb <plugin-command> [arguments...]
```

## Table Of Contents

- [Command Tree And Help](#command-tree-and-help)
- [Shell Completion](#shell-completion)
- [Output And Logging](#output-and-logging)
- [Confirmation And File Operations](#confirmation-and-file-operations)
- [Status And Health](#status-and-health)
- [Terminal User Interface](#terminal-user-interface)
- [Plugin Management](#plugin-management)
- [Memory](#memory)
- [Sessions And Deliveries](#sessions-and-deliveries)
- [Scheduling](#scheduling)
- [Initialization And Service Management](#initialization-and-service-management)
- [Providers And Agent Interoperability](#providers-and-agent-interoperability)

## Command Tree And Help

`krabb help` and `krabb --help` group commands into three types:

- **Native commands** are part of the Krabb CLI and do not depend on a plugin,
  such as `init`, `doctor`, and `plugin`.
- **Conditional native commands** are first-party Krabb commands that need an
  installed plugin or capability. For example, `session` needs a model,
  `delivery` needs a messaging channel, `schedule` needs the scheduler, and
  `memory`, `tui`, and `data` need their respective plugins. These commands
  expose a Krabb-managed capability and appear in help when usable; an
  underlying plugin may implement the capability.
- **Plugin commands** are declared by a plugin and delegated to it. They appear
  under Plugin Commands when installed, for example `codex`.
- `krabb service` prints service help; use `krabb service status` for its
  current state. `krabb --version` and `krabb version` are equivalent.

`ask` is a native command that is always listed and reports an actionable error
if no model is ready. SQLite's `data` command is a first-party conditional
command: runtime exposes it when SQLite is installed and routes its data
operations to the plugin. The agent-facing data tool uses the same conditional
capability.

Command discovery waits up to five seconds for concurrent metadata access. If the
metadata remains locked or cannot be read, the command exits with an error;
it does not report success without executing the requested plugin command.

For the boundary between the core library, runtime, and plugins, see
[Product Architecture](product.md#architecture-and-interfaces).

## Shell Completion

`krabb completion <shell>` writes a completion script to standard output.
Supported shells are Bash, Fish, PowerShell, and Zsh. Redirect the
script to the shell’s normal completion directory or source it according to
the shell’s installation conventions. The generated command name follows the
executable used to invoke it, so `krabb completion <shell>` generates a
completion script for `krabb`.

For example:

```sh
krabb completion bash > ~/.local/share/bash-completion/completions/krabb
krabb completion zsh > ~/.zfunc/_krabb
krabb completion fish > ~/.config/fish/completions/krabb.fish
```

In PowerShell, add the generated script to the current profile:

```powershell
krabb completion powershell >> $PROFILE
```

## Output And Logging

- `--json` writes structured command results to stdout. Diagnostics go to
  stderr; failures return a JSON error on stderr. Plugin commands send stream
  notifications, including device login instructions, to stderr and emit only
  the final structured result on stdout. ACP includes its answer in the result's
  `text` field.
- Multiline CLI error details are kept flush left for easier reading and copying.
- `--verbose` shows progress and nested error causes. `--debug` also prints
  internal diagnostics.
- `--help` (`-h`, `-H`) shows command help; `--version` (`-v`, `-V`) prints
  the version.
- Application logs are stored under `KRABB_HOME/logs/`. Set `KRABB_LOG` or
  `RUST_LOG` to filter them; set `KRABB_LOG_FORMAT=json` for JSON logs.

See [Configuration](configuration.md) for log paths and settings.

## Confirmation And File Operations

Destructive or duplicate-prone commands prompt in an interactive terminal.
Use `--yes` or `-y` to skip the prompt. With `--json` or non-terminal input,
pass `--yes` for operations that need confirmation. `--force` allows replacement
of existing configuration, Krabfiles, or service definitions.
Import reports a missing Krabfile with its expected path and
validates TOML before changing local state; malformed Krabfiles include the
line and column of the parse error. Use `--path <PATH>` to import a different
file.

`krabb export` writes `./Krabfile` by default. Pass a positional directory,
such as `krabb export .`, or `--path <PATH>` to choose an output location;
an existing directory receives a `Krabfile` file, while a new path is treated
as the exact output filename. Exporting with no local config or plugin lock
uses the default config and an empty plugin list. If the output Krabfile is
already present, export stops without changing it; pass `--force` to overwrite
that file.

## Status And Health

`krabb status` prints aligned installation health, background runtime state,
intelligence setup, messaging setup, version, and the installed plugin count.
Messaging is optional for local CLI and TUI use; health only requires a ready
messaging plugin when one is installed or explicitly selected. Health is
`unhealthy` when a required setup check fails, and the output lists the reasons.
Use `--json` for automation; the
health details are in `health_details`, and capability fields are objects with
a `status` and `plugins` array.

## Terminal User Interface

`krabb tui` requires the daemon. Use `krabb service start` or run
`krabb-daemon` in the foreground. `krabb tui --once "prompt"` sends one prompt
without opening the full-screen client; pass `--session` to continue an
existing session.

The TUI supports `/help`, `/status`, `/model`, `/session`, `/workspace`,
`/approvals`, `/schedule`, `/memory`, `/data`, and `/plugin`. Use `/help` for
the commands available in the current session. `/data value` manages simple
session-scoped values; `/data database`, `/data table`, and `/data column`
manage structured records. `/store` remains as a compatibility alias. See the
[TUI guide](../krabb-plugins/tui/README.md) for controls, sessions, and client
commands.

Typed `!command` shell execution requires `clients.tui.shell` and is supported on Linux
and Windows. Other platforms reject typed shell commands because descendant containment
is unavailable for this execution path. Esc waits for process cleanup before completing
cancellation, and the next command receives a fresh cancellation token.

## Plugin Management

```sh
krabb plugin list [page]
krabb plugin install <id>... [--source <path-or-url>] [--revision <rev>] [--link]
krabb plugin update
krabb plugin uninstall <id>...
```

Install accepts official IDs or one explicit source. Sources may be local
paths, Git revisions, or HTTPS archives with a `#sha256=` digest. `--link` is
for local development builds. Installation validates each manifest and stages
files before activation; a running daemon loads new plugins over local IPC.
Interactive install, update, and import operations show an in-place spinner.
Plugin results use one-line tree entries; failed installs and updates point to
the current application log file.

`plugin update` previews changes, then replaces changed plugins after
confirmation. Uninstall accepts multiple plugin IDs and confirms the batch once;
it waits for active daemon work to settle and reserves the lifecycle through state
checks, process unloading, and file removal. Removing
the last model or messaging plugin can require deleting dependent session or
delivery state; `--force` with confirmation performs that purge. See
[Plugins](plugins.md) for manifests, permissions, and update behavior.

## Memory

The memory plugin provides `krabb memory status|search|list|show|remember|edit|forget|audit|converge|learning`.
Use `krabb memory help` for syntax. Learning defaults to `guided`; autonomous
learning is opt-in. See the
[Memory plugin guide](../krabb-plugins/memory/README.md) for scopes and record
behavior.

## Structured Data

When SQLite is installed, `krabb data` exposes commands to create and inspect
logical databases and tables, and insert, query, update, and delete structured
records. The TUI offers the same operations through `/data`; those commands
share the active TUI session's data with the agent. CLI data uses a separate
local scope by default; use `krabb data --scope tui:default ...` to inspect
that TUI conversation's data. Running `krabb data` without a subcommand or
passing `krabb data help` shows the full usage, commands, options, and notes.
CLI results are human-readable by default; add `--json` for JSON output. Tables
can be created empty, then defined with `krabb data column add DB TABLE NAME TYPE`
and `krabb data column remove DB TABLE COLUMN`; JSON schemas remain available
for bulk creation. Use `text` or `string` for text columns. List results use
tree branches like other Krabb inventories.
See the
[SQLite guide](../krabb-plugins/sqlite/README.md) for examples, limits, and
column types.

## Sessions And Deliveries

```sh
krabb session new <id> [--model <name>]
krabb session list [page]
krabb session show <id>
krabb session fork <source> <target>
krabb session model <id> <model>
krabb session cancel <id>
krabb session delete <id> [--yes]
krabb delivery list [page]
krabb delivery retry <id> [--yes]
krabb delivery drop <id> [--yes]
```

Lists are paginated; `--json` returns structured state. `session show` displays
one transcript. Cancellation waits for the turn to stop. An uncertain delivery
may already have reached its destination; retry only when a duplicate is
acceptable. See [Concepts](concepts.md) for session and delivery state.

## Scheduling

The scheduler plugin adds `krabb schedule add|list|history|run|retry|pause|resume|remove`.
Create a task with one timing option and an explicit provider and model:

```sh
krabb schedule add --after 5m --provider codex --model <model> \
  "<instruction>"
krabb schedule add --at "2026-10-20 09:00" --timezone Europe/Rome \
  --provider codex --model <model> "<instruction>"
krabb schedule add --every 1d --provider codex --model <model> \
  "<instruction>"
krabb schedule add --cron '0 9 * * *' --timezone Europe/Rome \
  --provider codex --model <model> "<instruction>"
```

`--after` sets a delay; `--at` sets a start time. Use `--every` for an interval
and combine it with `--after` or `--at` to choose the first run. Use `--cron`
for a calendar schedule with a timezone. Other options include
`--missed skip|coalesce|replay`, `--retries 1..5`, and
`--on-failure pause|continue`. Shell use requires explicit enablement. Message
delivery requires an exact destination at task creation. Uncertain side effects
pause for review, and interrupted mutating turns are not replayed.
Pausing a running schedule lets its current run finish before preventing later
occurrences.

Use `krabb schedule help` for full syntax. The
[Scheduler guide](../krabb-plugins/scheduler/README.md) covers task
authorization, retries, recovery, and delivery behavior.

## Initialization And Service Management

First run:

```sh
krabb init
krabb plugin list
krabb doctor
krabb status
krabb-daemon
```

`krabb init` creates the home directory, starter configuration, and workspace
instructions. `krabb doctor` is read-only; `krabb doctor --fix` creates missing
safe defaults. `krabb init --force` erases all data under `KRABB_HOME` and
requires confirmation.

Manage the background service with
`krabb service install|uninstall|status|start|stop|restart`. Installing does
not start it. `--force` replaces an existing service configuration; uninstall
confirms before removing it. Generated services preserve explicitly configured
browser and computer settings, including Playwright paths and native desktop
opt-in. Windows installations register `krabb-daemon --service` with SCM, which
receives lifecycle status and sends stop/shutdown controls through normal daemon
cleanup. Running `krabb-daemon` without this flag stays in the foreground.
Service installation persists `KRABB_CODEX_HOME`, defaulting to the installing user's
`HOME/.codex` or `USERPROFILE/.codex`, so the daemon can use the existing Codex sign-in.
Windows services run as the dedicated `NT SERVICE\Krabb` account. Installation grants
that account access to the executable and existing private state, including the Codex
profile. Configured `KRABB_ROOT` and `KRABB_MEDIA` directories receive inheritable
modify access without replacing their existing permissions. Configured Playwright package
trees, Chromium directories, and explicit `KRABB_NODE` executable paths receive read/execute
access. The resolved `KRABB_CODEX_BINARY` executable also receives read/execute access,
without granting access to its parent directory or the entire user profile.
Reinstall with `krabb service install --force` to migrate an older LocalSystem
service; this stops the service, so run `krabb service start` afterward. Failed reinstalls
retain the unprivileged identity when restoring the previous configuration.
Private files remain accessible to the data-owning account after service writes.
See [Workflows](workflows.md) for platform setup.

## Providers And Agent Interoperability

Use `krabb plugin list` and `krabb doctor` to inspect installed capabilities.
For one-shot model requests, run:

```sh
krabb ask --provider <id> --model <name> <prompt...>
```

`krabb acp --workspace <path> <prompt>` routes filesystem requests through the
tools plugin. The selected directory must stay under the configured workspace;
writes require the configured approval policy. Approval prompts go to stderr;
JSON mode and noninteractive execution deny operations requiring a prompt.
`--session <id>` resumes the requested ACP session, including agents returning an
empty load result. Advertised authentication methods do not block an already signed-in
agent; an authentication-required session error is reported by the adapter.
`krabb ag-ui` serves until
interrupted, with bounded model work for each HTTP run.

Provider-specific setup and supported agent protocols are in
[Providers](providers.md). Each integration's README describes its limits.
