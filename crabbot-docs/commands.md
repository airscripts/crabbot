# Commands

```text
crabbot help
crabbot --version
crabbot init [--force] [--json]
crabbot doctor [--fix] [--json]
crabbot status [--json]
crabbot version [--json]
crabbot completion <bash|fish|powershell|zsh>
crabbot plugin list [--json]
crabbot plugin install <id> [source] [--revision <rev>] [--yes] [--json]
crabbot plugin link <id> [folder] [--revision <rev>] [--yes] [--json]
crabbot plugin update [--json]
crabbot plugin remove <id> [--yes] [--json]
crabbot session new <id> [--model <name>] [--json]
crabbot session list [--json]
crabbot session show <id> [--json]
crabbot session fork <source> <target> [--json]
crabbot session model <id> <model> [--json]
crabbot session cancel <id> [--json]
crabbot session delete <id> --yes [--json]
crabbot delivery list [--json]
crabbot delivery retry <id> --yes [--json]
crabbot delivery drop <id> --yes [--json]
crabbot export [PATH] [--path <PATH>] [--force] [--json]
crabbot validate [--path <PATH>] [--json]
crabbot import [--path <PATH>] --yes [--force] [--json]
crabbot service [install [--force]|remove --yes|status|start|stop] [--json]
crabbot <plugin-command> [arguments...]
```

`crabbot help` and `crabbot --help` show the command tree and Crabbot banner.
Native commands and currently registered plugin commands are shown in separate
lists. The plugin list is rebuilt from the installed plugin registry, so it
includes commands added by newly installed or linked plugins.
The installed `crab` executable is a shorter alias for `crabbot` and accepts
the same commands and options.
`crabbot --version` and `crabbot version` print the same package version.
Use `crabbot version --json` when a structured `{ "name", "version" }` result
is needed.

`crabbot completion <shell>` writes a completion script to standard output.
Supported shells are Bash, Fish, PowerShell, and Zsh. Redirect the
script to the shell’s normal completion directory or source it according to
the shell’s installation conventions. The generated command name follows the
executable used to invoke it, so `crab completion <shell>` generates a
completion script for `crab`.

For example:

```sh
crabbot completion bash > ~/.local/share/bash-completion/completions/crabbot
crabbot completion zsh > ~/.zfunc/_crabbot
crabbot completion fish > ~/.config/fish/completions/crabbot.fish
crab completion bash > ~/.local/share/bash-completion/completions/crab
```

In PowerShell, add the generated script to the current profile:

```powershell
crabbot completion powershell >> $PROFILE
```

The global `--json` flag selects structured JSON output for native commands,
including Crabfile export, validation, and import; plugin listing,
installation, linking, updating, and removal; session
creation, forking, model changes, cancellation, and deletion; and delivery
retry and drop operations. The command-level form remains available where the
subcommand declares it. Completion intentionally writes its shell script
directly to standard output instead of wrapping it in JSON. Arbitrary external
plugin commands own their own output schema. `--debug` prints detailed error
diagnostics. `--verbose` prints diagnostic progress, elapsed time, and nested
error causes for every command.
Help is available as `-h`, `-H`, or `--help`; version is available as `-v`,
`-V`, or `--version`. Global flags may appear before or after the subcommand.
Command failures are printed as concise `Error: ...` messages on stderr.
`--verbose` enables informational timing and cause events, and `--debug` also
enables debug representations and a redacted private report. JSON mode emits
the error as a pretty-printed `{"error":"..."}` object.
When `--debug` handles a failure, Crabbot also makes a redacted, private report
under `<CRABBOT_HOME>/debug/` when the filesystem permits it. The report path is
logged at info level; report creation is best effort and never replaces the
original command error.

Confirmation is explicit for unattended destructive or duplicate-prone work:
use `--yes` for plugin replacement or removal, session deletion, service
removal, delivery retry or drop, and Crabfile import. `--force` separately
permits replacing an existing imported configuration, exported Crabfile, or
service definition. Native commands do not read stdin for these confirmations.
Import reports a missing Crabfile with its expected path and
validates TOML before changing local state; malformed Crabfiles include the
line and column of the parse error. Use `--path <PATH>` to import a different
file.

`crabbot export` writes `./Crabfile` by default. Pass a positional directory,
such as `crabbot export .`, or `--path <PATH>` to choose an output location;
an existing directory receives a `Crabfile` file, while a new path is treated
as the exact output filename. Exporting with no local config or plugin lock
uses the default config and an empty plugin list. If the output Crabfile is
already present, export stops without changing it; pass `--force` to overwrite
that file.

`crabbot status` prints aligned installation health, daemon state, intelligence
setup, messaging setup, version, and the installed plugin count. Use `--json`
for automation; capability fields are objects with a `status` and `plugins`
array.

`crabbot validate` checks a Crabfile without changing local state. It uses
`./Crabfile` by default or the path supplied with `--path`, and reports the
first syntax or schema error. See the [Crabfile specification](crabfile.md)
for the supported version and keys.

`crabbot plugin list --json` returns an object with an `items` array. A visible
plugin directory with a missing or invalid manifest is reported as an error so
the inventory cannot silently hide broken installation state.

`crabbot plugin install` and `crabbot plugin link` validate and register one
plugin at a time. When the daemon is running, it starts the new plugin
immediately; otherwise, the next daemon start discovers it. The core artifact
contains the CLI and daemon executables, but no plugin binaries. Removing a
plugin unloads its active process before removing its files. Updating plugins
unloads and reloads only processes that were active, without restarting the
daemon; inactive plugins stay inactive.

Plugin commands are registered by installed plugins. The Pi agent plugin
registers `crabbot code`, the TUI plugin registers `crabbot tui`, and the Codex
plugin registers `crabbot codex`; native commands always take precedence, and
duplicate plugin command names are rejected during installation or update.

When an installed model plugin is available, Crabbot also registers the
host-managed `crabbot ask` command. It accepts `--plugin <id>`, `--model
<name>`, and prompt words, and selects a configured or available model plugin
when `--plugin` is omitted. The command is absent when no installed model
plugin can run, so the CLI does not advertise an unusable intelligence path.

Inside the terminal client, `/help` lists controls, `/status` reports the
authenticated daemon state, and `/approval` reports the daemon approval mode.
`/approvals` lists pending mutating-tool requests. Resolve one with
`/approve <id>` or `/deny <id>`; each action is sent through authenticated
daemon IPC and applies only to the matching pending request.
`/sessions` lists bounded session summaries.
`/deliveries` lists pending and uncertain outbox entries; `/retry <id>` and
`/drop <id>` apply the same explicit delivery controls as the native CLI.
`/model <name>` changes the selected session's model, `/session <id>` resumes an
existing persisted session, and `/new <id>` creates and selects a session.
Completed turns are stored through authenticated daemon IPC. `/plugins` lists
installed plugins. `/workspace` reports the selected workspace, `/workspace
<path>` selects a canonical existing directory for that session, and
`/workspace reset` returns to `CRABBOT_ROOT`. Workspace changes are persisted
through authenticated daemon IPC and apply to later model turns in that
session. `/clear` removes the selected session's transcript when it is idle.
`/timer add <seconds> <text>` schedules a reminder; `/timer list` and
`/timer remove <id>` inspect or remove reminders through authenticated daemon
IPC. `/memory remember <key>=<value>`, `/memory list`, and `/memory forget
<key>` manage memories scoped to the selected session through the same host
contract. Entering the `remember` command explicitly approves a suggest-mode
memory write. `/quit` and `/exit` close the terminal client.

`session list --json` returns bounded session summaries. Use `session show` for
the transcript of one session; it renders a readable transcript by default and
the bounded structured record with `--json`. Session deletion requires `--yes`.

`delivery list` shows pending and uncertain outbox entries without transcript
text. A delivery marked uncertain may already have reached the provider;
`delivery retry` is therefore explicit and requires `--yes`. Use `delivery
drop` to acknowledge and remove an entry without sending it again.

`session cancel` waits for the active turn to stop and acknowledge cancellation
before it reports success. If the turn does not acknowledge within its bounded
wait, the command reports an error instead of claiming cancellation completed.

The normal first-run sequence is:

```sh
crabbot init
crabbot plugin list
crabbot doctor
crabbot-daemon
```

`init` creates the home directory and starter configuration without starting
the daemon. If the home directory already exists, it reports that Crabbot is
already initialized and leaves existing state unchanged. Pass `--force` to
recreate the default configuration while preserving installed plugins and
runtime state. `doctor` is read-only by default and validates configuration,
plugin manifests, protocol compatibility, credentials, and local state. Pass
`doctor --fix` to create missing safe local state, such as the default config
or plugins directory; it never overwrites an existing config or repairs plugin
binaries. Plain human output ends with a health summary; unhealthy output
suggests `crabbot doctor --fix`. `doctor --fix` reports only the repairs
performed in both human and JSON output; regular `doctor --json` includes the
full structured result and `health` object. `crabbot-daemon` keeps
the daemon in the foreground so service managers and operators can observe its
diagnostics. Use `crabbot service install` only after the foreground flow is
healthy.

`crabbot service install` writes a native service definition for the current
platform, preserving the resolved CRABBOT_HOME and configured non-secret
environment paths. If provider variables are present, their declared values
are copied to a private service credential JSON file and the definition points
to it; existing CRABBOT_CREDENTIALS and CRABBOT_KEYRING=1 configuration is also
preserved. An existing definition is not replaced unless `--force` is supplied.
`remove` requires `--yes`. `start` and `stop` activate or deactivate it through systemd-user,
launchd, or the Windows Service Controller. remove stops or unloads the service
before deleting the definition, and status reports whether it is present.

`config.toml` accepts `update = "off"`, `"check"`, `"prompt"`, or `"auto"`;
the default is `prompt`. Direct messages are allowed by default. Group chats
require an explicit channel entry with `allow = ["id"]`. An optional
`mention = "@bot"` marker, `owner`, `admin`, `member`, `topic`, or `thread`
filter can narrow those entries further under `[channels.telegram]` or
`[channels.discord]`. Group turns use isolated Git worktrees by default; set
`worktree = false` to opt into the configured workspace. Role, topic, and
thread values are platform IDs represented as strings.

The core reports capability-specific guidance when a requested provider,
channel, store, timer, tool, speech plugin, or client is not installed.
Plugin installation and linking stage the manifest and binary before
atomically replacing an existing plugin. Sources may be local directories,
Git URLs with an optional `--revision`, or verified `.tar`, `.tar.gz`, `.tgz`,
and `.zip` archives. Archive sources must include a `#sha256=<64-hex-digits>`
fragment. Remote archives require HTTPS; extraction rejects unsafe paths and
symbolic links and hard links. Remote archive downloads use the system `curl`,
`tar`, or `unzip` commands and are capped at 64 MiB. The update command
refreshes every locked source through the same staged path, validates its
manifest and protocol, then activates it atomically. A failed source leaves
the previous plugin active.

Approved workspace commands and local Whisper processes have bounded output
and a two-minute execution deadline. Deleting an idle session reports whether
its Git worktree was reclaimed; failed cleanup is retried when the daemon next
starts.

For a one-shot local request, use `crabbot ask --plugin <id> --model <model>
<prompt...>` when an installed model plugin is available. This bypasses channel
routing and uses the selected model plugin directly. It still applies provider
validation and protocol limits, but it does not create a durable chat session.

`crabbot code` is provided by the optional Pi agent plugin. It keeps Crabbot as
the session, intelligence, workspace, tool, and approval owner while using an
external Pi runtime as the coding harness. Install Pi separately, install and
link the `pi` plugin, then run:

```sh
crabbot code "Inspect the repository and propose the next implementation step."
crabbot code --session feature-auth "Implement the approved change."
```

The Pi plugin disables its built-in tools and forwards registered coding tools
through Crabbot's confined tools plugin. Mutating tools obey the configured
`off`, `prompt`, or `auto` approval mode.

If no installed model plugin can run, `ask` is unavailable and reports that an
installed intelligence plugin is needed.
