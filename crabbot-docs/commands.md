# Commands

```text
crabbot help
crabbot --version
crabbot init [--force] [-y|--yes] [--json]
crabbot doctor [--fix] [--json]
crabbot config get [<key>] [--json]
crabbot config set <key=value> [--force] [--json]
crabbot status [--json]
crabbot version [--json]
crabbot completion <bash|fish|powershell|zsh>
crabbot plugin list [<page>] [--json]
crabbot plugin install <id>... [--source <path-or-url>] [--revision <rev>] [--link] [-y|--yes] [--json]
crabbot plugin update [-y|--yes] [--json]
crabbot plugin uninstall <id> [-y|--yes] [--force] [--json]
crabbot memory <status|search|list|show|remember|edit|forget|audit|learning> [arguments...]
crabbot session new <id> [--model <name>] [--json]
crabbot session list [<page>] [--json]
crabbot session show <id> [--json]
crabbot session fork <source> <target> [--json]
crabbot session model <id> <model> [--json]
crabbot session cancel <id> [--json]
crabbot session delete <id> [-y|--yes] [--json]
crabbot delivery list [<page>] [--json]
crabbot delivery retry <id> [-y|--yes] [--json]
crabbot delivery drop <id> [-y|--yes] [--json]
crabbot export [PATH] [--path <PATH>] [--force] [--json]
crabbot validate [--path <PATH>] [--json]
crabbot import [--path <PATH>] [-y|--yes] [--force] [--json]
crabbot service [install [--force]|uninstall [-y|--yes]|status|start|stop] [--json]
crabbot tui [--session <id>] [--model <name>] [--plugin <id>]
crabbot tui --once <prompt> [--session <id>] [--model <name>] [--plugin <id>]
crabbot <plugin-command> [arguments...]
```

`crabbot help` and `crabbot --help` show the command tree and Crabbot banner.
Native commands, conditional commands, and plugin-contributed commands are
shown in separate lists. The conditional list is rebuilt from installed
capabilities; `ask` and `session` require an intelligence plugin, while
`delivery` requires a messaging plugin. Plugin commands are rebuilt from the
installed plugin registry, so the list includes commands added by newly
installed or linked plugins.
Running `crabbot service` without a subcommand prints native service help,
including the global options; use `crabbot service status` to inspect the
installed service and its state.
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
plugin commands own their own output schema. `--verbose` prints readable
progress, elapsed time, and nested error causes. `--debug` also prints internal
diagnostic logs and detailed error information.
Help is available as `-h`, `-H`, or `--help`; version is available as `-v`,
`-V`, or `--version`. Global flags may appear before or after the subcommand.
Command failures appear directly on stderr, with continuation lines indented
and separate error groups divided by a blank line. `--verbose` and `--debug`
also log failures at `ERROR`.
Application logs are written to daily files in `CRABBOT_HOME/logs/`, named by
UTC date as `YYYY-MM-DD.log` or `YYYY-MM-DD.jsonl`; they include all tracing
levels by default. Command failures are recorded there even when diagnostics
are hidden.
Commands show only their human-readable output unless `--verbose` is set;
verbose progress and error details are human-readable. `--debug` also prints
internal logs (`DEBUG` and higher) to standard error. `--json` keeps diagnostics
off standard output so structured command output stays clean; failures are
reported as a JSON object on standard error.
Unrecoverable failures use `ERROR`, recoverable problems use
`WARN`, normal progress uses `INFO`, and `DEBUG` adds diagnostic details.
Successful state changes are recorded at `INFO`; routine message queue and
delivery acknowledgements use `DEBUG`. These success events omit message
content and user-supplied identifiers; configuration logs include the key but
not its value. Successful operations do not use a separate `OK` level.
Set `CRABBOT_LOG` to filter application logs, or use `RUST_LOG` as its fallback.
Set `CRABBOT_LOG_FORMAT=json` for structured JSON file logs; the default is
text. These settings apply to the daemon and plugin processes as well as the
CLI.
When the filesystem permits, commands run with `--verbose` or `--debug` save
their raw application log events to a private per-command file under
`CRABBOT_HOME/logs/`. Non-JSON commands print the file path when they finish.
The file uses the configured text or JSON log format; the daily application log
continues to collect events as well. JSON mode keeps the path off the terminal.

Destructive or duplicate-prone commands prompt in an interactive terminal.
Use `--yes` or `-y` to skip the prompt. With `--json` or non-terminal input,
pass `--yes` for operations that need confirmation. `--force` allows replacement
of existing configuration, Crabfiles, or service definitions.
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

`crabbot status` prints aligned installation health, background runtime state,
intelligence setup, messaging setup, version, and the installed plugin count.
Messaging is optional for local CLI and TUI use; health only requires a ready
messaging plugin when one is installed or explicitly selected. Health is
`unhealthy` when a required setup check fails, and the output lists the reasons.
Use `--json` for automation; the
health details are in `health_details`, and capability fields are objects with
a `status` and `plugins` array.

`crabbot tui` opens a full-screen terminal chat with a scrollable conversation
and an editable message box. Press Enter to send, Ctrl+O to add a line,
Up/Down to browse input history, Page Up/Page Down to move the conversation by
a page, and the mouse wheel to scroll the pane under the pointer. New output
stays in view unless you have scrolled back; scrolling down to the latest output
resumes following it. The input box follows the cursor while editing. Hold Shift
while dragging to select and copy transcript text; the mouse wheel remains
available for scrolling. Press Escape or Ctrl-C to quit. The bottom-right footer shows
the Crabbot version and `/help` hint. `/help` lists commands available in this session;
conditional commands appear only when their plugin capability and required
background runtime are available. Plugin-contributed commands remain CLI
commands and are run as `crab <command>`. `/model` shows the current model;
`/model <id>` changes it when an intelligence plugin is installed. Use
`/statusline` to open a checkbox menu for the title, model, context fill,
session, workspace, and status. Move with Up/Down, toggle with Space, save with
Enter, or cancel with Escape. At least one detail stays enabled. Long lines
scroll across the footer so each enabled detail remains available. Context fill
shows the latest provider-reported token count and model limit, for example
`12,345/128,000 (9.6%)`; it shows `context: unavailable` when the provider does
not report both values. Use `/statusline reset` to restore the default choices.
Choices are saved to `<CRABBOT_HOME>/data/plugins/tui/preferences.toml`. The
displayed Crabbot name defaults to `Crabbot` and can be changed globally with
`name` in `config.toml`.
Input history is saved in `<CRABBOT_HOME>/data/plugins/tui/history.json`; use
`/history list`, `/history clear`, and `/history help` inside the TUI to manage it.
When no intelligence plugin is installed, the statusline shows the model as
`unset`, regardless of the model value saved in the session.

The daemon must be running before opening the TUI or using `--once`; start it
with `crab service start` or run `crabbot-daemon` in the foreground. Both modes
use daemon-managed model turns, tools, approvals, and the shared session store.
The TUI refuses to start when the daemon is unavailable. `--once <prompt>` sends
one prompt and prints the response without opening the full-screen interface;
quote prompts containing spaces.
The default session ID is `default`; use `--session`, `--model`, and `--plugin`
to select a different session and model configuration. Without `--session`, the
TUI reopens the last TUI session, or starts `default` when none exist.

Local command replies and streamed model text appear with a brief typewriter
reveal. During model generation, a rotating crab-themed status is shown. Press
Escape to interrupt generation; the partial reply remains in the conversation.
Use `/animation off` to show replies immediately, `/animation on` to restore the
effect, and `/animation` to check its setting. The choice is saved with the TUI
preferences.

The conversation uses the available screen without an outer frame. Crabbot
replies appear in rounded orange bubbles on the left, while your messages appear
in rounded green bubbles on the right; the input border uses matching rounded
corners. Saved user and assistant messages show UTC timestamps, and generation
status includes elapsed time.
When Codex emits multiple assistant messages in one turn, each is separated
from the next by a blank line.
The TUI uses the daemon's shared tool and approval policy. Tool access is
disabled for TUI sessions unless `[clients.tui] tools = true` is set in
`config.toml`; when enabled, the global `approval` mode still governs mutating
tools. Tool paths remain confined to the active session workspace. On exit, the
input title shows that Crabbot is waiting for the daemon turn to stop.
During generation, normal messages are disabled; you can begin typing `/` to
enter an approval command even from an empty input, and Escape remains available.
Prefix a line with `!` to invoke the daemon's shell directly
in the active session workspace. This requires `clients.tui.shell = true`, but does not
require the Tools plugin or an approval prompt because the command is explicitly
entered by you. The daemon reads this setting at startup, so restart it after
changing the value. If shell is disabled, the TUI reports the config file used
by the running daemon and keeps that System response in the session history.
You remain the sender of both command types; their local replies and shell
output appear as neutral gray System entries, while intelligence responses
remain attributed to Crabbot.

`crabbot validate` checks a Crabfile without changing local state. It uses
`./Crabfile` by default or the path supplied with `--path`, and reports the
first syntax or schema error. See the [Crabfile specification](crabfile.md)
for the supported version and keys.

`crabbot plugin list [page]` shows up to five installed plugins at a time in a
tree view with version, health, protocol, capabilities, commands, and
permissions. Omit `page` to show the first page. The TUI's `/plugin [page]`
command presents the same metadata in compact, paginated entries.
`crabbot plugin list --json` returns the complete inventory in an object with
an `items` array, regardless of the selected page. A visible plugin
directory with a missing or invalid manifest is reported as an error so the
inventory cannot silently hide broken installation state.

`crabbot plugin install` accepts multiple IDs and validates and registers each
plugin independently. Use `--source` and `--revision` only when installing one
plugin from an explicit source. By default, installation copies plugin binaries;
`--link` instead links a local build for development. Linked plugins can be
rebuilt in place without reinstalling; restart the daemon with
`crab service restart` to load the rebuilt executable. The linked source and
manifest must remain at their recorded paths. When the background
runtime is running, it starts server plugins immediately; otherwise, the next
runtime start discovers them. Installation groups per-plugin progress and
results, then prints one success summary followed by separate runtime guidance
for plugins that need the daemon. The TUI also
requires the daemon to be running before it can open. The core artifact contains
the CLI and daemon executables, but no plugin binaries. Uninstalling a plugin
unloads its active process before removing its files. `crabbot plugin update`
previews available version, source-revision, and content changes without
changing installed plugins. Confirm in the terminal to apply the preview, or
use `--yes` to skip the prompt. Only changed plugins are replaced, and only
active plugins are unloaded and reloaded. Inactive plugins stay inactive.

Plugin-contributed commands are registered by installed plugins. The Pi agent
plugin registers `crabbot code`, the TUI plugin registers `crabbot tui`, the
Codex plugin registers `crabbot codex`, and the memory plugin registers
`crabbot memory`. Native command names are reserved, including those whose
availability depends on an installed capability, and duplicate plugin command
names are rejected during installation or update. Plugin-contributed commands
are available only when their owning plugin is installed.

Uninstalling the last installed intelligence plugin is blocked while persisted
sessions remain; delete them with `crabbot session delete <id> --yes` first.
Uninstalling the last messaging plugin is blocked while outbox or dead-letter
deliveries remain; retry or discard outbox deliveries with `crabbot delivery`
first. To intentionally uninstall the last matching plugin and purge its dependent
state, pass `-y --force` to `crabbot plugin uninstall <id>`. This permanently
deletes all sessions when uninstalling the last intelligence plugin, or all
outbox and dead-letter deliveries when uninstalling the last messaging
plugin. Session worktrees are removed too; any worktrees that cannot be removed
are reported for later cleanup. `--force` does not purge state when another
available plugin of the same capability remains.

The memory command supports `status`, `search <text>`, `list`, `show <key>`,
`remember <key> <text>`, `edit <key> <text>`, `forget <key>`, `audit`, and
`learning <guided|autonomous>`. Use `--scope <id>` on supported operations to
select a scope; JSON output is available through the global `--json` flag.
Learning defaults to `guided`, where the agent stores a memory only after an
explicit user request. `autonomous` lets it retain stable, useful facts while
excluding secrets and sensitive inferences. These instructions do not override
tool availability or channel policy.

When an installed model plugin is available, Crabbot also exposes the
host-managed `crabbot ask` command as a conditional command. It accepts
`--plugin <id>`, `--model <name>`, and prompt words, and selects a configured or
available model plugin when `--plugin` is omitted. The command is absent when
no installed model plugin can run, so the CLI does not advertise an unusable
intelligence path. `session` is exposed under the same condition; `delivery`
is exposed when an installed messaging plugin is available. These conditional
commands remain runtime-owned even though their availability depends on plugins.

Inside the terminal client, `/help` lists controls, `/status` reports the
authenticated daemon state, and `/approval` reports the daemon approval mode.
`/approvals` lists pending mutating-tool requests. Resolve one with
`/approve <id>` or `/deny <id>`; each action is sent through authenticated
daemon IPC and applies only to the matching pending request.
When an approval picker is open, Left/Right selects Approve or Deny and Enter
submits that choice.
`/session help` lists TUI session commands; `/session list [page]` shows five
bounded per-session rows per page with status, model, creation/update dates,
and message count. Sessions being worked on and the selected session appear
first; remaining sessions are ordered by most recent update, with archived
sessions last. Each list command reads a fresh snapshot, so multiple TUI
instances do not share a page cursor. The selected session is marked `active`,
work in progress is `working`, and the model is `unset` when no intelligence
plugin is installed.
TUI windows following the same session refresh its persisted transcript
automatically once per second. A submitted user message appears in
other windows while its turn is running; the assistant reply appears when the
daemon saves it at turn completion. Other windows also show the shared working
state and prevent another chat message until that turn finishes; approval
commands remain available.
`/session switch <id>` resumes an existing persisted session, while
`/session create <id>` creates and selects one. `/session rename <new-id>`
renames the active session without losing its history; channel-backed sessions
cannot be renamed. `/session archive <id>...`
and `/session unarchive <id>...` archive or restore one or more inactive sessions without
deleting it; repeating either action reports that the session is already in
that state. Add `--all` to archive, unarchive, or delete every matching session;
the active session is always kept for archive/delete. `/session delete <id>...`
permanently deletes sessions after an interactive confirmation; use `-y` or
`--yes` to skip it. Add `--deep` to also remove the matching shared session record
and the TUI's local fallback entry; when that removes the final local entry, the
fallback JSON file is deleted. Without `--deep`, deletion keeps the current
behavior.
Command action
errors show the specific actionable error directly, without a redundant
operation-failed prefix; add operation context only when the underlying error
does not explain the failure. `/new <id>` is a shortcut for session creation.
`/deliveries` lists pending and uncertain outbox entries; `/retry <id>` and
`/drop <id>` apply the same explicit delivery controls as the native CLI.
`/model help` shows model controls. With Codex selected, `/model list` lists
account models; `/model show` inspects the selected model, and `/model set <id>`
changes it. `/model <id>` remains a shortcut. The TUI does not assume a
provider-specific model by default. If a selected model is rejected, the failed
request is shown without closing the session, so the model can be corrected and
the prompt retried.
TUI commands and their displayed replies are also stored in the active session,
so command-only sessions and sessions used without an intelligence plugin can
be resumed with their visible interaction history. Completed model turns and
these command exchanges share the session's normal bounded history. `/plugin` lists
installed plugins. `/workspace` reports the selected workspace, `/workspace
<path>` selects a canonical existing directory for that session, and
`/workspace reset` returns to `CRABBOT_ROOT`. Workspace changes are persisted
through authenticated daemon IPC and apply to later model turns in that
session. The workspace is the filesystem root/current working area used by
file-oriented tools; it is separate from conversation history and is not a
guarantee that every plugin is confined to it. `/clear` removes the selected
session's transcript when it is idle.
`/timer add <seconds> <text>` schedules a reminder; `/timer list` and
`/timer remove <id>` inspect or remove reminders through authenticated daemon
IPC. `/memory remember <key>=<value>`, `/memory list`, and `/memory forget
<key>` manage memories scoped to the selected session through the same host
contract. Entering the `remember` command explicitly approves a guided memory
write. `/quit` and `/exit` close the terminal client.

`session list [page]` shows up to five sessions in a tree view, with working
sessions first and archived sessions last. `session list --json` returns the
complete bounded session summaries, regardless of page. Use `session show` for
the transcript of one session; it renders a readable transcript by default and
the bounded structured record with `--json`. Session deletion prompts in an
interactive terminal; pass `--yes` for non-interactive use.

`delivery list [page]` shows up to five pending and uncertain outbox entries
in a tree view, without transcript text. `delivery list --json` returns the
complete list regardless of page. A delivery marked uncertain may already
have reached the provider; `delivery retry` prompts before retrying. Use
`delivery drop` to remove an entry without sending it again.

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

`init` creates the home directory, starter configuration, and
`workspace/CRAB.md` and `workspace/CLAW.md` instruction templates without
starting the daemon. Edit those files manually: `CRAB.md` defines Crabbot's
identity and communication style; `CLAW.md` defines behavioral guidance and
workflows. If the home directory already exists, `init` reports that Crabbot
is already initialized and leaves it unchanged. `doctor --fix` can seed any
missing safe local state, including the workspace and instruction files.

`init --force` resets all Crabbot home data, including configuration, plugins,
sessions, workspace data, and edited instructions. It prompts for confirmation;
use `init --force --yes` for a non-interactive reset. Stop the daemon first.
The reset has no backup. Do not use `--yes` without `--force`.

The default file-tool root is `CRABBOT_HOME/workspace`; set `CRABBOT_ROOT` to
use a different root. This does not enable tools: channel `tools = true` and
the daemon approval policy are still required, and shell execution remains
separately disabled by default. Crabbot loads the two instruction files on each
turn alongside the bounded saved conversation. It does not load workspace
`AGENTS.md` into its own prompt; delegated coding agents may use those files.

`doctor` is read-only by default and validates configuration, plugin manifests,
protocol compatibility, credentials, and local state. Pass `doctor --fix` to
create missing safe local state; it never overwrites an existing config or
repairs plugin binaries. Human output groups the health summary and setup
checks; unhealthy output ends with a suggestion to run `crabbot doctor --fix`.
`doctor --fix` reports only the repairs performed in human and JSON output.
Regular `doctor --json` includes the full structured result and `health` object.
`crabbot-daemon` keeps
the daemon in the foreground so service managers and operators can observe its
diagnostics. Use `crabbot service install` only after the foreground flow is
healthy.

`crabbot service install` writes a native service definition for the current
platform, preserving the resolved CRABBOT_HOME and configured non-secret
environment paths. If provider variables are present, their declared values
are copied to a private service credential JSON file and the definition points
to it; existing CRABBOT_CREDENTIALS and CRABBOT_KEYRING=1 configuration is also
preserved. An existing definition is not replaced unless `--force` is supplied.
`uninstall` prompts before removal; use `--yes` (or `-y`) to skip the prompt.
`start` and `stop` activate or deactivate
it through systemd-user, launchd, or the Windows Service Controller. `restart`
restarts it through the native service manager without uninstalling or disabling
the service. Uninstall stops or unloads the service before deleting the
definition. Successful lifecycle commands say when the service has been
installed, started, stopped, restarted, or uninstalled. Installing the service
does not start it. `status` shows its state and service-definition path. On
Linux this is a user service, so use
`systemctl --user status crabbot.service` for detailed systemd output; plain
`systemctl status crabbot.service` checks the separate system-wide manager.
Repeated `start` and `stop` commands report when the service is already in the
requested state.

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
symbolic links and hard links. Remote downloads use the system `curl`, and
archive handling uses `tar` or `unzip` on Unix and Windows' built-in `tar`
command. Archives are capped at 64 MiB. The update command
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
