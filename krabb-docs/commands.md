# Commands

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
krabb plugin uninstall <id> [-y|--yes] [--force] [--json]
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

`krabb help` and `krabb --help` show the command tree and Krabb banner.
Native commands, conditional commands, and plugin-contributed commands are
shown in separate lists. The conditional list is rebuilt from installed
capabilities: `session` requires an intelligence plugin, `delivery` requires a
messaging plugin, and `schedule`, `memory`, and `tui` require their respective
plugins. `ask` is always listed because its command workflow belongs to
Krabb; it reports an actionable error when no intelligence provider is
installed and ready. Plugin commands are rebuilt from the installed plugin
registry, so the list includes commands added by newly installed or linked
plugins.
Running `krabb service` without a subcommand prints native service help,
including the global options; use `krabb service status` to inspect the
installed service and its state.
`krabb --version` and `krabb version` print the same package version.
Use `krabb version --json` when a structured `{ "name", "version" }` result
is needed.

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
krabb completion bash > ~/.local/share/bash-completion/completions/krabb
```

In PowerShell, add the generated script to the current profile:

```powershell
krabb completion powershell >> $PROFILE
```

The global `--json` flag selects structured JSON output for native commands,
including Krabfile export, validation, and import; plugin listing,
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
Application logs are written to daily files in `KRABB_HOME/logs/`, named by
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
Set `KRABB_LOG` to filter application logs, or use `RUST_LOG` as its fallback.
Set `KRABB_LOG_FORMAT=json` for structured JSON file logs; the default is
text. These settings apply to the daemon and plugin processes as well as the
CLI.
When the filesystem permits, commands run with `--verbose` or `--debug` save
their raw application log events to a private per-command file under
`KRABB_HOME/logs/debug/`. Non-JSON commands print the file path when they finish.
The file uses the configured text or JSON log format; the daily application log
continues to collect events as well. JSON mode keeps the path off the terminal.
Daily aggregate logs remain directly under `KRABB_HOME/logs/` with date-only
names. Schedule outcomes are stored in
`KRABB_HOME/logs/schedules/<schedule-name>-<id>.jsonl`; each file is an
append-only JSONL history with no fixed outcome-count limit. Records include a
schema version, RFC 3339 timestamp, schedule ID, name and timezone, run ID,
status, attempt and retry details, scheduled/next-run times, captured
provider/model/workspace, and redacted result text bounded to 4,000 characters.
`krabb schedule history <id-or-name> [PAGE]` shows five runs per page in a readable
format; add `--all` to show every run or `--raw` to print the original JSONL.
`/schedule history <id-or-name> [PAGE]` provides the same paginated view in the
TUI. The output also shows the log path for direct inspection. Scheduled times
show the schedule's timezone and UTC reference. Use `--json` for parsed records
as a JSON array.

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

`krabb status` prints aligned installation health, background runtime state,
intelligence setup, messaging setup, version, and the installed plugin count.
Messaging is optional for local CLI and TUI use; health only requires a ready
messaging plugin when one is installed or explicitly selected. Health is
`unhealthy` when a required setup check fails, and the output lists the reasons.
Use `--json` for automation; the
health details are in `health_details`, and capability fields are objects with
a `status` and `plugins` array.

`krabb tui` opens a full-screen terminal chat with a scrollable conversation
and an editable message box. Press Enter to send, Ctrl+O to add a line,
Up/Down to browse input history, Page Up/Page Down to move the conversation by
a page, and the mouse wheel to scroll the pane under the pointer. New output
stays in view unless you have scrolled back; scrolling down to the latest output
resumes following it. The input box follows the cursor while editing. Hold Shift
while dragging to select and copy transcript text; the mouse wheel remains
available for scrolling. Press Escape or Ctrl-C to quit. The bottom-right footer shows
the Krabb version and `/help` hint. `/help` lists commands available in this session;
conditional commands appear only when their plugin capability and required
background runtime are available. Plugin-contributed commands remain CLI
commands and are run as `krabb <command>`. `/model` shows the current model;
`/model <id>` changes it when an intelligence plugin is installed. Use
`/statusline` to open a checkbox menu for the title, model, context fill,
session, workspace, and status. Move with Up/Down, toggle with Space, save with
Enter, or cancel with Escape. At least one detail stays enabled. Long lines
scroll across the footer so each enabled detail remains available. Context fill
shows the latest provider-reported token count and model limit, for example
`12,345/128,000 (9.6%)`; it shows `context: unavailable` when the provider does
not report both values. Use `/statusline reset` to restore the default choices.
Choices are saved to `<KRABB_HOME>/data/plugins/tui/preferences.toml`. The
displayed Krabb name defaults to `Krabb` and can be changed globally with
`name` in `config.toml`.
Input history is saved in `<KRABB_HOME>/data/plugins/tui/history.json`; use
`/history list`, `/history clear`, and `/history help` inside the TUI to manage it.
When no intelligence plugin is installed, the statusline shows the model as
`unset`, regardless of the model value saved in the session.

The daemon must be running before opening the TUI or using `--once`; start it
with `krabb service start` or run `krabb-daemon` in the foreground. Both modes
use daemon-managed model turns, tools, approvals, and the shared session store.
The TUI refuses to start when the daemon is unavailable. `--once <prompt>` sends
one prompt and prints the response without opening the full-screen interface;
quote prompts containing spaces. Without `--session`, each one-shot run gets a
fresh temporary session that is deleted after the response. Pass `--session`
to continue an existing session. Interactive TUI runs resume the last session,
or start `default` when none exist. Use `--model` and `--plugin` to select the
model configuration.

Local command replies and streamed model text appear with a brief typewriter
reveal. During model generation, a rotating krabb-themed status is shown. Press
Escape to interrupt generation; the partial reply remains in the conversation.
Use `/animation off` to show replies immediately, `/animation on` to restore the
effect, and `/animation` to check its setting. The choice is saved with the TUI
preferences.

The conversation uses the available screen without an outer frame. Krabb
replies appear in rounded orange bubbles on the left, while your messages appear
in rounded green bubbles on the right; the input border uses matching rounded
corners. Saved user and assistant messages show local timestamps, and generation
status includes elapsed time.
When Codex emits multiple assistant messages in one turn, each is separated
from the next by a blank line.
The TUI uses the daemon's shared tool and approval policy. Tool access is
disabled for TUI sessions unless `[clients.tui] tools = true` is set in
`config.toml`; when enabled, the global `approval` mode still governs mutating
tools. Tool paths remain confined to the active session workspace. On exit, the
input title shows that Krabb is waiting for the daemon turn to stop.
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
remain attributed to Krabb.

`krabb validate` checks a Krabfile without changing local state. It uses
`./Krabfile` by default or the path supplied with `--path`, and reports the
first syntax or schema error. See the [Krabfile specification](krabfile.md)
for the supported version and keys.

`krabb plugin list [page]` shows up to five installed plugins at a time in a
tree view with version, health, protocol, capabilities, commands, and
permissions. Omit `page` to show the first page. The TUI's `/plugin [page]`
command presents the same metadata in compact, paginated entries.
`krabb plugin list --json` returns the complete inventory in an object with
an `items` array, regardless of the selected page. A visible plugin
directory with a missing or invalid manifest is reported as an error so the
inventory cannot silently hide broken installation state.

`krabb plugin install` accepts multiple IDs and validates and registers each
plugin independently. Use `--source` and `--revision` only when installing one
plugin from an explicit source. By default, installation copies plugin binaries;
`--link` instead links a local build for development. Linked plugins can be
rebuilt in place without reinstalling; restart the daemon with
`krabb service restart` to load the rebuilt executable. The linked source and
manifest must remain at their recorded paths. When the background
runtime is running, it starts server plugins immediately; otherwise, the next
runtime start discovers them. Installation groups per-plugin progress and
results, then prints one success summary followed by separate runtime guidance
for plugins that need the daemon. The TUI also
requires the daemon to be running before it can open. The core artifact contains
the CLI and daemon executables, but no plugin binaries. Uninstalling a plugin
unloads its active process before removing its files. `krabb plugin update`
previews available version, source-revision, and content changes without
changing installed plugins. Confirm in the terminal to apply the preview, or
use `--yes` to skip the prompt. Only changed plugins are replaced, and only
active plugins are unloaded and reloaded. Inactive plugins stay inactive.

Conditional commands are defined by the native CLI and appear when their
implementing plugin is installed; their feature code stays in that plugin.
This includes `krabb schedule`, `krabb memory`, and `krabb tui`. Other
plugin-contributed commands are registered by installed plugins. Native
command names are reserved, including conditional
commands, and duplicate plugin command names are rejected during installation
or update. Plugin-contributed commands are available only when their owning
plugin is installed.

Uninstalling the last installed intelligence plugin is blocked while persisted
sessions remain; delete them with `krabb session delete <id> --yes` first.
Uninstalling the last messaging plugin is blocked while outbox or dead-letter
deliveries remain; retry or discard outbox deliveries with `krabb delivery`
first. To intentionally uninstall the last matching plugin and purge its dependent
state, pass `-y --force` to `krabb plugin uninstall <id>`. This permanently
deletes all sessions when uninstalling the last intelligence plugin, or all
outbox and dead-letter deliveries when uninstalling the last messaging
plugin. Session worktrees are removed too; any worktrees that cannot be removed
are reported for later cleanup. `--force` does not purge state when another
available plugin of the same capability remains.

The memory command supports `status`, `search <text>`, `list`, `show <key>`,
`remember <key> <text>`, `edit <key> <text>`, `forget <key>`, `audit`,
`converge`, and `learning <guided|autonomous>`. `converge` reports likely
contradictions within the selected scope and identifies the newer record; it
uses text matching rather than a model, and does not delete or modify memories.
Each result includes a `krabb memory forget` command for the older record if you
choose to remove it. Newer saved preferences take precedence when relevant
preferences conflict. Run `krabb memory help` for command syntax and available
options. `list [PAGE] --type global|session` filters paginated results by type;
use `--scope <id>` on supported operations to select a scope. JSON output is
available through the global `--json` flag.
Learning defaults to `guided`, where the agent stores a memory only after an
explicit user request. `autonomous` lets it retain stable, useful facts while
excluding secrets and sensitive inferences. These instructions do not override
tool availability or channel policy.

The native `krabb ask` command requires `--provider <id>` and `--model <name>`,
followed by prompt words. The provider must identify an installed model plugin.
If no model provider is installed and ready, `ask` prints an installation hint.
`session` is exposed as a conditional command
when an intelligence plugin is available; `delivery` is exposed when an
installed messaging plugin is available. These conditional commands remain
runtime-owned even though their availability depends on plugins.

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
`/workspace reset` returns to `KRABB_ROOT`. Workspace changes are persisted
through authenticated daemon IPC and apply to later model turns in that
session. The workspace is the filesystem root/current working area used by
file-oriented tools; it is separate from conversation history and is not a
guarantee that every plugin is confined to it. `/clear` removes the selected
session's transcript when it is idle.
`/schedule help` shows schedule controls. `/schedule add` accepts the same
`--after`, `--at`, `--every`, `--cron`, `--timezone`, `--missed`, `--retries`,
and `--on-failure` options as `krabb schedule add`; it also accepts the legacy
`/schedule add <seconds> <instruction>` form. A schedule is activated
immediately. `/schedule list [page]` shows 20 tasks per page, their next run,
and the most recent outcome; `/schedule history <id-or-name> [page]` shows five
execution records per page. Upcoming run and retry times show the schedule's
timezone with a UTC reference. Each row uses a short schedule name rather than
the full instruction. Unnamed schedules get a unique random adjective-animal
name; pass `--name` to choose your own. Names cannot contain spaces and can be
used instead of numeric IDs for run, retry, pause, resume, remove, and history.
`--after` and `--every` accept human-readable durations
such as `5m`, `1m30s`, `1mo`, or `"1 month"`. Months are treated as 30 days
and years as 365 days. CLI-created schedules require explicit `--provider` and
`--model` values, captured for every run. They also capture the directory where
`krabb schedule add` was run as their workspace, so file paths are resolved
relative to that directory and the confined file tools cannot escape it. When
local shell access is explicitly enabled for a task, it can also search the
daemon machine for requested folders, files, and binaries using read-only
commands. It searches the current directory and home first, then other readable
locations, and skips virtual system trees. It inspects only relevant results
and avoids credentials and unrelated private files. A change outside the
captured workspace must be specifically requested by the scheduled
instruction. Without shell access, discovery is limited to the captured
workspace. Use `/schedule pause <id>` if you need to stop one.
`/schedule run <id-or-name>` runs a
task immediately: a one-time task completes, while a recurring task keeps its
normal schedule. Completed and skipped one-time tasks can be run again; a rerun
may repeat side effects. `/schedule retry <id-or-name>` retries a task paused
after failure or a one-time task skipped while the daemon was offline. Retrying
a skipped task runs its missed action now and may repeat side effects.
`/schedule pause <id-or-name>`, `/schedule resume
<id-or-name>`, and `/schedule remove <id-or-name>` control it through authenticated daemon IPC.
Removing a task stops future runs. If an action is already running, removal
requests cancellation and interrupts the agent turn; side effects that already
happened cannot be undone, so the result reports that possibility.
Schedules run as isolated agent turns with the captured provider,
model, and workspace. When tools were enabled for the originating client, the
agent can use the confined workspace tools; shell commands and scripts are
available only when shell access is enabled in configuration. Enable TUI tools
with `krabb config set clients.tui.tools=true`; shell access uses
`krabb config set clients.tui.shell=true` and takes effect after a daemon
restart. For messaging-channel schedules, set `tools = true` and `shell = true`
under the channel's `[channels.<id>]` table and set global `approval` to
`prompt` or `auto`. These settings let authorized channel turns schedule local
scripts; shell remains disabled for channels by default. Shell tasks run on the
machine hosting the daemon, so they can use
local scripts, command line tools, and configured services such as `mail` or
`sendmail`. Scheduled tasks can send a message only when task creation captures
the exact channel, recipient, recipient type, and optional thread authorized by
the user. They can also deliver a message to an explicitly requested local TUI
session; phrases such as "default TUI chat" target `tui-default`. The task cannot
send to another destination, and messaging is unavailable when no destination
was captured. External messages use the normal durable delivery queue and can be inspected
with `/deliveries` or `krabb delivery list`. Results appear in the originating
TUI session or are delivered back to the originating messaging conversation;
CLI results are available with `krabb schedule history <id-or-name>`. Schedule
listings show a concise outcome and history command instead of the agent's full
reply. Each run starts with the scheduled instruction and does not include
messages or tool results from earlier runs in its model context. An explicitly
requested agent schedule becomes active immediately
without another approval prompt. At run time, the daemon
does not wait for an interactive approval that may have no user present:
scheduling authorizes the task to use only its captured, enabled capabilities.
A missing capability or uncertain side effect pauses the task and reports why.
Pending tasks, if any, can still be activated with the approve command.

That authorization is for the specific scheduled instruction and its captured
capabilities; it does not guarantee that the machine, daemon, provider, channel,
or external service will be available at the due time. Keep the daemon running
and configure the required tools and recipients before scheduling. If the
machine is offline when work becomes due, `--missed` decides whether to skip,
catch up once, or replay up to five occurrences. Retries cover reported failures
before a mutating action may have happened. If an email, message, or other side
effect might already have happened, the task pauses for review to avoid
duplicating it; Krabb cannot promise exactly-once execution or delivery.

CLI users can create one-time or recurring tasks with `krabb schedule add
--after`, `--at`, `--every`, or `--cron`. `--every` creates a fixed interval;
combine it with `--after` or `--at` to choose the first run time. Add `--retries 1` through
`--retries 5` to opt into bounded retries with exponential backoff for failures
before a mutating action starts. If an action may already have happened, the
task pauses for review instead of retrying automatically. Manual retries may
repeat side effects, so tasks that send mail or perform other irreversible
actions should be written to detect work they already completed. Workspace
tools are captured when the tools plugin is installed. Add `--shell` to enable
local shell access for a CLI-created schedule when shell access is enabled in
the Krabb configuration; otherwise, `--shell` is rejected. Without it,
CLI-created tasks do not get shell access. TUI and messaging schedules capture their respective
client or channel tool settings. With the shell sandbox off, commands run on the
daemon host and can use its installed mail client and scripts. If Docker or
Podman shell isolation is configured, commands run inside that container and
cannot use host-installed mail clients by default.
To authorize a CLI-created scheduled message, provide `--message-channel`
`CHANNEL`, `--message-chat` `CHAT`, and `--message-kind private|group` together.
Add `--message-thread` `THREAD` when sending to a channel thread. The exact
destination is captured with the task and cannot be changed by the agent.
For recurring tasks, `--on-failure continue` skips an occurrence after its
retries are exhausted and keeps the next run active; the default is to pause
the schedule. Uncertain side effects still pause for review.
`--missed skip|coalesce|replay` controls tasks that became due while the daemon
was offline: skip drops missed work,
coalesce runs once to catch up, and replay runs up to five missed occurrences.
This policy does not control failed-task retries.

For example:

```sh
krabb schedule add --after 5m --provider codex --model gpt-6-luna --name project-backup "Create a backup of the project in the configured workspace"
krabb schedule add --at "2026-10-20 09:00" --timezone Europe/Rome --missed replay --shell --provider codex --model gpt-6-luna "Send the daily report by email using the configured local mail client"
krabb schedule add --after 5s --message-channel telegram --message-chat 12345 --message-kind private --provider codex --model gpt-6-luna "Send a private Telegram message to chat 12345: Hey there"
krabb schedule add --after 5s --provider codex --model gpt-6-luna "Ping me on the default TUI chat"
krabb schedule add --every 5m --provider codex --model gpt-6-luna "Check the backup and report any missing files"
krabb schedule add --every 1d --at 2026-10-20T09:00:00+02:00 --provider codex --model gpt-6-luna "Create the daily report"
krabb schedule add --after 5m --shell --provider codex --model gpt-6-luna "Use the configured local mail client to email the daily report to me"
krabb schedule add --cron '0 9 * * *' --timezone Europe/Rome --provider codex --model gpt-6-luna "Review the daily report"
krabb schedule add --after 5m --retries 2 --provider codex --model gpt-6-luna "Check the backup and report any missing files"
```

`--at` accepts `YYYY-MM-DD HH:MM` in `--timezone`, `scheduler.timezone`, or the
machine's timezone, as well as an RFC 3339 timestamp with an explicit UTC offset.
Quote local date/time values so the shell passes them as one argument.
Agent-proposed one-time tasks can use RFC 3339 timestamps. Fixed intervals use `--every`;
calendar schedules use cron and a timezone. Every CLI-created task needs both
`--provider` and `--model`, for example `--provider codex --model gpt-6-luna`.

The Telegram plugin must be installed and configured for the Telegram example.
Local email commands need a configured mail client or script on the daemon's machine.
`/memory list [PAGE] [--type global|session]` shows up to five global
and session memories in a paginated tree. Each row is labeled `type: global` or
`type: session`; use `--type` to filter the list. `/memory remember KEY=VALUE`
saves to the current session.
`/memory forget KEY` removes a unique matching record from either scope;
`/memory forget global KEY` and `/memory forget session KEY` select a scope
explicitly. If the key exists in both scopes, choose one explicitly. `/memory
help` shows the available commands, and selecting `/memory` from the command
picker shows the same help. Entering the `remember` command explicitly approves
a guided memory write.
`/quit` and `/exit` close the terminal client.

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
krabb init
krabb plugin list
krabb doctor
krabb-daemon
```

`init` creates the home directory, starter configuration, and
`workspace/KRAB.md` and `workspace/KLAW.md` instruction templates without
starting the daemon. Edit those files manually: `KRAB.md` defines Krabb's
identity and communication style; `KLAW.md` defines behavioral guidance and
workflows. If the home directory already exists, `init` reports that Krabb
is already initialized and leaves it unchanged. `doctor --fix` can seed any
missing safe local state, including the workspace and instruction files.

`init --force` resets all Krabb home data, including configuration, plugins,
sessions, workspace data, and edited instructions. It prompts for confirmation;
use `init --force --yes` for a non-interactive reset. Stop the daemon first.
The reset has no backup. Do not use `--yes` without `--force`.

The default file-tool root is `KRABB_HOME/workspace`; set `KRABB_ROOT` to
use a different root. This does not enable tools: channel `tools = true` and
the daemon approval policy are still required, and shell execution remains
separately disabled by default. Krabb loads the two instruction files on each
turn alongside the bounded saved conversation. It does not load workspace
`AGENTS.md` into its own prompt; delegated coding agents may use those files.

`doctor` is read-only by default and validates configuration, plugin manifests,
protocol compatibility, credentials, and local state. Pass `doctor --fix` to
create missing safe local state; it never overwrites an existing config or
repairs plugin binaries. Human output groups the health summary and setup
checks; unhealthy output ends with a suggestion to run `krabb doctor --fix`.
`doctor --fix` reports only the repairs performed in human and JSON output.
Regular `doctor --json` includes the full structured result and `health` object.
`krabb-daemon` keeps
the daemon in the foreground so service managers and operators can observe its
diagnostics. Use `krabb service install` only after the foreground flow is
healthy.

`krabb service install` writes a native service definition for the current
platform, preserving the resolved KRABB_HOME and configured non-secret
environment paths. If provider variables are present, their declared values
are copied to a private service credential JSON file and the definition points
to it; existing KRABB_CREDENTIALS and KRABB_KEYRING=1 configuration is also
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
`systemctl --user status krabb.service` for detailed systemd output; plain
`systemctl status krabb.service` checks the separate system-wide manager.
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
channel, store, scheduler, tool, speech plugin, or client is not installed.
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

For a one-shot local request, use `krabb ask --provider <id> --model <model>
<prompt...>` when an installed model plugin is available. This bypasses channel
routing and uses the selected model plugin directly. It still applies provider
validation and protocol limits, but it does not create a durable chat session.

External agents use the ACP and A2A protocol plugins when their runtimes
implement the corresponding protocol. AG-UI provides a local run endpoint for
compatible clients. See the plugin guides for protocol limits and setup.

If no installed model plugin can run, `ask` is unavailable and reports that an
installed intelligence plugin is needed.
