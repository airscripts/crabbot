# TUI

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- tui
```

The host terminal client is available with `krabb tui`. Start the daemon with
`krabb service start` or run `krabb-daemon` in the foreground first; the TUI
requires it for both interactive and `--once` use. Prompts, session history,
tools, and approvals go through the shared daemon runtime. TUI tools are
disabled by default; enable them with `[clients.tui] tools = true` in the home
`config.toml` to opt in under the global approval policy.
Run `krabb tui --once "prompt"` to print one response without opening the
full-screen interface. It uses and deletes a fresh temporary session by
default; pass `--session <id>` to continue an existing session.
Markdown fenced code blocks appear in a labeled panel in the transcript; the
fence's language tag is shown above the code.
Prefix a line with `!` to run it directly through the daemon's shell. This
requires `[clients.tui] shell = true`, but not the Tools plugin or separate
approval; it is an explicit user action in the selected workspace.
Set `[clients.tui] theme = false` to use the terminal's default text color
instead of TUI colors; the theme is enabled by default and changes apply on the
next `krabb tui` launch.

In an interactive terminal, use `/help`, `/status`, `/approval`, `/approvals`, `/approve <id>`,
`/deny <id>`, `/session help`, `/session list`, `/deliveries`, `/retry <id>`,
`/drop <id>`, `/model <name>`, `/new <id>`,
`/plugin [page]`, `/history <list|clear|help>`, `/workspace [path|reset]`,
`/schedule <help|list|add|approve|run|retry|pause|resume|remove>`, `/memory
<list|remember|forget>`, `/compact`, `/clear`, and `/quit`. Plain lines are sent through
the configured model plugin. Sessions, selected models, and completed turns
are persisted through authenticated daemon IPC. `/session switch <id>` resumes
an existing session; `/new <id>` creates and selects one. `/session rename`
renames the active session without losing history. `/session archive` and
`/session unarchive` accept multiple IDs or `--all`; `/session delete` accepts
the same targets and prompts before deletion; use `-y` or `--yes` to skip it.
The active session is kept when
archiving or deleting all sessions. `/workspace <path>` selects and persists a
canonical existing directory for that session.
Without `--session`, the interactive TUI resumes the last session it opened, or
starts the default session when none exist. Up/Down input history is saved under
`<KRABB_HOME>/data/plugins/tui/history.json`.
Multiple TUI windows following the same session refresh its saved transcript
automatically once per second. A window shows the other client's
messages as they are saved, reflects when another window is generating, and
locks chat input until that turn finishes. The waiting state is shown as a
System notice; approval commands remain available.
`/workspace reset` uses `KRABB_ROOT` again. `/clear` removes the selected
session's transcript and refuses to clear an active session.
`/compact` asks the selected model to summarize older turns and keeps the two
most recent user turns. The TUI also compacts automatically before a turn when
the saved conversation exceeds its history threshold.
While generation waits for an approval, start typing `/` from an empty input to
enter `/approvals`, `/approve <id>`, or `/deny <id>` to resolve the current
approval; ordinary chat stays locked. Hold Shift while dragging to select and
copy transcript text; the mouse wheel remains available for scrolling.
`/approvals` lists pending mutating-tool requests. `/approve <id>` and
`/deny <id>` resolve a request through authenticated daemon IPC; the request
ID expires with the pending approval.
`/schedule add` accepts the same timing and retry options as the CLI, including
`--after`, `--at`, `--every`, `--cron`, `--timezone`, `--missed`, `--retries`,
and `--on-failure`; the legacy `/schedule add <seconds> <instruction>` form
also works. Schedules activate immediately. Check one with `/schedule list
[page]` (including the latest outcome), and use `/schedule
run|retry|pause|resume|remove <id-or-name>` to control it through authenticated
daemon IPC. Schedule names contain no spaces. Agent-proposed tasks activate
without a separate approval and can opt into up to five automatic retries with
backoff; retries can repeat side effects.
Scheduled tasks run as isolated agent turns with their captured provider,
model, and workspace. `/memory remember <key>=<value>` saves a session memory,
and `/memory list [page] [--type global|session]` shows up to five global and
session records in a paginated tree. Each row includes its `type:`. `/memory forget <key>`
removes a unique matching record; use `/memory forget global <key>` or
`/memory forget session <key>` to choose a scope explicitly. Entering `remember`
authorizes a suggest-mode write.

`/status`, `/session list [page]`, and `/plugin [page]` use authenticated daemon
state over local IPC. Both lists are paginated and show compact status and
metadata; the plugin view includes capabilities, commands, and permissions.
