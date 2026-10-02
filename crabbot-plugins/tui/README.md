# TUI

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- tui
```

The host terminal client is available with `crabbot tui`. Start the daemon with
`crab service start` or run `crabbot-daemon` in the foreground first; the TUI
requires it for both interactive and `--once` use. Prompts, session history,
tools, and approvals go through the shared daemon runtime. TUI tools are
disabled by default; enable them with `[clients.tui] tools = true` in the home
`config.toml` to opt in under the global approval policy.
Prefix a line with `!` to run it directly through the daemon's shell. This
requires `shell = true`, but not the Tools plugin or a separate approval; the
command is already an explicit user action and runs in the selected workspace.
Set `[clients.tui] theme = false` to use the terminal's default text color
instead of TUI colors; the theme is enabled by default and changes apply on the
next `crab tui` launch.

In an interactive terminal, use `/help`, `/status`, `/approval`, `/approvals`, `/approve <id>`,
`/deny <id>`, `/session help`, `/session list`, `/deliveries`, `/retry <id>`,
`/drop <id>`, `/model <name>`, `/new <id>`,
`/plugins [page]`, `/workspace [path|reset]`, `/timer <list|add|remove>`, `/memory
<list|remember|forget>`, `/clear`, and `/quit`. Plain lines are sent through
the configured model plugin. Sessions, selected models, and completed turns
are persisted through authenticated daemon IPC. `/session switch <id>` resumes
an existing session; `/new <id>` creates and selects one. `/session rename`
renames the active session without losing history. `/session archive` and
`/session unarchive` accept multiple IDs or `--all`; `/session delete` accepts
the same targets and requires `-y` or `--yes`. The active session is kept when
archiving or deleting all sessions. `/workspace <path>` selects and persists a
canonical existing directory for that session.
Multiple TUI windows following the same session refresh its saved transcript
automatically once per second. A window shows the other client's
messages as they are saved, reflects when another window is generating, and
locks chat input until that turn finishes. The waiting state is shown as a
System notice; approval commands remain available.
`/workspace reset` uses `CRABBOT_ROOT` again. `/clear` removes the selected
session's transcript and refuses to clear an active session.
While generation waits for an approval, start typing `/` from an empty input to
enter `/approvals`, `/approve <id>`, or `/deny <id>` to resolve the current
approval; ordinary chat stays locked. Hold Shift while dragging to select and
copy transcript text; the mouse wheel remains available for scrolling.
`/approvals` lists pending mutating-tool requests. `/approve <id>` and
`/deny <id>` resolve a request through authenticated daemon IPC; the request
ID expires with the pending approval.
`/timer add <seconds> <text>` schedules a reminder, while `/timer list` and
`/timer remove <id>` manage it through authenticated daemon IPC. `/memory
remember <key>=<value>`, `/memory list`, and `/memory forget <key>` manage
memories scoped to the selected session; explicitly entering `remember`
authorizes a suggest-mode write.

`/status`, `/session list [page]`, and `/plugins [page]` use authenticated daemon
state over local IPC. Both lists are paginated and show compact status and
metadata; the plugin view includes capabilities, commands, and permissions.
