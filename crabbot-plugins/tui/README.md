# tui

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- tui
```

The host terminal client is available with `crabbot tui`. In an interactive
terminal, use `/help`, `/status`, `/approval`, `/approvals`, `/approve <id>`,
`/deny <id>`, `/sessions`, `/deliveries`, `/retry <id>`, `/drop <id>`,
`/model <name>`, `/session <id>`, `/new <id>`,
`/plugins`, `/workspace [path|reset]`, `/timer <list|add|remove>`, `/memory
<list|remember|forget>`, `/clear`, and `/quit`. Plain lines are sent through
the configured model plugin. Sessions, selected models, and completed turns
are persisted through authenticated daemon IPC. `/session <id>` resumes an
existing session; `/new <id>` creates and selects one. `/workspace <path>`
selects and persists a canonical existing directory for that session.
`/workspace reset` uses `CRABBOT_ROOT` again. `/clear` removes the selected
session's transcript and refuses to clear an active session.
`/approvals` lists pending mutating-tool requests. `/approve <id>` and
`/deny <id>` resolve a request through authenticated daemon IPC; the request
ID expires with the pending approval.
`/timer add <seconds> <text>` schedules a reminder, while `/timer list` and
`/timer remove <id>` manage it through authenticated daemon IPC. `/memory
remember <key>=<value>`, `/memory list`, and `/memory forget <key>` manage
memories scoped to the selected session; explicitly entering `remember`
authorizes a suggest-mode write.

`/status`, `/sessions`, and `/plugins` use authenticated daemon state over
local IPC. If the daemon is unavailable, `/plugins` falls back to the local
installation layout for diagnostics; the host view includes whether each
plugin binary is ready or missing.
