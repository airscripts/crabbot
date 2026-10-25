# Scheduler

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- scheduler
```

The scheduler persists agent tasks under
`KRABB_HOME/data/schedules/schedules.json`. Execution history is kept in
`KRABB_HOME/logs/schedules/<id>.jsonl`.
On upgrade, existing tasks from the configured `KRABB_TIMER` state file are
imported and activated. The scheduler reports the number of imported tasks on
stderr after saving them. Existing scheduler tasks are retained, and imported
timer tasks are not duplicated on restart.
The list operation returns 20 tasks per page, including each task's latest
outcome without returning its full model response. Schedule names are unique,
Docker-style single tokens such as `mighty-raccoon`; they can select tasks in
CLI and TUI run commands. Full run output remains in the per-task JSONL history.
Up to 1,000 tasks are retained;
new tasks are refused at the limit rather than evicting an existing schedule.

Tasks created by an explicit user scheduling request become active immediately.
An explicitly requested agent scheduling tool call creates and activates the
task without a second approval prompt. The daemon claims due tasks durably and runs up to four isolated agent
turns in parallel using their captured provider, model, workspace, and enabled
tools. The agent can create or update workspace files, use other configured
tools, and run shell commands or scripts when shell access is enabled. It can
queue messages through installed channel plugins only when task creation captures
the exact destination authorized by the user. The task cannot send to another
destination, and messaging is unavailable when no destination was captured.
Scheduling explicitly authorizes that isolated task to use its
captured capabilities; the daemon does not wait for an interactive approval at
run time.
The agent must explicitly confirm completion in its final response. Unmarked
responses pause for review rather than being recorded as successful.
Shell runs through the tools plugin and its configured confinement; shell is
disabled by default. CLI schedules require `--shell`; TUI and channel schedules
capture the shell setting for their originating client or channel. Shell
commands run on the daemon's machine when shell isolation is off, so a task can
use its configured local mail client or mail script without a separate Krabb
email plugin. With Docker or Podman shell isolation, commands run inside that
container and cannot access host-installed mail clients by default. Outbound
channel messages use the durable delivery queue. Task results are recorded in
the schedule history and reported to the originating TUI session or messaging
conversation. A task can also deliver an explicitly requested message to a
captured local TUI session. By default,
tasks that fail after their configured retries are exhausted pause. Recurring
tasks can instead skip that occurrence and keep the next run active. Tasks
interrupted while running always pause for review instead of being replayed
automatically.
Channel-originated schedules can use local shell commands only when that
channel has both `tools = true` and `shell = true` and the daemon approval mode
is `prompt` or `auto`; channel shell access is off by default.
One-time delays, exact RFC 3339 times, fixed repeat intervals, and five-field
cron expressions are supported; cron requires an IANA timezone and accounts for
daylight-saving transitions. Tasks can select `skip`, `coalesce`, or bounded `replay` behavior
for occurrences missed while the daemon was offline. `skip` drops missed work,
`coalesce` catches up once, and `replay` runs up to five missed occurrences.
Active recurring tasks can be run immediately without moving their regular due
time; manually running a one-time task completes it.
Tasks can opt into up to five automatic retries with exponential backoff for
failures before a mutating action starts. If a mutating action may already have
happened, the task pauses for review instead of retrying it automatically.
Interrupted runs also pause because their side effects may already have
happened. Recurring tasks can choose `on_failure = "continue"` to skip an
occurrence after its retries are exhausted and keep the next run active; the
default is to pause. Uncertain side effects always pause for review. A
configured missed-run policy is separate from failure retries.
Removing an active schedule stops future runs. If its action is already
running, removal requests cancellation and interrupts the agent turn; side
effects that already happened cannot be undone.

The plugin exposes `add`, `list`, `approve`, `run`, `retry`, `pause`,
`resume`, `claim`, `finish`, `recover`, and `remove` operations over its
private JSON state. It refuses to start when that state is unreadable or
corrupt, preserving the file for repair instead of silently dropping scheduled
work. The daemon owns model execution, workspace confinement, tool policy, and
reporting.
