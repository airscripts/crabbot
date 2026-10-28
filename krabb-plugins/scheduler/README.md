# Scheduler

The scheduler runs one-time and recurring agent tasks through the daemon.
Install the plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- scheduler
```

## Table Of Contents

- [Create Tasks](#create-tasks)
- [Execution And Safety](#execution-and-safety)
- [Timing And Recovery](#timing-and-recovery)
- [Inspect And Control Tasks](#inspect-and-control-tasks)
- [Storage And Migration](#storage-and-migration)

## Create Tasks

CLI tasks require a provider and model. Choose a delay, start time, interval,
or cron expression:

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

`--every` can be combined with `--after` or `--at` to set the first run.
Cron schedules require an IANA timezone. Tasks created by an explicit user
request activate immediately; a second approval is not required.

## Execution And Safety

The daemon runs up to four tasks concurrently. Each task uses the provider,
model, workspace, and tools captured at creation. It must explicitly confirm
completion in its final response; otherwise the run pauses for review.

- Shell is disabled by default. CLI tasks need `--shell` and the matching
  configuration. TUI and channel tasks capture their client or channel settings.
- A task can send a message only to a destination explicitly captured when it
  was created. Messages use the durable delivery queue.
- Scheduling authorizes only the task's captured capabilities. The daemon does
  not wait for an interactive approval while a task runs.
- Interrupted tasks and uncertain side effects pause for review. They are not
  replayed automatically.

## Timing And Recovery

- `--missed skip` drops runs missed while the daemon was offline. `coalesce`
  catches up once; `replay` runs up to five missed occurrences.
- `--retries 1..5` retries failures only when no mutating action may have run.
  Retries use exponential backoff.
- Recurring tasks pause after retry exhaustion by default. Set
  `--on-failure continue` to skip that occurrence and keep the schedule active.
- If an action may already have happened, the task pauses even when retries
  remain. Manual retries can repeat side effects.

The schedule list shows 20 tasks per page and includes each task's latest
outcome. Up to 1,000 tasks are retained; new tasks are rejected at the limit.

## Inspect And Control Tasks

Use `krabb schedule help` for command syntax. The CLI and TUI support listing,
history, run, retry, pause, resume, and remove operations. `run` starts a task
now without shifting a recurring task's next scheduled time. Removing a
running task requests cancellation; completed side effects cannot be undone.

Full results are stored in append-only JSONL files under
`KRABB_HOME/logs/schedules/`. The daemon reports results to the originating
conversation. See [Commands](../../krabb-docs/commands.md) for CLI usage.

## Storage And Migration

Task state is stored in `KRABB_HOME/data/schedules/schedules.json`. On upgrade,
tasks in the configured `KRABB_TIMER` file are imported once and activated.
Corrupt scheduler state prevents startup; the plugin preserves the file for
repair instead of discarding tasks.
