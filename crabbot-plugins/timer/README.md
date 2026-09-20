# Timer

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- timer
```

Set `CRABBOT_TIMER` to persist schedules. `add` accepts a delay in seconds,
optional repeat interval, five-field cron expressions, and an IANA timezone.
Cron entries compute their next occurrence in the selected timezone, including
daylight-saving transitions. `due` atomically returns ready tasks and advances
repeating tasks; `list`, `wait`, and `remove` provide inspection and control.

Set `CRABBOT_TIMER` to persist scheduled entries as JSON; without it, timers
are process-local.
