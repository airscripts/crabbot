# memory

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- memory
```

Set `CRABBOT_MEMORY` to persist records. `remember` defaults to `suggest` and
requires `approved: true`; `auto` and `off` are also supported. Records carry
a scope and timestamps, while `list`, `forget`, and `audit` keep changes
editable and reviewable. Older flat key/value files are read as global records.

Set `CRABBOT_MEMORY` to persist the memory map as JSON; without it, memory is
process-local.
