# sqlite

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- sqlite
```

Set `CRABBOT_DB` to choose the database path. The store uses WAL mode and
performs an idempotent schema migration on startup. It supports `put`, `get`,
and `delete` for values, `lease` and `release` for ownership, `enqueue`,
`outbox`, `retry`, and `ack` for durable delivery, and `seen` for expiring
event deduplication.
