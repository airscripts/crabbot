# Discord

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- discord
```

Set `CRABBOT_DISCORD_TOKEN`, or set `CRABBOT_KEYRING=1` to
use the `discord` operating-system keyring entry. The plugin receives
`MESSAGE_CREATE` events through the Gateway, answers heartbeats, ignores bot
authors, normalizes text and attachments, and sends replies through the REST
API. Gateway session state is persisted under `CRABBOT_HOME` and advances only
after the host acknowledges an accepted event. Outgoing text is split at
Discord's message limit; `edit` updates the first existing message and sends
any remaining chunks to the same channel or thread.
`CRABBOT_DISCORD_GATEWAY_URL` and `CRABBOT_DISCORD_INTENTS` are optional
overrides for local integration tests.

Image, voice-message audio, and bounded text-file attachments are downloaded
through opaque references into the private `CRABBOT_MEDIA` cache. The plugin
supports attachment uploads, but the current daemon sends generated replies as
text. Direct messages work by default; guild channels and threads require the
daemon channel allowlist.
