# Signal

Signal uses a managed `signal-cli` process. Install and register an account,
install the Crabbot plugin, then set `CRABBOT_SIGNAL_ACCOUNT`:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- signal
```

Override the executable with
`CRABBOT_SIGNAL_COMMAND` when it is not on `PATH`. Text, images, voice, and text
files are normalized through the channel boundary. Attachments are resolved
through `signal-cli` and cached in `CRABBOT_MEDIA` (or `CRABBOT_HOME/media`);
polled events are retained in a crash-safe inbox under `CRABBOT_HOME` until
the host acknowledges them. Cached attachments expire after 24 hours and are
bounded to 64 MiB; local attachment files are confined there before they are
sent. Outgoing replies use the standard Crabbot delivery contract.
