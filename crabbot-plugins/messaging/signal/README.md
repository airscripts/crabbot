# Signal

Signal uses a managed `signal-cli` process. Install and register an account,
install the Crabbot plugin, then set `CRABBOT_SIGNAL_ACCOUNT`:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- signal
```

Override the executable with
`CRABBOT_SIGNAL_COMMAND` when it is not on `PATH`. Text, images, voice, and text
files are normalized through the channel boundary; local attachment files are
confined to `CRABBOT_MEDIA` before they are sent. Outgoing replies use the
standard Crabbot delivery contract.
