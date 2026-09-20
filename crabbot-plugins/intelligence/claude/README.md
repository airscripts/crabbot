# Claude

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- claude
```

The plugin supports Anthropic Messages responses, including Server-Sent Event
streams. Set `CRABBOT_CLAUDE_KEY`, or use the
`claude` operating-system keyring entry with `CRABBOT_KEYRING=1`.

The plugin advertises image-request support during its handshake and sends
confined images as bounded base64 or HTTPS image blocks. The selected Claude
model must support vision.
