# Anthropic

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- anthropic
```

The plugin supports Anthropic Messages responses, including Server-Sent Event
streams. Set `KRABB_ANTHROPIC_KEY`, or use the
`anthropic` operating-system keyring entry with `KRABB_KEYRING=1`.

The plugin advertises image-request support during its handshake and sends
confined images as bounded base64 or HTTPS image blocks. The selected Claude
model must support vision.
