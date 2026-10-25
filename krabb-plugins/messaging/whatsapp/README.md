# WhatsApp

This channel is optional and is distributed separately from the
core release package.

Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- whatsapp
```

This plugin uses the official WhatsApp Cloud API. It receives direct messages
through a locally bound webhook listener and sends replies through the Graph
API. Configure a public HTTPS reverse proxy to forward the Meta webhook to the
local `KRABB_WHATSAPP_LISTEN` address.

Required values are `KRABB_WHATSAPP_TOKEN`, `KRABB_WHATSAPP_APP_SECRET`,
`KRABB_WHATSAPP_VERIFY`, and `KRABB_WHATSAPP_PHONE`. Set
`KRABB_WHATSAPP_GRAPH_URL` to the Graph API base including its version.
`KRABB_WHATSAPP_LISTEN` defaults to `127.0.0.1:8787`.

Inbound text, images, voice messages, and text files are normalized. The plugin
can send all four content types, while the current daemon response path sends
generated replies as text. Verified webhook events are stored under
`KRABB_HOME` until the host acknowledges them.
