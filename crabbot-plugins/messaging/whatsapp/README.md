# whatsapp

This channel is optional and is distributed separately from the
core release package.

Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- whatsapp
```

This plugin uses the official WhatsApp Cloud API. It receives direct messages
through a locally bound webhook listener and sends replies through the Graph
API. Configure a public HTTPS reverse proxy to forward the Meta webhook to the
local `CRABBOT_WHATSAPP_LISTEN` address.

Required values are `CRABBOT_WHATSAPP_TOKEN`, `CRABBOT_WHATSAPP_APP_SECRET`,
`CRABBOT_WHATSAPP_VERIFY`, and `CRABBOT_WHATSAPP_PHONE`. Set
`CRABBOT_WHATSAPP_GRAPH_URL` to the Graph API base including its version.
`CRABBOT_WHATSAPP_LISTEN` defaults to `127.0.0.1:8787`.

Inbound text, images, voice messages, and text files are normalized. The plugin
can send all four content types, while the current daemon response path sends
generated replies as text.
