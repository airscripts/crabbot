# OpenRouter

This provider is optional and is distributed separately from the
core release package.

Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- openrouter
```

Set `CRABBOT_OPENROUTER_KEY` and select a model with
`CRABBOT_OPENROUTER_MODEL` or the normal session model setting. The adapter
uses OpenRouter's OpenAI-compatible chat completions API, including bounded
streaming, images, and tool calls. The selected model must support the input
features requested by the turn.

`CRABBOT_OPENROUTER_BASE_URL` is available for HTTPS-compatible gateways and
loopback test fixtures. `CRABBOT_OPENROUTER_REFERER` and
`CRABBOT_OPENROUTER_TITLE` optionally provide OpenRouter attribution headers.
