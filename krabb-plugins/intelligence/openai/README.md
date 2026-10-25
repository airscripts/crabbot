# OpenAI

This provider is optional and is distributed separately from the
core release package.

Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- openai
```

Set `KRABB_OPENAI_KEY` and select a model with the normal session model
setting. Set `KRABB_OPENAI_MODEL` when the request uses the `default` model.
The plugin uses OpenAI's Chat Completions API for text, image input, streaming,
and function calls. The selected model must support the requested features.

Credentials can also come from the protected `KRABB_CREDENTIALS` JSON file,
or the `openai` operating-system keyring entry when `KRABB_KEYRING=1`.
`KRABB_OPENAI_BASE_URL` supports HTTPS-compatible gateways and loopback test
fixtures.
