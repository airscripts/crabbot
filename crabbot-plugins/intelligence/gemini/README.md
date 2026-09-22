# Gemini

Gemini is an optional intelligence plugin for the Google Gemini Developer API.
Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- gemini
```

Set `CRABBOT_GEMINI_KEY` and select a model with `CRABBOT_MODEL` or the normal
session model setting. Requests without a model, or with model `default`, use
the supported `gemini-3.8-flash` model. The host's generic `gpt-6-luna`
fallback is treated the same way when Gemini is selected. Gemini 3 tool calls
preserve the provider's thought signatures across turns; legacy marker
transcripts using `default` temporarily use `gemini-2.5-flash`, and explicitly selected Gemini
2.x models remain supported through the legacy marker compatibility layer. That
layer can be removed after Gemini 2.x support is retired. The default API
endpoint is HTTPS and can be replaced with `CRABBOT_GEMINI_BASE_URL` for a
loopback test fixture.
