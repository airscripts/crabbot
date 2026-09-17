# Gemini

Gemini is an optional intelligence plugin for the Google Gemini Developer API.
Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- gemini
```

Set `CRABBOT_GEMINI_KEY` and select a model with `CRABBOT_MODEL` or the normal
session model setting. The default API endpoint is HTTPS and can be replaced
with `CRABBOT_GEMINI_BASE_URL` for a loopback test fixture.
