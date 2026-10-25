# Ollama

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- ollama
```

The plugin supports local or cloud Ollama chat responses, including streamed
JSON-RPC 2.0 frames. Set `KRABB_OLLAMA_HOST` for a non-default server and
`KRABB_OLLAMA_API_KEY` for cloud access, or use the `ollama` operating-system keyring
entry with `KRABB_KEYRING=1`.

The plugin advertises image-request support during its handshake and sends
confined images as bounded base64 data in Ollama chat requests. The selected
local or cloud model must support vision.
