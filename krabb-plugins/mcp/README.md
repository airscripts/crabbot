# MCP

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- mcp
```

The plugin bridges MCP JSON-RPC requests over a short-lived stdio process or
an HTTPS/loopback Streamable HTTP endpoint. Use `stdio` with `approved: true`,
`command`, optional string `args`, and a JSON `request`. Each child negotiates
MCP version `2025-06-18` and receives `notifications/initialized` before the
requested operation. The bridge waits for its matching response before closing
stdin and stopping the child; stdio sessions do not persist across calls.

Use `http` with `url`, `request`, and an optional bearer `token`. HTTP accepts
JSON or bounded SSE responses, returns the matching JSON-RPC response, and
returns `null` for an empty HTTP 202 notification/response acknowledgement.
The caller sends `initialize`, then `notifications/initialized`, then operations.
When a server returns `Mcp-Session-Id`, the bridge adds `session_id` to the returned
JSON-RPC object. Pass that value as the next call's `session_id` parameter on the
same URL with the same authentication. Pass the negotiated `protocol_version`
on subsequent calls; it defaults to `2025-06-18`. Session IDs are caller-managed
and are never automatically reused for another endpoint or identity.

HTTP response streams are bounded to 4 MiB, stdio frames to 1 MiB, and transport
calls have a 30-second deadline. Oversized host protocol responses are rejected.
