# mcp

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- mcp
```

The plugin bridges MCP JSON-RPC requests over a short-lived stdio process or
an HTTPS/loopback Streamable HTTP endpoint. Use `stdio` with `command`,
optional string `args`, and a JSON `request`; use `http` with `url` and
`request`. Responses are bounded to eight megabytes and transport calls have
a 30-second deadline.
