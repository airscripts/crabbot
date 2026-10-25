# A2A

The A2A plugin is a client for the released Agent2Agent Protocol 1.0 JSON-RPC
binding. Configure `KRABB_A2A_AGENT_CARD` with an agent-card URL, then use
`krabb a2a discover`, `krabb a2a send PROMPT`, `krabb a2a status --task ID`,
or `krabb a2a cancel --task ID`. Add `--return-immediately` to request a
non-blocking task response, then poll it with `status`.

The client discovers the JSON-RPC endpoint and streaming capability from the
agent card, sends text messages, consumes task, status, and artifact updates
over SSE when advertised, retrieves task status, and requests cancellation.
It accepts an optional bearer token through `KRABB_A2A_TOKEN`. OAuth and A2A
server mode are not implemented. It rejects non-HTTPS endpoints except
loopback fixtures and never follows redirects.
