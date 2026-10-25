# AG-UI plugin

The AG-UI plugin serves a local HTTP endpoint that accepts AG-UI run input and
streams events as chunked server-sent events. It calls the configured Krabb
model through the host. The listener binds to `127.0.0.1:8787` by default; set
`KRABB_AG_UI_LISTEN` to change it. Cross-origin browser requests are accepted
only from loopback origins.

Run `krabb ag-ui`. The command stays active until interrupted and prints its
bound address. Send a JSON run input to `POST /` with `threadId`, `runId`, and
`messages`. Text and tool events are forwarded as the configured model streams
them. Client tools are returned with `pendingToolCallIds`; the frontend executes
those tools and sends their results in a later run. Client state and context are
passed as untrusted model context, and input state is emitted as `STATE_SNAPSHOT`.
Model failures produce `RUN_ERROR` events.

The plugin does not mutate client state or persist AG-UI thread sessions.
Disconnecting a run cancels its pending host request; the host restarts the
configured model process to clear its in-flight protocol stream. Human approval
for frontend tools remains the frontend's responsibility. The host still
selects the model and applies its configured policy.
