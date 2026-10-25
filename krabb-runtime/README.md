# Krabb Runtime

`krabb-runtime` is the internal host library shared by the `krabb` CLI and
the `krabb-daemon` executable. It owns configuration, authenticated IPC,
plugin lifecycle, persistent state, channel bridging, and daemon orchestration.

The crate deliberately does not depend on optional plugins. Plugin processes
remain independently installable and communicate through the core JSON-RPC
protocol.
