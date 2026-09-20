# Crabbot Daemon

`crabbot-daemon` is the thin foreground entrypoint for Crabbot’s runtime. The
runtime owns plugin supervision, channel bridges, authenticated IPC, leases,
delivery, and persistent session state; the daemon package does not depend on
the CLI package.

Install it with:

```sh
cargo install --path crabbot-daemon --locked
```

Initialize the client configuration first, then start the daemon:

```sh
crabbot init
crabbot doctor
crabbot-daemon
```

The daemon reads `CRABBOT_HOME`, `CRABBOT_ROOT`, and the configured plugin
variables from its process environment. It does not parse client commands; use
`crabbot` for inspection, session management, plugin updates, and service
installation.

Plugins are separate executables installed under the Crabbot home. A running
daemon accepts verified plugin installs through authenticated local IPC and
starts them without restarting the daemon. Plugins installed while it is
stopped are discovered at startup. Plugin updates unload and reload only the
active plugin processes; they do not restart the daemon.
