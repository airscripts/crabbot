# Krabb Daemon

`krabb-daemon` is the thin foreground entrypoint for Krabb’s runtime. The
runtime owns plugin supervision, channel bridges, authenticated IPC, leases,
delivery, and persistent session state; the daemon package does not depend on
the CLI package.

Install it with:

```sh
cargo install --path krabb-daemon --locked
```

Initialize the client configuration first, then start the daemon:

```sh
krabb init
krabb doctor
krabb-daemon
```

The daemon reads `KRABB_HOME`, `KRABB_ROOT`, and the configured plugin
variables from its process environment. It does not parse client commands; use
`krabb` for inspection, session management, plugin updates, and service
installation.

Plugins are separate executables installed under the Krabb home. A running
daemon accepts verified plugin installs through authenticated local IPC and
starts them without restarting the daemon. Plugins installed while it is
stopped are discovered at startup. Plugin updates unload and reload only the
active plugin processes; they do not restart the daemon.
