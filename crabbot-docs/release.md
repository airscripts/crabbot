# Release

The `VERSION` file, Cargo manifests, `CITATION.cff`, and changelog must agree.
Use Keep a Changelog headings and a semantic version tag such as `vX.Y.Z`.

```sh
crabbot-scripts/release.sh vX.Y.Z
```

Release automation validates `VERSION` and the matching changelog section,
builds separate core and plugin archives for Linux, macOS, and Windows on
x86_64 and ARM64, emits `SHA256SUMS` for all release archives, and publishes
the artifacts. Installers verify checksums before activation. Crabbot does not
publish crates or containers.

## Release Checklist

1. Update `VERSION`, the workspace manifests, `CITATION.cff`, and the matching
   `CHANGELOG.md` heading.
2. Run `make verify` and confirm the core package remains above 80% and the host
   package remains above 50% line coverage.
3. Run `crabbot-scripts/release.sh vX.Y.Z` to build the release binaries.
4. Inspect archive contents and checksums with `crabbot-scripts/check.sh`.
5. Review installer output on each supported platform before publishing.

For a local installer smoke test, set `CRABBOT_RELEASE_BASE` to a `file://`
directory containing the archives and `SHA256SUMS`, for example
`file:///tmp/crabbot-release`. The default remains the versioned HTTPS release
directory, and installers reject other release-base schemes.

Release scripts are intentionally explicit. They do not publish crates,
containers, or services, and they must not receive credentials through command
arguments.

Core archives contain the `crabbot`, `crab`, and `crabbot-daemon` executables
plus license files, but no plugin binaries. Every plugin has its own archive with
its manifest and executable under `bin/`. Plugin archives are registered one
at a time in `plugins.lock` under
`CRABBOT_HOME/plugins/<id>` (or the platform configuration directory). A
running daemon starts a newly installed plugin over authenticated local IPC;
otherwise, it discovers the plugin the next time it starts.
Service activation remains an explicit `crabbot service start` action after
installation.
