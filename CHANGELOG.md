# Changelog

All notable Krabb changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Fixed

- Cancelling plugin startup or restart during the hello handshake terminates the new
  process tree and reaps the child.
- Windows service installation migrates configured external memory files and existing
  writer locks to service-compatible private ACLs without replacing parent permissions.
- TUI automatic compaction and generation share one turn deadline; failed or cancelled
  compaction retires its provider request before generation can reuse the connection.
- TUI streaming failures wait for request cleanup and finalize session and cancellation state.
- Reserved TUI approval history writes remain available while plugin uninstall drains turns.

- Typed terminal commands acknowledge cancellation after process cleanup, including streaming
  failures, and retire their cancellation token before subsequent commands.
- Typed commands and manual compaction retain their session reservation while executing.
- Typed shell commands reject execution on macOS and other platforms without supported
  descendant containment; Linux and Windows remain supported.
- Daemon-owned TUI turns retain their session reservation through long approval waits.
- Tool discovery waits for active plugin calls before starting its response timeout.
- Cancellation retains each affected tool provider's connection until cleanup completes,
  preventing queued calls from receiving retired responses when request IDs are reused.
- Concurrent plugins accept sequential reuse of completed request IDs.

- Linux host shell descendants cannot change process groups or sessions to escape cleanup.
  macOS and other Unix hosts require the Docker/Podman shell sandbox for containment.
- Windows service installation grants read/execute access to the resolved Codex executable
  without granting access to its containing user profile.
- Model steps forward buffered text and reconcile missing final suffixes, preserving final
  answers after streamed tool preambles and provider notification limits.
- Windows shell processes enter kill-on-close jobs before execution, so background descendants
  stop on completion or cancellation; process cleanup keeps stdout free of status messages.
- Turn cancellation waits for interrupted model and tools requests to finish process and
  container cleanup before acknowledgement, preventing stale model responses from reaching
  later TUI turns and preserving unrelated browser and desktop sessions in channel turns.
- Windows service installation grants additive read/execute access to configured Playwright
  dependency trees, Chromium directories, and Node.js executable paths.
- Automatic room worktrees disable executable Git helpers and reject checkout filters and
  partial clones that require an explicitly approved shell operation.
- The Windows service dispatcher has a distinct entrypoint from CLI service commands.
- Memory path selection recovers interrupted current and legacy index saves under
  their writer locks before choosing the store.
- Windows service installation grants access to external memory and SQLite parent
  directories and existing sidecars, including when the store has not been created.
- Sandboxed shell cancellation and plugin restart remove containers through host-owned
  tracking, including containers detached from the terminated plugin process.
- Windows service installation grants additive access to configured external workspace
  and media directories while preserving existing user permissions.
- Live plugin uninstall reserves daemon work through settling active turns, checking
  dependent state, and removing files; forced purges use the same reservation.

- Windows services run as the dedicated `NT SERVICE\Krabb` account with restricted
  privileges and access to their data. Forced reinstalls stop the previous service;
  forced configuration changes wait for service shutdown before restarting.
- Browser WebSocket routes accept allowed public hosts while retaining URL and address checks.
- Memory access and I/O failures preserve the existing index and active state and report errors.
- ACP response reads enforce the frame limit before allocating an entire line, including
  oversized output without a newline.

- Unix plugin restart cleanup reaches shell commands in separate process groups.
- Browser sessions can reopen tabs after the final tab closes, and website-closed
  tabs release their observations and capacity.

- Slack Web API requests preserve the `/api` path for socket setup, history, and replies.
- A2A streamed answers retain artifacts in the final structured result after completion
  status updates, including artifact replacement and append semantics.

- CLI command discovery waits for concurrent metadata access and reports metadata errors
  instead of silently skipping installed commands. Daemon ownership remains fail-fast.

- Service installation pins the installing user's default Codex profile, including
  Windows service-account access migration.
- Discord attachment downloads preserve binary image and audio bytes.
- MCP stdio operations initialize their child connection before use; HTTP supports
  JSON, bounded SSE, empty acknowledgements, and explicit session-ID forwarding.

- ACP resumes sessions with empty load responses and accepts agents advertising sign-in
  methods until a session request actually requires authentication.
- AG-UI hands provider tool callbacks to the frontend as pending calls, including Codex sign-in.
- OpenRouter resolves daemon sessions' unset model through `KRABB_OPENROUTER_MODEL`.
- Linux service environments preserve percent-encoded browser proxy URLs.
- Windows private state grants access to its owning account, Krabb service, and LocalSystem; service
  installation migrates existing state, and service-created files retain client access.

- Windows services use an SCM dispatcher, report startup and shutdown progress, and
  stop the daemon through its normal cleanup path; foreground execution remains available.
- Codex sign-in and Gemini tool responses preserve screenshot pixels alongside text.
- ACP approvals use stderr and refuse interactive prompts in JSON or noninteractive mode.

- Runtime configuration and plugin locks recover interrupted metadata writes under exclusive access.
- Plugin recovery restores backups matching the committed lock after interrupted updates.
- SQLite records reserve page overhead within the host’s 256 KiB text budget; query JSON
  and pagination metadata remain intact through model requests.
- Confined Unix file edits preserve ordinary permissions, including executable bits.
- JSON plugin commands route streaming notifications and login instructions to stderr;
  ACP retains answer text in the final JSON result.
- AG-UI command advertisements match the packaged manifest for runtime admission.
- Telegram media downloads preserve binary image and voice bytes in the confined cache.
- Gemini treats the host's unset model sentinel as its default, including legacy history routing.

- Plugin uninstall guards query the live daemon and require exclusive ownership for offline recovery.
- Session replies, delivery indexes, and client cursors commit through one atomic snapshot.
- AG-UI reconciles partial notifications with final text and tool calls without duplicate events.
- Dropped and expired plugin host calls release registrations and cancel active or queued work;
  late responses no longer stop protocol input.
- Gemini transport errors omit credential-bearing request URLs.

- Memory processes coordinate disk access and reload records before each operation.
- Recovery journals are replaced atomically; session catalogs recover interrupted
  turns and deliveries using the same rules as legacy session files.
- ACP filesystem requests use host workspace confinement and approval checks.
- Native desktop opt-in and browser/computer settings survive plugin and service launches.
- Anthropic, Ollama, Codex API-key, ACP, and A2A streams finish within the notification
  budget while retaining response text; A2A artifact updates are emitted once.
- AG-UI listeners run until interrupted, with host request limits applied per run.
- Windows browser setup installs Chromium in the configured runtime directory.
- Local CI selects the existing Test and Build workflow jobs.
- Release metadata now reaches packaging and publication, and Unix archive
  checks use the native Linux or macOS service definition.
- OpenAI and OpenRouter preserve the complete tool catalog and finish long
  streamed responses after reaching the notification budget.
- Model-requested tools count once toward the runtime's 16-call turn limit.
