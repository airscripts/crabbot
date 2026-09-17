# Acceptance

Crabbot keeps acceptance flows deterministic and credential-free. The tests use
JSON-RPC fixture processes for channels, providers, and tools, then exercise the
same host bridge and persistence paths used by a running daemon.

## Product Flows

| Flow | Evidence |
| --- | --- |
| Telegram Text And Duplicate Delivery | `loads_channel_and_model_during_bridge`, `drains_queued_channel_events` |
| Telegram Image Handling | `resolves_telegram_media_references`, `prepares_only_bounded_confined_images`, `pins_image_media_outside_the_expiring_cache` |
| Telegram Voice Handling | `transcribes_voice_with_the_speech_plugin_and_removes_raw_audio`, `omits_unsafe_voice_files_without_leaking_their_paths` |
| Telegram Tools And Approvals | `runs_a_tool_turn_before_replying`, `prompts_for_signed_approval_before_mutating_tools`, `denied_prompt_never_dispatches_a_mutating_tool` |
| Discord Media And Uploads | Discord normalization and media confinement tests, including text files, images, and voice-message audio |
| Memory And Timers | `memory_can_remember_list_and_forget`, `timer_can_add_list_wait_and_remove` |
| Discord Routing And Authorization | Discord `classifies_gateway_events`, `normalizes_gateway_messages`, and `prepares_approval_controls_and_normalizes_interactions` |
| Isolated Worktrees | `isolates_group_workspaces` |
| Provider And Model Switching | `manages_local_sessions_and_plugins`, `runs_session_commands_without_daemon` |
| Core-Only Diagnostics | `runs_non_interactive_commands`, `checks_plugin_readiness` |
| Plugin Update And Rollback | `updates_active_plugins_without_restarting_the_daemon`, `updates_plugins_offline_when_the_daemon_is_absent`, `manages_local_sessions_and_plugins` |

## Native Checks

The test workflow runs the full workspace suite on Linux, macOS, and Windows,
while the build workflow compiles every supported release target. Package CI
extracts every archive, starts the packaged CLI, writes the native Unix service
definition, and runs the Unix and PowerShell installers against local checked
archives. Linux CI also runs `crabbot-scripts/sandbox.sh` against a preloaded
Docker image.

Live Telegram, Discord, provider, and service-manager credentials are not
required for acceptance. Operators should perform one credentialed smoke run
before publishing a release, using the checklist in [Release](release.md).
