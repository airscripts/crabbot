# Concepts

Crabbot separates the agent loop from the capabilities that make it useful.
The daemon receives normalized events, the core executes a bounded turn, and
plugins provide models, channels, storage, memory, timers, tools, or clients.

## Events

Channel plugins convert provider payloads into stable events before the host
sees them. An event carries an ID, channel, chat, sender, privacy scope, text,
attachments, and optional topic or thread metadata. The host uses the event ID
for deduplication and advances the channel cursor only after durable admission.

## Sessions

A session owns a bounded transcript, queued messages, an optional workspace,
and a selected model. Session IDs combine the channel and chat identity so two
platforms cannot accidentally share history. A session processes one turn at a
time; later messages wait in its bounded queue.

## Agent Instructions And Conversation Context

Crabbot's global workspace is `CRABBOT_HOME/workspace`. `CRAB.md` defines its
identity and communication style; `CLAW.md` defines behavioral guidance and
workflows. The runtime reads both as system context for every user turn,
including resumed sessions and internal tool-loop requests. It does not copy
their contents into the saved transcript, so edits apply on the next turn.
Conversation history remains available as a bounded transcript and may be
compacted when limits require it. Crabbot does not load `AGENTS.md` for itself;
coding agents it delegates to may use workspace `AGENTS.md` instructions.

These Markdown files guide the model but do not grant capabilities. The host
continues to enforce workspace confinement, channel tool settings, approvals,
and shell policy.

## Turns

The core turn loop alternates model replies and tool calls until the model
stops, a step or call limit is reached, cancellation arrives, or the deadline
expires. The host decides whether a tool is allowed and supplies the isolated
workspace. A model cannot grant itself approval.

## Delivery

Replies are persisted with the transcript and placed in a durable outbox.
Channel acknowledgement is separate from reply generation. The host marks a
send before dispatch and quarantines any non-success response as uncertain;
operators decide whether a retry is safe. Deduplication prevents a provider
retry from creating a second session message.

## Leases

An active turn holds a durable lease. Safe leases can be restored after a
daemon restart; leases that may have reached a mutating tool are recorded as
interrupted instead of replayed. Cancellation clears the lease and does not
retry the canceled message. The cancel command reports success only after the
active turn has stopped and acknowledged the request.
