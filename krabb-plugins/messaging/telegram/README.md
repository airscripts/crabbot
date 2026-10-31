# Telegram

The Telegram plugin receives messages with long polling and sends replies
through the Bot API. Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- telegram
```

## Table Of Contents

- [Credentials](#credentials)
- [Messages And Media](#messages-and-media)
- [Replies](#replies)

## Credentials

Set `KRABB_TELEGRAM_TOKEN`. With `KRABB_KEYRING=1`, the plugin can use the
`telegram` operating-system keyring entry.

## Messages And Media

The plugin normalizes private and group messages, topics, text, photos,
documents, and voice notes. Media downloads preserve raw bytes in the confined
cache; only textual Bot API responses are decoded as UTF-8.

- Images: PNG, JPEG, GIF, or WebP; up to 4 MiB total per turn. The selected
  model must support image input.
- Documents: bounded UTF-8 text or code files up to 64 KiB each.
- Voice: up to 4 MiB and requires the optional speech plugin. Raw audio is
  removed after transcription.

Media is downloaded into `KRABB_MEDIA`, or `KRABB_HOME/media` by default.
Cached files older than 24 hours are removed. Durable session images are kept
in the session's protected media area.

## Replies

Outgoing text is split at Telegram's message limit. An `edit` updates the first
message and sends any remaining chunks in the same topic.
