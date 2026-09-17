# telegram

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- telegram
```

Set `CRABBOT_TELEGRAM_TOKEN`, or set `CRABBOT_KEYRING=1`
to use the `telegram` operating-system keyring entry. Long polling normalizes
private/group messages, topics, text, photos, documents, and voice notes into
channel-safe content records. The host sends supported PNG, JPEG, GIF, and
WebP images from the confined media cache to the selected model, with a 4 MiB
aggregate per-turn limit. The selected model must support image inputs. The
host expands only bounded UTF-8 text/code documents up to 64 KiB each and
transcribes voice notes up to 4 MiB when the optional speech plugin is
available. Raw voice files are removed after successful transcription; other
cached media expires after 24 hours. Outgoing text is split at Telegram's
message limit before delivery; `edit` updates the first existing message and
sends any remaining chunks in the same topic.
When the daemon requests a `telegram://file/<id>` media
reference, the plugin downloads it into the private `CRABBOT_MEDIA` directory
(or `CRABBOT_HOME/media`), returns a local `file://` URI, and removes files
older than 24 hours.
