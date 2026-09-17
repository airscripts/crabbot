# whisper

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- whisper
```

Configure `CRABBOT_WHISPER_COMMAND` with a local Whisper-compatible executable.
The `transcribe` method accepts a path confined to `CRABBOT_MEDIA` (defaulting
to `CRABBOT_HOME/media`) and returns normalized text. After a successful
transcription, Crabbot removes the raw voice file. No model or native runtime
is bundled into the core.
