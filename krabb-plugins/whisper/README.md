# Whisper

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- whisper
```

Configure `KRABB_WHISPER_COMMAND` with a local Whisper-compatible executable.
The `transcribe` method accepts a path confined to `KRABB_MEDIA` (defaulting
to `KRABB_HOME/media`) and returns normalized text. After a successful
transcription, Krabb removes the raw voice file. No model or native runtime
is bundled into the core.
