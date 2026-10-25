# Slack

Install the plugin, then configure the bot token and app token from Socket
Mode:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- slack
```

Set
`KRABB_SLACK_BOT_TOKEN` and `KRABB_SLACK_APP_TOKEN`. The plugin receives
Socket Mode events and falls back to Web API history polling for configured
conversations. Text, images, voice, and text files are normalized through the
channel boundary, and bounded downloads are stored under `KRABB_MEDIA`. Set
`KRABB_SLACK_CHANNELS` to a comma-separated list of channel IDs. Inbound
events and the per-channel history cursor are stored under `KRABB_HOME` and
are removed only after the host acknowledges them.
