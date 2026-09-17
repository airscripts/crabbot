# Slack

Install the plugin, then configure the bot token and app token from Socket
Mode:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- slack
```

Set
`CRABBOT_SLACK_BOT_TOKEN` and `CRABBOT_SLACK_APP_TOKEN`. The plugin receives
Socket Mode events and falls back to Web API history polling for configured
conversations. Text, images, voice, and text files are normalized through the
channel boundary, and bounded downloads are stored under `CRABBOT_MEDIA`. Set
`CRABBOT_SLACK_CHANNELS` to a comma-separated list of channel IDs.
