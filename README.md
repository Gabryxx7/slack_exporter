# Slack Exporter Bot

This is a fairly simple bot to export your OWN data from Slack.
It can export:
- Private conversations
- Group conversations (non-channels)
- Private channels (the ones that you are in)
- Public channels (all of them)
- List of users in the workspace
- Reactions to messages

Additionally it *_calculates the rate of calls to the Slack API to avoid getting rate limited.*_ Which is probably the most interesting feature.

Check the API rate limits here: https://api.slack.com/docs/rate-limits

