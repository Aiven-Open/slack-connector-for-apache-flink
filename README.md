# Slack Connector for Apache Flink®

## Compatibility matrix

|Apache Flink |Slack Connector for Apache Flink|Release date|
--------------|--------------------------------|-----------
|1.15.x|1.15.0|TBD|
|1.14.x|1.14.0|TBD|

## Set-up Slack Application

If youre new to the slack platform, there's a lot of information at [An introduction to the Slack platform](https://api.slack.com/start/overview).

1. Create your Slack app at [Slack API](https://api.slack.com/apps)
2. In Oauth & Permissions in Scopes add `chat:write`
3. In Oauth & Permissions copy `Bot User OAuth Token` and use for placeholder `[BOT_USER_OAUTH_ACCESS_TOKEN]`
4. Reinstall the app
5. Add app to the channel where it should post 

## Send message

Messages can be sent as plain text or formatted. Also, messages can be sent as normal messages or as replies.

To send a messages it is required that the app has `chat:write` permission and the bot is added to the channel where the message should be posted.

First create table
```sql
CREATE TEMPORARY TABLE slack_example (
    `channel_id` STRING,
    `message` STRING 
) WITH (
    'connector' = 'slack-connector',
    'token' = '[BOT_USER_OAUTH_ACCESS_TOKEN]'
);
```
To be able to send messages in a table there should be columns with names `channel_id` and `message` (or `text`).

To send a simple plain text message to channel_id `CHANNEL_ID`
```sql
INSERT INTO slack_example VALUES('CHANNEL_ID', 'Hello world!');
```

Note: `CHANNEL_ID` can be the id of the channel, or its name. However, the name can change, while the id cannot.

### Send message as reply
To send a message as a reply it is required to specify the `thread` to reply to.
```sql
CREATE TEMPORARY TABLE slack_example_with_reply (
    `channel_id` STRING,
    `thread` STRING,
    `message` STRING 
) WITH (
    'connector' = 'slack-connector',
    'token' = '[BOT_USER_OAUTH_ACCESS_TOKEN]'
);
```
To reply with a simple plain text message to thread `[THREAD_ID]` in channel_id `[CHANNEL_ID]`
```sql
INSERT INTO slack_example_with_reply VALUES('[CHANNEL_ID]', '[THREAD_ID]', 'Hello world!');
```
If `[THREAD_ID]` is null then it will be a normal message (not in a thread) e.g.
```sql
INSERT INTO slack_example_with_reply VALUES('[CHANNEL_ID]', CAST(NULL AS STRING), 'Hello world!');
```

### Send formatted message
To send a formatted message there should be a `formatted` column
```sql
CREATE TEMPORARY TABLE slack_example_formatted (
    `channel_id` STRING,
    `thread` STRING,
    `formatted` STRING 
) WITH (
    'connector' = 'slack-connector',
    'token' = '[BOT_USER_OAUTH_ACCESS_TOKEN]'
);
```
To send a formatted message to channel_id `[CHANNEL_ID]`
```sql
INSERT INTO slack_example_formatted VALUES('[CHANNEL_ID]', CAST(NULL AS STRING), '[{"type": "divider"}]');
```
And a similar message replying to `[THREAD_ID]`
```sql
INSERT INTO slack_example_formatted VALUES('[CHANNEL_ID]', '[THREAD_ID]', '[{"type": "divider"}]');
```

More examples of formatted messages can be found at [block-kit-builder](https://app.slack.com/block-kit-builder)

Note: the text `formatted` should be the value for `blocks`, do not specify the `"blocks"` key itself.

## Trademarks

Apache Flink is either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.

Slack is a trademark and service mark of Slack Technologies, Inc., registered in the U.S. and in other countries.
