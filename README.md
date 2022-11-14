# Slack Connector for Apache Flink®

## Compatibility matrix

| Apache Flink    | Slack Connector for Apache Flink | Release date |
|-----------------|----------------------------------|--------------|
| 1.14.x - 1.15.x | v1.0                             | 17.10.2022   |
| 1.15.x - 1.16.x | v2.0                             | TBD          |

To start using Slack connector for Flink put `flink-slack-connector-<version>.jar`
to `lib/` folder of Flink and restart Flink.

## Build locally
```bash
./mvnw clean verify -DskipTests
```
or in case of Windows
```
mvnw clean verify -DskipTests
```
To build with tests it is required to define `SLACK_FOR_FLINK_BOT_TOKEN` and `SLACK_FOR_FLINK_CHANNEL_ID` env variables.
`SLACK_FOR_FLINK_BOT_TOKEN` contains Bot User OAuth Token (see below)
`SLACK_FOR_FLINK_CHANNEL_ID` contains a channel id used during unit tests

## Set-up Slack Application
1. Create your Slack app at [Slack API](https://api.slack.com/apps)
2. In Oauth & Permissions in Scopes add `chat:write`
3. In Oauth & Permissions copy `Bot User OAuth Token` and use for placeholder `BOT_USER_OAUTH_ACCESS_TOKEN`
4. Reinstall the app
5. Add app to the channel where it should post 

## Send message

Messages could be sent in plain text or formatted way. Also, messages could be sent as normal messages or as replies.
To send messages it is required that app has `chat:write` permission and bot is added to channel where a message should be post.

First create table
```sql
CREATE TEMPORARY TABLE slack_example (
    `channel_id` STRING,
    `message` STRING 
) WITH (
    'connector' = 'slack',
    'token' = BOT_USER_OAUTH_ACCESS_TOKEN
);
```
To be able to send messages in a table there should be column with name `channel_id` and `message` (or `text`).
Now to send a simple plain text message to channel_id `CHANNEL_ID`
```sql
INSERT INTO slack_example VALUES('CHANNEL_ID', 'Hello world!');
```

### Send message as reply
To send message as reply it is required to specify `thread` to reply.
```sql
CREATE TEMPORARY TABLE slack_example_with_reply (
    `channel_id` STRING,
    `thread` STRING,
    `message` STRING 
) WITH (
    'connector' = 'slack',
    'token' = BOT_USER_OAUTH_ACCESS_TOKEN
);
```
Now to reply a simple plain text message to thread `THREAD_ID` in channel_id `CHANNEL_ID`
```sql
INSERT INTO slack_example_with_reply VALUES('CHANNEL_ID', 'THREAD_ID', 'Hello world!');
```
In case `THREAD_ID` is null it will be a normal message e.g.
```sql
INSERT INTO slack_example_with_reply VALUES('CHANNEL_ID', CAST(NULL AS STRING), 'Hello world!');
```

### Send formatted message
To send formatted message there should be `blocks_as_str` column
```sql
CREATE TEMPORARY TABLE slack_example_formatted (
    `channel_id` STRING,
    `thread` STRING,
    `formatted` STRING 
) WITH (
    'connector' = 'slack',
    'token' = BOT_USER_OAUTH_ACCESS_TOKEN
);
```
Now to send a formatted message to channel_id `CHANNEL_ID`
```sql
INSERT INTO slack_example_formatted VALUES('CHANNEL_ID', CAST(NULL AS STRING), '[{"type": "divider"}]');
```
And similar message replying to `THREAD_ID`
```sql
INSERT INTO slack_example_formatted VALUES('CHANNEL_ID', 'THREAD_ID', '[{"type": "divider"}]');
```

A more examples of formatted messages could be found at [block-kit-builder](https://app.slack.com/block-kit-builder)
Note: in `formatted` there should be passed only value for `blocks`.

## Trademarks

Apache Flink is either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.

Slack is a trademark and service mark of Slack Technologies, Inc., registered in the U.S. and in other countries.
