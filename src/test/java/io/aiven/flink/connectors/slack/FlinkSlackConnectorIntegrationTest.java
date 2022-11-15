package io.aiven.flink.connectors.slack;

import static io.aiven.flink.connectors.slack.Constants.IDENTIFIER;
import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class FlinkSlackConnectorIntegrationTest {

  private static final Map<String, String> ENV_PROP_MAP = System.getenv();
  private static final String BOT_TOKEN = ENV_PROP_MAP.get("SLACK_FOR_FLINK_BOT_TOKEN");
  private static final String CHANNEL_ID = ENV_PROP_MAP.get("SLACK_FOR_FLINK_CHANNEL_ID");
  private static final String APP_TOKEN = ":app_token";

  @BeforeAll
  static void beforeAll() {
    Objects.requireNonNull(BOT_TOKEN);
    Objects.requireNonNull(CHANNEL_ID);
  }

  @Test
  void testSink(TestInfo testInfo) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    final String tableName = "slack_alarm";
    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("channel_id", DataTypes.STRING().nullable(), "channel_id", false),
            Column.metadata("message", DataTypes.STRING().nullable(), "message", false));
    tableEnv.executeSql(constructCreateTableSql(schema, tableName)).await();

    tableEnv
        .fromValues(schema.toSinkRowDataType(), row(CHANNEL_ID, getMessageForSlack(testInfo)))
        .executeInsert(tableName)
        .await();
  }

  @Test
  void testSinkFormatted(TestInfo testInfo) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    final String tableName = "slack_alarm_formatted";
    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("channel_id", DataTypes.STRING().nullable(), "channel_id", false),
            Column.metadata("formatted", DataTypes.STRING().nullable(), "message", false));
    tableEnv.executeSql(constructCreateTableSql(schema, tableName)).await();

    tableEnv
        .fromValues(
            schema.toSinkRowDataType(), row(CHANNEL_ID, getFormattedMessageForSlack(testInfo)))
        .executeInsert(tableName)
        .await();
  }

  @Test
  public void testFlinkSource() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    final String tableName = "testFlinkSourceTable";
    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("channel_id", DataTypes.STRING().nullable(), "channel_id", false),
            Column.metadata("message", DataTypes.STRING().nullable(), "message", false));
    tableEnv.executeSql(constructCreateTableSql(schema, tableName)).await();
    final String message =
        "io.aiven.flink.connectors.slack.FlinkSlackConnectorIntegrationTest.testFlinkSource "
            + System.nanoTime();
    tableEnv
        .fromValues(schema.toSinkRowDataType(), row(CHANNEL_ID, message))
        .executeInsert(tableName)
        .await();
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE testFlinkSourceTable2 ( \n"
            + "  `ts` DECIMAL(16, 6),\n"
            + "  `channel` STRING,\n"
            + "  `type` STRING,\n"
            + "  `username` STRING,\n"
            + "  `user` STRING,\n"
            + "  `text` STRING \n"
            + ") WITH (\n"
            + "  'connector' = '"
            + IDENTIFIER
            + "',"
            + "  'token' = '"
            + BOT_TOKEN
            + "',"
            + "  'channel_id' = '"
            + CHANNEL_ID
            + "'"
            + ")");

    assertThat(
            tableEnv
                .executeSql(
                    "SELECT count(1) FROM testFlinkSourceTable2 where text = '" + message + "'")
                .collect()
                .next()
                .getField(0))
        .isEqualTo(1L);
  }

  private String constructCreateTableSql(ResolvedSchema schema, String tableName) {
    StringBuilder result =
        new StringBuilder("CREATE TEMPORARY TABLE ").append(tableName).append(" (");
    for (int i = 0; i < schema.getColumnCount(); i++) {
      Column column = schema.getColumn(i).get();
      result
          .append("\n  `")
          .append(column.getName())
          .append("` ")
          .append(column.getDataType().toString());
      if (i < schema.getColumnCount() - 1) {
        result.append(",");
      }
    }
    result
        .append(") WITH (\n  'connector' = '" + IDENTIFIER + "',  'token' = '")
        .append(BOT_TOKEN)
        .append("')");
    return result.toString();
  }

  private String getMessageForSlack(TestInfo testInfo) {
    return LocalDateTime.now()
        + ": Hello world message from "
        + "Java "
        + System.getProperty("java.version")
        + " "
        + System.getProperty("os.name")
        + "\n "
        + testInfo.getTestClass().get().getName()
        + "#"
        + testInfo.getTestMethod().get().getName();
  }

  private String getFormattedMessageForSlack(TestInfo testInfo) {
    return "[{\n"
        + "\"type\": \"section\",\n"
        + " \"text\": {\n"
        + " \"type\": \"mrkdwn\",\n"
        + " \"text\": \""
        + LocalDateTime.now()
        + " Hello \uD83D\uDC4B from "
        + "Java "
        + System.getProperty("java.version")
        + " "
        + System.getProperty("os.name")
        + "\n*"
        + testInfo.getTestClass().get().getName()
        + "#"
        + testInfo.getTestMethod().get().getName()
        + "*\"\n"
        + " }\n"
        + " }]";
  }
}
