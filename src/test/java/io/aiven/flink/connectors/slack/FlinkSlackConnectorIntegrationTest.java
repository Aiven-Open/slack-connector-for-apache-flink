package io.aiven.flink.connectors.slack;

import static org.apache.flink.table.api.Expressions.row;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
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

  // @Test
  public void testFlinkSource() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE slack_alarm ( \n"
            + "  `ts` DECIMAL(16, 6),\n"
            + "  `channel` STRING,\n"
            + "  `type` STRING,\n"
            + "  `client_msg_id` STRING,\n"
            + "  `text` STRING \n"
            + ") WITH (\n"
            + "  'connector' = 'slack',"
            + "  'apptoken' = '"
            + APP_TOKEN
            + "'"
            + ")");

    Table tableResult = tableEnv.sqlQuery("SELECT * FROM slack_alarm");

    DataStream<Row> rowDataStream = tableEnv.toDataStream(tableResult);
    rowDataStream.print();
    env.execute("Slack connector example Job");
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
    result.append(") WITH (\n  'connector' = 'slack',  'token' = '").append(BOT_TOKEN).append("')");
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
