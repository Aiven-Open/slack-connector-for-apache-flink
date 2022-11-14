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

    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("channel_id", DataTypes.STRING().nullable(), "channel_id", false),
            Column.metadata("message", DataTypes.STRING().nullable(), "message", false));
    tableEnv
        .executeSql(
            "CREATE TEMPORARY TABLE slack_alarm ( \n"
                + "  `channel_id` STRING,\n"
                + "  `message` STRING \n"
                + ") WITH (\n"
                + "  'connector' = 'slack',"
                + "  'token' = '"
                + BOT_TOKEN
                + "'"
                + ")")
        .await();

    tableEnv
        .fromValues(
            schema.toSinkRowDataType(),
            row(
                CHANNEL_ID,
                LocalDateTime.now()
                    + ": Hello world message from "
                    + testInfo.getTestClass().get().getName()
                    + "#"
                    + testInfo.getTestMethod().get().getName()))
        .executeInsert("slack_alarm")
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
}
