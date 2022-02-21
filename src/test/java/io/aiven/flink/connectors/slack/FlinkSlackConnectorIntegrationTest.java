package io.aiven.flink.connectors.slack;

import static org.apache.flink.table.api.Expressions.row;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;

public class FlinkSlackConnectorIntegrationTest {
  private static final String BOT_TOKEN = ":bot_token";
  private static final String APP_TOKEN = ":app_token";

  // @Test
  public void testSink() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
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
                + "  'connector' = 'slack-connector',"
                + "  'token' = '"
                + BOT_TOKEN
                + "'"
                + ")")
        .await();

    for (int i = 0; i < 10; i++) {
      tableEnv
          .fromValues(schema.toSinkRowDataType(), row("<channel_id>", "<message_content>"))
          .executeInsert("slack_alarm")
          .await();
    }
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
            + "  'connector' = 'slack-connector',"
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
