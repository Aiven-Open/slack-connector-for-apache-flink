package io.aiven.flink.connectors.slack;

import static org.apache.flink.table.api.Expressions.row;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.slack.api.Slack;
import com.slack.api.SlackConfig;
import com.slack.api.socket_mode.SocketModeClient;
import com.slack.api.socket_mode.listener.WebSocketMessageListener;
import com.slack.api.util.json.GsonFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class FlinkSlackIntegrationTest {

  static final Gson GSON = GsonFactory.createSnakeCase();
  static final String VALID_APP_TOKEN = "xapp-valid-123123123123123123123123123123123123";

  MockWebApiServer webApiServer = new MockWebApiServer();
  MockWebSocketServer wsServer = new MockWebSocketServer();
  SlackConfig config = new SlackConfig();
  Slack slack = Slack.getInstance(config);

  @BeforeEach
  public void setup() throws Exception {
    webApiServer.start();
    wsServer.start();
    config = new SlackConfig();
    config.setMethodsEndpointUrlPrefix(webApiServer.getMethodsEndpointPrefix());
    slack = Slack.getInstance(config);
  }

  @AfterEach
  public void tearDown() throws Exception {
    webApiServer.stop();
    wsServer.stop();
  }

  // @Test
  public void messageFromFlink() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    final ResolvedSchema schema =
        ResolvedSchema.of(
            Column.metadata("title", DataTypes.STRING().nullable(), "title", false),
            Column.metadata("description", DataTypes.STRING().nullable(), "title", false));
    tableEnv
        .executeSql(
            "CREATE TEMPORARY TABLE slack_table ( \n"
                + "  `title` STRING,\n"
                + "  `description` STRING \n"
                + ") WITH (\n"
                + "  'connector' = 'slack',"
                + "  'token' = '"
                + VALID_APP_TOKEN
                + "'"
                + ")")
        .await();

    for (int i = 0; i < 10; i++) {
      tableEnv
          .fromValues(schema.toSinkRowDataType(), row("C111", "Message content"))
          .executeInsert("slack_table")
          .await();
    }

    try (SocketModeClient client =
        slack.socketMode(VALID_APP_TOKEN, SocketModeClient.Backend.JavaWebSocket)) {
      AtomicBoolean helloReceived = new AtomicBoolean(false);
      AtomicBoolean received = new AtomicBoolean(false);
      client.addWebSocketMessageListener(helloListener(helloReceived));
      client.addWebSocketMessageListener(envelopeListener(received));
      client.connect();
      int counter = 0;
      while (!received.get() && counter < 50) {
        Thread.sleep(100L);
        counter++;
      }
      assertTrue(helloReceived.get());
      assertTrue(received.get());
    }
  }

  // -------------------------------------------------

  private static Optional<String> getEnvelopeType(String message) {
    JsonElement msg = GSON.fromJson(message, JsonElement.class);
    if (msg != null && msg.isJsonObject()) {
      JsonElement typeElem = msg.getAsJsonObject().get("type");
      if (typeElem != null && typeElem.isJsonPrimitive()) {
        return Optional.of(typeElem.getAsString());
      }
    }
    return Optional.empty();
  }

  private static WebSocketMessageListener helloListener(AtomicBoolean received) {
    return message -> {
      Optional<String> type = getEnvelopeType(message);
      if (type.isPresent()) {
        if (type.get().equals("hello")) {
          received.set(true);
        }
      }
    };
  }

  private static final List<String> MESSAGE_TYPES =
      Arrays.asList("events", "interactive", "slash_commands");

  private static WebSocketMessageListener envelopeListener(AtomicBoolean received) {
    return message -> {
      Optional<String> type = getEnvelopeType(message);
      if (type.isPresent()) {
        if (MESSAGE_TYPES.contains(type.get())) {
          received.set(true);
        }
      }
    };
  }
}
