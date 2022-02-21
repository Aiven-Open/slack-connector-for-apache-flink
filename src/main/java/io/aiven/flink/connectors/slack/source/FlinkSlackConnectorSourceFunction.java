package io.aiven.flink.connectors.slack.source;

import com.google.gson.JsonObject;
import com.slack.api.Slack;
import com.slack.api.socket_mode.SocketModeClient;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class FlinkSlackConnectorSourceFunction extends RichSourceFunction<RowData> {

  private static final String EVENT_TAG = "event";
  private final String appToken;
  private final List<String> columnNames;
  private volatile boolean running = true;

  private static final Map<String, Function<JsonObject, Object>> NAME2TYPE =
      Map.of(
          "ts",
          (event) ->
              DecimalData.fromBigDecimal(new BigDecimal(event.get("ts").getAsString()), 16, 6),
          "channel",
          (event) -> StringData.fromString(event.get("channel").getAsString()),
          "type",
          (event) -> StringData.fromString(event.get("type").getAsString()),
          "client_msg_id",
          (event) -> StringData.fromString(event.get("client_msg_id").getAsString()),
          "text",
          (event) -> StringData.fromString(event.get("text").getAsString()));

  public FlinkSlackConnectorSourceFunction(String appToken, List<String> columnNames) {
    this.columnNames = columnNames;
    this.appToken = appToken;
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    try (SocketModeClient client = Slack.getInstance().socketMode(appToken)) {
      client.addEventsApiEnvelopeListener(
          envelope -> {
            JsonObject eventElement =
                envelope.getPayload().getAsJsonObject().getAsJsonObject(EVENT_TAG);
            GenericRowData rowData = new GenericRowData(columnNames.size());
            for (int i = 0; i < columnNames.size(); i++) {
              rowData.setField(i, NAME2TYPE.get(columnNames.get(i)).apply(eventElement));
            }
            ctx.collect(rowData);
          });
      client.connect();
      while (running) {
        Thread.sleep(500);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
