package io.aiven.flink.connectors.slack.sink;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

public class FlinkSlackSinkFunction extends RichSinkFunction<RowData> {
  private final String token;

  public FlinkSlackSinkFunction(String token) {
    this.token = token;
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    MethodsClient slack = Slack.getInstance().methods();
    slack.chatPostMessage(
        r ->
            r.token(token)
                .channel(value.getString(0).toString())
                .text(value.getString(1).toString()));
  }
}
