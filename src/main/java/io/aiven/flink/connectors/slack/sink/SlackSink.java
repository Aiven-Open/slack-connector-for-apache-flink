package io.aiven.flink.connectors.slack.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

public class SlackSink implements DynamicTableSink {
  private final String token;

  public SlackSink(String token) {
    this.token = token;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final SinkFunction<RowData> sinkFunction = new FlinkSlackSinkFunction(token);
    return SinkFunctionProvider.of(sinkFunction);
  }

  @Override
  public DynamicTableSink copy() {
    return new SlackSink(token);
  }

  @Override
  public String asSummaryString() {
    return "Slack sink";
  }
}
