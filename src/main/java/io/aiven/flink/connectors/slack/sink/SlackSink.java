package io.aiven.flink.connectors.slack.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SlackSink implements DynamicTableSink {
  private final String token;
  private final DataType rowType;

  public SlackSink(DataType rowType, String token) {
    this.token = token;
    this.rowType = rowType;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final SinkFunction<RowData> sinkFunction = new FlinkSlackSinkFunction(rowType, token);
    return SinkFunctionProvider.of(sinkFunction);
  }

  @Override
  public DynamicTableSink copy() {
    return new SlackSink(rowType, token);
  }

  @Override
  public String asSummaryString() {
    return "Slack sink";
  }
}
