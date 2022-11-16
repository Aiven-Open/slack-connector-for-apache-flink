package io.aiven.flink.connectors.slack.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlackTableSource implements ScanTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(SlackTableSource.class);
  private final String appToken;
  private final String botToken;
  private final String channelId;
  private final ResolvedSchema schema;

  public SlackTableSource(
      String appToken, String botToken, String channelId, ResolvedSchema schema) {
    this.appToken = appToken;
    this.botToken = botToken;
    this.channelId = channelId;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

    final SourceFunction<RowData> sourceFunction =
        appToken == null
            ? new FlinkSlackBotSourceFunction(botToken, channelId, schema.getColumnNames())
            : new FlinkSlackAppSourceFunction(appToken, schema.getColumnNames());

    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new SlackTableSource(appToken, botToken, channelId, schema);
  }

  @Override
  public String asSummaryString() {
    return null;
  }
}
