package io.aiven.flink.connectors.slack.source;

import java.util.List;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSlackConnectorTableSource implements ScanTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSlackConnectorTableSource.class);
  private final String appToken;
  private final List<String> columnNames;

  public FlinkSlackConnectorTableSource(String appToken, List<String> columnNames) {
    this.appToken = appToken;
    this.columnNames = columnNames;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

    final SourceFunction<RowData> sourceFunction =
        new FlinkSlackConnectorSourceFunction(appToken, columnNames);

    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkSlackConnectorTableSource(appToken, columnNames);
  }

  @Override
  public String asSummaryString() {
    return null;
  }
}
