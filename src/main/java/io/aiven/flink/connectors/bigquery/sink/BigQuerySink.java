package io.aiven.flink.connectors.bigquery.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class BigQuerySink implements DynamicTableSink {
  private final CatalogTable catalogTable;
  private final ResolvedSchema tableSchema;
  private final String serviceAccount;
  private final String gcpProjectId;
  private final String dataset;
  private final String table;
  private final DataType rowType;

  public BigQuerySink(
      CatalogTable catalogTable,
      ResolvedSchema tableSchema,
      DataType rowType,
      String serviceAccount,
      String gcpProjectId,
      String dataset,
      String table) {
    this.catalogTable = catalogTable;
    this.tableSchema = tableSchema;
    this.serviceAccount = serviceAccount;
    this.gcpProjectId = gcpProjectId;
    this.dataset = dataset;
    this.table = table;
    this.rowType = rowType;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final SinkFunction<RowData> sinkFunction =
        new FlinkBigQuerySinkFunction(rowType, serviceAccount, gcpProjectId, dataset, table);

    String[] fieldNames =
        tableSchema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getName)
            .toArray(String[]::new);
    DataType[] fieldTypes =
        tableSchema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getDataType)
            .toArray(DataType[]::new);

    AbstractBigQueryOutputFormat outputFormat =
        new AbstractBigQueryOutputFormat.Builder()
            .withFieldNames(fieldNames)
            .withFieldDataTypes(fieldTypes)
            .withOptions(new BigQueryConnectionOptions(gcpProjectId, dataset, table))
            .build();
    return OutputFormatProvider.of(outputFormat);
  }

  @Override
  public DynamicTableSink copy() {
    return new BigQuerySink(
        catalogTable, tableSchema, rowType, serviceAccount, gcpProjectId, dataset, table);
  }

  @Override
  public String asSummaryString() {
    return "BigQuery sink";
  }
}
