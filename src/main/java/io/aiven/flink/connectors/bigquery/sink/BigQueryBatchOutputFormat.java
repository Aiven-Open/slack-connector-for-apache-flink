package io.aiven.flink.connectors.bigquery.sink;

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

public class BigQueryBatchOutputFormat extends AbstractBigQueryOutputFormat {

  private static final long serialVersionUID = 1L;

  private final String[] fieldNames;

  private final String[] keyFields;

  private final String[] partitionFields;

  private final LogicalType[] fieldTypes;

  private final BigQueryConnectionOptions options;

  protected BigQueryBatchOutputFormat(
      @Nonnull String[] fieldNames,
      @Nonnull String[] keyFields,
      @Nonnull String[] partitionFields,
      @Nonnull LogicalType[] fieldTypes,
      @Nonnull BigQueryConnectionOptions options) {
    this.fieldNames = Preconditions.checkNotNull(fieldNames);
    this.keyFields = Preconditions.checkNotNull(keyFields);
    this.partitionFields = Preconditions.checkNotNull(partitionFields);
    this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void closeOutputFormat() {}

  @Override
  public void flush() throws IOException {}

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {}

  @Override
  public void writeRecord(RowData record) throws IOException {}
}
