package io.aiven.flink.connectors.bigquery.sink;

import java.io.Flushable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract class of BigQuery output format. */
public abstract class AbstractBigQueryOutputFormat extends RichOutputFormat<RowData>
    implements Flushable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQueryOutputFormat.class);

  protected transient volatile boolean closed = false;
  protected transient ScheduledExecutorService scheduler;

  protected transient ScheduledFuture<?> scheduledFuture;

  protected transient volatile Exception flushException;

  public AbstractBigQueryOutputFormat() {}

  @Override
  public void configure(Configuration parameters) {}

  public void scheduledFlush(long intervalMillis, String executorName) {
    Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
    scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
    scheduledFuture =
        scheduler.scheduleWithFixedDelay(
            () -> {
              synchronized (this) {
                if (!closed) {
                  try {
                    flush();
                  } catch (Exception e) {
                    flushException = e;
                  }
                }
              }
            },
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;

      try {
        flush();
      } catch (Exception exception) {
        LOG.warn("Flushing records to BigQuery failed.", exception);
      }

      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
        this.scheduler.shutdown();
      }

      closeOutputFormat();
      checkFlushException();
    }
  }

  protected void checkFlushException() {
    if (flushException != null) {
      throw new RuntimeException("Flush exception found.", flushException);
    }
  }

  protected abstract void closeOutputFormat();

  public static class Builder {

    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    private DataType[] fieldDataTypes;

    private String[] fieldNames;

    private BigQueryConnectionOptions options;

    private List<String> partitionKeys;

    public Builder() {}

    public Builder withFieldDataTypes(DataType[] fieldDataTypes) {
      this.fieldDataTypes = fieldDataTypes;
      return this;
    }

    public Builder withFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder withOptions(BigQueryConnectionOptions options) {
      this.options = options;
      return this;
    }

    public AbstractBigQueryOutputFormat build() {
      Preconditions.checkNotNull(fieldNames);
      Preconditions.checkNotNull(fieldDataTypes);
      LogicalType[] logicalTypes =
          Arrays.stream(fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
      return createBatchOutputFormat(logicalTypes);
    }

    private BigQueryBatchOutputFormat createBatchOutputFormat(LogicalType[] logicalTypes) {
      String[] keyFields = new String[0];

      return new BigQueryBatchOutputFormat(
          fieldNames, keyFields, listToStringArray(partitionKeys), logicalTypes, options);
    }

    private String[] listToStringArray(List<String> list) {
      if (list == null) {
        return new String[0];
      } else {
        return list.toArray(new String[0]);
      }
    }
  }
}
