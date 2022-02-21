package io.aiven.flink.connectors.bigquery.sink;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class FlinkBigQuerySinkFunction extends RichSinkFunction<RowData> {
  private final String serviceAccount;
  private final String gcpProjectId;
  private final String dataset;
  private final String table;
  private final DataType rowType;

  public FlinkBigQuerySinkFunction(
      DataType rowType, String serviceAccount, String gcpProjectId, String dataset, String table) {
    this.serviceAccount = serviceAccount;
    this.gcpProjectId = gcpProjectId;
    this.dataset = dataset;
    this.table = table;
    this.rowType = rowType;
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    final Path serviceAccountPath = Path.of(serviceAccount);
    if (Files.isDirectory(serviceAccountPath) || Files.notExists(serviceAccountPath)) {
      throw new IllegalArgumentException(
          "Service account should contain a valid path to service account file");
    }
    BigQuery bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(gcpProjectId)
            .setCredentials(
                ServiceAccountCredentials.fromStream(new FileInputStream(serviceAccount)))
            .build()
            .getService();
    InsertAllRequest.Builder builder =
        InsertAllRequest.newBuilder(TableId.of(gcpProjectId, dataset, table));
    for (int i = 0; i < 10; i++) {
      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("id", i);
      builder.addRow(rowContent);
    }
    bigQuery.insertAll(builder.build());
  }
}
