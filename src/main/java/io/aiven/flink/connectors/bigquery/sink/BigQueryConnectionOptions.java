package io.aiven.flink.connectors.bigquery.sink;

import java.io.Serializable;

public class BigQueryConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private final String projectId;

  private final String dataset;

  private final String tableName;

  public BigQueryConnectionOptions(String projectId, String dataset, String tableName) {
    this.projectId = projectId;
    this.dataset = dataset;
    this.tableName = tableName;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getDataset() {
    return dataset;
  }

  public String getTableName() {
    return tableName;
  }
}
