package io.aiven.flink.connectors.bigquery.sink;

import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.DATASET;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.PROJECT_ID;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.SERVICE_ACCOUNT;
import static io.aiven.flink.connectors.bigquery.sink.BigQueryConfigOptions.TABLE;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class BigQueryTableSinkFactory implements DynamicTableSinkFactory {
  private static final Set<ConfigOption<?>> REQUIRED_OPTIONS =
      Set.of(SERVICE_ACCOUNT, PROJECT_ID, DATASET, TABLE);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final DataType rowType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
    return new BigQuerySink(
        context.getCatalogTable(),
        context.getCatalogTable().getResolvedSchema(),
        rowType,
        helper.getOptions().get(SERVICE_ACCOUNT),
        helper.getOptions().get(PROJECT_ID),
        helper.getOptions().get(DATASET),
        helper.getOptions().get(TABLE));
  }

  @Override
  public String factoryIdentifier() {
    return "bigquery";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return REQUIRED_OPTIONS;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
