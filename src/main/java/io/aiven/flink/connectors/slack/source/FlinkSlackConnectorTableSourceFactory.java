package io.aiven.flink.connectors.slack.source;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSlackConnectorTableSourceFactory implements DynamicTableSourceFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkSlackConnectorTableSourceFactory.class);

  public static final ConfigOption<String> APP_TOKEN =
      ConfigOptions.key("apptoken").stringType().noDefaultValue();

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    helper.validate();

    final List<String> columnNames =
        context.getCatalogTable().getResolvedSchema().getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getName)
            .collect(Collectors.toList());
    return new FlinkSlackConnectorTableSource(helper.getOptions().get(APP_TOKEN), columnNames);
  }

  @Override
  public String factoryIdentifier() {
    return "slack-connector";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(APP_TOKEN);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
