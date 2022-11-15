package io.aiven.flink.connectors.slack.source;

import static io.aiven.flink.connectors.slack.Constants.APP_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.BOT_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.CHANNEL_ID;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlackTableSourceFactory implements DynamicTableSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SlackTableSourceFactory.class);

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
    return new SlackTableSource(
        helper.getOptions().get(APP_TOKEN),
        helper.getOptions().get(BOT_TOKEN),
        helper.getOptions().get(CHANNEL_ID),
        context.getCatalogTable().getResolvedSchema());
  }

  @Override
  public String factoryIdentifier() {
    return "slack";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(BOT_TOKEN);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(APP_TOKEN, CHANNEL_ID);
  }
}
