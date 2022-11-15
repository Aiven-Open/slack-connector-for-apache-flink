package io.aiven.flink.connectors.slack.source;

import static io.aiven.flink.connectors.slack.Constants.APP_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.BOT_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.CHANNEL_ID;
import static io.aiven.flink.connectors.slack.Constants.IDENTIFIER;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

@Internal
public class SlackCatalogFactory implements CatalogFactory {
  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(BOT_TOKEN);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(CHANNEL_ID, APP_TOKEN);
  }

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper factoryHelper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    factoryHelper.validate();

    ReadableConfig options = factoryHelper.getOptions();
    return new SlackCatalog(context.getName(), options);
  }
}
