package io.aiven.flink.connectors.slack.sink;

import static io.aiven.flink.connectors.slack.Constants.BOT_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.IDENTIFIER;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlackTableSinkFactory implements DynamicTableSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SlackTableSinkFactory.class);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final DataType rowType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
    return new SlackSink(rowType, helper.getOptions().get(BOT_TOKEN));
  }

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
    return Collections.emptySet();
  }
}
