package io.aiven.flink.connectors.slack.sink;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class SlackTableSinkFactory implements DynamicTableSinkFactory {
  public static final ConfigOption<String> TOKEN =
      ConfigOptions.key("token").stringType().noDefaultValue();

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final DataType rowType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
    return new SlackSink(rowType, helper.getOptions().get(TOKEN));
  }

  @Override
  public String factoryIdentifier() {
    return "slack-connector";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(TOKEN);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
