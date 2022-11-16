package io.aiven.flink.connectors.slack;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Constants {
  public static final String IDENTIFIER = "slack";
  public static final ConfigOption<String> BOT_TOKEN =
      ConfigOptions.key("token").stringType().noDefaultValue();
  public static final ConfigOption<String> APP_TOKEN =
      ConfigOptions.key("apptoken").stringType().noDefaultValue();
  public static final ConfigOption<String> CHANNEL_ID =
      ConfigOptions.key("channel_id").stringType().noDefaultValue();
}
