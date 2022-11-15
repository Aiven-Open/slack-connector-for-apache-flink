package io.aiven.flink.connectors.slack.source;

import com.slack.api.model.Message;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

public enum SlackMessageMetadata {
  TYPE("type", DataTypes.STRING(), message -> StringData.fromString(message.getType())),
  TS(
      "ts",
      DataTypes.BIGINT(),
      message -> DecimalData.fromBigDecimal(new BigDecimal(message.getTs()), 16, 6)),
  USER("user", DataTypes.STRING(), message -> StringData.fromString(message.getUser())),
  TEXT("text", DataTypes.STRING(), message -> StringData.fromString(message.getText())),
  TEAM("team", DataTypes.STRING(), message -> StringData.fromString(message.getTeam())),
  THREAD_TS(
      "thread_ts", DataTypes.STRING(), message -> StringData.fromString(message.getThreadTs()));
  private final String name;
  private final DataType dataType;
  private final Function<Message, Object> function;

  static final Map<String, SlackMessageMetadata> MAP =
      Arrays.stream(values()).collect(Collectors.toMap(t -> t.name, Function.identity()));

  SlackMessageMetadata(String name, DataType dataType, Function<Message, Object> function) {
    this.name = name;
    this.dataType = dataType;
    this.function = function;
  }

  public String getName() {
    return name;
  }

  public Function<Message, Object> getFunction() {
    return function;
  }

  public DataType getDataType() {
    return dataType;
  }

  public static Object getData(String columnName, Message message) {
    if (columnName == null) {
      return null;
    }
    String lowerCaseName = columnName.toLowerCase(Locale.ROOT);
    SlackMessageMetadata slackMessageMetadata = MAP.get(lowerCaseName);
    if (slackMessageMetadata == null) {
      return null;
    }
    return slackMessageMetadata.function.apply(message);
  }
}
