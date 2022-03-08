package io.aiven.flink.connectors.slack.sink;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

import com.slack.api.Slack;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

public class FlinkSlackSinkFunction extends RichSinkFunction<RowData> {
  private final String token;
  private final DataType rowType;

  public FlinkSlackSinkFunction(DataType rowType, String token) {
    this.token = token;
    this.rowType = rowType;
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    Map<SlackColumns, String> map = new EnumMap<>(SlackColumns.class);
    List<String> columnNames = getFieldNames(rowType);
    for (int i = 0; i < rowType.getChildren().size(); i++) {
      if (value.getString(i) != null) {
        map.put(SlackColumns.of(columnNames.get(i)), value.getString(i).toString());
      }
    }
    ChatPostMessageResponse response =
        Slack.getInstance()
            .methods()
            .chatPostMessage(
                r -> {
                  ChatPostMessageRequest.ChatPostMessageRequestBuilder
                      chatPostMessageRequestBuilder =
                          r.token(token).channel(map.get(SlackColumns.CHANNEL));
                  if (map.containsKey(SlackColumns.BLOCKS_AS_STRING)) {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.blocksAsString(
                            map.get(SlackColumns.BLOCKS_AS_STRING));
                  } else {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.text(map.get(SlackColumns.TEXT));
                  }
                  if (map.containsKey(SlackColumns.THREAD)) {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.threadTs(map.get(SlackColumns.THREAD));
                  }
                  return chatPostMessageRequestBuilder;
                });
    if (!response.isOk()) {
      throw new RuntimeException(response.getError());
    }
  }

  public enum SlackColumns {
    BLOCKS_AS_STRING("blocks_as_str", "blocks_as_string", "formatted"),
    CHANNEL("channel_id", "channel"),
    THREAD("thread_id", "thread"),
    TEXT("text", "message");

    private final Set<String> names = new HashSet<>();

    private static final Map<String, SlackColumns> NAMES2COLUMNS = new HashMap<>();

    static {
      for (SlackColumns slackColumns : values()) {
        for (String s : slackColumns.names) {
          NAMES2COLUMNS.put(s, slackColumns);
        }
      }
    }

    SlackColumns(String columnName, String... secondaryNames) {
      names.add(columnName.toLowerCase(Locale.ROOT));
      if (secondaryNames != null) {
        for (String s : secondaryNames) {
          names.add(s.toLowerCase(Locale.ROOT));
        }
      }
    }

    public static SlackColumns of(String value) {
      SlackColumns val;
      if (value != null && (val = NAMES2COLUMNS.get(value.toLowerCase(Locale.ROOT))) != null) {
        return val;
      }
      throw new IllegalArgumentException(
          "Unrecognized column '"
              + value
              + "'. Available column name: "
              + Arrays.toString(values()));
    }
  }

  // TODO: Replace this with DataType#getFieldNames with upgrade to Flink 1.15.x
  private static List<String> getFieldNames(DataType dataType) {
    final LogicalType type = dataType.getLogicalType();
    if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
      return getFieldNames(dataType.getChildren().get(0));
    } else if (isCompositeType(type)) {
      return LogicalTypeChecks.getFieldNames(type);
    }
    return Collections.emptyList();
  }
}
