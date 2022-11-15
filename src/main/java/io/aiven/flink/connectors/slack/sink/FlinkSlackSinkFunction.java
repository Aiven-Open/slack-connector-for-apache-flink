package io.aiven.flink.connectors.slack.sink;

import com.slack.api.Slack;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import java.util.Arrays;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSlackSinkFunction extends RichSinkFunction<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSlackSinkFunction.class);
  private final String token;
  private final DataType rowType;

  public FlinkSlackSinkFunction(DataType rowType, String token) {
    this.token = token;
    this.rowType = rowType;
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    Map<SlackMessageColumns, String> map = new EnumMap<>(SlackMessageColumns.class);
    List<String> columnNames = DataType.getFieldNames(rowType);
    for (int i = 0; i < rowType.getChildren().size(); i++) {
      if (value.getString(i) != null) {
        map.put(SlackMessageColumns.of(columnNames.get(i)), value.getString(i).toString());
      }
    }
    ChatPostMessageResponse response =
        Slack.getInstance()
            .methods()
            .chatPostMessage(
                r -> {
                  ChatPostMessageRequest.ChatPostMessageRequestBuilder
                      chatPostMessageRequestBuilder =
                          r.token(token).channel(map.get(SlackMessageColumns.CHANNEL));
                  if (map.containsKey(SlackMessageColumns.BLOCKS_AS_STRING)) {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.blocksAsString(
                            map.get(SlackMessageColumns.BLOCKS_AS_STRING));
                  } else {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.text(map.get(SlackMessageColumns.TEXT));
                  }
                  if (map.containsKey(SlackMessageColumns.THREAD)) {
                    chatPostMessageRequestBuilder =
                        chatPostMessageRequestBuilder.threadTs(map.get(SlackMessageColumns.THREAD));
                  }
                  return chatPostMessageRequestBuilder;
                });
    if (!response.isOk()) {
      LOG.error(response.getError());
      throw new RuntimeException(response.getError());
    }
  }

  public enum SlackMessageColumns {
    BLOCKS_AS_STRING("blocks_as_str", "blocks_as_string", "formatted"),
    CHANNEL("channel_id", "channel"),
    THREAD("thread_id", "thread"),
    TEXT("text", "message");

    private final Set<String> names = new HashSet<>();

    private static final Map<String, SlackMessageColumns> NAMES2COLUMNS = new HashMap<>();

    static {
      for (SlackMessageColumns slackColumns : values()) {
        for (String s : slackColumns.names) {
          NAMES2COLUMNS.put(s, slackColumns);
        }
      }
    }

    SlackMessageColumns(String columnName, String... secondaryNames) {
      names.add(columnName.toLowerCase(Locale.ROOT));
      if (secondaryNames != null) {
        for (String s : secondaryNames) {
          names.add(s.toLowerCase(Locale.ROOT));
        }
      }
    }

    public static SlackMessageColumns of(String value) {
      SlackMessageColumns val;
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
}
