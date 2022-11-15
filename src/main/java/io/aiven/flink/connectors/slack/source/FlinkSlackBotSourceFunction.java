package io.aiven.flink.connectors.slack.source;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.response.conversations.ConversationsHistoryResponse;
import com.slack.api.model.Message;
import java.util.List;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSlackBotSourceFunction extends RichSourceFunction<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSlackBotSourceFunction.class);
  private final String botToken;
  private final String channelId;
  private final List<String> columnNames;
  private transient volatile boolean running = true;

  public FlinkSlackBotSourceFunction(String botToken, String channelId, List<String> columnNames) {
    this.columnNames = columnNames;
    this.botToken = botToken;
    this.channelId = channelId;
  }

  @Override
  public void run(SourceContext<RowData> ctx) {
    MethodsClient client = Slack.getInstance().methods(botToken);
    ConversationsHistoryResponse response;
    try {
      response = client.conversationsHistory(r -> r.channel(channelId).includeAllMetadata(true));
    } catch (Exception e) {
      LOG.info("Exception: ", e);
      throw new RuntimeException(e);
    }

    synchronized (ctx.getCheckpointLock()) {
      for (Message message : response.getMessages()) {
        GenericRowData rowData = new GenericRowData(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
          rowData.setField(i, SlackMessageMetadata.getData(columnNames.get(i), message));
        }
        ctx.collect(rowData);
      }
    }
    ctx.markAsTemporarilyIdle();
  }

  @Override
  public void cancel() {
    running = false;
  }
}
