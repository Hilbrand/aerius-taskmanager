/*
 * Copyright the State of the Netherlands
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package nl.aerius.taskmanager.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;

/**
 * Sends tasks to the queue, and opens a consumer to wait for a data being sent back. The data will be passed to the callback.
 */
public class TaskManagerClientSenderWithCallback extends TaskManagerClientSender {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerClientSenderWithCallback.class);

  private final TaskResultCallback callback;
  private final String replyQueueName;
  private final AtomicReference<Channel> replyChannel = new AtomicReference<>();

  /**
   * Constructor.
   *
   * @param factory factory to create connection
   * @param replyQueueName queue name prefix to use as name for the reply queue
   * @param callback The resultCallback which will receive results.
   */
  public TaskManagerClientSenderWithCallback(final BrokerConnectionFactory factory, final String replyQueueName, final TaskResultCallback callback) {
    super(factory);
    this.replyQueueName = replyQueueName + ".reply";
    this.callback = callback;
  }

  @Override
  protected void prepareBeforeSend(final Builder builder) throws IOException {
    if (super.ensureChannel(replyChannel)) {
      createReplyConsumer();
    }
    builder.replyTo(replyQueueName);
  }

  /**
   * Starts the consumer to receive the replied messages.
   *
   * @throws IOException
   */
  private void createReplyConsumer() throws IOException {
    final Channel channel = replyChannel.get();
    channel.queueDeclare(replyQueueName, false, true, true, null);
    final TaskResultConsumer resultConsumer = new TaskResultConsumer(channel, callback);
    channel.basicConsume(replyQueueName, true, resultConsumer);
  }

  @Override
  public void close() {
    synchronized (this) {
      super.close();
      closeChannel(replyChannel.get());
    }
  }
}
