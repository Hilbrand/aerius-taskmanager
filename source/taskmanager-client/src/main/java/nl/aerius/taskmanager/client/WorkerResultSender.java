/*
 * Copyright (c) Contributors to the project
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
import java.io.Serializable;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import nl.aerius.taskmanager.client.util.QueueHelper;

/**
 * Class for workers to handle incoming tasks and send results back to the client.
 */
public class WorkerResultSender implements WorkerIntermediateResultSender {

  private static final byte[] EMPTY_ARRAY = new byte[0];
  private static final String EXCHANGE = ""; //no exchange, direct message

  private final Channel channel;
  private final BasicProperties properties;

  public WorkerResultSender(final Channel channel, final BasicProperties properties) {
    this.channel = channel;
    this.properties = properties;
  }

  @Override
  public void sendIntermediateResult(final Serializable result) throws IOException {
    if (properties.getReplyTo() != null && !properties.getReplyTo().isEmpty()) {
      sendMessage(properties.getReplyTo(), QueueHelper.objectToBytes(result));
    }
  }

  /**
   * Send the last result back to the client. Because it is the last message, the taskmanager should be informed.
   * This is done by sending a message back to the worker reply queue.
   * @param data to send on the queue
   * @throws IOException when the result could not be send.
   */
  public void sendFinalResult(final Serializable data) throws IOException {
    sendIntermediateResult(data);
    if (properties.getHeaders() != null) {
      final String taskManagerReplyQueue = QueueHelper.getHeaderString(properties, QueueConstants.TASKMANAGER_REPLY_QUEUE);

      if (taskManagerReplyQueue != null) {
        sendMessage(taskManagerReplyQueue, EMPTY_ARRAY);
      }
    }
  }

  private void sendMessage(final String queue, final byte[] data) throws IOException {
    final BasicProperties basicProperties = new BasicProperties.Builder()
        .correlationId(properties.getCorrelationId())
        .messageId(properties.getMessageId())
        .headers(constructReturnHeaders(properties.getHeaders()))
        .build();
    // reply to the requested queue, converting the object to bytes first.
    channel.basicPublish(EXCHANGE, queue, basicProperties, data);
  }

  private static Map<String, Object> constructReturnHeaders(final Map<String, Object> map) {
    final long startTime = TaskMetrics.longValue(map, TaskMetrics.AER_WORK_STARTTIME);

    if (startTime > 0) {
      return new TaskMetrics(map).determineDuration().build();
    } else {
      return Map.of();
    }
  }
}
