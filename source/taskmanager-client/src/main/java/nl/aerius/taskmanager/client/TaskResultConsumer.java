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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.client.util.QueueHelper;

/**
 * RabbitMQ implementation of a RabbitMQ {@link com.rabbitmq.client.Consumer}.
 * <p>On handleDelivery, the received bytes will be converted to a Java object,
 * which will be passed to the defined {@link TaskResultCallback}.
 */
class TaskResultConsumer extends DefaultConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(TaskResultConsumer.class);

  private final TaskResultCallback callback;

  /**
   * Constructor.
   *
   * @param channel Channel the message is received from
   * @param callback call back to pass the received message or error in case of a returned exception.
   */
  protected TaskResultConsumer(final Channel channel, final TaskResultCallback callback) {
    super(channel);
    this.callback = Optional.ofNullable(callback)
        .orElseThrow(() -> new IllegalArgumentException("There should be a result callback when using TaskResultConsumer."));
  }

  @Override
  public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body)
      throws IOException {
    Object result = null;
    try {
      // convert the byte array to a workable Java object.
      result = QueueHelper.bytesToObject(body);
      // let the workerHandler do its job.
    } catch (final RuntimeException | ClassNotFoundException | IOException e) {
      LOG.error("Exception while deserializing result.", e);
      result = e;
    }
    final String correlationId = properties.getCorrelationId();
    final String messageId = properties.getMessageId();
    if (result instanceof Exception) {
      callback.onFailure((Exception) result, correlationId, messageId);
    } else {
      callback.onSuccess(result, correlationId, messageId);
    }
  }

  @Override
  public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
    if (!sig.isInitiatedByApplication()) {
      LOG.warn("Connection was shutdown", sig);
      callback.onFailure(sig, null, null);
    }
  }
}
