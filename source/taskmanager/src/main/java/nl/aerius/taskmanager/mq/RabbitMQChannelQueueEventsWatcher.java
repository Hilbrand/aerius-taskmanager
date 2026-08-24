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
package nl.aerius.taskmanager.mq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;

/**
 * Watches the RabbitMQ event channel for changes in consumers to dynamically monitor the number of workers added or removed.
 */
class RabbitMQChannelQueueEventsWatcher {

  private static final String AMQ_RABBITMQ_EVENT = "amq.rabbitmq.event";
  private static final String CHANNEL_PATTERN = "consumer.*";
  private static final String HEADER_PARAM_QUEUE = "queue";

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQChannelQueueEventsWatcher.class);

  private final BrokerConnectionFactory factory;
  private final WorkerSizeProviderProxy proxy;
  private Channel channel;

  /**
   * Constructor.
   *
   * @param factory connection factory
   * @param proxy proxy to get observers for specific worker queues
   */
  public RabbitMQChannelQueueEventsWatcher(final BrokerConnectionFactory factory, final WorkerSizeProviderProxy proxy) {
    this.factory = factory;
    this.proxy = proxy;
  }

  /**
   * Start the watcher.
   *
   * @throws IOException Throws IOException in case of communication problems with RabbitMQ
   */
  public void start() {
    try {
      final Connection c = factory.getConnection();
      c.addShutdownListener(this::handleShutdownSignal);
      channel = c.createChannel();
      final String q = channel.queueDeclare().getQueue();
      channel.queueBind(q, AMQ_RABBITMQ_EVENT, CHANNEL_PATTERN);
      channel.basicConsume(q, true, createConsumer());
    } catch (final IOException e) {
      LOG.error("Failed to bind to RabbitMQ event queue. No Queue Event watch not available. Message: {}", e.getMessage());
    }
  }

  private void handleShutdownSignal(final ShutdownSignalException sse) {
    if (sse != null && sse.isInitiatedByApplication()) {
      return;
    }
    LOG.debug("Channel RabbitMQChannelQueueEventsWatcher was shut down.");
    // restart
    try {
      if (channel != null) {
        channel.abort();
      }
    } catch (final IOException e) {
      // Eat error when closing channel.
    }
    start();
    LOG.info("Restarted RabbitMQChannelQueueEventsWatcher");
  }

  private DefaultConsumer createConsumer() {
    return new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body)
          throws IOException {
        final Map<String, Object> headers = properties.getHeaders();
        final Object queue = headers.get(HEADER_PARAM_QUEUE);
        final String queueName = queue == null ? null : queue.toString();

        LOG.trace("Event: {} - queue: {}", envelope.getRoutingKey(), queueName);
        proxy.triggerWorkerQueueState(queueName);
      }
    };
  }

  public void shutdown() {
    try {
      channel.close();
    } catch (final IOException | TimeoutException e) {
      LOG.trace("Channel watcher shutdown failed", e);
    }
  }
}
