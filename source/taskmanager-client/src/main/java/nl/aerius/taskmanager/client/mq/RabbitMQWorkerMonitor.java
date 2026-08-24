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
package nl.aerius.taskmanager.client.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.client.BrokerConnectionFactory;

/**
 * Watches the RabbitMQ event channel for changes in consumers to dynamically monitor the number of workers added or removed.
 */
public class RabbitMQWorkerMonitor {

  /**
   * Observer registered for a specific worker queue that gets called when an update event is received.
   */
  public interface RabbitMQWorkerObserver {
    /**
     * Gives updated values for worker queue size and utilisation.
     *
     * @param size number of workers available
     * @param utilisation number of workers being bussy
     */
    void updateWorkers(String workerQueueName, int size, int utilisation);
  }

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQWorkerMonitor.class);

  public static final String EXCHANGE_TYPE = "fanout";
  public static final String AERIUS_EVENT_EXCHANGE = "aerius.events";

  public static final String HEADER_PARAM_QUEUE = "queue";
  public static final String HEADER_PARAM_WORKER_SIZE = "size";
  public static final String HEADER_PARAM_UTILISATION = "utilisation";

  private final BrokerConnectionFactory factory;
  private final List<RabbitMQWorkerObserver> observers = new ArrayList<>();
  private final AtomicBoolean tryStartingConsuming = new AtomicBoolean();
  private boolean isShutdown;
  private DefaultConsumer consumer;
  private Channel channel;
  private String queueName;

  /**
   * Constructor
   *
   * @param factory connection factory
   */
  public RabbitMQWorkerMonitor(final BrokerConnectionFactory factory) {
    this.factory = factory;
  }

  /**
   * Add the observer.
   *
   * @param observer observer to add
   */
  public void addObserver(final RabbitMQWorkerObserver observer) {
    synchronized (observers) {
      observers.add(observer);
    }
  }

  /**
   * Remove the observer.
   *
   * @param observer observer to remove
   */
  public void removeObserver(final RabbitMQWorkerObserver observer) {
    synchronized (observers) {
      observers.remove(observer);
    }
  }

  /**
   * Start the watcher.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    tryStartingConsuming.set(true);
    while (!isShutdown) {
      try {
        stopAndStartConsumer();
        LOG.debug("Successfully (re)started consumer RabbitMQWorkerMonitor");
        if (consumer.getChannel().isOpen()) {
          tryStartingConsuming.set(false);
          break;
        }
      } catch (final ShutdownSignalException | IOException e1) {
        LOG.warn("(Re)starting consumer RabbitMQWorkerMonitor failed, retrying", e1);
      }
    }
  }

  private void stopAndStartConsumer() throws IOException {
    synchronized (this) {
      if (consumer != null) {
        try {
          if (consumer.getChannel().isOpen()) {
            consumer.getChannel().basicCancel(queueName);
          }
        } catch (final AlreadyClosedException | IOException e) {
          LOG.debug("Exception while stopping consuming, ignoring.", e);
        }
      }
      final Connection c = factory.getConnection();
      channel = c.createChannel();
      channel.exchangeDeclare(AERIUS_EVENT_EXCHANGE, EXCHANGE_TYPE);
      queueName = channel.queueDeclare().getQueue();
      channel.queueBind(queueName, AERIUS_EVENT_EXCHANGE, "");
      consumer = new MonitorConsumer(channel);
      channel.basicConsume(queueName, true, consumer);
    }
  }

  private void handleDelivery(final AMQP.BasicProperties properties) {
    final Map<String, Object> headers = properties.getHeaders();
    final String workerQueueName = getParam(headers, HEADER_PARAM_QUEUE);
    final int workerSize = getParamInt(headers, HEADER_PARAM_WORKER_SIZE, -1);
    final int utilisationSize = getParamInt(headers, HEADER_PARAM_UTILISATION, -1);

    synchronized (observers) {
      observers.forEach(ro -> ro.updateWorkers(workerQueueName, workerSize, utilisationSize));
    }
  }

  /**
   * Returns the value in the header parameter or null.
   *
   * @param headers headers map
   * @param param param to return the value for
   * @return value of param or null if not present
   */
  private static String getParam(final Map<String, Object> headers, final String param) {
    final Object value = headers.get(param);

    return value == null ? null : value.toString();
  }

  /**
   * Returns the int value in the header parameter or the other value.
   *
   * @param headers headers map
   * @param param param to return the value for
   * @param other if param is not present this value is returned
   * @return int value of param or other if not present
   */
  private static int getParamInt(final Map<String, Object> headers, final String param, final int other) {
    final String value = getParam(headers, param);

    try {
      return value == null ? other : Integer.valueOf(value);
    } catch (final NumberFormatException e) {
      LOG.error("Error parsing param " + param + ", not an int value but: " + value, e);
      return other;
    }
  }

  /**
   * Shutdown the monitoring process.
   */
  public void shutdown() {
    isShutdown = true;
    try {
      channel.close();
    } catch (final IOException | TimeoutException e) {
      LOG.trace("Worker event monitor shutdown failed", e);
    }
  }

  private class MonitorConsumer extends DefaultConsumer {
    public MonitorConsumer(final Channel channel) {
      super(channel);
    }

    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body)
        throws IOException {
      RabbitMQWorkerMonitor.this.handleDelivery(properties);
    }

    @Override
    public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
      if (sig.isInitiatedByApplication()) {
        isShutdown = true;
        LOG.info("Worker event monitor {} was shut down by the application.", consumerTag);
      } else {
        LOG.debug("Worker event monitor {} was shut down.", consumerTag);
        // restart
        try {
          abort();
          if (!tryStartingConsuming.get()) {
            start();
            LOG.info("Restarted worker event monitor {}", consumerTag);
          }
        } catch (final IOException e) {
          LOG.debug("Worker event monitor restart failed", e);
        }
      }
    }

    private void abort() {
      try {
        channel.abort();
      } catch (final IOException e) {
        // Eat error when closing channel.
      }
    }
  }
}
