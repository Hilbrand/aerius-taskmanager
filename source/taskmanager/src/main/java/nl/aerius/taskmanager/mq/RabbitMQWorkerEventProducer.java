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

import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.AERIUS_EVENT_EXCHANGE;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.EXCHANGE_TYPE;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_QUEUE;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_UTILISATION;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_WORKER_SIZE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerMetrics;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;

/**
 * Publishes on a regular basis worker metrics events to a dedicated RabbitMQ exchange.
 * Consumers can listen to this exchange to retrieve updates.
 *
 * It uses caching to only send these events in case the value has changed.
 */
public class RabbitMQWorkerEventProducer {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQWorkerEventProducer.class);

  private static final int REFRESH_TIME_SECONDS = 5;

  private final BrokerConnectionFactory factory;
  private final Map<String, WorkerMetrics> metrics = new HashMap<>();
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  // maps to keep track of last know values to only log differences.
  private final Map<String, Integer> logCacheSize = new HashMap<>();
  private final Map<String, Integer> logCacheUtilisation = new HashMap<>();

  /**
   * Constructor.
   *
   * @param executor scheduled executor to run the repeating task on
   * @param factory connection factory
   */
  public RabbitMQWorkerEventProducer(final ScheduledExecutorService executor, final BrokerConnectionFactory factory) {
    this.executor = executor;
    this.factory = factory;
  }

  /**
   * Add metrics provider for the given worker queue name.
   *
   * @param workerQueueName name of the worker queue the metrics are
   * @param workerMetrics provider of the metrics
   */
  public void addMetrics(final String workerQueueName, final WorkerMetrics workerMetrics) {
    metrics.put(workerQueueName, workerMetrics);
  }

  /**
   * Remove metrics provider for the given worker queue name.
   *
   * @param workerQueueName worker queue name to remove
   */
  public void removeMetrics(final String workerQueueName) {
    metrics.remove(workerQueueName);
  }

  /**
   * Start a scheduled thread to regularly publish new worker metrics.
   */
  public void start() {
    future = executor.scheduleWithFixedDelay(this::updateMetrics, 0, REFRESH_TIME_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Stops publishing metrics.
   */
  public void shutdown() {
    if (future != null) {
      future.cancel(true);
    }
  }

  private void updateMetrics() {
    try {
      metrics.forEach((q, wpm) -> {
        final int size = wpm.getReportedWorkerSize();
        final int utilisation = wpm.getNumberOfUsedWorkers();

        try {
          publish(q, size, utilisation);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        } catch (final TimeoutException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (final RuntimeException e) {
      LOG.error("Trying to send worker metrics update event failed: ", e);
    }
  }

  private void publish(final String queueName, final int size, final int utilisation) throws IOException, TimeoutException {
    try (final Channel channel = factory.getConnection().createChannel()) {
      channel.exchangeDeclare(AERIUS_EVENT_EXCHANGE, EXCHANGE_TYPE);

      final Map<String, Object> headers = new HashMap<>();
      headers.put(HEADER_PARAM_QUEUE, queueName);
      headers.put(HEADER_PARAM_WORKER_SIZE, size);
      headers.put(HEADER_PARAM_UTILISATION, utilisation);
      final BasicProperties props = new BasicProperties().builder().headers(headers).build();
      channel.basicPublish(AERIUS_EVENT_EXCHANGE, "", props, null);
      debugLogState(queueName, size, utilisation);
    }
  }

  private void debugLogState(final String queueName, final int size, final int utilisation) {
    if (LOG.isDebugEnabled()) {
      final Integer previousSize = Optional.ofNullable(logCacheSize.put(queueName, size)).orElse(0);
      final Integer previousUtilisation = Optional.ofNullable(logCacheUtilisation.put(queueName, utilisation)).orElse(0);

      if (utilisation != previousUtilisation || size != previousSize) {
        LOG.debug("Publish event for queue {} - size: {}, utilisation: {}", queueName, size, utilisation);
      }
    }
  }
}
