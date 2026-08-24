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
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import nl.aerius.taskmanager.client.util.QueueHelper;

/**
 * Client to be used to communicate with the taskmanager (more importantly, send tasks to taskmanager):
 */
public class TaskManagerClientSender implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerClientSender.class);

  private static final int DELIVERY_MODE_PERSISTENT = 2;

  private static final int CLIENT_START_RETRY_PERIOD = 5000;

  private final BrokerConnectionFactory factory;
  private boolean running = true;
  private final AtomicReference<Channel> channel = new AtomicReference<>();

  /**
   * Creates a {@link TaskManagerClientSender}.
   *
   * @param factory Broker connection factory
   */
  public TaskManagerClientSender(final BrokerConnectionFactory factory) {
    if (factory == null) {
      throw new IllegalArgumentException("BrokerConnectionFactory not allowed to be null.");
    }
    this.factory = factory;
    factory.registerClient(this);
  }

  /**
   * Sends the task directly to the worker or via the Task Manager with no additional metrics.
   *
   * @param data The input object which the worker needs to do the work.
   * @param correlationId The correlation ID to use for this task. Will be used when a result is received to
   *          tell the TaskResultCallback which task was the cause. Can be used to correlate a task in the callback.
   * @param messageId The message ID to use for this task. Will be used when a result is received to
   *          tell the TaskResultCallback which task was the cause. Should be unique.
   * @param queueName The name of the queue on which this task should be placed.
   * @throws IOException In case of errors communicating with queue.
   */
  public void sendTask(final Serializable data, final String correlationId, final String messageId, final String queueName) throws IOException {
    sendTask(data, correlationId, messageId, queueName, new TaskMetrics());
  }

  /**
   * Sends the task to the worker via the Task Manager with custom statistics.
   *
   * @param data The input object which the worker needs to do the work.
   * @param correlationId The correlation ID to use for this task. Will be used when a result is received to
   *          tell the TaskResultCallback which task was the cause. Can be used to correlate a task in the callback.
   * @param messageId The message ID to use for this task. Will be used when a result is received to
   *          tell the TaskResultCallback which task was the cause. Should be unique.
   * @param queueName The name of the queue on which this task should be placed.
   * @param taskMetrics Additional statistics to communicate about the task to the taskmanager
   * @throws IOException In case of errors communicating with queue.
   */
  public void sendTask(final Serializable data, final String correlationId, final String messageId, final String queueName,
      final TaskMetrics taskMetrics) throws IOException {
    if (!running) {
      throw new IllegalStateException("Attempt to use closed connection");
    }
    if (queueName == null || queueName.isEmpty()) {
      throw new IllegalArgumentException("Blank taskQueueName not allowed.");
    }
    if (data == null) {
      throw new IllegalArgumentException("input value of null not allowed.");
    }
    boolean done = false;
    while (running && !done) {
      try {
        // Create a channel to send the message over.
        ensureChannel(channel);
        final BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder();
        prepareBeforeSend(propertiesBuilder);
        propertiesBuilder.correlationId(correlationId);
        propertiesBuilder.messageId(messageId);
        propertiesBuilder.deliveryMode(DELIVERY_MODE_PERSISTENT);
        propertiesBuilder.headers(taskMetrics.queueName(queueName).build());

        final Channel chn = channel.get();
        if (chn != null) {
          chn.basicPublish("", queueName, propertiesBuilder.build(), QueueHelper.objectToBytes(data));
        }
        // task has been send successfully, return.
        done = true;
      } catch (final ConnectException e) {
        // don't catch all IOExceptions, just ConnectExceptions.
        // exceptions like wrong host-name should cause a bigger disturbance.
        // those indicate that connection has temporarily been lost with RabbitMQ, though could be not that temporarily...
        LOG.error("Sending task (id: {}) failed, retrying in a bit.", correlationId, e);
        try {
          Thread.sleep(CLIENT_START_RETRY_PERIOD);
        } catch (final InterruptedException e1) {
          // no need to log.
          Thread.currentThread().interrupt();
        }
        LOG.info("Retrying to send task (id: {})", correlationId);
      }
    }
  }

  /**
   * Method to enrich the builder with additional data before sending the task.
   *
   * @param builder RabbitMQ builder to enrich properties with additional information
   * @throws IOException
   */
  protected void prepareBeforeSend(final Builder builder) throws IOException {
    // Default implementation does nothing.
  }

  /**
   * Ensures there is an open channel in the reference object (if running), and returns if the channel was opened.
   *
   * @param channelReference channel to check
   * @return true if the channel was opened
   * @throws IOException
   */
  protected boolean ensureChannel(final AtomicReference<Channel> channelReference) throws IOException {
    synchronized (this) {
      final Channel chn = channelReference.get();
      if (running && (chn == null || !chn.isOpen())) {
        channelReference.set(getConnection().createChannel());
        return true;
      }
      return false;
    }
  }

  private Connection getConnection() {
    return factory.getConnection();
  }

  /**
   * Exit this client, making it impossible to send new tasks or retrieve any more results.
   * Should be used by a TaskResultCallback when all the results are in (to free up resources).
   */
  @Override
  public void close() {
    synchronized (this) {
      running = false;
      if (factory != null) {
        factory.deRegisterClient(this);
      }
      closeChannel(channel.get());
    }
  }

  protected void closeChannel(final Channel channelToClose) {
    if (channelToClose != null && channelToClose.isOpen()) {
      try {
        channelToClose.close();
      } catch (IOException | TimeoutException e) {
        LOG.debug("Could not close channel", e);
      }
    }
  }

  /**
   * @return True if tasks can be send through this client.
   * @throws IOException message bus exception
   */
  boolean isUsable() throws IOException {
    return running && factory.isOpen();
  }
}
