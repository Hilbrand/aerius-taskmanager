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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.adaptor.TaskMessageHandler;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.client.WorkerResultSender;
import nl.aerius.taskmanager.domain.MessageReceivedHandler;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.mq.RabbitMQMessageConsumer.ConsumerCallback;

/**
 * RabbitMQ implementation of a {@link TaskMessageHandler}. RabbitMQ will starts listening to the queue and when messages are received they are send
 * to the {@link MessageReceivedHandler}.
 */
class RabbitMQMessageHandler implements TaskMessageHandler<RabbitMQMessage>, ConsumerCallback {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQMessageHandler.class);

  private static final int DEFAULT_RETRY_SECONDS = 10;

  private final BrokerConnectionFactory factory;
  private final QueueConfig queueConfig;
  private final String taskQueueName;

  private MessageReceivedHandler messageReceivedHandler;
  private RabbitMQMessageConsumer consumer;
  private boolean isShutdown;
  private boolean warned;

  /**
   * Set a boolean that is set as long as we're trying to (re)connect to RabbitMQ.
   */
  private final AtomicBoolean tryConnecting = new AtomicBoolean();

  private final AtomicBoolean tryStartingConsuming = new AtomicBoolean();

  /**
   * Time to wait before retrying connection.
   */
  private long retryTimeMilliseconds = TimeUnit.SECONDS.toMillis(DEFAULT_RETRY_SECONDS);

  /**
   * Constructor.
   *
   * @param factory the factory to get the a RabbitMQ connection from
   * @param queueConfig the configuration parameters of the queue
   */
  public RabbitMQMessageHandler(final BrokerConnectionFactory factory, final QueueConfig queueConfig) {
    this.factory = factory;
    this.queueConfig = queueConfig;
    this.taskQueueName = queueConfig.queueName();
  }

  @Override
  public void addMessageReceivedHandler(final MessageReceivedHandler messageReceivedHandler) {
    this.messageReceivedHandler = messageReceivedHandler;
  }

  @Override
  public void start() throws IOException {
    tryStartingConsuming.set(true);
    while (!isShutdown) {
      try {
        if (stopAndStartConsumer()) {
          LOG.info("Successfully (re)started consumer for {}", taskQueueName);
          if (consumer.getChannel().isOpen()) {
            tryStartingConsuming.set(false);
            return;
          }
        }
      } catch (final ShutdownSignalException | IOException e1) {
        if (!warned) {
          LOG.warn("(Re)starting consumer for {} failed, retrying in a while", taskQueueName, e1);
          warned = true;
        }
      }
      if (!isShutdown) {
        delayRetry();
      }
    }
  }

  @Override
  public void shutDown() throws IOException {
    isShutdown = true;
    if (consumer != null) {
      consumer.stopConsuming();
    }
  }

  @Override
  public void messageDeliveredToWorker(final RabbitMQMessage message) throws IOException {
    consumer.ack(message);
  }

  @Override
  public void messageDeliveryToWorkerFailed(final RabbitMQMessage message) throws IOException {
    consumer.nack(message);
  }

  @Override
  public void messageDeliveryAborted(final RabbitMQMessage message, final RuntimeException exception) throws IOException {
    final WorkerResultSender sender = new WorkerResultSender(factory.getConnection().createChannel(), message.getProperties());
    sender.sendIntermediateResult(exception);
    messageDeliveredToWorker(message);
  }

  @Override
  public void onMessageReceived(final RabbitMQMessage message) {
    if (messageReceivedHandler != null) {
      messageReceivedHandler.onMessageReceived(message);
    }
  }

  public void setRetryTimeMilliseconds(final long retryTimeMilliseconds) {
    this.retryTimeMilliseconds = retryTimeMilliseconds;
  }

  private void delayRetry() {
    try {
      Thread.sleep(retryTimeMilliseconds);
    } catch (final InterruptedException ex) {
      LOG.debug("Waiting interrupted", ex);
      Thread.currentThread().interrupt();
    }
  }

  private boolean stopAndStartConsumer() throws IOException {
    synchronized (this) {
      if (consumer != null) {
        consumer.stopConsuming();
      }
      final Optional<Channel> channel = factory.getConnection().openChannel();

      if (channel.isPresent()) {
        consumer = new RabbitMQMessageConsumer(channel.get(), queueConfig, this);
        consumer.getChannel().addShutdownListener(this::handleShutdownSignal);
        consumer.startConsuming();
        tryConnecting.set(false);
        warned = false;
        return true;
      } else {
        return false;
      }
    }
  }

  private void handleShutdownSignal(final ShutdownSignalException sse) {
    if (sse != null && sse.isInitiatedByApplication()) {
      return;
    }
    if (!tryStartingConsuming.get() && tryConnecting.compareAndSet(false, true) && messageReceivedHandler != null) {
      messageReceivedHandler.handleShutdownSignal();
    }
  }
}
