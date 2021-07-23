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
package nl.aerius.taskmanager.mq;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Implementation of RabbitMQ's DefaultConsumer for the taskmanager.
 *
 * When the channel is broken (due to the connection being lost for instance), this consumer will stop working.
 */
class RabbitMQMessageConsumer extends DefaultConsumer {

  interface ConsumerCallback {

    void onMessageReceived(RabbitMQMessage message);
  }

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQMessageConsumer.class);

  private final String queueName;
  private final ConsumerCallback callback;

  RabbitMQMessageConsumer(final Channel channel, final String queueName, final ConsumerCallback callback) {
    super(channel);
    this.queueName = queueName;
    this.callback = callback;
  }

  public void startConsuming() throws IOException {
    LOG.debug("Starting consumer {}.", queueName);
    final Channel taskChannel = getChannel();
    // ensure a durable channel exists
    taskChannel.queueDeclare(queueName, true, false, false, null);
    //ensure only one message gets delivered at a time.
    taskChannel.basicQos(1);

    taskChannel.basicConsume(queueName, false, queueName, this);
    LOG.debug("Consumer {} was started.", queueName);
  }

  public void stopConsuming() {
    LOG.debug("Stopping consumer {}.", queueName);

    try {
      if (getChannel().isOpen()) {
        getChannel().basicCancel(queueName);
      }
    } catch (final AlreadyClosedException | IOException e) {
      LOG.debug("Exception while stopping consuming, ignoring.", e);
    }
  }

  @Override
  public void handleDelivery(final String consumerTag, final Envelope envelope,
      final BasicProperties properties, final byte[] body) throws IOException {
    Thread.currentThread().setName("RabbitMQMessageConsumer " + queueName);
    // take care of cases where original client did not supply messageId by making one ourselves.
    BasicProperties actualProperties = properties;
    if (properties.getMessageId() == null) {
      actualProperties = properties.builder().messageId(UUID.randomUUID().toString()).build();
    }
    final RabbitMQMessage message = new RabbitMQMessage(queueName, getChannel(), envelope.getDeliveryTag(), actualProperties, body);
    // the queueName as we use it is actually the routingKey used by AMQP.
    try {
      callback.onMessageReceived(message);
    } catch (final RuntimeException e) {
      LOG.trace("Exception while handling message", e);
      nack(message.getMetaData());
    }
  }

  public void ack(final RabbitMQMessageMetaData message) throws IOException {
    getChannel().basicAck(message.getDeliveryTag(), false);
  }

  public void nack(final RabbitMQMessageMetaData message) throws IOException {
    final Channel channel = getChannel();
    if (channel.isOpen()) {
      channel.basicNack(message.getDeliveryTag(), false, true);
    }
  }
}
