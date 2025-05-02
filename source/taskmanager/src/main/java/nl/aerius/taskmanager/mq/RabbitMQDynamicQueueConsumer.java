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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import nl.aerius.taskmanager.adaptor.DynamicQueueConsumer;
import nl.aerius.taskmanager.client.QueueConstants;
import nl.aerius.taskmanager.client.util.QueueHelper;
import nl.aerius.taskmanager.domain.RabbitMQQueueType;

/**
 * RabbitMQ consumer to listen to new dynamic queues being reported.
 */
public class RabbitMQDynamicQueueConsumer extends DefaultConsumer implements DynamicQueueConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQDynamicQueueConsumer.class);
  private static final String QUEUE_NAME = QueueConstants.TASKMANAGER_NEW_DYNAMIC_QUEUE;

  private final List<DynamicQueueAddedCallback> callbacks = new ArrayList<>();

  public RabbitMQDynamicQueueConsumer(final Channel channel) {
    super(channel);
  }

  @Override
  public void addDynamicQueueAddedCallback(final DynamicQueueAddedCallback callback) {
    callbacks.add(callback);
  }

  public void removeDynamicQueueAddedCallback(final DynamicQueueAddedCallback callback) {
    callbacks.remove(callback);
  }

  @Override
  public void startConsuming() throws IOException {
    LOG.debug("Starting consumer listening to new added queues send to queue: {}", QUEUE_NAME);
    final Channel taskChannel = getChannel();
    // ensure a durable channel exists
    taskChannel.queueDeclare(QUEUE_NAME, true, false, false, RabbitMQQueueUtil.queueDeclareArguments(true, RabbitMQQueueType.QUORUM));
    //ensure only one message gets delivered at a time.
    taskChannel.basicQos(1);
    taskChannel.basicConsume(QUEUE_NAME, false, QUEUE_NAME, this);
    LOG.debug("Consumer {} was started.", QUEUE_NAME);
  }

  @Override
  public void handleDelivery(final String consumerTag, final Envelope envelope,
      final BasicProperties properties, final byte[] body) throws IOException {
    try {
      final String queue = (String) QueueHelper.bytesToObject(body);

      callbacks.forEach(c -> c.onDynamicQueueAdded(queue));
      getChannel().basicAck(envelope.getDeliveryTag(), false);
    } catch (final RuntimeException | ClassNotFoundException e) {
      LOG.trace("Exception while handling 'new dynamic queue added' message.", e);
      if (getChannel().isOpen()) {
        getChannel().basicNack(envelope.getDeliveryTag(), false, true);
      }
    }
  }

  @Override
  public void stopConsuming() {
    LOG.debug("Stopping consumer {}.", QUEUE_NAME);

    try {
      if (getChannel().isOpen()) {
        getChannel().basicCancel(QUEUE_NAME);
      }
    } catch (final AlreadyClosedException | IOException e) {
      LOG.debug("Exception while stopping consuming, ignoring.", e);
    }
  }
}
