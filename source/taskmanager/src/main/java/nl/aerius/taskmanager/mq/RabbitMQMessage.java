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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import nl.aerius.taskmanager.domain.Message;

/**
 * RabbitMQ Message object.
 */
public class RabbitMQMessage extends Message {
  private final Channel channel;
  private final BasicProperties properties;
  private final byte[] body;
  private final long deliveryTag;

  /**
   * Constructor
   *
   * @param queueName Name of the queue the message is from
   * @param channel RabbitMQ the mssage is from
   * @param deliveryTag RabbitMQ tag used to ack or nack a message to the RabbitMQ queue
   * @param properties Basicproperties from the original message.
   * @param body The body of the original message.
   */
  public RabbitMQMessage(final String queueName, final Channel channel, final long deliveryTag, final BasicProperties properties, final byte[] body) {
    this.deliveryTag = deliveryTag;
    this.channel = channel;
    this.properties = properties;
    this.body = body == null ? new byte[0] : body.clone();
  }

  public long getDeliveryTag() {
    return deliveryTag;
  }

  @Override
  public String getCorrelationId() {
    return properties.getCorrelationId();
  }

  @Override
  public String getMessageId() {
    return properties.getMessageId();
  }

  public BasicProperties getProperties() {
    return properties;
  }

  public byte[] getBody() {
    return body.clone();
  }

  public Channel getChannel() {
    return channel;
  }
}
