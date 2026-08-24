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
package nl.aerius.taskmanager;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.UUID;

import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskConsumer;
import nl.aerius.taskmanager.mq.RabbitMQMessage;

/**
 * Mock implementation of {@link Task}.
 */
public class MockTask extends Task {

  public MockTask(final TaskConsumer taskConsumer) {
    this(taskConsumer, UUID.randomUUID().toString());
  }

  public MockTask(final TaskConsumer taskConsumer, final String messageId) {
    super(taskConsumer);
    final RabbitMQMessage message = mock(RabbitMQMessage.class);

    doReturn(messageId).when(message).getMessageId();
    doReturn(taskConsumer.getQueueName()).when(message).getCorrelationId();
    setData(message);
  }
}
