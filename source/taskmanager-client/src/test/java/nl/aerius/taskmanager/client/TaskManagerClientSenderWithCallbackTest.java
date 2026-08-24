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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Test class for {@link TaskManagerClientSenderWithCallback}.
 */
@ExtendWith(MockitoExtension.class)
class TaskManagerClientSenderWithCallbackTest {

  private final List<Channel> channels = new ArrayList<>();
  private @Mock BrokerConnectionFactory factory;
  private @Mock MockTaskResultHandler mockTaskResultHandler;

  @BeforeEach
  void setUp() throws IOException {
    final Connection connection = mock(Connection.class);
    doAnswer(a -> {
      final Channel channel = mock(Channel.class);
      channels.add(channel);
      lenient().doReturn(true).when(channel).isOpen();
      return channel;
    }).when(connection).createChannel();
    lenient().doReturn(connection).when(factory).getConnection();
    lenient().doReturn(true).when(factory).isOpen();
  }

  /**
   * Test method for {@link TaskManagerClientSender#sendTask(Object, WorkerQueueType, String)}.
   */
  @Test
  void testSendTask() throws IOException {
    final ArgumentCaptor<BasicProperties> propertiesCaptor = ArgumentCaptor.forClass(BasicProperties.class);
    final TaskResultCallback callback = mock(TaskResultCallback.class);
    final TaskManagerClientSenderWithCallback taskManagerClient = new TaskManagerClientSenderWithCallback(factory, "queue", callback);

    taskManagerClient.sendTask("Test", "1", "2", "the_queue");
    taskManagerClient.close();
    verify(channels.get(0)).basicPublish(eq(""), eq("the_queue"), propertiesCaptor.capture(), any());
    verify(channels.get(1)).basicConsume(eq("queue.reply"), eq(true), any());
    assertEquals("queue.reply", propertiesCaptor.getValue().getReplyTo(), "Should have reply queue");
  }

}
