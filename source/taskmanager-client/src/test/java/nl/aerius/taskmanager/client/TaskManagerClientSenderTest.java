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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;

import nl.aerius.taskmanager.client.util.QueueHelper;
import nl.aerius.taskmanager.test.MockConnectionUtil;

/**
 * Test class for {@link TaskManagerClientSender}.
 */
@ExtendWith(MockitoExtension.class)
class TaskManagerClientSenderTest {

  private static final String NORMAL_TASK_ID = "SomeTaskId";
  private static final String TASK_QUEUE_NAME = "task";
  private TaskManagerClientSender taskManagerClient;
  private @Mock BrokerConnectionFactory factory;
  private Connection connection;

  @BeforeEach
  void setUp() {
    connection = MockConnectionUtil.mockConnection();
    lenient().doReturn(connection).when(factory).getConnection();
    lenient().doReturn(true).when(factory).isOpen();
    taskManagerClient = new TaskManagerClientSender(factory);
  }

  @AfterEach
  void tearDown() {
    taskManagerClient.close();
  }

  @Test
  void testSendTask() throws IOException, ClassNotFoundException {
    final ArgumentCaptor<BasicProperties> propertiesCaptor = ArgumentCaptor.forClass(BasicProperties.class);
    final ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
    final Serializable input = new MockTaskInput(1);
    final String messgeId = UUID.randomUUID().toString();

    assertSendTask("1", messgeId, propertiesCaptor, dataCaptor, input, 1);

    final String secondMessageId = UUID.randomUUID().toString();
    assertSendTask("1", secondMessageId, propertiesCaptor, dataCaptor, input, 2);
    assertNotSame(messgeId, secondMessageId, "Ensure the task ID's don't happen to be the same");
    assertTrue(taskManagerClient.isUsable(), "Taskmanagerclient should still be usable.");
  }

  private void assertSendTask(final String correlationId, final String messageId, final ArgumentCaptor<BasicProperties> propertiesCaptor,
      final ArgumentCaptor<byte[]> dataCaptor, final Serializable input, final int times) throws IOException, ClassNotFoundException {
    taskManagerClient.sendTask(input, correlationId, messageId, TASK_QUEUE_NAME);
    verify(connection.createChannel(), times(times)).basicPublish(eq(""), eq(TASK_QUEUE_NAME), propertiesCaptor.capture(),
        dataCaptor.capture());
    final MockTaskInput dataSent = (MockTaskInput) QueueHelper.bytesToObject(dataCaptor.getValue());
    assertEquals(input, dataSent, "Should sent the data as given to sender.");
    final BasicProperties properties = propertiesCaptor.getValue();
    assertEquals(correlationId, properties.getCorrelationId(), "Should have correct correlationId");
    assertEquals(messageId, properties.getMessageId(), "Should have correct messageId");
  }

  @Test
  void testSendTasks() throws IOException {
    taskManagerClient.sendTask(new MockTaskInput(1), NORMAL_TASK_ID, NORMAL_TASK_ID, TASK_QUEUE_NAME);
    assertTrue(taskManagerClient.isUsable(), "Taskmanagerclient should still be usable.");
  }

  @Test
  void testSendTasksWithNullId() throws IOException {
    taskManagerClient.sendTask(new MockTaskInput(1), null, NORMAL_TASK_ID, TASK_QUEUE_NAME);
    assertTrue(taskManagerClient.isUsable(), "Taskmanagerclient should still be usable.");
  }

  @Test
  void testTaskManagerClientWithoutConnectionConfiguration() {
    assertThrows(IllegalArgumentException.class, () -> taskManagerClient = new TaskManagerClientSender(null),
        "Expected IllegalArgumentException when configuration is null.");
  }

  @Test
  void testTaskManagerClientWithConnectionConfigurationBean() throws IOException, InterruptedException, ClassNotFoundException {
    testSendTask();
  }

  /**
   * Test method for {@link nl.aerius.taskmanager.client.TaskManagerClientSender#sendTask(Object, String, String)}.
   */
  @Test
  void testSendUnserializableTask() {
    assertThrows(NotSerializableException.class, () -> {
      //anonymous inner type isn't serializable (even if the type is Serializable).
      final Serializable input = new Serializable() {
        private static final long serialVersionUID = 7681080846084936169L;
      };
      taskManagerClient.sendTask(input, NORMAL_TASK_ID, NORMAL_TASK_ID, TASK_QUEUE_NAME);
    }, "Expected NotSerializableException for unserialisable object.");
  }

  /**
   * Test method for {@link TaskManagerClientSender#shutdown()}.
   */
  @Test
  void testExit() throws IOException, InterruptedException, ClassNotFoundException {
    testSendTask();
    taskManagerClient.close();
    assertFalse(taskManagerClient.isUsable(), "Taskmanagerclient shouldn't be usable anymore.");
  }

  /**
   * Test method for {@link TaskManagerClientSender#shutdown()}.
   */
  @Test
  void testSendTaskAfterExit() {
    assertThrows(IllegalStateException.class, () -> {
      taskManagerClient.close();
      testSendTask();
    }, "Expected IllegalStateException for sending task after sender was shutdown.");
  }

  /**
   * Test method for {@link TaskManagerClientSender#sendTask(Object, String, String)}.
   */
  @Test
  void testSendTaskToNullQueue() {
    assertThrows(IllegalArgumentException.class,
        () -> taskManagerClient.sendTask(new MockTaskInput(1), NORMAL_TASK_ID, NORMAL_TASK_ID, null),
        "Expected IllegalArgumentException when the queue passed is null.");
  }

  /**
   * Test method for {@link TaskManagerClientSender#sendTask(Object, String, String)}.
   */
  @Test
  void testSendNullObjectAsTask() {
    assertThrows(IllegalArgumentException.class, () -> taskManagerClient.sendTask(null, NORMAL_TASK_ID, NORMAL_TASK_ID, TASK_QUEUE_NAME),
        "Expected IllegalArgumentException when a null object is given to be send.");
  }

  static record MockTaskInput(int aNumber) implements Serializable {
  }
}
