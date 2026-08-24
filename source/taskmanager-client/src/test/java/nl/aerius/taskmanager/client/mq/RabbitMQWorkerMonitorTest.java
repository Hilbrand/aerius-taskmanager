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

import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_QUEUE;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_UTILISATION;
import static nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.HEADER_PARAM_WORKER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.client.mq.RabbitMQWorkerMonitor.RabbitMQWorkerObserver;

/**
 * Test class for {@link RabbitMQWorkerMonitor}.
 */
class RabbitMQWorkerMonitorTest {

  private static final String TEST_QUEUENAME = "test";

  private static ExecutorService executor;

  private RabbitMQWorkerMonitor monitor;
  private Channel mockChannel;

  @BeforeAll
  static void setupClass() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterAll
  static void afterClass() {
    executor.shutdown();
  }

  @BeforeEach
  void setUp() throws Exception {
    final Connection mockConnection = Mockito.mock(Connection.class);
    mockChannel = Mockito.mock(Channel.class);
    doReturn(mockChannel).when(mockConnection).createChannel();
    when(mockChannel.isOpen()).thenReturn(true);
    final Queue.DeclareOk mockDeclareOk = Mockito.mock(Queue.DeclareOk.class);
    doReturn(mockDeclareOk).when(mockChannel).queueDeclare();
    monitor = new RabbitMQWorkerMonitor(new BrokerConnectionFactory(executor) {
      @Override
      protected Connection createNewConnection() throws IOException {
        return mockConnection;
      }
    });
  }

  @Test
  void testReceiving() throws IOException {
    final AtomicInteger sizeAI = new AtomicInteger();
    final AtomicInteger utilisationAI = new AtomicInteger();
    final RabbitMQWorkerObserver observer = (workerQueueName, size, utilisation) -> {
      sizeAI.set(size);
      utilisationAI.set(utilisation);
    };
    monitor.addObserver(observer);
    doAnswer(i -> {
      final Map<String, Object> headers = new HashMap<>();

      headers.put(HEADER_PARAM_QUEUE, TEST_QUEUENAME);
      headers.put(HEADER_PARAM_WORKER_SIZE, 10);
      headers.put(HEADER_PARAM_UTILISATION, 5);
      ((Consumer) i.getArgument(2)).handleDelivery(null, null, new BasicProperties().builder().headers(headers).build(), null);
      return null;
    }).when(mockChannel).basicConsume(any(), eq(true), any());
    monitor.start();
    assertEquals(10, sizeAI.get(), "Expected received size doesn't match");
    assertEquals(5, utilisationAI.get(), "Expected utilisation doesn't match");
    monitor.removeObserver(observer);
  }
}
