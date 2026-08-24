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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import com.rabbitmq.client.Envelope;

import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;

/**
 * Test class for {@link RabbitMQChannelQueueEventsWatcher}.
 */
class RabbitMQChannelQueueEventsWatcherTest {
  private static final String TEST_QUEUENAME = "test";
  private static final String HEADER_PARAM_QUEUE = "queue";

  private static ExecutorService executor;

  private RabbitMQChannelQueueEventsWatcher watcher;
  private Channel mockChannel;

  private WorkerSizeProviderProxy proxy;

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
    final Queue.DeclareOk mockDeclareOk = Mockito.mock(Queue.DeclareOk.class);
    doReturn(mockDeclareOk).when(mockChannel).queueDeclare();
    proxy = Mockito.mock(WorkerSizeProviderProxy.class);
    watcher = new RabbitMQChannelQueueEventsWatcher(new BrokerConnectionFactory(executor) {
      @Override
      protected Connection createNewConnection() throws IOException {
        return mockConnection;
      }
    }, proxy);
  }

  @Test
  void testReceiving() throws IOException {
    assertDeltaCheck("consumer.created");
    verify(proxy).triggerWorkerQueueState(TEST_QUEUENAME);
    assertDeltaCheck("consumer.removed");
    verify(proxy, times(2)).triggerWorkerQueueState(TEST_QUEUENAME);
  }

  private void assertDeltaCheck(final String event) throws IOException {
    doAnswer(i -> {
      final Envelope envelope = Mockito.mock(Envelope.class);
      doReturn(event).when(envelope).getRoutingKey();
      final Map<String, Object> headers = new HashMap<>();

      headers.put(HEADER_PARAM_QUEUE, TEST_QUEUENAME);
      ((Consumer) i.getArgument(2)).handleDelivery(null, envelope, new BasicProperties().builder().headers(headers).build(), null);
      return null;
    }).when(mockChannel).basicConsume(any(), eq(true), any());
    watcher.start();
  }
}
