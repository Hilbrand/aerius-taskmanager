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

import static org.mockito.Mockito.lenient;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;
import nl.aerius.taskmanager.test.MockedChannelFactory;

/**
 * Abstract base class for RabbitMQ tests.
 */
class AbstractRabbitMQTest {

  protected static ScheduledExecutorService executor;
  protected BrokerConnectionFactory factory;
  protected Channel mockChannel;
  protected AdaptorFactory adapterFactory;
  protected int brokerManagementRefreshRate;

  @BeforeAll
  static void setupClass() {
    executor = Executors.newScheduledThreadPool(5);
  }

  @AfterAll
  static void afterClass() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(10, TimeUnit.MILLISECONDS);
  }

  @BeforeEach
  void setUp() throws Exception {
    final Connection mockConnection = Mockito.mock(Connection.class);
    final ConnectionConfiguration configuration = ConnectionConfiguration.builder()
        .brokerHost("localhost").brokerUsername("guest").brokerPassword("guest").brokerManagementRefreshRate(brokerManagementRefreshRate).build();
    mockChannel = MockedChannelFactory.create();

    lenient().doReturn(mockChannel).when(mockConnection).createChannel();
    lenient().doReturn(Optional.of(mockChannel)).when(mockConnection).openChannel();
    factory = new BrokerConnectionFactory(executor, configuration) {
      @Override
      protected Connection createNewConnection() {
        return mockConnection;
      }
    };
    adapterFactory = new RabbitMQAdaptorFactory(executor, factory);
  }

  protected class DataDock {
    private byte[] data;

    public byte[] getData() {
      return data;
    }

    public void setData(final byte[] data) {
      this.data = data;
    }
  }
}
