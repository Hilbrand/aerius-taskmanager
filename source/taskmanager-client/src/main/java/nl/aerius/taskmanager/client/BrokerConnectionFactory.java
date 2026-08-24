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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;

/**
 * Factory to manager connection communication, RabbitMQ.
 */
public class BrokerConnectionFactory implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BrokerConnectionFactory.class);
  private static final int WAIT_BEFORE_RETRY_SECONDS = 5;

  private Connection connection;
  private final ExecutorService executorService;
  private final Lock lock = new ReentrantLock();
  private final ConnectionFactory factory = new ConnectionFactory();
  private ConnectionConfiguration connectionConfiguration;
  private final List<TaskManagerClientSender> taskManagerClients = new ArrayList<>();

  public BrokerConnectionFactory(final ExecutorService executorService) {
    this.executorService = executorService;
  }

  public BrokerConnectionFactory(final ExecutorService executorService, final ConnectionConfiguration connectionConfiguration) {
    this(executorService);
    this.connectionConfiguration = connectionConfiguration;
    if (connectionConfiguration == null) {
      throw new IllegalArgumentException("No arguments are allowed to be null.");
    }
    factory.setHost(connectionConfiguration.getBrokerHost());
    factory.setPort(connectionConfiguration.getBrokerPort());
    factory.setUsername(connectionConfiguration.getBrokerUsername());
    factory.setPassword(connectionConfiguration.getBrokerPassword());
    factory.setVirtualHost(connectionConfiguration.getBrokerVirtualHost());
    factory.setMaxInboundMessageBodySize(connectionConfiguration.getBrokerMaxInboundMessageBodySize());

    // Set automatic recovery to false - as of 4.0.0 this is true by default which will not mix well with our own connection retries.
    // In a future version we might want to use this rather than have our own implementation.
    // See https://www.rabbitmq.com/api-guide.html#recovery for more information.
    factory.setAutomaticRecoveryEnabled(false);
  }

  public ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  /**
   * Returns a connection. If the connection doesn't exists of is closed the connection is created.
   * @return connection An open RabbitMQ connection.
   */
  public Connection getConnection() {
    lock.lock();
    try {
      if (connection == null || !connection.isOpen()) {
        connection = openConnection();
      }
    } finally {
      lock.unlock();
    }
    return connection;
  }

  private Connection openConnection() {
    Connection localConnection = null;
    boolean retry = true;
    int retryTime = 0;

    while (retry) {
      retryTime += WAIT_BEFORE_RETRY_SECONDS;
      try {
        localConnection = createNewConnection();
        retry = false;
      } catch (final IOException | TimeoutException e) {
        LOG.error("Connecting to rabbitmq failed, retry in {} seconds. Cause: {}", retryTime, e.getMessage());
        delayRetry(retryTime);
      }
    }
    localConnection.addShutdownListener(cause -> {
      if (cause.isInitiatedByApplication()) {
        LOG.info("Connection was shut down (by application)");
      } else {
        LOG.warn("Connection has been shut down, trying to reconnect", cause);
      }
    });
    return localConnection;
  }

  private static void delayRetry(final int retryTime) {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(retryTime));
    } catch (final InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns true if there is a connection and the connection is open.
   * @return true if connection open
   */
  public boolean isOpen() {
    return connection != null && connection.isOpen();
  }

  /**
   * Registers a TaskManagerClient with this manager.
   * @param taskManagerClient the taskManagerCient to register
   */
  public void registerClient(final TaskManagerClientSender taskManagerClient) {
    taskManagerClients.add(taskManagerClient);
  }

  /**
   * Removes a TaskManagerClient from this manager.
   * @param taskManagerClient the taskManagerCient to remove
   */
  public void deRegisterClient(final TaskManagerClientSender taskManagerClient) {
    taskManagerClients.remove(taskManagerClient);
  }

  /**
   * Shuts down the broker connection. Call when application finishes.
   */
  @Override
  public void close() {
    shutdownTaskManagerClients();
    if (connection != null && connection.isOpen()) {
      try {
        connection.close();
      } catch (final IOException e) {
        LOG.error("BrokerConnectionFactory shutdown failed.", e);
      }
    }
  }

  /**
   * Shutdown all registered task manager clients.
   */
  private void shutdownTaskManagerClients() {
    for (int i = taskManagerClients.size() - 1; i >= 0; i--) {
      taskManagerClients.get(i).close();
    }
  }

  protected Connection createNewConnection() throws IOException, TimeoutException {
    return factory.newConnection(executorService);
  }
}
