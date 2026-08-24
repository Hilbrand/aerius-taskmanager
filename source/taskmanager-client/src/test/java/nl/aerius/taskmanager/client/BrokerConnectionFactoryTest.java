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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;

/**
 * Test for class {@link BrokerConnectionFactory}.
 */
class BrokerConnectionFactoryTest {

  private static ExecutorService executor;

  @BeforeAll
  static void setupClass() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterAll
  static void afterClass() {
    executor.shutdown();
  }

  @Test
  void testTaskManagerClientWithoutBrokerHost() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerHost(null);

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when no broker host is set.");
  }

  @Test
  void testTaskManagerClientWithEmptyBrokerHost() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerHost("");

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when broker host is empty.");
  }

  @Test
  void testTaskManagerClientWithoutBrokerUsername() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerUsername(null);

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when no broker username is set.");
  }

  @Test
  void testTaskManagerClientWithEmptyBrokerUsername() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerUsername("");

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when broker username is empty.");
  }

  @Test
  void testTaskManagerClientWithoutBrokerPassword() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerPassword(null);

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when no broker password is set.");
  }

  @Test
  void testTaskManagerClientWithEmptyBrokerPassword() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerPassword("");
    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when broker password is empty.");
  }

  @Test
  void testTaskManagerClientWithoutBrokerVirtualHost() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerVirtualHost(null);
    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when no broker virtual host is set.");
  }

  @Test
  void testTaskManagerClientWithEmptyBrokerVirtualHost() {
    final ConnectionConfiguration.Builder builder = getFullConnectionConfigurationBuilder().brokerVirtualHost("");

    assertThrows(IllegalArgumentException.class, () -> builder.build(), "Expected IllegalArgumentException when broker virtual host is empty.");
  }

  private ConnectionConfiguration.Builder getFullConnectionConfigurationBuilder() {
    final ConnectionConfiguration.Builder builder = ConnectionConfiguration.builder();
    builder.brokerHost("localhost");
    builder.brokerUsername("username");
    builder.brokerPassword("password");
    builder.brokerPort(1234);
    builder.brokerVirtualHost("/");
    return builder;
  }
}
