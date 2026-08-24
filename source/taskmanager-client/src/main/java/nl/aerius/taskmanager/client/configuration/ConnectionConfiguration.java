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
package nl.aerius.taskmanager.client.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Configuration object for different (queue) properties.
 */
public final class ConnectionConfiguration {

  /**
   * RabbitMQ default port.
   */
  private static final int DEFAULT_BROKER_PORT = 5672;

  /**
   * RabbitMQ default virtualHost (root).
   */
  private static final String DEFAULT_BROKER_VIRTUAL_HOST = "/";

  /**
   * RabbitMQ default management port.
   */
  private static final int DEFAULT_BROKER_MANAGEMENT_PORT = 15672;

  /**
   * Default refresh time in seconds.
   */
  private static final int DEFAULT_MANAGEMENT_REFRESH_RATE = 60; //seconds

  /**
   * Default wait time before retrying to connect.
   */
  private static final int DEFAULT_RETRY_WAIT_TIME = 60; //seconds

  /**
   * Default maximum size of inbound messages set to 134_217_728 bytes.
   */
  private static final int DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE = 1024 * 1024 * 128;

  private final String brokerHost;

  private final int brokerPort;

  private final String brokerUsername;

  private final String brokerPassword;

  private final String brokerVirtualHost;

  private final int brokerManagementPort;

  private final int brokerManagementRefreshRate;

  private final int brokerRetryWaitTime;

  private final int brokerMaxInboundMessageBodySize;

  private ConnectionConfiguration(
      final String brokerHost,
      final int brokerPort,
      final String brokerUsername,
      final String brokerPassword,
      final String brokerVirtualHost,
      final int brokerManagementPort,
      final int brokerManagementRefreshRate,
      final int brokerRetryWaitTime,
      final int brokerMaxInboundMessageBodySize) {
    this.brokerHost = brokerHost;
    this.brokerPort = brokerPort;
    this.brokerUsername = brokerUsername;
    this.brokerPassword = brokerPassword;
    this.brokerVirtualHost = brokerVirtualHost;
    this.brokerManagementPort = brokerManagementPort;
    this.brokerManagementRefreshRate = brokerManagementRefreshRate;
    this.brokerRetryWaitTime = brokerRetryWaitTime;
    this.brokerMaxInboundMessageBodySize = brokerMaxInboundMessageBodySize;
  }

  public static Builder builder() {
    return new ConnectionConfiguration.Builder()
        .brokerPort(DEFAULT_BROKER_PORT)
        .brokerManagementPort(DEFAULT_BROKER_MANAGEMENT_PORT)
        .brokerVirtualHost(DEFAULT_BROKER_VIRTUAL_HOST)
        .brokerManagementRefreshRate(DEFAULT_MANAGEMENT_REFRESH_RATE)
        .brokerRetryWaitTime(DEFAULT_RETRY_WAIT_TIME)
        .brokerMaxInboundMessageBodySize(DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE);
  }

  /**
   * @return The host used to communicate with the broker
   */
  public String getBrokerHost() {
    return brokerHost;
  }

  /**
   * @return The port used to communicate with the broker (rabbitMQ default: 5672)
   */
  public int getBrokerPort() {
    return brokerPort;
  }

  /**
   * @return The username to be used while communicating with the broker
   */
  public String getBrokerUsername() {
    return brokerUsername;
  }

  /**
   * @return The password to be used while communicating with the broker
   */
  public String getBrokerPassword() {
    return brokerPassword;
  }

  /**
   * @return The virtual host to be used on the broker (rabbitMQ default: /)
   */
  public String getBrokerVirtualHost() {
    return brokerVirtualHost;
  }

  /**
   * @return The port used to communicate with the management interface of the broker (rabbitMQ default: 15672)
   */
  public int getBrokerManagementPort() {
    return brokerManagementPort;
  }

  /**
   * @return The refresh rate in seconds the RabbitMQ management api is queried for status changes.
   */
  public int getBrokerManagementRefreshRate() {
    return brokerManagementRefreshRate;
  }

  /**
   * @return The wait time in seconds before retrying to connect with the broker.
   */
  public int getBrokerRetryWaitTime() {
    return brokerRetryWaitTime;
  }

  /**
   * @return The maximum size of inbound messages.
   */
  public int getBrokerMaxInboundMessageBodySize() {
    return brokerMaxInboundMessageBodySize;
  }

  @Override
  public String toString() {
    return "ConnectionConfiguration{"
        + "brokerHost=" + brokerHost + ", "
        + "brokerPort=" + brokerPort + ", "
        + "brokerUsername=" + brokerUsername + ", "
        + "brokerPassword=" + brokerPassword + ", "
        + "brokerVirtualHost=" + brokerVirtualHost + ", "
        + "brokerManagementPort=" + brokerManagementPort + ", "
        + "brokerManagementRefreshRate=" + brokerManagementRefreshRate + ", "
        + "brokerRetryWaitTime=" + brokerRetryWaitTime + ", "
        + "brokerMaxInboundMessageBodySize=" + brokerMaxInboundMessageBodySize
        + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ConnectionConfiguration that = (ConnectionConfiguration) obj;
    return this.brokerHost.equals(that.getBrokerHost())
        && this.brokerPort == that.getBrokerPort()
        && this.brokerUsername.equals(that.getBrokerUsername())
        && this.brokerPassword.equals(that.getBrokerPassword())
        && this.brokerVirtualHost.equals(that.getBrokerVirtualHost())
        && this.brokerManagementPort == that.getBrokerManagementPort()
        && this.brokerManagementRefreshRate == that.getBrokerManagementRefreshRate()
        && this.brokerRetryWaitTime == that.getBrokerRetryWaitTime()
        && this.brokerMaxInboundMessageBodySize == this.getBrokerMaxInboundMessageBodySize();
  }

  @Override
  public int hashCode() {
    return Objects.hash(brokerHost.hashCode(), brokerPort, brokerUsername.hashCode(), brokerPassword.hashCode(), brokerVirtualHost.hashCode(),
        brokerManagementPort, brokerManagementRefreshRate, brokerRetryWaitTime, brokerMaxInboundMessageBodySize);
  }

  public static final class Builder {
    private String brokerHost;
    private Integer brokerPort;
    private String brokerUsername;
    private String brokerPassword;
    private String brokerVirtualHost;
    private Integer brokerManagementPort;
    private Integer brokerManagementRefreshRate;
    private Integer brokerRetryWaitTime;
    private Integer brokerMaxInboundMessageBodySize;

    Builder() {}

    public ConnectionConfiguration build() {
      final ConnectionConfiguration connectionConfiguration = autoBuild();

      checkBlank("Broker Host", connectionConfiguration.getBrokerHost());
      checkBlank("Broker Username", connectionConfiguration.getBrokerUsername());
      checkBlank("Broker Password", connectionConfiguration.getBrokerPassword());
      checkBlank("Broker Virtual Host", connectionConfiguration.getBrokerVirtualHost());
      return connectionConfiguration;
    }

    private static void checkBlank(final String name, final String value) {
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException(name + " not allowed to be null or empty.");
      }
    }

    public ConnectionConfiguration.Builder brokerHost(final String brokerHost) {
      this.brokerHost = brokerHost;
      return this;
    }

    public ConnectionConfiguration.Builder brokerPort(final int brokerPort) {
      this.brokerPort = brokerPort;
      return this;
    }

    public ConnectionConfiguration.Builder brokerUsername(final String brokerUsername) {
      this.brokerUsername = brokerUsername;
      return this;
    }

    public ConnectionConfiguration.Builder brokerPassword(final String brokerPassword) {
      this.brokerPassword = brokerPassword;
      return this;
    }

    public ConnectionConfiguration.Builder brokerVirtualHost(final String brokerVirtualHost) {
      this.brokerVirtualHost = brokerVirtualHost;
      return this;
    }

    public ConnectionConfiguration.Builder brokerManagementPort(final int brokerManagementPort) {
      this.brokerManagementPort = brokerManagementPort;
      return this;
    }

    public ConnectionConfiguration.Builder brokerManagementRefreshRate(final int brokerManagementRefreshRate) {
      this.brokerManagementRefreshRate = brokerManagementRefreshRate;
      return this;
    }

    public ConnectionConfiguration.Builder brokerRetryWaitTime(final int brokerRetryWaitTime) {
      this.brokerRetryWaitTime = brokerRetryWaitTime;
      return this;
    }

    public Builder brokerMaxInboundMessageBodySize(final int brokerMaxInboundMessageBodySize) {
      this.brokerMaxInboundMessageBodySize = brokerMaxInboundMessageBodySize;
      return this;
    }

    ConnectionConfiguration autoBuild() {
      final List<String> missings = new ArrayList<>();

      addMissing(missings, this.brokerHost, "brokerHost");
      addMissing(missings, this.brokerPort, "brokerPort");
      addMissing(missings, this.brokerUsername, "brokerUsername");
      addMissing(missings, this.brokerPassword, "brokerPassword");
      addMissing(missings, this.brokerVirtualHost, "brokerVirtualHost");
      addMissing(missings, this.brokerManagementPort, "brokerManagementPort");
      addMissing(missings, this.brokerManagementRefreshRate, "brokerManagementRefreshRate");
      addMissing(missings, this.brokerRetryWaitTime, "brokerRetryWaitTime");
      addMissing(missings, this.brokerMaxInboundMessageBodySize, "brokerMaxInboundMessageBodySize");
      if (!missings.isEmpty()) {
        throw new IllegalArgumentException("Missing required properties: " + String.join(", ", missings));
      }
      return new ConnectionConfiguration(
          this.brokerHost,
          this.brokerPort,
          this.brokerUsername,
          this.brokerPassword,
          this.brokerVirtualHost,
          this.brokerManagementPort,
          this.brokerManagementRefreshRate,
          this.brokerRetryWaitTime,
          this.brokerMaxInboundMessageBodySize);
    }

    private final void addMissing(final List<String> missings, final Object value, final String text) {
      if (value == null) {
        missings.add(text);
      }
    }
  }
}
