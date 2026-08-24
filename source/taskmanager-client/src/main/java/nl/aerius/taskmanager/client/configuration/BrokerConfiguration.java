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
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker queue configuration implementation.
 */
public class BrokerConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(BrokerConfiguration.class);

  private static final String BROKER_PREFIX = "broker.";
  private static final String BROKER_HOST = BROKER_PREFIX + "host";
  private static final String BROKER_PORT = BROKER_PREFIX + "port";
  private static final String BROKER_USERNAME = BROKER_PREFIX + "username";
  private static final String BROKER_PASSWORD = BROKER_PREFIX + "password";
  private static final String BROKER_VIRTUALHOST = BROKER_PREFIX + "virtualhost";
  private static final String BROKER_MANAGEMENT_PORT = BROKER_PREFIX + "management.port";
  private static final String BROKER_MANAGEMENT_REFRESH_RATE = BROKER_PREFIX + "management.refresh";
  private static final String BROKER_RETRY_WAIT_TIME = BROKER_PREFIX + "retryWaitTime";
  private final Properties properties;

  /**
   * Configuration of broker.
   * @param properties Properties containing the predefined properties.
   */
  public BrokerConfiguration(final Properties properties) {
    this.properties = properties;
  }

  public List<String> getValidationErrors() {
    final List<String> reasons = new ArrayList<>();
    validateRequiredProperty(BROKER_HOST, reasons);
    validateRequiredProperty(BROKER_USERNAME, reasons);
    validateRequiredProperty(BROKER_PASSWORD, reasons);
    return reasons;
  }

  public ConnectionConfiguration buildConnectionConfiguration() {
    final ConnectionConfiguration.Builder builder = ConnectionConfiguration.builder();

    builder.brokerHost(getProperty(BROKER_HOST));
    builder.brokerUsername(getProperty(BROKER_USERNAME));
    builder.brokerPassword(getProperty(BROKER_PASSWORD));
    getPropertyIntOptional(BROKER_PORT).ifPresent(builder::brokerPort);
    Optional.ofNullable(getProperty((BROKER_VIRTUALHOST))).ifPresent(builder::brokerVirtualHost);
    getPropertyIntOptional(BROKER_MANAGEMENT_PORT).ifPresent(builder::brokerManagementPort);
    getPropertyIntOptional(BROKER_MANAGEMENT_REFRESH_RATE).ifPresent(builder::brokerManagementRefreshRate);
    getPropertyIntOptional(BROKER_RETRY_WAIT_TIME).ifPresent(builder::brokerRetryWaitTime);
    return builder.build();
  }

  protected String getProperty(final String key) {
    final String prop = System.getProperty(key);

    if (prop == null) {
      final String env = System.getenv(("AERIUS." + key.toUpperCase(Locale.ENGLISH)).replace('.', '_'));

      if (env == null) {
        return properties.getProperty(key);
      } else {
        LOG.info("Reading property {} from environment variable", key);
        return env;
      }
    } else {
      LOG.info("Reading property {} from property variable", key);
      return prop;
    }
  }

  /**
   * Validate if the required property is set. If set the method returns true
   * @param key key to validate
   * @param reasons add error to list of reasons in case the property isn't set
   * @return true if property is set
   */
  protected boolean validateRequiredProperty(final String key, final List<String> reasons) {
    final String property = getProperty(key);
    final boolean empty = property == null || property.isEmpty();

    if (empty) {
      reasons.add("Required property '" + key + "' is not set.");
    }
    return !empty;
  }

  protected Integer getPropertyInt(final String key) {
    final String value = getProperty(key);
    return value == null ? null : Integer.valueOf(value);
  }

  protected Optional<Integer> getPropertyIntOptional(final String key) {
    return Optional.ofNullable(getPropertyInt(key));
  }

}
