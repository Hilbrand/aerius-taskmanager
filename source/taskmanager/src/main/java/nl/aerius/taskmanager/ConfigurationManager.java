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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.client.configuration.BrokerConfiguration;
import nl.aerius.taskmanager.domain.TaskManagerConfiguration;

/**
 * Class to help with loading and saving configuration.
 */
final class ConfigurationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationManager.class);

  private ConfigurationManager() {}

  /**
   * Load the configuration from default properties and a directory configured in a property file
   * (either from a "taskmanager.properties" or from classpath).
   * @param props properties
   * @return The configuration file to be used by the taskmanager.
   */
  public static TaskManagerConfiguration loadConfiguration(final Properties props) {
    return loadConfigurationFromFile(new TaskManagerProperties(props));
  }

  /**
   *
   * @param properties taskmanager configuration options
   * @return The configuration file to be used by the taskmanager.
   */
  private static TaskManagerConfiguration loadConfigurationFromFile(final TaskManagerProperties properties) {
    final List<String> validationErrors = properties.getValidationErrors();

    if (!validationErrors.isEmpty()) {
      throw new IllegalArgumentException(validationErrors.stream().collect(Collectors.joining(", ")));
    }
    final File configurationDirectory = properties.getConfigDirectory();
    final TaskManagerConfiguration tmc = new TaskManagerConfiguration(properties.buildConnectionConfiguration(), configurationDirectory);
    LOG.info("Read configuration from {}", configurationDirectory);
    return tmc;
  }

  public static Properties getPropertiesFromFile(final String propertiesFileName) throws IOException {
    final File localFile = new File(propertiesFileName);
    final Properties properties = new Properties();

    try (final InputStream inputStream = new FileInputStream(localFile)) {
      properties.load(inputStream);
    }
    return properties;
  }

  /**
   * TaskManager property configurations.
   */
  private static class TaskManagerProperties extends BrokerConfiguration {

    private static final String PROPERTY_CONFIG_DIRECTORY = "taskmanager.configuration.directory";

    /**
     * @param properties Properties containing the predefined properties.
     */
    public TaskManagerProperties(final Properties properties) {
      super(properties);
    }

    @Override
    public List<String> getValidationErrors() {
      final List<String> reasons = super.getValidationErrors();

      final File configurationDirectory = getConfigDirectory();

      if (configurationDirectory == null) {
        reasons.add("Missing configuration directory '" + PROPERTY_CONFIG_DIRECTORY + "'");
      } else if (!(configurationDirectory.exists() && configurationDirectory.isDirectory() && configurationDirectory.canRead())) {
        reasons.add("Can't read taskmanager queue configuration directory: " + configurationDirectory);
      }
      return reasons;
    }

    public File getConfigDirectory() {
      final String configDirectory = getProperty(PROPERTY_CONFIG_DIRECTORY);

      return configDirectory == null ? null : new File(configDirectory);
    }
  }

}
