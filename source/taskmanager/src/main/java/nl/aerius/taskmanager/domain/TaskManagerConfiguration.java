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
package nl.aerius.taskmanager.domain;

import java.io.File;

import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;

/**
 * configuration for the TaskManager.
 *
 */
public class TaskManagerConfiguration {

  private final ConnectionConfiguration brokerConfiguration;
  private final File configurationDirectory;

  public TaskManagerConfiguration(final ConnectionConfiguration brokerConfiguration, final File configurationDirectory) {
    this.brokerConfiguration = brokerConfiguration;
    this.configurationDirectory = configurationDirectory;
  }

  public ConnectionConfiguration getBrokerConfiguration() {
    return brokerConfiguration;
  }

  public File getConfigurationDirectory() {
    return configurationDirectory;
  }
}
