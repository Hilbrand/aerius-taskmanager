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
package nl.aerius.taskmanager.scheduler;

import java.io.File;
import java.io.IOException;

import nl.aerius.taskmanager.domain.TaskQueue;
import nl.aerius.taskmanager.domain.TaskSchedule;

/**
 * Interface to read and write specific scheduler configuration files.
 * @param <T> queue configuration
 * @param <S> scheduler configuration
 */
public interface SchedulerFileConfigurationHandler<T extends TaskQueue, S extends TaskSchedule<T>> {

  /**
   * Reads a scheduler configuration file and returns the parsed data object
   *
   * @param file file to read
   * @return data object
   * @throws IOException
   */
  S read(File file) throws IOException;
}
