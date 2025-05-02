/*
 * Copyright the State of the Netherlands
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

import java.util.Locale;
import java.util.Objects;

/**
 * Contains the names of the group of queues.
 */
public class WorkerQueueType {
  /**
   * Main prefix for queue names.
   */
  private static final String NAMING_PREFIX = "aerius.";
  private static final char DOT = '.';

  private final String name;

  /**
   * Constructor
   *
   * @param name name of the queue type
   */
  public WorkerQueueType(final String name) {
    this.name = name.toLowerCase(Locale.ROOT);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return name.equals(((WorkerQueueType) obj).name);
  }

  /**
   * @param taskName The name of the task to get the queueName for.
   * @return The queuename that should be used for declaring the queue.
   */
  public String getTaskQueueName(final String taskName) {
    return taskName == null ? null : (NAMING_PREFIX + propertyName() + DOT + taskName);
  }

  public String getTaskQueueName(final String taskName, final String id) {
    return taskName == null ? null : (NAMING_PREFIX + propertyName() + DOT + taskName + DOT + id);
  }

  public static String[] getTaskQueueParts(final String queueName) {
    if (queueName != null) {
      final int lastIndexOf = queueName.lastIndexOf(DOT);
      if (lastIndexOf > 0) {
        return new String[] {queueName.substring(0, lastIndexOf), queueName.substring(lastIndexOf)};
      }
    }
    return null;
  }

  public String getWorkerQueueName() {
    return NAMING_PREFIX + "worker." + propertyName();
  }

  public String propertyName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
