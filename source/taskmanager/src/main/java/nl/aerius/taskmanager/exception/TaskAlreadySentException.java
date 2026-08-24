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
package nl.aerius.taskmanager.exception;

/**
 * Exception thrown when the same task was offered again to be sent to a worker.
 */
public class TaskAlreadySentException extends RuntimeException {

  private static final long serialVersionUID = 2L;

  private final String workerQueueName;
  private final String taskQueueName;

  /**
   * TaskAlreadySentException.
   * @param workerQueueName worker type this error occurred on
   * @param taskQueueName queue of the task already send
   */
  public TaskAlreadySentException(final String workerQueueName, final String taskQueueName) {
    this.workerQueueName = workerQueueName;
    this.taskQueueName = taskQueueName;
  }

  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public String getTaskQueueName() {
    return taskQueueName;
  }
}
