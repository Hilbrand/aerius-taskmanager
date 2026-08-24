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
 * Exception thrown when a message is send to a worker, but no free workers are actually available. For example if
 * in between the time a worker was reserved, the number of workers was decreased.
 */
public class NoFreeWorkersException extends RuntimeException {

  private static final long serialVersionUID = 3926363221094715425L;

  private final String workerQueueName;

  /**
   * NoFreeWorkersException.
   * @param workerQueueName worker queue that has no free workers
   */
  public NoFreeWorkersException(final String workerQueueName) {
    this.workerQueueName = workerQueueName;
  }

  public String getWorkerQueueName() {
    return workerQueueName;
  }
}
