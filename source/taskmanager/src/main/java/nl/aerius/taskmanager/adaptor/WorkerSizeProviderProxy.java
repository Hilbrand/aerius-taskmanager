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
package nl.aerius.taskmanager.adaptor;

import java.io.IOException;

/**
 * Proxy class for the provider that delivers the update information about the worker queue size.
 */
public interface WorkerSizeProviderProxy {

  /**
   * Add a WorkerSizeObserver. Only 1 workerSizeObserver per worker queue should be added.
   *
   * @param workerQueueName name of the worker queue
   * @param workerSizeObserver observer for the worker queue
   */
  void addObserver(String workerQueueName, WorkerSizeObserver workerSizeObserver);

  /**
   * Removes the observer for the worker queue.
   *
   * @param workerQueueName name of the worker queue
   * @return returns true is removing was successful
   */
  boolean removeObserver(String workerQueueName);

  /**
   * Triggers to get the worker queue state.
   *
   * @param queueName name of the worker queue
   */
  void triggerWorkerQueueState(final String queueName);

  /**
   * Starts the worker size provider.
   *
   * @throws IOException Throws IOException in case of communication problems with RabbitMQ
   */
  void start() throws IOException;

  /**
   * Shutdown the worker size provider.
   */
  void shutdown();
}
