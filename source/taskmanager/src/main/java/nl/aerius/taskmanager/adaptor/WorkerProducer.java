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
import java.util.Map;

import nl.aerius.taskmanager.domain.Message;

/**
 * Interface for communication service with worker. A WorkerProducer delivers messages to the worker queue.
 */
public interface WorkerProducer {

  /**
   * Sets the handler to call when a task is finished by a worker.
   * @param workerProducerHandler handler.
   */
  void addWorkerProducerHandler(WorkerProducerHandler workerProducerHandler);

  /**
   * Starts the worker producer.
   */
  void start();

  /**
   * Dispatch a message to the worker.
   * @param message message to dispatch
   * @throws IOException connection errors
   */
  void dispatchMessage(final Message message) throws IOException;

  /**
   * Shuts down this worker producer.
   */
  void shutdown();

  /**
   * Interface called when the tasks is finished by the worker.
   */
  interface WorkerProducerHandler {
    /**
     * Called when work dispatched to the worker.
     *
     * @param messageId unique id of the message
     * @param messageMetaData message meta data
     */
    default void onWorkDispatched(final String messageId, final Map<String, Object> messageMetaData) {
      // Default nothing to do
    }

    /**
     * Called when worker finished a task.
     *
     * @param messageId unique id of the message
     * @param messageMetaData message meta data
     */
    void onWorkerFinished(String messageId, Map<String, Object> messageMetaData);
  }

  /**
   * Interface to retrieve metrics about the current worker sizes.
   */
  public interface WorkerMetrics {
    /**
     * @return Returns the number of workers currently busy.
     */
    int getNumberOfUsedWorkers();

    /**
     * @return Returns the number total number of workers based on what the queue reports as being active.
     */
    int getReportedWorkerSize();
  }
}
