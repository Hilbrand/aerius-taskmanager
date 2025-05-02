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
package nl.aerius.taskmanager.scheduler;

import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskQueue;
import nl.aerius.taskmanager.domain.TaskSchedule;
import nl.aerius.taskmanager.domain.WorkerUpdateHandler;

/**
 * Interface for the scheduling algorithm. The implementation should maintain an internal list of all tasks added and return the task to be processed
 * in {@link #getNextTask()} based on whatever priority algorithm the scheduler implements.
 */
public interface TaskScheduler<T extends TaskQueue> extends WorkerUpdateHandler {

  /**
   * Adds a Task to the scheduler to being processed. The scheduler will return this task in {@link #getNextTask()}
   * when the task is next in line.
   * <p>NOTE: This method may not be blocking.
   * @param task task to add
   */
  void addTask(Task task);

  /**
   * Mark all tasks still waiting to be processed as dead as they can't be processed anymore.
   */
  void killTasks();

  /**
   * Returns the next task to process. When no task is present it should wait until a task becomes available.
   * If this method is called it means a worker is available. Therefore the implementation should not be checking
   * something regarding availability of workers.
   * @return task to send to a worker.
   * @throws InterruptedException throws exception when thread was interrupted
   */
  Task getNextTask() throws InterruptedException;

  /**
   * Adds or updates the given task queue
   *
   * @param taskQueue data of new/updated task queue
   */
  void updateQueue(T taskQueue);

  /**
   * Remove the given task queue
   *
   * @param taskQueueName name of the queue to remove
   */
  void removeQueue(String taskQueueName);

  /**
   * Interface for factory class to create new schedulers based on a given configuration.
   */
  interface TaskSchedulerFactory<T extends TaskQueue, S extends TaskSchedule<T>> {

    /**
     * Create a new scheduler.
     *
     * @param workerQueueName the worker queue the scheduler is creatd for
     * @return new task scheduler
     */
    TaskScheduler<T> createScheduler(String workerQueueName, boolean dynamicQueues);

    /**
     * Returns the handler that persists the scheduler configurations.
     *
     * @return handler to read/write scheduler configurations
     */
    SchedulerFileConfigurationHandler<T, S> getHandler();
  }
}
