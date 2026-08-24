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

/**
 * Handler to be informed when changed related to handling or tasks by workers and worker pool changes.
 */
public interface WorkerUpdateHandler {

  /**
   * Task has be processed by the worker pool and finished with results.
   * @param taskRecord record of the task that was finished
   */
  void onTaskFinished(TaskRecord taskRecord);

  /**
   * Called when the number of workers has been changed. The value passed is the new value.
   * @param numberOfWorkers number of available workers.
   */
  void onWorkerPoolSizeChange(int numberOfWorkers);
}
