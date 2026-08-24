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
 * Interface for class implementing forwarding to the scheduler.
 */
public interface ForwardTaskHandler {

  /**
   * Forwards the task to the scheduler, which adds the task to the pool of tasks to be handled. This method
   * returns directly so the sender can start processing the next task to be handled. However, the scheduling
   * only allows one task per task consumer to be handled. To support this a lock is set for  a task consumer
   * when a task is forwarded. As soon as the task is dispatched to the worker this lock is removed and the
   * next task added to the scheduler.
   *
   * @param task task to schedule
   */
  void forwardTask(Task task);

  /**
   * Call to kill all tasks still waiting to be processed.
   */
  void killTasks();
}
