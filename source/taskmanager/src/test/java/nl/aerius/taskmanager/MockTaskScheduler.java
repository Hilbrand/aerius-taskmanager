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
package nl.aerius.taskmanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskRecord;
import nl.aerius.taskmanager.scheduler.TaskScheduler;
import nl.aerius.taskmanager.scheduler.priorityqueue.PriorityTaskSchedulerFileHandler;

/**
 * Mock scheduler, implementing a FIFO algorithm.
 */
public class MockTaskScheduler implements TaskScheduler<PriorityTaskQueue> {

  private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();

  @Override
  public void addTask(final Task task) {
    tasks.add(task);
  }

  @Override
  public void killTasks() {
    tasks.stream().forEach(Task::killTask);
  }

  @Override
  public Task getNextTask() throws InterruptedException {
    return tasks.take();
  }

  @Override
  public void updateQueue(final PriorityTaskQueue queue) {
    // Not used
  }

  @Override
  public void removeQueue(final String queueName) {
    // Not used
  }

  @Override
  public void onTaskFinished(final TaskRecord taskRecord) {
    // Not used
  }

  @Override
  public void onWorkerPoolSizeChange(final int numberOfWorkers) {
    // Not used
  }

  public static class MockSchedulerFactory implements TaskSchedulerFactory<PriorityTaskQueue, PriorityTaskSchedule> {
    private final PriorityTaskSchedulerFileHandler handler = new PriorityTaskSchedulerFileHandler();

    @Override
    public MockTaskScheduler createScheduler(final QueueConfig queueConfig) {
      return new MockTaskScheduler();
    }

    @Override
    public PriorityTaskSchedulerFileHandler getHandler() {
      return handler;
    }
  }

  @Override
  public void reset() {
    // No-op
  }

}
