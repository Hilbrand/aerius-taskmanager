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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.domain.ForwardTaskHandler;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.exception.NoFreeWorkersException;
import nl.aerius.taskmanager.exception.TaskAlreadySentException;
import nl.aerius.taskmanager.scheduler.TaskScheduler;

/**
 * Control center for processing tasks.
 */
class TaskDispatcher implements ForwardTaskHandler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);

  /**
   * Represents the current state the TaskDispatcher is in.
   */
  public enum State {
    WAIT_FOR_WORKER, WAIT_FOR_TASK, DISPATCH_TASK;
  }

  private final WorkerPool workerPool;
  private final TaskScheduler<?> scheduler;
  private final String workerQueueName;

  private boolean running;
  private State state;

  public TaskDispatcher(final String workerQueueName, final TaskScheduler<?> scheduler, final WorkerPool workerPool) {
    this.workerQueueName = workerQueueName;
    this.scheduler = scheduler;
    this.workerPool = workerPool;
  }

  /**
   * Forwards task to the scheduler. It allows only one task per taskconsumer to be ready to be scheduled.
   * To do this it blocks completing this method after passing the task to the scheduler.
   * Since each taskconsumer runs in it's own thread this blocks per taskconsumer.
   */
  @Override
  public void forwardTask(final Task task) {
    scheduler.addTask(task);
    LOG.debug("Task for {} added to scheduler {}", workerQueueName, task.getId());
  }

  @Override
  public void killTasks() {
    // Tell the scheduler to mark all tasks it tracks to be dead.
    scheduler.killTasks();
    // Remove all locks to let RabbitMQ message handlers continue and close shutdown.
    LOG.debug("Tasks removed from scheduler {}", workerQueueName);
  }

  /**
   * Returns the current state of the dispatcher.
   *
   * @return state of the dispatcher
   */
  public State getState() {
    return state;
  }

  /**
   * @return true if running
   */
  public boolean isRunning() {
    return running;
  }

  @Override
  public void run() {
    Thread.currentThread().setName("TaskDispatcher-" + workerQueueName);
    running = true;
    Task retryTask = null;
    try {
      while (running) {
        state = State.WAIT_FOR_WORKER;
        LOG.debug("Wait for worker {}", workerQueueName);
        workerPool.reserveWorker();

        state = State.WAIT_FOR_TASK;
        LOG.debug("Wait for task {}", workerQueueName);
        final Task task = retryTask == null ? getNextTask() : retryTask;
        LOG.debug("Send task to worker {}, ({})", workerQueueName, task.getId());
        state = State.DISPATCH_TASK;
        retryTask = dispatch(task);
      }
    } catch (final RuntimeException e) {
      LOG.error("TaskDispatcher crashed with RuntimeException: {}", getState(), e);
    } catch (final InterruptedException e) {
      LOG.error("TaskDispatcher interrupted: {}", getState(), e);
      Thread.currentThread().interrupt();
    }
    running = false;
  }

  private Task getNextTask() throws InterruptedException {
    Task task;
    do {
      task = scheduler.getNextTask();
    } while (!task.isAlive());
    return task;
  }

  private Task dispatch(final Task task) {
    try {
      workerPool.sendTaskToWorker(task);
    } catch (final NoFreeWorkersException e) {
      LOG.info("[NoFreeWorkersException] Workers for queue {} decreased while waiting for task. Rescheduling task.", e.getWorkerQueueName());
      LOG.trace("NoFreeWorkersException thrown", e);
      return task;
    } catch (final TaskAlreadySentException e) {
      LOG.error("Duplicate task detected for worker queue: {}, from task queue: {}", e.getWorkerQueueName(), e.getTaskQueueName(), e);
      taskAbortedOnDuplicateMessageId(task);
    } catch (final IOException | ShutdownSignalException e) {
      LOG.error("Sending task to worker failed", e);
      taskDeliveryFailed(task);
    }
    return null;
  }

  /**
   * Shutdown the task dispatcher.
   */
  public void shutdown() {
    running = false;
  }

  private void taskDeliveryFailed(final Task task) {
    try {
      workerPool.releaseWorker(task.getId(), task.getTaskRecord());
      task.getTaskConsumer().messageDeliveryFailed(task.getMessage());
    } catch (final IOException | ShutdownSignalException e) {
      LOG.error("taskDeliveryFailed method failed", e);
    }
  }

  private static void taskAbortedOnDuplicateMessageId(final Task task) {
    try {
      task.getTaskConsumer().messageDeliveryAborted(task.getMessage(),
          new RuntimeException("Duplicate messageId found for task" + task.getMessage().getMessageId()));
    } catch (final IOException | ShutdownSignalException e) {
      LOG.error("taskDeliveryFailed method failed", e);
    }
  }
}
