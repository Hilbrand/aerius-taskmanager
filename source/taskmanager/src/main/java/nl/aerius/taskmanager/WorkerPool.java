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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerMetrics;
import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerProducerHandler;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.domain.QueueWatchDogListener;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskRecord;
import nl.aerius.taskmanager.domain.WorkerUpdateHandler;
import nl.aerius.taskmanager.exception.NoFreeWorkersException;
import nl.aerius.taskmanager.metrics.UsageMetricsProvider;

/**
 * Class to manage workers. Contains a list of all available workers, which are: free workers, reserved workers and running workers.
 * <p>Free workers are workers available for processing tasks.
 * <p>Reserved workers are workers that are waiting for a task to become available on the queue.
 * <p>Running workers are workers for that are busy running the task and are waiting for the task to finish.
 */
class WorkerPool implements WorkerSizeObserver, WorkerProducerHandler, UsageMetricsProvider, QueueWatchDogListener, WorkerMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);

  private final Semaphore freeWorkers = new Semaphore(0);
  private final Map<String, TaskRecord> runningWorkers = new ConcurrentHashMap<>();
  private final String workerQueueName;
  private final WorkerProducer wp;
  private final WorkerUpdateHandler workerUpdateHandler;

  private int totalReportedWorkers;
  private int initialUnaccountedWorkers;
  private boolean firstUpdateReceived;

  public WorkerPool(final String workerQueueName, final WorkerProducer wp, final WorkerUpdateHandler workerUpdateHandler) {
    this.workerQueueName = workerQueueName;
    this.wp = wp;
    this.workerUpdateHandler = workerUpdateHandler;
    wp.addWorkerProducerHandler(this);
  }

  /**
   * Send the task to the worker by placing it on the worker queue.
   *
   * @param task task to send to the worker
   * @throws IOException
   */
  public void sendTaskToWorker(final Task task) throws IOException {
    if (runningWorkers.containsKey(task.getId())) {
      LOG.error("Duplicate task detected for worker queue: {}, from task queue: {}", workerQueueName, task.getTaskRecord().queueName());
    } else {
      synchronized (this) {
        if (!freeWorkers.tryAcquire()) {
          throw new NoFreeWorkersException(workerQueueName);
        }
        runningWorkers.put(task.getId(), task.getTaskRecord());
      }
    }
    wp.dispatchMessage(task.getMessage());
    task.getTaskConsumer().messageDelivered(task.getMessage());
    LOG.trace("[{}][taskId:{}] Task sent", workerQueueName, task.getId());
  }

  @Override
  public String getWorkerQueueName() {
    return workerQueueName;
  }

  @Override
  public int getNumberOfWorkers() {
    synchronized (this) {
      return freeWorkers.availablePermits() + runningWorkers.size() + initialUnaccountedWorkers;
    }
  }

  @Override
  public int getReportedWorkerSize() {
    return totalReportedWorkers;
  }

  @Override
  public int getNumberOfUsedWorkers() {
    synchronized (this) {
      return runningWorkers.size() + initialUnaccountedWorkers;
    }
  }

  @Override
  public int getNumberOfFreeWorkers() {
    return freeWorkers.availablePermits();
  }

  @Override
  public void onWorkerFinished(final String messageId, final Map<String, Object> messageMetaData) {
    releaseWorker(messageId);
  }

  /**
   * Adds the worker to the pool of available workers and calls onWorkerReady.
   *
   * @param taskId Id of the task to release
   */
  void releaseWorker(final String taskId) {
    releaseWorker(taskId, runningWorkers.get(taskId));
  }

  /**
   * Adds the worker to the pool of available workers and calls onWorkerReady.
   *
   * @param taskId Id of the task that was reported done and can be released
   * @param taskRecord the task is expected to be on.
   */
  void releaseWorker(final String taskId, final TaskRecord taskRecord) {
    synchronized (this) {
      if (runningWorkers.containsKey(taskId)) {
        freeWorker();
        runningWorkers.remove(taskId);
      } else {
        if (initialUnaccountedWorkers > 0) {
          freeWorker();
        }
        initialUnaccountedWorkers = Math.max(initialUnaccountedWorkers - 1, 0);
        LOG.info("[{}][taskId:{}] Unknown task received, possible left over of restart).", workerQueueName, taskId);
      }
    }
    if (taskRecord != null) {
      workerUpdateHandler.onTaskFinished(taskRecord);
      LOG.debug("[{}][taskId:{}] Task released).", workerQueueName, taskId);
    }
  }

  private void freeWorker() {
    // if currentSize is smaller than the worker size it means the worker
    // must not be re-added as free worker but removed from the pool.
    if (totalReportedWorkers >= (runningWorkers.size() + initialUnaccountedWorkers)) {
      freeWorkers.release(1);
    }
  }

  /**
   * Takes a worker from the free workers list and add it to the reserved workers list. Blocks until a worker becomes available on the free worker
   * list.
   * @return worker marked as reserved.
   */
  public void reserveWorker() {
    try {
      // should not be synchronized because acquire is blocking.
      freeWorkers.acquire();
      freeWorkers.release(1);
      LOG.trace("Worker {} aquired", workerQueueName);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void onNumberOfWorkersUpdate(final int numberOfWorkers, final int numberOfMessages, final int numberOfMessagesInProgress) {
    synchronized (this) {
      if (!firstUpdateReceived) {
        initialUnaccountedWorkers = numberOfMessages;
        firstUpdateReceived = true;
      }
      updateNumberOfWorkers(numberOfWorkers);
    }
  }

  /**
   * Sets the number of workers which are actually available. This number should
   * be determined on the number of workers that are actually in operation.
   * If the new number is higher than the currently available workers new
   * workers are added to the free workers list. If the number is lower, nothing
   * is done here, but when a workers is finished it won't be added to the
   * list of free workers. This will continue until the total number of
   * workers matches the actual number.
   *
   * @param numberOfWorkers Actual size of number of workers in operation
   */
  private void updateNumberOfWorkers(final int numberOfWorkers) {
    final int previousTotalReportedWorkers = totalReportedWorkers;
    totalReportedWorkers = numberOfWorkers;
    final int deltaWorkers = totalReportedWorkers - getNumberOfWorkers();

    if (deltaWorkers > 0) {
      freeWorkers.release(deltaWorkers);
      LOG.info("# Workers of {} increased to {}(+{})", workerQueueName, totalReportedWorkers, deltaWorkers);
    } else if ((deltaWorkers < 0) && (freeWorkers.availablePermits() > 0)
        && freeWorkers.tryAcquire(Math.min(freeWorkers.availablePermits(), -deltaWorkers))) {
      LOG.info("# Workers of {} decreased to {}({})", workerQueueName, totalReportedWorkers, deltaWorkers);
    }
    if (previousTotalReportedWorkers != totalReportedWorkers) {
      workerUpdateHandler.onWorkerPoolSizeChange(totalReportedWorkers);
    }
  }

  @Override
  public void reset() {
    synchronized (this) {
      for (final Entry<String, TaskRecord> taskEntry : runningWorkers.entrySet()) {
        releaseWorker(taskEntry.getKey(), taskEntry.getValue());
      }
    }
  }
}
