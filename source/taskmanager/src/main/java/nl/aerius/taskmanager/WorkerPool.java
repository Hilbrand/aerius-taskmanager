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
package nl.aerius.taskmanager;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerFinishedHandler;
import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerMetrics;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.exception.NoFreeWorkersException;

/**
 * Class to manage workers. Contains a list of all available workers, which are: free workers, reserved workers and running workers.
 * <p>Free workers are workers available for processing tasks.
 * <p>Reserved workers are workers that are waiting for a task to become available on the queue.
 * <p>Running workers are workers for that are busy running the task and are waiting for the task to finish.
 */
class WorkerPool implements WorkerSizeObserver, WorkerFinishedHandler, WorkerMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);

  private final Semaphore freeWorkers = new Semaphore(0);
  private final Map<String, String> runningWorkers = new ConcurrentHashMap<>();
  private final QueueWatchDog watchDog = new QueueWatchDog();
  private int totalConfiguredWorkers;
  private final String workerQueueName;
  private final WorkerProducer wp;
  private final WorkerUpdateHandler workerUpdateHandler;

  private int numberOfMessagesUnacknowledged;

  public WorkerPool(final String workerQueueName, final WorkerProducer wp, final WorkerUpdateHandler workerUpdateHandler) {
    this.workerQueueName = workerQueueName;
    this.wp = wp;
    this.workerUpdateHandler = workerUpdateHandler;
    wp.setWorkerFinishedHandler(this);
  }

  /**
   * Send the task to the worker by placing it on the worker queue.
   *
   * @param task task to send to the worker
   * @throws IOException
   */
  public void sendTaskToWorker(final Task task) throws IOException {
    if (runningWorkers.containsKey(task.getId())) {
      LOG.error("Duplicate task detected for worker queue: {}, from task queue: {}", workerQueueName, task.getTaskConsumer().getQueueName());
    } else {
      synchronized (this) {
        if (!freeWorkers.tryAcquire()) {
          throw new NoFreeWorkersException(workerQueueName);
        }
        runningWorkers.put(task.getId(), task.getMessage().getMetaData().getQueueName());
      }
    }
    wp.forwardMessage(task.getMessage());
    task.getTaskConsumer().messageDelivered(task.getMessage().getMetaData());
    LOG.trace("[{}][taskId:{}] Task sent", workerQueueName, task.getId());
  }

  public int getWorkerSize() {
    synchronized (this) {
      return totalConfiguredWorkers;
    }
  }

  @Override
  public int getCurrentWorkerSize() {
    synchronized (this) {
      return freeWorkers.availablePermits() + runningWorkers.size();
    }
  }

  @Override
  public int getRunningWorkerSize() {
    synchronized (this) {
      return Math.max(runningWorkers.size(), numberOfMessagesUnacknowledged);
    }
  }

  @Override
  public void onWorkerFinished(final String taskId) {
    releaseWorker(taskId);
  }

  @Override
  public void reset() {
    synchronized (this) {
      for (final Entry<String, String> taskEntry : runningWorkers.entrySet()) {
        releaseWorker(taskEntry.getKey(), taskEntry.getValue());
      }
      updateNumberOfWorkers(0);
    }
  }

  /**
   * Adds the worker to the pool of available workers and calls onWorkerReady.
   *
   * @param taskId Id of the task to release
   */
  public void releaseWorker(final String taskId) {
    releaseWorker(taskId, runningWorkers.get(taskId));
  }

  /**
   * Adds the worker to the pool of available workers and calls onWorkerReady.
   *
   * @param taskId Id of the task that was reported done and can be released
   * @param queueName queue the task is expected to be on.
   */
  public void releaseWorker(final String taskId, final String queueName) {
    if (queueName != null) {
      synchronized (this) {
        if (runningWorkers.containsKey(taskId)) {
          // if currentSize is smaller than the worker size it means the worker
          // must not be re-added as free worker but removed from the pool.
          if (totalConfiguredWorkers >= getCurrentWorkerSize()) {
            freeWorkers.release(1);
          }
          runningWorkers.remove(taskId);
        } else {
          LOG.info("[{}][taskId:{}] Task for queue '{}' not found, maybe it was already released).", workerQueueName, taskId, queueName);
        }
      }
      workerUpdateHandler.onTaskFinished(queueName);
      LOG.debug("[{}][taskId:{}] Task released).", workerQueueName, taskId);
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
  public void onDeltaNumberOfWorkersUpdate(final int delta) {
    synchronized (this) {
      updateNumberOfWorkers(totalConfiguredWorkers + delta);
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
   * @param numberOfMessages Actual total number of messages on the queue
   */
  @Override
  public void onNumberOfWorkersUpdate(final int numberOfWorkers, final int numberOfMessages, final int numberOfMessagesUnacknowledged) {
    synchronized (this) {
      this.numberOfMessagesUnacknowledged = numberOfMessagesUnacknowledged;
      updateNumberOfWorkers(numberOfWorkers);
      checkDeadTasks(numberOfMessages);
    }
  }

  private void updateNumberOfWorkers(final int numberOfWorkers) {
    totalConfiguredWorkers = numberOfWorkers;
    final int deltaWorkers = totalConfiguredWorkers - getCurrentWorkerSize();

    if (deltaWorkers > 0) {
      freeWorkers.release(deltaWorkers);
      LOG.info("# Workers of {} increased to {}(+{})", workerQueueName, totalConfiguredWorkers, deltaWorkers);
    } else if ((deltaWorkers < 0) && (freeWorkers.availablePermits() > 0)) {
      freeWorkers.acquireUninterruptibly(Math.min(freeWorkers.availablePermits(), -deltaWorkers));
      LOG.info("# Workers of {} decreased to {}({})", workerQueueName, totalConfiguredWorkers, deltaWorkers);
    }
    if (deltaWorkers != 0) {
      workerUpdateHandler.onWorkerPoolSizeChange(totalConfiguredWorkers);
    }
  }

  private void checkDeadTasks(final int numberOfMessages) {
    if (watchDog.isItDead(!runningWorkers.isEmpty(), numberOfMessages)) {
      LOG.info("It looks like some tasks are zombies on {} worker queue, so all tasks currently in state running are released.", workerQueueName);
      reset();
    }
  }
}
