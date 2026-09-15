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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerProducerHandler;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.domain.QueueWatchDogListener;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

/**
 * WatchDog to detect dead messages. Dead messages are messages once put on the queue, but those messages have gone. For example because
 * the queue was purged after some restart. In such a case the scheduler keeps the tasks locked and since there will never come an message
 * for the task it's locked indefinitely. This watch dog tries to detect such tasks and release them at some point.
 */
class QueueWatchDog implements WorkerSizeObserver, WorkerProducerHandler {

  private static final Logger LOG = LoggerFactory.getLogger(QueueWatchDog.class);

  /**
   * If for more than 10 minutes the problem remains the sign to reset is given.
   */
  private static final long RESET_TIME_MINUTES = 10;

  private final String workerQueueName;
  private final List<QueueWatchDogListener> listeners = new ArrayList<>();
  private final Set<String> runningTasks = ConcurrentHashMap.newKeySet();

  private LocalDateTime firstProblem;

  public QueueWatchDog(final String workerQueueName) {
    this.workerQueueName = workerQueueName;
  }

  public void addQueueWatchDogListener(final QueueWatchDogListener listener) {
    synchronized (runningTasks) {
      listeners.add(listener);
    }
  }

  @Override
  public void onWorkDispatched(final String messageId, final Map<String, Object> messageMetaData) {
    synchronized (runningTasks) {
      runningTasks.add(messageId);
    }
  }

  @Override
  public void onWorkerFinished(final String messageId, final Map<String, Object> messageMetaData) {
    runningTasks.remove(messageId);
  }

  @Override
  public void onNumberOfWorkersUpdate(final RabbitMQQueueStatus queueStatus) {
    synchronized (runningTasks) {
      if (isItDead(!runningTasks.isEmpty(), queueStatus.messages())) {
        LOG.info("It looks like some tasks are zombies on {} worker queue. All tasks in state running are released (running:{}).", workerQueueName,
            runningTasks.size());
        runningTasks.clear();
        listeners.forEach(QueueWatchDogListener::reset);
      }
    }
  }

  /**
   * Check if the condition is met to do a reset. This is if for more than {@link #RESET_TIME_MINUTES} workers are running,
   * but no messages were on the queue it's time to free all tasks.
   * @param runningWorkers number of workers running
   * @param numberOfMessages number of messages on queue
   * @return true if it's time to free all tasks
   */
  private boolean isItDead(final boolean runningWorkers, final int numberOfMessages) {
    boolean doReset = false;
    if (runningWorkers && numberOfMessages == 0) {
      if (firstProblem == null) {
        firstProblem = now();
      } else {
        doReset = now().isAfter(firstProblem.plusMinutes(RESET_TIME_MINUTES));
      }
    } else {
      firstProblem = null;
    }
    return doReset;
  }

  /**
   * Wrap actual timestamp in this method to be able to use emulated time in unit tests.
   */
  protected LocalDateTime now() {
    return LocalDateTime.now();
  }

}
