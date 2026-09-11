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
package nl.aerius.taskmanager.metrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.StartupGuard;
import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerProducerHandler;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.domain.QueueWatchDogListener;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

/**
 * This class provides the input for the {@link TaskManagerUsageMetricsProvider}. It will register updates on the amount of worker/workers from
 * different sources. It also make sure the update will not be started before startup guard gives a green light.
 * Note the startup guard doesn't have to be used in the {@link #onWorkDispatched(String, Map)} and {@link #onWorkerFinished(String, Map)} because
 * these methods won't be called before the scheduling is started.
 */
public class TaskManagerMetricsRegister implements WorkerProducerHandler, WorkerSizeObserver, QueueWatchDogListener {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerMetricsRegister.class);

  private final TaskManagerUsageMetricsProvider taskManagerUsageMetricsProvider;
  private final StartupGuard startupGuard;
  // Keep track of dispatched tasks, because when taskmanager restarts it should not register tasks already on the queue
  // as it doesn't have any metrics on it anymore.
  private final Set<String> dispatchedTasks = ConcurrentHashMap.newKeySet();

  private int numberOfWorkers;

  public TaskManagerMetricsRegister(final TaskManagerUsageMetricsProvider taskManagerUsageMetricsProvider, final StartupGuard startupGuard) {
    this.taskManagerUsageMetricsProvider = taskManagerUsageMetricsProvider;
    this.startupGuard = startupGuard;
  }

  @Override
  public void onWorkDispatched(final String messageId, final Map<String, Object> messageMetaData) {
    synchronized (dispatchedTasks) {
      dispatchedTasks.add(messageId);
      taskManagerUsageMetricsProvider.register(1, numberOfWorkers);
    }
  }

  @Override
  public void onWorkerFinished(final String messageId, final Map<String, Object> messageMetaData) {
    synchronized (dispatchedTasks) {
      if (dispatchedTasks.remove(messageId)) {
        taskManagerUsageMetricsProvider.register(-1, numberOfWorkers);
      }
    }
  }

  @Override
  public void onNumberOfWorkersUpdate(final RabbitMQQueueStatus queueStatus) {
    this.numberOfWorkers = queueStatus.consumers();
    final int messages = queueStatus.messages();

    if (!startupGuard.isOpen() && messages > 0) {
      LOG.info("Queue {} will be started with {} messages already on the queue.", taskManagerUsageMetricsProvider.getWorkerQueueName(),
          messages);
      taskManagerUsageMetricsProvider.register(messages, numberOfWorkers);
    }
  }

  @Override
  public void reset() {
    synchronized (dispatchedTasks) {
      dispatchedTasks.clear();
      taskManagerUsageMetricsProvider.reset();
    }
  }
}
