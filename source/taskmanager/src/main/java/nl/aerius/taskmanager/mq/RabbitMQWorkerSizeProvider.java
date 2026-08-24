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
package nl.aerius.taskmanager.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerMetrics;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;

/**
 * Provider to use different means to get information about the size and utilisation of the workers.
 * It both queries the RabbitMQ admin interface to get a total insight as it listens to changes in the number of consumers registering
 * with RabbitMQ.
 */
public class RabbitMQWorkerSizeProvider implements WorkerSizeProviderProxy {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQWorkerSizeProvider.class);

  /**
   * Delay first read from the RabittMQ admin to give the taskmanager some time to start up and register all observers.
   */
  private static final int INITIAL_DELAY_SECONDS = 10;
  /**
   * The minimum time before the RabbitMQ management api is fetched again to get an update on the queue state.
   */
  private static final long DELAY_BEFORE_UPDATE_TIME_SECONDS = 15;

  private final ScheduledExecutorService executorService;
  private final BrokerConnectionFactory factory;
  private final RabbitMQChannelQueueEventsWatcher channelQueueEventsWatcher;
  private final RabbitMQWorkerEventProducer eventProducer;
  /**
   * The time in seconds between each scheduled update.
   */
  private final long refreshRateSeconds;
  /**
   * The time delay in seconds before the update call is made.
   */
  private final long refreshDelayBeforeUpdateSeconds;

  private final Object sync = new Object();

  private final Map<String, ScheduledFuture<?>> lastRuns = new HashMap<>();
  private final Map<String, WorkerSizeObserverComposite> observers = new HashMap<>();
  private final Map<String, RabbitMQQueueMonitor> monitors = new HashMap<>();

  private boolean running;

  public RabbitMQWorkerSizeProvider(final ScheduledExecutorService executorService, final BrokerConnectionFactory factory) {
    this.executorService = executorService;
    this.factory = factory;
    channelQueueEventsWatcher = new RabbitMQChannelQueueEventsWatcher(factory, this);
    refreshRateSeconds = factory.getConnectionConfiguration().getBrokerManagementRefreshRate();
    eventProducer = new RabbitMQWorkerEventProducer(executorService, factory);
    refreshDelayBeforeUpdateSeconds = Math.min(refreshRateSeconds / 2, DELAY_BEFORE_UPDATE_TIME_SECONDS);
  }

  @Override
  public void addObserver(final String workerQueueName, final WorkerSizeObserver observer) {
    if (!observers.containsKey(workerQueueName)) {
      if (refreshRateSeconds > 0) {
        final RabbitMQQueueMonitor monitor = new RabbitMQQueueMonitor(factory.getConnectionConfiguration());

        putMonitor(workerQueueName, monitor);
      } else {
        LOG.info("Not monitoring RabbitMQ admin api because refresh delay was {} seconds", refreshRateSeconds);
      }
    }
    observers.computeIfAbsent(workerQueueName, k -> new WorkerSizeObserverComposite()).add(observer);
    if (observer instanceof WorkerMetrics) {
      eventProducer.addMetrics(workerQueueName, (WorkerMetrics) observer);
    }
  }

  /**
   * Store the monitor. Should only be called outside of this class from unit tests to add a mock monitor.
   *
   * @param workerQueueName
   * @param monitor
   */
  void putMonitor(final String workerQueueName, final RabbitMQQueueMonitor monitor) {
    monitors.put(workerQueueName, monitor);
  }

  @Override
  public boolean removeObserver(final String workerQueueName) {
    final RabbitMQQueueMonitor monitor = monitors.remove(workerQueueName);

    if (monitor != null) {
      monitor.shutdown();
    }
    eventProducer.removeMetrics(workerQueueName);
    return observers.remove(workerQueueName) != null;
  }

  @Override
  public void start() throws IOException {
    channelQueueEventsWatcher.start();
    eventProducer.start();
    if (refreshRateSeconds > 0) {
      running = true;
      executorService.scheduleWithFixedDelay(this::updateWorkerQueueState, INITIAL_DELAY_SECONDS, refreshRateSeconds, TimeUnit.SECONDS);
    }
  }

  @Override
  public void shutdown() {
    for (final String key : new ArrayList<>(observers.keySet())) {
      removeObserver(key);
    }
    eventProducer.shutdown();
    channelQueueEventsWatcher.shutdown();
  }

  private void updateWorkerQueueState() {
    if (running) {
      try {
        monitors.forEach((k, v) -> triggerWorkerQueueState(k));
      } catch (final RuntimeException e) {
        LOG.error("Runtime error during updateWorkerQueueState", e);
      }
    }
  }

  @Override
  public void triggerWorkerQueueState(final String queueName) {
    // This uses a delayed update. It schedules a task to run in x-seconds.
    // If a new update is received before the schedule has run it will cancel the current schedule and reschedule.
    // This is mainly for when multiple events are triggered to not trigger a call for every event,
    // and also to manage the events trigger in combination with the scheduled process.
    synchronized (sync) {
      Optional.ofNullable(lastRuns.get(queueName)).ifPresent(f -> f.cancel(false));
      final Runnable updateTask = () -> updateWorkerQueueState(queueName);

      lastRuns.put(queueName, executorService.schedule(updateTask, refreshDelayBeforeUpdateSeconds, TimeUnit.SECONDS));
    }
  }

  private void updateWorkerQueueState(final String queueName) {
    synchronized (sync) {
      Optional.ofNullable(monitors.get(queueName)).ifPresent(m -> m.updateWorkerQueueState(queueName, observers.get(queueName)));
      lastRuns.remove(queueName);
    }
  }

  private static class WorkerSizeObserverComposite implements WorkerSizeObserver {
    private final List<WorkerSizeObserver> observers = new ArrayList<>();

    public void add(final WorkerSizeObserver observer) {
      observers.add(observer);
    }

    @Override
    public void onNumberOfWorkersUpdate(final int numberOfWorkers, final int numberOfMessages, final int numberOfMessagesInProgress) {
      for (final WorkerSizeObserver observer : observers) {
        try {
          observer.onNumberOfWorkersUpdate(numberOfWorkers, numberOfMessages, numberOfMessagesInProgress);
        } catch (final RuntimeException e) {
          LOG.error("RuntimeException during onNumberOfWorkersUpdate in {}", observer.getClass(), e);
        }
      }
    }
  }
}
