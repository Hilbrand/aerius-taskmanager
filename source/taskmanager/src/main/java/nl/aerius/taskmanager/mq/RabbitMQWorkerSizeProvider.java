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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.WorkerProducer.WorkerMetrics;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

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
  private final RabbitMQQueueMonitor monitor;
  // Map to keep track of the last known states of the queues as retrieved form the RabbitMQ admin API.
  private Map<String, RabbitMQQueueStatus> lastKnownQueueStates = new HashMap<>();
  private boolean running;

  public RabbitMQWorkerSizeProvider(final ScheduledExecutorService executorService, final BrokerConnectionFactory factory) {
    this(executorService, factory, new RabbitMQQueueMonitor(factory.getConnectionConfiguration()));
  }

  RabbitMQWorkerSizeProvider(final ScheduledExecutorService executorService, final BrokerConnectionFactory factory,
      final RabbitMQQueueMonitor monitor) {
    this.executorService = executorService;
    this.monitor = monitor;
    channelQueueEventsWatcher = new RabbitMQChannelQueueEventsWatcher(factory, this);
    refreshRateSeconds = factory.getConnectionConfiguration().getBrokerManagementRefreshRate();
    eventProducer = new RabbitMQWorkerEventProducer(executorService, factory);
    refreshDelayBeforeUpdateSeconds = Math.min(refreshRateSeconds / 2, DELAY_BEFORE_UPDATE_TIME_SECONDS);
  }

  @Override
  public void addObserver(final String queueName, final WorkerSizeObserver observer) {
    observers.computeIfAbsent(queueName, k -> new WorkerSizeObserverComposite()).add(observer);
    if (observer instanceof WorkerMetrics) {
      eventProducer.addMetrics(queueName, (WorkerMetrics) observer);
    }
  }

  @Override
  public boolean removeObserver(final String queueName) {
    eventProducer.removeMetrics(queueName);
    return observers.remove(queueName) != null;
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
        lastKnownQueueStates = new HashMap<>(monitor.getWorkerQueueStates());
        observers.forEach((q, v) -> scheduledUpdateWorkerQueueState(q, () -> lastKnownQueueStates.get(q)));
      } catch (final RuntimeException e) {
        LOG.error("Runtime error during updateWorkerQueueState", e);
      }
    }
  }

  @Override
  public void triggerWorkerQueueState(final String queueName) {
    if (!observers.containsKey(queueName)) {
      return;
    }
    scheduledUpdateWorkerQueueState(queueName, () -> monitor.getWorkerQueueState(queueName));
  }

  private void scheduledUpdateWorkerQueueState(final String queueName, final Supplier<RabbitMQQueueStatus> statusSupplier) {
    // This uses a delayed update. It schedules a task to run in x-seconds.
    // If a new update is received before the schedule has run it will cancel the current schedule and reschedule.
    // This is mainly for when multiple events are triggered to not trigger a call for every event,
    // and also to manage the events trigger in combination with the scheduled process.
    synchronized (sync) {
      Optional.ofNullable(lastRuns.get(queueName)).ifPresent(f -> f.cancel(false));
      final Runnable updateTask = () -> updateWorkerQueueState(queueName, statusSupplier);

      lastRuns.put(queueName, executorService.schedule(updateTask, refreshDelayBeforeUpdateSeconds, TimeUnit.SECONDS));
    }
  }

  private void updateWorkerQueueState(final String queueName, final Supplier<RabbitMQQueueStatus> statusSupplier) {
    synchronized (sync) {
      try {
        Optional.ofNullable(statusSupplier.get())
            .ifPresent(s -> {
              lastKnownQueueStates.put(queueName, s);
              lastRuns.remove(queueName);
              Optional.ofNullable(observers.get(queueName)).ifPresent(observer -> observer.onNumberOfWorkersUpdate(s));
            });
      } catch (final RuntimeException e) {
        LOG.error("RuntimeException during updateWorkerQueueState", e);
      }
    }
  }

  private static class WorkerSizeObserverComposite implements WorkerSizeObserver {
    private final List<WorkerSizeObserver> observers = new ArrayList<>();

    public void add(final WorkerSizeObserver observer) {
      observers.add(observer);
    }

    @Override
    public void onNumberOfWorkersUpdate(final RabbitMQQueueStatus queueStatus) {
      for (final WorkerSizeObserver observer : observers) {
        try {
          observer.onNumberOfWorkersUpdate(queueStatus);
        } catch (final RuntimeException e) {
          LOG.error("RuntimeException during onNumberOfWorkersUpdate in {}", observer.getClass(), e);
        }
      }
    }
  }
}
