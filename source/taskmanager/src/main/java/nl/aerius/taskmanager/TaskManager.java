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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.adaptor.DynamicQueueConsumer;
import nl.aerius.taskmanager.adaptor.DynamicQueueConsumer.DynamicQueueAddedCallback;
import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.RabbitMQQueueType;
import nl.aerius.taskmanager.domain.TaskConsumer;
import nl.aerius.taskmanager.domain.TaskQueue;
import nl.aerius.taskmanager.domain.TaskSchedule;
import nl.aerius.taskmanager.scheduler.TaskScheduler;
import nl.aerius.taskmanager.scheduler.TaskScheduler.TaskSchedulerFactory;

/**
 * Main task manager class, manages all schedulers.
 */
class TaskManager<T extends TaskQueue, S extends TaskSchedule<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

  private final ExecutorService executorService;
  private final AdaptorFactory factory;
  private final TaskSchedulerFactory<T, S> schedulerFactory;
  private final WorkerSizeProviderProxy workerSizeObserverProxy;
  private final Map<String, TaskScheduleBucket> buckets = new HashMap<>();
  private final DynamicQueueConsumer dynamicQueueConsumer;

  public TaskManager(final ExecutorService executorService, final AdaptorFactory factory, final TaskSchedulerFactory<T, S> schedulerFactory,
      final WorkerSizeProviderProxy workerSizeObserverProxy) throws IOException {
    this.executorService = executorService;
    this.factory = factory;
    this.schedulerFactory = schedulerFactory;
    this.workerSizeObserverProxy = workerSizeObserverProxy;
    this.dynamicQueueConsumer = factory.createDynamicQueueConsumer();
    this.dynamicQueueConsumer.startConsuming();
  }

  /**
   * Add or Update a new task scheduler.
   *
   * @param schedule scheduler configuration
   * @throws InterruptedException
   */
  public boolean updateTaskScheduler(final TaskSchedule<T> schedule) throws InterruptedException {
    // Set up scheduler with worker pool
    final String workerQueueName = schedule.getWorkerQueueName();

    if (!buckets.containsKey(workerQueueName)) {
      final QueueConfig queueConfig = new QueueConfig(workerQueueName, schedule.isDurable(), schedule.isDynamicQueues(), schedule.getQueueType());

      LOG.info("Added scheduler for worker queue {}", queueConfig);
      final TaskScheduleBucket bucket = new TaskScheduleBucket(queueConfig);

      if (schedule.isDynamicQueues()) {
        dynamicQueueConsumer.addDynamicQueueAddedCallback(bucket);
      }
      buckets.put(workerQueueName, bucket);
    }
    final TaskScheduleBucket taskScheduleBucket = buckets.get(workerQueueName);

    taskScheduleBucket.updateQueues(schedule.getQueues(), schedule.isDurable(), schedule.isDynamicQueues(), schedule.getQueueType());
    return taskScheduleBucket.isRunning();
  }

  /**
   * Removed the scheduler for the given worker type.
   *
   * @param workerQueueName queueName
   */
  public void removeTaskScheduler(final String workerQueueName) {
    LOG.info("Removed schedule for worker queue {}", workerQueueName);
    final TaskScheduleBucket bucket = buckets.get(workerQueueName);

    if (bucket != null) {
      bucket.shutdown();
      buckets.remove(workerQueueName);
    }
  }

  /**
   * Shuts down all schedulers and consumers.
   */
  public void shutdown() {
    buckets.forEach((k, v) -> v.shutdown());
    buckets.clear();
    dynamicQueueConsumer.stopConsuming();
  }

  private class TaskScheduleBucket implements DynamicQueueAddedCallback {
    private final TaskDispatcher dispatcher;
    private final WorkerProducer workerProducer;
    private final Map<String, TaskConsumer> taskConsumers = new HashMap<>();
    private final TaskScheduler<T> taskScheduler;
    private final String workerQueueName;

    public TaskScheduleBucket(final QueueConfig queueConfig) throws InterruptedException {
      this.workerQueueName = queueConfig.queueName();
      taskScheduler = schedulerFactory.createScheduler(workerQueueName, queueConfig.dynamicQueues());
      LOG.info("Worker Queue Name:{} (durable:{}, queueType:{})", workerQueueName, queueConfig.durable(), queueConfig.queueType());
      workerProducer = factory.createWorkerProducer(queueConfig);
      final WorkerPool workerPool = new WorkerPool(workerQueueName, workerProducer, taskScheduler);
      workerSizeObserverProxy.addObserver(workerQueueName, workerPool);
      workerProducer.start();
      // Set up metrics
      WorkerPoolMetrics.setupMetrics(workerPool, workerQueueName);

      // Set up dispatcher
      dispatcher = new TaskDispatcher(workerQueueName, taskScheduler, workerPool);
      executorService.execute(dispatcher);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1)); // just wait a little second to make sure the process is actually running.
      LOG.info("Started taskscheduler for {} of type {}", workerQueueName, taskScheduler.getClass().getSimpleName());
    }

    /**
     * @return
     */
    public boolean isRunning() {
      return dispatcher.isRunning();
    }

    @Override
    public void onDynamicQueueAdded(final String queueName) {
      // TODO HSB: filter out queue specific naming
      addTaskConsumerIfAbsent(new QueueConfig(queueName, false, true, RabbitMQQueueType.CLASSIC));
    }

    private void updateQueues(final List<T> newTaskQueues, final boolean durable, final boolean dynamicQueues,
        final RabbitMQQueueType rabbitMQQueueType) {
      final Map<String, ? extends TaskQueue> newTaskQueuesMap = newTaskQueues.stream().filter(Objects::nonNull)
          .collect(Collectors.toMap(TaskQueue::getQueueName, Function.identity()));

      if (!dynamicQueues) {
        // Remove queues that are not in the new list
        final List<Entry<String, TaskConsumer>> removedQueues = taskConsumers.entrySet().stream()
            .filter(e -> !newTaskQueuesMap.containsKey(e.getKey()))
            .toList();
        removedQueues.forEach(e -> removeTaskConsumer(e.getKey()));
        // Add and Update existing queues
      }
      newTaskQueues.stream().filter(Objects::nonNull).forEach(tc -> addOrUpdateTaskQueue(tc, durable, dynamicQueues, rabbitMQQueueType));
    }

    private void addOrUpdateTaskQueue(final T taskQueueConfiguration, final boolean durable, final boolean dynamicQueues,
        final RabbitMQQueueType rabbitMQQueueType) {
      if (!dynamicQueues) {
        addTaskConsumerIfAbsent(new QueueConfig(taskQueueConfiguration.getQueueName(), durable, dynamicQueues, rabbitMQQueueType));
      }
      taskScheduler.updateQueue(taskQueueConfiguration);
    }

    /**
     * Adds a task consumer.
     *
     * @param queueConfig Configuration parameters for the queue
     */
    public void addTaskConsumerIfAbsent(final QueueConfig queueConfig) {
      taskConsumers.computeIfAbsent(queueConfig.queueName(), tqn -> {
        try {
          final TaskConsumer taskConsumer = new TaskConsumerImpl(executorService, queueConfig, dispatcher, factory);
          taskConsumer.start();
          LOG.info("Started task queue {} (durable:{}, queueType:{})", queueConfig.queueName(), queueConfig.durable(), queueConfig.queueType());
          return taskConsumer;
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }

    /**
     * Removes a task consumer with the given queue name.
     *
     * @param workerQueueName queue name of the task consumer
     */
    private void removeTaskConsumer(final String taskQueueName) {
      LOG.info("Removed task queue {}", taskQueueName);
      taskScheduler.removeQueue(taskQueueName);
      Optional.ofNullable(taskConsumers.remove(taskQueueName)).ifPresent(tc -> tc.shutdown());
    }

    public void shutdown() {
      dispatcher.shutdown();
      workerProducer.shutdown();
      WorkerPoolMetrics.removeMetrics(workerQueueName);
      taskConsumers.forEach((k, v) -> v.shutdown());
    }
  }
}
