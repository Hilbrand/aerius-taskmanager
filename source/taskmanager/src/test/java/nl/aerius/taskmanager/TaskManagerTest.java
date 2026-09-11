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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import nl.aerius.taskmanager.MockTaskScheduler.MockSchedulerFactory;
import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;
import nl.aerius.taskmanager.domain.RabbitMQQueueType;
import nl.aerius.taskmanager.scheduler.priorityqueue.PriorityTaskSchedulerFileHandler;

/**
 * Test class for {@link TaskManager}.
 */
class TaskManagerTest {

  private static ExecutorService executor;
  private static ScheduledExecutorService scheduledExecutorService;

  private final PriorityTaskSchedulerFileHandler handler = new PriorityTaskSchedulerFileHandler();

  private PriorityTaskSchedule schedule;
  private TaskManager<PriorityTaskQueue, PriorityTaskSchedule> taskManager;

  @BeforeEach
  void setUp() throws IOException {
    executor = Executors.newCachedThreadPool();
    scheduledExecutorService = Executors.newScheduledThreadPool(1);
    final AdaptorFactory factory = new MockAdaptorFactory();
    final MockSchedulerFactory schedulerFactory = new MockTaskScheduler.MockSchedulerFactory();
    final WorkerSizeProviderProxy workerSizeProvider = factory.createWorkerSizeProvider();

    doAnswer(a -> {
      // This will unblock the startup guard
      ((WorkerSizeObserver) a.getArgument(1)).onNumberOfWorkersUpdate(new RabbitMQQueueStatus(0, 0, 0));
      return null;
    }).when(workerSizeProvider).addObserver(any(), any());
    taskManager = new TaskManager<>(executor, scheduledExecutorService, factory, schedulerFactory, workerSizeProvider);
    schedule = handler.read(new File(getClass().getClassLoader().getResource("queue/priority-task-scheduler.ops.json").getFile()));
  }

  @AfterEach
  void after() throws InterruptedException {
    taskManager.shutdown();
    executor.shutdownNow();
    scheduledExecutorService.shutdownNow();
    executor.awaitTermination(10, TimeUnit.MILLISECONDS);
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.SECONDS)
  void testAddScheduler() throws InterruptedException {
    updateTaskScheduler();
    assertEquals(RabbitMQQueueType.STREAM, schedule.getQueueType(), "Should have queueType STREAM");
    taskManager.removeTaskScheduler(schedule.getWorkerQueueName());
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.SECONDS)
  void testModifyQueue() throws InterruptedException {
    updateTaskScheduler();
    final PriorityTaskQueue queue = schedule.getQueues().get(0);

    assertTrue(taskManager.getTaskScheduleBucket(schedule.getWorkerQueueName()).hasTaskConsumer(queue.getQueueName()), "Queue should be present");
    queue.setPriority(30);
    updateTaskScheduler();
    assertTrue(taskManager.getTaskScheduleBucket(schedule.getWorkerQueueName()).hasTaskConsumer(queue.getQueueName()),
        "Queue should be still present");
    taskManager.removeTaskScheduler(schedule.getWorkerQueueName());
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.SECONDS)
  void testRemoveQueue() throws InterruptedException, IOException {
    updateTaskScheduler();
    assertTrue(taskManager.getTaskScheduleBucket(schedule.getWorkerQueueName()).hasTaskConsumer(schedule.getQueues().get(0).getQueueName()),
        "Queue should be present");
    schedule = handler.read(new File(getClass().getClassLoader().getResource("queue/priority-task-scheduler.ops_reduced.json").getFile()));

    updateTaskScheduler();
    assertFalse(taskManager.getTaskScheduleBucket(schedule.getWorkerQueueName()).hasTaskConsumer("ops"),
        "Queue should have been removed");
    taskManager.removeTaskScheduler(schedule.getWorkerQueueName());
  }

  private void updateTaskScheduler() throws InterruptedException {
    taskManager.updateTaskScheduler(schedule);
    while (!taskManager.isRunning(schedule.getWorkerQueueName())) {
      Thread.sleep(300);
    }
  }
}
