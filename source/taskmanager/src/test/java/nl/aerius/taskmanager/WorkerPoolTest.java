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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import nl.aerius.taskmanager.domain.MessageMetaData;
import nl.aerius.taskmanager.exception.NoFreeWorkersException;
import nl.aerius.taskmanager.exception.TaskAlreadySentException;
import nl.aerius.taskmanager.mq.RabbitMQMessageMetaData;

/**
 * Test class for {@link WorkerPool}.
 */
class WorkerPoolTest {

  private static final String WORKER_QUEUE_NAME_TEST = "TEST";

  private WorkerPool workerPool;
  private TaskConsumer taskConsumer;
  private RabbitMQMessageMetaData message;
  private WorkerUpdateHandler workerUpdateHandler;
  private int numberOfWorkers;

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    numberOfWorkers = 0;
    workerUpdateHandler = new MockTaskFinishedHandler() {
      @Override
      public void onWorkerPoolSizeChange(final int numberOfWorkers) {
        WorkerPoolTest.this.numberOfWorkers = numberOfWorkers;
      }
    };
    workerPool = new WorkerPool(WORKER_QUEUE_NAME_TEST, new MockWorkerProducer(), workerUpdateHandler);
    taskConsumer = new TaskConsumer(mock(ExecutorService.class), "testqueue", false, mock(ForwardTaskHandler.class), new MockAdaptorFactory()) {
      @Override
      public void messageDelivered(final MessageMetaData message) {
        WorkerPoolTest.this.message = (RabbitMQMessageMetaData) message;
      }
    };
  }

  @Test
  void testWorkerPoolSizing() throws InterruptedException, IOException {
    assertSame(0, workerPool.getCurrentWorkerSize(), "Check if workerPool size is empty at start");
    workerPool.onNumberOfWorkersUpdate(10, 0, 0);
    assertSame(10, workerPool.getCurrentWorkerSize(), "Check if workerPool size is changed after sizing");
    assertEquals(10, numberOfWorkers, "Check if workerPool change handler called.");
    workerPool.reserveWorker();
    assertSame(10, workerPool.getCurrentWorkerSize(), "Check if workerPool size is same after reserving 1 worker");
    final Task task = createTask();
    workerPool.sendTaskToWorker(task);
    assertSame(10, workerPool.getCurrentWorkerSize(), "Check if workerPool size is same after reserving 1 worker");
    workerPool.releaseWorker(task.getId());
    assertSame(10, workerPool.getCurrentWorkerSize(), "Check if workerPool size is same after releasing 1 worker");
  }

  @Test
  void testNoFreeWorkers() throws IOException, InterruptedException {
    assertThrows(NoFreeWorkersException.class, () -> workerPool.sendTaskToWorker(createTask()));
  }

  @Test
  void testWorkerPoolScaleDown() throws IOException, InterruptedException {
    workerPool.onNumberOfWorkersUpdate(5, 0, 0);
    final Task task1 = createTask();
    workerPool.sendTaskToWorker(task1);
    final Task task2 = createTask();
    workerPool.sendTaskToWorker(task2);
    final Task task3 = createTask();
    workerPool.sendTaskToWorker(task3);
    assertSame(5, workerPool.getCurrentWorkerSize(), "Check if workerPool size is same after 2 workers running");
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    assertSame(1, workerPool.getWorkerSize(), "Check if workerPool size is lower");
    assertSame(3, workerPool.getCurrentWorkerSize(), "Check if current workerPool size is same after decreasing # workers");
    workerPool.releaseWorker(task1.getId());
    assertSame(2, workerPool.getCurrentWorkerSize(), "Check if workerPool size is lower, but not yet same as total because still process running");
    workerPool.releaseWorker(task2.getId());
    assertSame(1, workerPool.getCurrentWorkerSize(), "Check if workerPool size is lower");
    workerPool.releaseWorker(task3.getId());
    assertSame(1, workerPool.getCurrentWorkerSize(), "Check if workerPool size should remain the same");
  }

  @Test
  void testReleaseTaskTwice() throws IOException, InterruptedException {
    workerPool.onNumberOfWorkersUpdate(2, 0, 0);
    final Task task1 = createTask();
    workerPool.sendTaskToWorker(task1);
    final String id = task1.getId();
    workerPool.releaseWorker(id);
    final int currentWorkerSize1 = workerPool.getCurrentWorkerSize();
    workerPool.releaseWorker(id);
    final int currentWorkerSize2 = workerPool.getCurrentWorkerSize();
    assertEquals(currentWorkerSize1, currentWorkerSize2, "Check if task is not sent twice");
    assertEquals(2, workerPool.getCurrentWorkerSize(), "Check if task worker size not decreased to much");
  }

  @Disabled("Exception is not thrown anymore, so test ignored for now")
  @Test
  void testSendSameTaskTwice() throws IOException, InterruptedException {
    assertThrows(TaskAlreadySentException.class, () -> {
      workerPool.onNumberOfWorkersUpdate(3, 0, 0);
      final Task task1 = createTask();
      workerPool.sendTaskToWorker(task1);
      workerPool.sendTaskToWorker(task1);
    });
  }

  @Test
  void testMessageDeliverd() throws IOException, InterruptedException {
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    final Task task1 = createTask();
    workerPool.sendTaskToWorker(task1);
    assertNotSame(0, message.getDeliveryTag(), "Check if message is delivered");
  }

  /**
   * Peforms a reset() as if the watchdog triggered.
   */
  @Test
  void testZombies() throws IOException {
    workerPool.onNumberOfWorkersUpdate(5, 0, 0);
    for (int i = 0; i < 5; i++) {
      workerPool.sendTaskToWorker(createTask());
    }
    assertEquals(5, workerPool.getRunningWorkerSize());
    assertEquals(5, workerPool.getCurrentWorkerSize());
    assertEquals(5, workerPool.getWorkerSize());
    workerPool.onNumberOfWorkersUpdate(5, 0, 0);
    workerPool.reset(); // force reset as if it is called in onNumberOfWorkersUpdate.
    assertEquals(0, workerPool.getRunningWorkerSize());
    assertEquals(0, workerPool.getCurrentWorkerSize());
    assertEquals(0, workerPool.getWorkerSize());
    workerPool.onNumberOfWorkersUpdate(5, 0, 0); // update workers again and check if running as if started over
    assertEquals(0, workerPool.getRunningWorkerSize());
    assertEquals(5, workerPool.getCurrentWorkerSize());
    assertEquals(5, workerPool.getWorkerSize());
  }

  private Task createTask() {
    return new MockTask(taskConsumer, "calculator");
  }
}
