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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;

/**
 * Test class for {@link RabbitMQWorkerSizeProvider}
 */
class RabbitMQWorkerSizeProviderTest extends AbstractRabbitMQTest {

  private static final String TEST_QUEUE = "test";

  private RabbitMQWorkerSizeProvider provider;

  @Override
  @BeforeEach
  void setUp() throws Exception {
    brokerManagementRefreshRate = 5;
    super.setUp();
    provider = new RabbitMQWorkerSizeProvider(executor, factory);
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void testTriggerWorkerQueueState() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final RabbitMQQueueMonitor mockMonitor = mock(RabbitMQQueueMonitor.class);

    doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(mockMonitor).updateWorkerQueueState(eq(TEST_QUEUE), any());
    provider.putMonitor(TEST_QUEUE, mockMonitor);
    // Call twice, which should result in only 1 call to updateWorkerQueueState
    provider.triggerWorkerQueueState(TEST_QUEUE);
    provider.triggerWorkerQueueState(TEST_QUEUE);
    latch.await();
    verify(mockMonitor).updateWorkerQueueState(eq(TEST_QUEUE), any());
  }

  @Test
  void testStartShutdown() throws IOException {
    final WorkerSizeObserver dummyObserver = mock(WorkerSizeObserver.class);

    provider.addObserver(TEST_QUEUE, dummyObserver);
    provider.start();
    provider.shutdown();
    assertFalse(provider.removeObserver("test"), "Observer should already have been removed");
  }
}
