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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import nl.aerius.taskmanager.StartupGuard;
import nl.aerius.taskmanager.client.TaskMetrics;

/**
 * Test class for {@link TaskManagerMetricsRegister}.
 */
@ExtendWith(MockitoExtension.class)
public class TaskManagerMetricsRegisterTest {

  private static final String QUEUE_1 = "queue 1";
  private static final String QUEUE_2 = "queue 2";

  private @Mock TaskManagerUsageMetricsProvider taskManagerUsageMetricsProvider;

  private @Captor ArgumentCaptor<Integer> startUpNrOfMessagesCaptor;
  private @Captor ArgumentCaptor<Integer> lastNrOfMessagesCaptor;

  private StartupGuard startupGuard;
  private TaskManagerMetricsRegister register;

  @BeforeEach
  void beforeEach() {
    startupGuard = new StartupGuard();
    register = new TaskManagerMetricsRegister(taskManagerUsageMetricsProvider, startupGuard);
  }

  @Test
  void testOnWorkDispatched() {
    startUp(10, 0);
    // Should have called register only once, but with 0 delta
    verifyTaskManagerUsageMetricsProvider(1, 0, startUpNrOfMessagesCaptor);
    register.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    register.onWorkDispatched("2", createMap(QUEUE_2, 200L));
    register.onNumberOfWorkersUpdate(10, 2, 2);
    // Should have called register 4 times (startup, 2 for dispatch and 1 for update.
    // But total delta should be +2 for the 2 dispatched messages.
    verifyTaskManagerUsageMetricsProvider(4, 2, lastNrOfMessagesCaptor);
  }

  @Test
  void testOnWorkerFinished() {
    startUp(2, 2);
    // Should have called register only once, but with 2 as delta as it should match total number of messages.
    verifyTaskManagerUsageMetricsProvider(1, 2, startUpNrOfMessagesCaptor);
    register.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    register.onWorkerFinished("1", createMap(QUEUE_1, 100L));
    register.onWorkerFinished("2", createMap(QUEUE_2, 200L));
    register.onNumberOfWorkersUpdate(10, 2, 2);

    // Should have called register 5 times (startup, 1 for dispatch, 2 for finished and 1 for update.
    // But total delta should be 0 for 2 startup + 1 dispatch - 2 for finish..
    verifyTaskManagerUsageMetricsProvider(5, 1, lastNrOfMessagesCaptor);
  }

  @Test
  void testReset() throws InterruptedException {
    startUp(4, 1);
    register.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    register.onWorkDispatched("2", createMap(QUEUE_2, 200L));
    Thread.sleep(2); // Add a bit of delay to get some time frame between updates..
    register.reset();

    // Verify load metric have been reset.
    verify(taskManagerUsageMetricsProvider, times(1)).reset();
  }

  private void verifyTaskManagerUsageMetricsProvider(final int times, final int sum, final ArgumentCaptor<Integer> captor) {
    verify(taskManagerUsageMetricsProvider, times(times)).register(captor.capture(), anyInt());
    assertEquals(sum, captor.getAllValues().stream().mapToInt(Integer::intValue).sum(),
        "Should have registered a total sum of " + sum);
  }

  private void startUp(final int numberOfWorkers, final int numberOfMessages) {
    register.onNumberOfWorkersUpdate(numberOfWorkers, numberOfMessages, 0);
    startupGuard.onNumberOfWorkersUpdate(numberOfWorkers, numberOfMessages, 0);
  }

  private Map<String, Object> createMap(final String queueName, final long duration) {
    return new TaskMetrics().duration(duration).queueName(queueName).start(System.currentTimeMillis() - 100).build();
  }
}
