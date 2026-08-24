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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;

import nl.aerius.taskmanager.client.TaskMetrics;

/**
 * Test class for {@link PerformanceMetricsReporter}.
 *
 */
@ExtendWith(MockitoExtension.class)
class PerformanceMetricsReporterTest {

  private static final String QUEUE_GROUP_NAME = "ops";
  private static final String QUEUE_1 = "queue 1";
  private static final String QUEUE_2 = "queue 2";

  private final Map<String, DoubleGauge> mockedGauges = new HashMap<>();

  private @Mock Meter mockedMeter;
  private @Mock ScheduledExecutorService scheduledExecutorService;
  private @Captor ArgumentCaptor<Runnable> methodCaptor;
  private @Captor ArgumentCaptor<Double> doubleValue1Captor;
  private @Captor ArgumentCaptor<Double> doubleValue2Captor;

  private PerformanceMetricsReporter reporter;

  @BeforeEach
  void beforeEach() {
    final DoubleGaugeBuilder mockGaugeBuilder = mock(DoubleGaugeBuilder.class);
    doAnswer(inv -> {
      final DoubleGauge gauge = mock(DoubleGauge.class);

      doReturn(mockGaugeBuilder).when(mockGaugeBuilder).setDescription(any());
      doReturn(gauge).when(mockGaugeBuilder).build();
      mockedGauges.put(inv.getArgument(0, String.class), gauge);
      return mockGaugeBuilder;
    }).when(mockedMeter).gaugeBuilder(any());
    lenient().doReturn(mockGaugeBuilder).when(mockGaugeBuilder).setDescription(any());

    reporter = new PerformanceMetricsReporter(scheduledExecutorService, QUEUE_GROUP_NAME, mockedMeter);
    verify(scheduledExecutorService).scheduleWithFixedDelay(methodCaptor.capture(), anyLong(), anyLong(),
        any(TimeUnit.class));
  }

  @Test
  void testOnWorkDispatched() {
    reporter.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    reporter.onWorkDispatched("2", createMap(QUEUE_2, 200L));
    methodCaptor.getValue().run();
    assertGaugeCalls("dispatched", "wait", 2.0, v -> v > 99.0);
  }

  @Test
  void testOnWorkerFinished() {
    reporter.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    reporter.onWorkerFinished("1", createMap(QUEUE_1, 100L));
    reporter.onWorkerFinished("2", createMap(QUEUE_2, 200L));
    methodCaptor.getValue().run();
    assertGaugeCalls("work", "duration", 2.0, v -> v > 99.0);
  }

  private void assertGaugeCalls(final String label, final String type, final double expected, final Predicate<Double> duration) {
    verify(mockedGauges.get("aer.taskmanager." + label)).set(eq(expected), any());
    verify(mockedGauges.get("aer.taskmanager.%s.%s".formatted(label, type))).set(doubleValue1Captor.capture(), any());
    verify(mockedGauges.get("aer.taskmanager.%s.queue".formatted(label))).set(eq(expected), any());
    verify(mockedGauges.get("aer.taskmanager.%s.queue.%s".formatted(label, type))).set(doubleValue1Captor.capture(),
        any());
    doubleValue1Captor.getAllValues()
    .forEach(v -> assertTrue(duration.test(v), "Duration should report at least 100.0 as it is the offset of the start time, but was " + v));
  }

  @Test
  void testReset() throws InterruptedException {
    reporter.onWorkDispatched("1", createMap(QUEUE_1, 100L));
    reporter.onWorkDispatched("2", createMap(QUEUE_2, 200L));
    Thread.sleep(2); // Add a bit of delay to get some time frame between updates..
    reporter.reset();
    methodCaptor.getValue().run();
    // Verify dispatched metrics have been reset.
    assertGaugeCalls("dispatched", "wait", 0.0, v -> v == 0.0);
  }

  private Map<String, Object> createMap(final String queueName, final long duration) {
    return new TaskMetrics().duration(duration).queueName(queueName).start(System.currentTimeMillis() - 100).build();
  }
}
