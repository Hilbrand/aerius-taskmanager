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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.ToDoubleBiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link LoadMetric}.
 */
@ExtendWith(MockitoExtension.class)
class LoadMetricTest {

  @Mock ToDoubleBiFunction<Integer, Integer> countFunction;
  @Captor ArgumentCaptor<Integer> countUsedWorkersCaptor;
  @Captor ArgumentCaptor<Integer> countWorkersCaptor;

  private LoadMetric loadMetric;

  @BeforeEach
  void beforeEach() {
    doAnswer(a -> ((Integer) a.getArgument(0)).doubleValue()).when(countFunction).applyAsDouble(any(), any());

    loadMetric = new LoadMetric(countFunction, (total, time) -> total);
  }

  /**
   * Register
   */
  @Test
  void testProcess() throws InterruptedException {
    loadMetric.register(5, 10);
    loadMetric.process();
    verify(countFunction, times(2)).applyAsDouble(countWorkersCaptor.capture(),countUsedWorkersCaptor.capture());
    assertEquals(10, countWorkersCaptor.getValue(), "Should got original number of workers");
    assertEquals(5, countUsedWorkersCaptor.getValue(), "Should got original number of used workers");
  }

  @Test
  void testWorkersChanged() throws InterruptedException {
    loadMetric.register(0, 10);
    loadMetric.register(0, 10);
    // 2nd call to register should not trigger updating internal state.
    verify(countFunction, times(1)).applyAsDouble(countWorkersCaptor.capture(), countUsedWorkersCaptor.capture());
    assertEquals(0, countWorkersCaptor.getValue(), "Should got inital number of workers, which was 0");
    assertEquals(0, countUsedWorkersCaptor.getValue(), "Should got inital number of used workers, which was 0");
    loadMetric.register(0, 5);
    // call with changed number of workers should trigger update state.
    verify(countFunction, times(2)).applyAsDouble(any(), any());
    // process call should also trigger update.
    loadMetric.process();
    verify(countFunction, times(3)).applyAsDouble(any(), any());
  }

  @Test
  void testReset() throws InterruptedException {
    // call 2 times. because first time used workers is initialized.
    loadMetric.register(5, 10);
    loadMetric.register(6, 10);
    verify(countFunction, times(2)).applyAsDouble(any(), countUsedWorkersCaptor.capture());
    assertEquals(5, countUsedWorkersCaptor.getValue(), "Should get the number of used workers of the first call");
    loadMetric.reset();
    verify(countFunction, times(3)).applyAsDouble(any(), countUsedWorkersCaptor.capture());
    assertEquals(0, countUsedWorkersCaptor.getValue(), "Reset has set used number to 0, so that is at is expected here.");
    loadMetric.register(8, 10);
    verify(countFunction, times(4)).applyAsDouble(any(), countUsedWorkersCaptor.capture());
    assertEquals(0, countUsedWorkersCaptor.getValue(), "This call should get prevoious value of 0 used workers");
  }
}
