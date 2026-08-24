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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import nl.aerius.taskmanager.domain.QueueWatchDogListener;

/**
 * Test class for {@link QueueWatchDog}.
 */
class QueueWatchDogTest {

  private static final String QUEUE_NAME = "queue1";

  @ParameterizedTest
  @MethodSource("isDeadTests")
  void testIsItDead(final int runningWorkers, final int finishedWorkers, final int numberOfMessages, final int expected, final String message) {
    final AtomicReference<LocalDateTime> now = new AtomicReference<>(LocalDateTime.now());
    final QueueWatchDog qwd = new QueueWatchDog(QUEUE_NAME) {
      @Override
      protected LocalDateTime now() {
        return now.get();
      }
    };
    final QueueWatchDogListener listener = mock(QueueWatchDogListener.class);

    qwd.addQueueWatchDogListener(listener);
    IntStream.range(0, runningWorkers).forEach(i -> qwd.onWorkDispatched(String.valueOf(i), null));
    IntStream.range(0, finishedWorkers).forEach(i -> qwd.onWorkerFinished(String.valueOf(i), null));

    qwd.onNumberOfWorkersUpdate(0, numberOfMessages, 0);
    // reset should never trigger the first time the problem was reported.
    verify(listener, never()).reset();

    // Fast forward 20 minutes to trigger reset if there is a problem.
    now.set(now.get().plusMinutes(20));
    qwd.onNumberOfWorkersUpdate(0, numberOfMessages, 0);
    verify(listener, times(expected)).reset();
    // Call update again. This should not trigger reset again because we just called reset.
    qwd.onNumberOfWorkersUpdate(0, numberOfMessages, 0);
    verify(listener, times(expected)).reset();
  }

  private static List<Arguments> isDeadTests() {
    return List.of(
        Arguments.of(5, 5, 0, 0, "No running workers, with no messages, no problem"),
        Arguments.of(5, 5, 10, 0, "No running workers, 10 messages, no problem"),
        Arguments.of(5, 1, 10, 0, "Running workers, with messages, no problem"),
        Arguments.of(5, 1, 0, 1, "Running workers, with no messages, after specified time; yes reset"));
  }
}
