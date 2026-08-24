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

import java.util.function.DoubleSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link TaskManagerUsageMetricsProvider}.
 */
class TaskManagerUsageMetricsProviderTest {

  private TaskManagerUsageMetricsProvider provider;

  @BeforeEach
  void beforeEach() {
    provider = new TaskManagerUsageMetricsProvider("TEST");
  }

  @Test
  void testLoad() throws InterruptedException {
    assertMetricAndZero(5, 10, provider::getLoad, 50, "Expected a load of 50%.", 0);
  }

  @Test
  void testNumberOfWorkers() throws InterruptedException {
    assertMetricAndZero(4, 8, provider::getNumberOfWorkers, 8, "Expected 8 workers total.", 8);
  }

  @Test
  void testNumberOfUsedWorkers() throws InterruptedException {
    assertMetricAndZero(4, 10, provider::getNumberOfUsedWorkers, 4, "Expected 4 worker being used.", 0);
  }

  @Test
  void testNumberOfFreeWorkers() throws InterruptedException {
    assertMetricAndZero(3, 10, provider::getNumberOfFreeWorkers, 7, "Expected 7 worker to be free.", 10);
  }

  private void assertMetricAndZero(final int numberOfUsed, final int numberOfWorkers, final DoubleSupplier supplier, final int expected,
      final String description, final int expectedAfterReset) throws InterruptedException {
    assertMetric(numberOfUsed, numberOfWorkers, supplier, expected, description, expectedAfterReset);
    assertMetric(0, 0, supplier, 0, "0 workers is not correctly handled.", 0);
  }

  private void assertMetric(final int numberOfUsed, final int numberOfWorkers, final DoubleSupplier supplier, final int expected,
      final String description, final int expectedAfterReset) throws InterruptedException {
    provider.register(numberOfUsed, numberOfWorkers);
    supplier.getAsDouble();
    // Add a little delay to initalise the time
    Thread.sleep(10);
    assertEquals(expected, supplier.getAsDouble(), description);
    provider.reset();
    Thread.sleep(10);
    assertEquals(expectedAfterReset, supplier.getAsDouble(), "Not the expected number after a reset");
  }
}
