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

import org.junit.jupiter.api.Test;

import io.opentelemetry.api.common.Attributes;

/**
 * Test class for {@link OpenTelemetryMetrics}.
 */
class OpenTelemetryMetricsTest {

  @Test
  void testWorkerAttributes() {
    final Attributes attributes = OpenTelemetryMetrics.workerAttributes("aer.worker.OPS");

    assertEquals("ops", attributes.get(OpenTelemetryMetrics.WORKER_TYPE_ATTRIBUTE), "Should have expected worker type attribute");
  }

  @Test
  void testQueueAttributes() {
    final Attributes attributes = OpenTelemetryMetrics.queueAttributes("aer.worker.OPS", "aer.calculator.calculator_ui_small");

    assertEquals("ops", attributes.get(OpenTelemetryMetrics.WORKER_TYPE_ATTRIBUTE), "Should have expected worker type attribute");
    assertEquals("calculator_ui_small", attributes.get(OpenTelemetryMetrics.QUEUE_ATTRIBUTE), "Should have expected queue name attribute");
  }
}
