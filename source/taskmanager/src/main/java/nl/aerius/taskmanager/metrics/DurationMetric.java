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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import io.opentelemetry.api.common.Attributes;

import nl.aerius.taskmanager.client.TaskMetrics;

/**
 * Class to keep track of duration of tasks.
 */
class DurationMetric {

  /**
   * Duration metrics information
   *
   * @param avgDuration Average duration of tasks
   * @param count Count of number of tasks
   */
  public record DurationMetricValue(double avgDuration, int count) {
  }

  private long duration;
  private int count;
  private final Attributes attributes;

  public DurationMetric(final Attributes attributes) {
    this.attributes = attributes;
  }

  public Attributes getAttributes() {
    return attributes;
  }

  /**
   * Register a task received.
   *
   * @param taskMetrics task information
   */
  public synchronized void register(final TaskMetrics taskMetrics) {
    duration += taskMetrics.duration();
    count++;
  }

  /**
   * Returns the duration metric information since the last time this method was called. Internals are reset in this method to a new measure point.
   *
   * @return Average duration and count of tasks
   */
  public synchronized DurationMetricValue process() {
    final double avgDuration = count > 0
        ? BigDecimal.valueOf(duration).divide(BigDecimal.valueOf(count), MathContext.DECIMAL64).setScale(0, RoundingMode.HALF_UP).doubleValue()
        : 0.0;
    final DurationMetricValue value = new DurationMetricValue(avgDuration, count);

    duration = 0;
    count = 0;
    return value;
  }
}
