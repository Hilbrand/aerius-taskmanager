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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

/**
 * Class that creates the Telemetry Gauge that uses the callback to record metrics when triggered.
 * When triggered it will iterate over the list of added {@link UsageMetric}s.
 */
class UsageMetricsReporter {

  private record UsageMetric(DoubleSupplier metricSupplier, Attributes attributes) {}

  private final Map<String, List<UsageMetric>> metricsMap = new ConcurrentHashMap<>();
  private final ObservableDoubleGauge gauge;

  public UsageMetricsReporter(final Meter meter, final String metricName, final String description) {
    gauge = meter
        .gaugeBuilder(metricName)
        .setDescription(description)
        .buildWithCallback(this::recordMetrics);
  }

  private void recordMetrics(final ObservableDoubleMeasurement measurement) {
    for (final Entry<String, List<UsageMetric>> workerMetrics : metricsMap.entrySet()) {
      for (final UsageMetric metric : workerMetrics.getValue()) {

        measurement.record(metric.metricSupplier().getAsDouble(), metric.attributes());
      }
    }
  }

  /**
   * Add a metric supplier for a specific worker queue/attributes.
   *
   * @param workerQueueName The worker queue this metric supplier is for
   * @param metricSupplier Supplies the metric value when called
   * @param attributes attributes for the metric
   */
  public void addMetrics(final String workerQueueName, final DoubleSupplier metricSupplier, final Attributes attributes) {
    metricsMap.computeIfAbsent(workerQueueName, k -> new ArrayList<>()).add(new UsageMetric(metricSupplier, attributes));
  }

  /**
   * Removes the metric reporter for the given worker queue to not report the metric anymore.
   *
   * @param workerQueueName Worker queue to remove the metric reporter for
   */
  public void removeMetrics(final String workerQueueName) {
    metricsMap.remove(workerQueueName);
  }

  public void close() {
    gauge.close();
    metricsMap.clear();
  }
}
