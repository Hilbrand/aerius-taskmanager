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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import nl.aerius.taskmanager.metrics.OpenTelemetryMetrics;
import nl.aerius.taskmanager.metrics.UsageMetricsProvider;

/**
 * Set up metric collection for this worker pool with the given type name.
 */
public final class WorkerPoolMetrics {

  private static final Map<String, ObservableDoubleGauge> REGISTERED_METRICS = new HashMap<>();

  private enum WorkerPoolMetricType {
    // @formatter:off
    WORKER_SIZE(UsageMetricsProvider::getNumberOfWorkers, "Number of workers based on internal state of taskmanager"),
    @Deprecated
    CURRENT_WORKER_SIZE(WorkerPool::getReportedWorkerSize,
        "Current number of workers according to taskmanager (deprecated replaced with 'aer.taskmanager.workerppol.worker.usage')"),
    @Deprecated
    RUNNING_WORKER_SIZE(UsageMetricsProvider::getNumberOfUsedWorkers,
        "Used number of workers according to taskmanager (deprecated replaced with 'aer.taskmanager.workerppol.worker.usage')");
    // @formatter:on

    private final Function<WorkerPool, Integer> function;
    private final String description;

    WorkerPoolMetricType(final Function<WorkerPool, Integer> function, final String description) {
      this.function = function;
      this.description = description;
    }

    int getValue(final WorkerPool workerPool) {
      return function.apply(workerPool);
    }

    String getGaugeName() {
      return "aer.taskmanager." + name().toLowerCase(Locale.ROOT);
    }

    String getDescription() {
      return description;
    }
  }

  private WorkerPoolMetrics() {
    // Util-like class
  }

  public static void setupMetrics(final WorkerPool workerPool, final String workerQueueName) {
    final Attributes attributes = OpenTelemetryMetrics.workerAttributes(workerQueueName);

    for (final WorkerPoolMetricType metricType : WorkerPoolMetricType.values()) {
      REGISTERED_METRICS.put(gaugeIdentifier(workerQueueName, metricType),
          OpenTelemetryMetrics.METER.gaugeBuilder(metricType.getGaugeName())
              .setDescription(metricType.getDescription())
              .buildWithCallback(
                  result -> result.record(metricType.getValue(workerPool), attributes)));
    }
  }

  public static void removeMetrics(final String workerQueueName) {
    for (final WorkerPoolMetricType metricType : WorkerPoolMetricType.values()) {
      final String gaugeId = gaugeIdentifier(workerQueueName, metricType);
      if (REGISTERED_METRICS.containsKey(gaugeId)) {
        REGISTERED_METRICS.remove(gaugeId).close();
      }
    }
  }

  private static String gaugeIdentifier(final String workerQueueName, final WorkerPoolMetricType gaugeType) {
    return workerQueueName + "_" + gaugeType.name();
  }

}
