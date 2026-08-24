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
package nl.aerius.taskmanager.scheduler.priorityqueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntSupplier;

import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import nl.aerius.taskmanager.metrics.OpenTelemetryMetrics;

/**
 * Class to collect metrics about the number of tasks actively running from a specific client queue.
 */
class PriorityTaskSchedulerMetrics {

  /**
   * @deprecated replaced by "aer.taskmanager.client.queue" metric.
   */
  @Deprecated
  private static final String METRIC_PREFIX_LEGACY = "aer.taskmanager.running_client_size";
  private static final String METRIC_PREFIX = "aer.taskmanager.client.queue";
  private static final String DESCRIPTION = "Number of tasks running on client queues";

  private final Map<String, ObservableDoubleGauge> metrics = new HashMap<>();
  private final Map<String, ObservableDoubleGauge> usageMetrics = new HashMap<>();
  private final Map<String, ObservableDoubleGauge> waitingMetrics = new HashMap<>();

  /**
   * Adds collecting metrics to count the number of active tasks by client queue.
   *
   * @param countSupplier supplier that returns the active count for the given client queue name when called
   * @param workerQueueName worker queue name
   * @param clientQueueName client queue name
   */
  public void addMetric(final IntSupplier countSupplier, final String workerQueueName, final String clientQueueName) {
    metrics.put(clientQueueName, OpenTelemetryMetrics.METER
        .gaugeBuilder(METRIC_PREFIX_LEGACY)
        .setDescription(DESCRIPTION)
        .buildWithCallback(
            result -> result.record(countSupplier.getAsInt(),
                OpenTelemetryMetrics.queueAttributes(workerQueueName, clientQueueName, "state", "used"))));
    metrics.put(clientQueueName, OpenTelemetryMetrics.METER
        .gaugeBuilder(METRIC_PREFIX)
        .setDescription(DESCRIPTION)
        .buildWithCallback(
            result -> result.record(countSupplier.getAsInt(),
                OpenTelemetryMetrics.queueAttributes(workerQueueName, clientQueueName, "state", "used"))));
  }

  /**
   * Adds collecting metrics to count the number of tasks waiting on the client queue.
   *
   * @param countSupplier supplier that returns the active count for the given client queue name when called
   * @param workerQueueName worker queue name
   * @param clientQueueName client queue name
   */
  public void addMetricWaiting(final IntSupplier countSupplier, final String workerQueueName, final String clientQueueName) {
    waitingMetrics.put(clientQueueName, OpenTelemetryMetrics.METER
        .gaugeBuilder(METRIC_PREFIX)
        .setDescription(DESCRIPTION)
        .buildWithCallback(
            result -> result.record(countSupplier.getAsInt(),
                OpenTelemetryMetrics.queueAttributes(workerQueueName, clientQueueName, "state", "waiting"))));
  }

  /**
   * Remove collecting metrics for the given client queue name.
   *
   * @param clienQueueName
   */
  public void removeMetric(final String clienQueueName) {
    Optional.ofNullable(metrics.remove(clienQueueName)).ifPresent(ObservableDoubleGauge::close);
    Optional.ofNullable(usageMetrics.remove(clienQueueName)).ifPresent(ObservableDoubleGauge::close);
    Optional.ofNullable(waitingMetrics.remove(clienQueueName)).ifPresent(ObservableDoubleGauge::close);
  }
}
