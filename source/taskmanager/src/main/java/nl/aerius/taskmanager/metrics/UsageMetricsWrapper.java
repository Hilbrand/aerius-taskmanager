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

import io.opentelemetry.api.metrics.Meter;

/**
 * Wraps several metrics to be reported on.
 * The metrics supported are limit (i.e. number of workers available).
 *
 */
class UsageMetricsWrapper {
  private final boolean hasWaiting;
  private final UsageMetricsReporter limitReporter;
  private final UsageMetricsReporter usageReporter;

  public UsageMetricsWrapper(final Meter meter, final String metricPrefix, final boolean hasWaiting) {
    this.hasWaiting = hasWaiting;
    limitReporter = new UsageMetricsReporter(meter, metricPrefix + ".worker.limit", "Report nunber of workers available");
    usageReporter = new UsageMetricsReporter(meter, metricPrefix + ".worker.usage", "Report worker usage");
  }

  public void add(final UsageMetricsProvider provider) {
    final String workerQueueName = provider.getWorkerQueueName();

    limitReporter.addMetrics(workerQueueName, provider::getNumberOfWorkers, OpenTelemetryMetrics.workerAttributes(workerQueueName));
    usageReporter.addMetrics(workerQueueName, provider::getNumberOfUsedWorkers,
        OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "used"));
    usageReporter.addMetrics(workerQueueName, provider::getNumberOfFreeWorkers,
        OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "free"));
    if (hasWaiting) {
      usageReporter.addMetrics(workerQueueName, provider::getNumberOfWaiting,
          OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "waiting"));
    }
  }

  public void remove(final String workerQueueName) {
    limitReporter.removeMetrics(workerQueueName);
    usageReporter.removeMetrics(workerQueueName);
  }

  public void close() {
    limitReporter.close();
    usageReporter.close();
  }
}
