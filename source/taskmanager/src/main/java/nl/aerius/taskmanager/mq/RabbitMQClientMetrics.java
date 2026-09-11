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
package nl.aerius.taskmanager.mq;

import java.util.HashMap;
import java.util.Map;
import java.util.function.IntSupplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import nl.aerius.taskmanager.metrics.OpenTelemetryMetrics;

public class RabbitMQClientMetrics {

  private static final String METRIC_PREFIX = "aer.rabbitmq.client.queue";
  private static final String DESCRIPTION = "Number of tasks running on client queues";

  private final Map<String, ObservableDoubleGauge> gauges = new HashMap<>();

  public void addMetricWaiting(final IntSupplier countSupplier, final String workerQueueName, final String clientQueueName) {
    extracted(countSupplier, workerQueueName, clientQueueName);
  }

  private ObservableDoubleGauge extracted(final IntSupplier countSupplier, final String workerQueueName, final String clientQueueName) {
    return gauges.computeIfAbsent(workerQueueName, w -> {
      final Attributes queueAttributes = OpenTelemetryMetrics.queueAttributes(workerQueueName, clientQueueName, "state", "waiting");

      return OpenTelemetryMetrics.METER
          .gaugeBuilder(METRIC_PREFIX)
          .setDescription(DESCRIPTION)
          .buildWithCallback(result -> result.record(countSupplier.getAsInt(), queueAttributes));
    });
  }
}
