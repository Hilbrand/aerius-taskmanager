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

import java.util.Locale;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

/**
 * Class to help with Open Telemetry metrics within the TaskManager.
 */
public final class OpenTelemetryMetrics {

  public static final Meter METER = GlobalOpenTelemetry.getMeter("nl.aerius.taskmanager");

  static final AttributeKey<String> WORKER_TYPE_ATTRIBUTE = AttributeKey.stringKey("worker_type");
  static final AttributeKey<String> QUEUE_ATTRIBUTE = AttributeKey.stringKey("queue_name");

  private OpenTelemetryMetrics() {
    // Util class
  }

  public static Attributes workerAttributes(final String workerType) {
    return Attributes.builder()
        .put(WORKER_TYPE_ATTRIBUTE, onlyLastPart(workerType))
        .build();
  }

  public static Attributes workerAttributes(final String workerType, final String atttributeName, final String attributeValue) {
    return Attributes.builder()
        .put(WORKER_TYPE_ATTRIBUTE, onlyLastPart(workerType))
        .put(atttributeName, attributeValue)
        .build();
  }

  public static Attributes queueAttributes(final String workerQueueName, final String queueName) {
    return Attributes.builder()
        .put(WORKER_TYPE_ATTRIBUTE, onlyLastPart(workerQueueName))
        .put(QUEUE_ATTRIBUTE, onlyLastPart(queueName))
        .build();
  }

  public static Attributes queueAttributes(final String workerQueueName, final String queueName, final String atttributeName,
      final String attributeValue) {
    return Attributes.builder()
        .put(WORKER_TYPE_ATTRIBUTE, onlyLastPart(workerQueueName))
        .put(QUEUE_ATTRIBUTE, onlyLastPart(queueName))
        .put(atttributeName, attributeValue)
        .build();
  }

  private static String onlyLastPart(final String name) {
    final int lastIndex = name.lastIndexOf('.');

    return (lastIndex < 0 ? name : name.substring(lastIndex + 1)).toLowerCase(Locale.ROOT);
  }
}
