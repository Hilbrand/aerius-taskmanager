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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import nl.aerius.taskmanager.domain.RabbitMQQueueType;

/**
 * Utility to construct queueDeclaire arguments.
 */
final class RabbitMQQueueUtil {

  private static final Set<RabbitMQQueueType> PERSISTENT_QUEUE_TYPES = EnumSet.of(RabbitMQQueueType.QUORUM, RabbitMQQueueType.STREAM);

  private static final String ARG_QUEUE_TYPE = "x-queue-type";

  private RabbitMQQueueUtil() {}

  /**
   * Returns a map with queueDeclare arguments.
   * If queueType is null assume backward compatibility and don't set x-queue-type.
   * If durable always use classic type because other types are durable, and therefore not compatible with durable.
   *
   * @param durable if queue is durable
   * @param queueType the queueType to be set
   * @return
   */
  public static Map<String, Object> queueDeclareArguments(final boolean durable, final RabbitMQQueueType queueType) {
    if (queueType == null) {
      return Map.of();
    }
    final RabbitMQQueueType actualType = !durable && PERSISTENT_QUEUE_TYPES.contains(queueType) ? RabbitMQQueueType.CLASSIC : queueType;

    return Map.of(ARG_QUEUE_TYPE, actualType.type());
  }
}
