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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import nl.aerius.taskmanager.domain.RabbitMQQueueType;

/**
 * Test class for {@link RabbitMQQueueUtil}
 */
class RabbitMQQueueUtilTest {

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testQueueDeclareArgumentsLegacy(final boolean durable) {
    assertTrue(RabbitMQQueueUtil.queueDeclareArguments(durable, null).isEmpty(), "Should return empty map if type is null, and durable: " + durable);
  }

  @ParameterizedTest
  @MethodSource("combinations")
  void testQueueDeclareArguments(final boolean durable, final RabbitMQQueueType inputType, final RabbitMQQueueType expectedType) {
    assertEquals(expectedType.type(), RabbitMQQueueUtil.queueDeclareArguments(durable, inputType).get("x-queue-type"),
        "Should return the expected queue type");
  }

  private static List<Arguments> combinations() {
    return List.of(
        Arguments.of(false, RabbitMQQueueType.CLASSIC, RabbitMQQueueType.CLASSIC),
        // when durable is false always return classic because only queue type compatible with non-durable
        Arguments.of(false, RabbitMQQueueType.QUORUM, RabbitMQQueueType.CLASSIC),
        Arguments.of(false, RabbitMQQueueType.STREAM, RabbitMQQueueType.CLASSIC),

        Arguments.of(true, RabbitMQQueueType.CLASSIC, RabbitMQQueueType.CLASSIC),
        Arguments.of(true, RabbitMQQueueType.QUORUM, RabbitMQQueueType.QUORUM),
        Arguments.of(true, RabbitMQQueueType.STREAM, RabbitMQQueueType.STREAM));
  }
}
