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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

/**
 * Test class for {@link RabbitMQQueueMonitor}.
 */
class RabbitMQQueueMonitorTest {

  private static final String DUMMY = "dummy";
  private static final String QUEUENAME = "aerius.worker.ops";
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testGetWorkerQueueState() {
    assertRabbitMQQueueMonitor("queue_aerius.worker.ops.txt", 4, 3, 5, rpm -> rpm.getWorkerQueueState(DUMMY));
  }

  @Test
  void testGetWorkerQueueStates() {
    assertRabbitMQQueueMonitor("queue_aerius.txt", 51, 10, 30, rpm -> rpm.getWorkerQueueStates().get(QUEUENAME));
  }

  private void assertRabbitMQQueueMonitor(final String filename, final int expectedConsumers, final int expectedMessages,
      final int expectedUnacknowledged, final Function<RabbitMQQueueMonitor, RabbitMQQueueStatus> collector) {
    final ConnectionConfiguration configuration = ConnectionConfiguration.builder()
        .brokerHost(DUMMY).brokerPort(0).brokerUsername(DUMMY).brokerPassword(DUMMY).build();
    final RabbitMQQueueMonitor rpm = new RabbitMQQueueMonitor(configuration) {
      @Override
      protected JsonNode getJsonResultFromApi(final String apiPath) throws IOException {
        try (final InputStream fr = getClass().getResourceAsStream(filename);
            final InputStreamReader is = new InputStreamReader(fr)) {
          return objectMapper.readTree(is);
        }
      }
    };
    try {
      final RabbitMQQueueStatus status = collector.apply(rpm);

      assertEquals(expectedConsumers, status.consumers(), "Number of workers");
      assertEquals(expectedMessages, status.messages(), "Number of messages");
      assertEquals(expectedUnacknowledged, status.unacknowledged(), "Number of unacknowledged");
    } finally {
      rpm.shutdown();
    }
  }

}
