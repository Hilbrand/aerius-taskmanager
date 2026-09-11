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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;

/**
 * Test class for {@link RabbitMQQueueMonitor}.
 */
class RabbitMQQueueMonitorTest {

  private static final String DUMMY = "dummy";
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testGetWorkerQueueState() {
    final ConnectionConfiguration configuration = ConnectionConfiguration.builder()
        .brokerHost(DUMMY).brokerPort(0).brokerUsername(DUMMY).brokerPassword(DUMMY).build();
    final AtomicInteger workerSize = new AtomicInteger();
    final WorkerSizeObserver mwps = status -> workerSize.set(status.consumers());
    final RabbitMQQueueMonitor rpm = new RabbitMQQueueMonitor(configuration) {
      @Override
      protected JsonNode getJsonResultFromApi(final String apiPath) throws IOException {
        try (final InputStream fr = getClass().getResourceAsStream("queue_aerius.worker.ops.txt");
            final InputStreamReader is = new InputStreamReader(fr)) {
          return objectMapper.readTree(is);
        }
      }
    };
    try {
      //      rpm.updateWorkerQueueState(DUMMY, mwps);
      assertEquals(4, workerSize.get(), "Number of workers");
    } finally {
      rpm.shutdown();
    }
  }

}
