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
package nl.aerius.taskmanager.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.scheduler.priorityqueue.PriorityTaskScheduleFile;

/**
 * Test class for {@link EnvOverrideBeanDeserializerModifier}.
 */
class EnvOverrideBeanDeserializerModifierTest {

  private static final String SCHEDULE = """
      {
        "workerQueueName": "chunker",
        "durable": "true",
        "clientQueues": {
          "calculator_ui_small":
            {
              "priority": 4,
              "maxCapacityUse": 0.8
            }
        }
      }
      """;

  @Test
  void testOverrideWithEnv() throws JsonMappingException, JsonProcessingException {
    final ObjectMapper objectMapper = EnvOverrideBeanDeserializerModifier.objectMapper("AERIUS.pts", this::envVariableMapper);
    final PriorityTaskSchedule schedule = objectMapper.readValue(SCHEDULE, PriorityTaskScheduleFile.class);

    ((PriorityTaskScheduleFile) schedule).updateQueues();
    assertEquals("ops", schedule.getWorkerQueueName(), "Worker queue name should be overriden");
    assertEquals(10, schedule.getQueues().get(0).getPriority(), "Queue priority should be overriden");
    assertEquals(100, schedule.getMaxWorkersAvailable(), "Max workers available not set, but with environment variable should be 100");
  }

  private String envVariableMapper(final String key) {
    return switch (key) {
      case "AERIUS_PTS_WORKERQUEUENAME" -> "ops";
      case "AERIUS_PTS_MAXWORKERSAVAILABLE" -> "100";
      case "AERIUS_PTS_CLIENTQUEUES_CALCULATOR_UI_SMALL_PRIORITY" -> "10";
      default -> null;
    };
  }
}
