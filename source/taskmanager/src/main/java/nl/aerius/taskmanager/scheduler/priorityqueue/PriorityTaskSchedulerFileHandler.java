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

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import nl.aerius.taskmanager.config.EnvOverrideBeanDeserializerModifier;
import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.scheduler.SchedulerFileConfigurationHandler;

/**
 * Handler to read and write Priority Task Scheduler configuration files.
 */
public class PriorityTaskSchedulerFileHandler implements SchedulerFileConfigurationHandler<PriorityTaskQueue, PriorityTaskSchedule> {

  private static final String PREFIX = "AERIUS_PTS_";

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public PriorityTaskSchedule read(final File file) throws IOException {
    final PriorityTaskSchedule fileSchedule = readFromFile(objectMapper, file);

    return readFromFile(EnvOverrideBeanDeserializerModifier.objectMapper(PREFIX + fileSchedule.getWorkerQueueName()), file);
  }

  private PriorityTaskSchedule readFromFile(final ObjectMapper objectMapper, final File file) throws IOException {
    final PriorityTaskScheduleFile schedule = objectMapper.readValue(file, PriorityTaskScheduleFile.class);

    schedule.updateQueues();
    return schedule;
  }
}
