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

import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;

/**
 * The object mapping the supported configuration options of PriorityTaskSchedule.
 * This is for supporting both client queue configurations as array and as map.
 */
public class PriorityTaskScheduleFile extends PriorityTaskSchedule {

  private Map<String, PriorityTaskQueue> clientQueues = new HashMap<>();

  /**
   * This method should be called once this class is filled by reading from json.
   */
  public void updateQueues() {
    if (!clientQueues.isEmpty()) {
      setQueues(clientQueues.entrySet().stream().map(e -> {
        e.getValue().setQueueName(e.getKey());
        return e.getValue();
      }).toList());
    }
  }

  void setClientQueues(final Map<String, PriorityTaskQueue> clientQueues) {
    this.clientQueues = clientQueues;
  }
}
