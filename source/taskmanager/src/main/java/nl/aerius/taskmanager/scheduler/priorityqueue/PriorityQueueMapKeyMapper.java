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

import nl.aerius.taskmanager.domain.TaskRecord;

/**
 * Key mapper to group tasks per queue. Used for scheduling tasks by fetching 1 message from the queue at a time.
 */
public class PriorityQueueMapKeyMapper {

  public String key(final TaskRecord taskRecord) {
    return taskRecord.queueName();
  }

  public String queueName(final String key) {
    return key;
  }
}
