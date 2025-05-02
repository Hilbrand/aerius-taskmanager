/*
 * Copyright the State of the Netherlands
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
import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.scheduler.TaskScheduler;
import nl.aerius.taskmanager.scheduler.TaskScheduler.TaskSchedulerFactory;

/**
 * Factory to create a scheduler.
 */
public class PriorityTaskSchedulerFactory implements TaskSchedulerFactory<PriorityTaskQueue, PriorityTaskSchedule> {
  private final PriorityTaskSchedulerFileHandler handler = new PriorityTaskSchedulerFileHandler();

  @Override
  public TaskScheduler<PriorityTaskQueue> createScheduler(final String workerQueueName, final boolean dynamicQueues) {
    final PriorityQueueMap map = dynamicQueues ? new PriorityQueueMap(this::dynamicQueueMapper) : new PriorityQueueMap();

    return new PriorityTaskScheduler(map, workerQueueName);
  }

  private String dynamicQueueMapper(final String queueName) {
    final int nrOfDots = (int) queueName.chars().filter(c -> c == '.').count();
    if (nrOfDots == 2) {
      return queueName;
    } else {
      System.out.println("> NAME:" + queueName.substring(0, queueName.lastIndexOf('.')));
      return queueName.substring(0, queueName.lastIndexOf('.'));
    }
  }

  @Override
  public PriorityTaskSchedulerFileHandler getHandler() {
    return handler;
  }
}
