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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import nl.aerius.taskmanager.domain.Message;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskConsumer;

/**
 * Test class for {@link GroupedPriorityQueue}.
 */
class GroupedPriorityQueueTest {

  private GroupedPriorityQueue queue;

  @Test
  void testPoll() {
    queue = new GroupedPriorityQueue(10, (a, b) -> 0);
    final Task task1 = mockTask("1");
    final Task task2 = mockTask("1");
    final Task task3 = mockTask("2");
    final Task task4 = mockTask("2");
    final Task task5 = mockTask("1");
    queue.add(task1);
    queue.add(task2);
    queue.add(task3);
    queue.add(task4);
    queue.add(task5);

    assertNotNull(queue.peek(), "Queue should have task at the queue.");
    assertEquals(task1, queue.poll(), "Poll should return first task added.");
    assertEquals(task3, queue.poll(), "Poll should return first task of task with correlationId 2.");
    assertEquals(task2, queue.poll(), "Poll should return second task of correlationId 1, because it's at the front of the queue.");
    assertEquals(task4, queue.poll(), "Poll should return second task of correlationId 2, because it should be at front of the queue.");
    assertEquals(task5, queue.poll(), "Poll should return last added task.");
  }

  private static Task mockTask(final String correlationId) {
    final TaskConsumer taskConsumer = mock(TaskConsumer.class);
    final Task task = new Task(taskConsumer);
    final Message data = mock(Message.class);
    doReturn(correlationId).when(data).getCorrelationId();

    task.setData(data);
    return task;
  }
}
