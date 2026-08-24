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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Stream;

import nl.aerius.taskmanager.domain.Task;

/**
 * The {@link GroupedPriorityQueue} is a priority queue that holds back tasks with the same correlationId from the main queue.
 * When a task with the same correlationId is added to the queue it parks it on a separate internal queue.
 * When the first task added is polled from the queue, it will take the next task from the internal queue and puts it on the main queue.
 *
 * The idea behind this queue is to make it possible to have a priority system where tasks on the same queue but with different correlationId can
 * be prioritised depending on how many of tasks for a specific correlationId are already being processed on a worker.
 */
class GroupedPriorityQueue implements Queue<Task> {

  private final Queue<Task> queue;
  private final Map<String, Queue<Task>> groupedQueue = new HashMap<>();

  public GroupedPriorityQueue(final int initialSize, final Comparator<Task> comparator) {
    queue = new PriorityQueue<>(initialSize, comparator);
  }

  @Override
  public synchronized boolean add(final Task task) {
    final String correlationId = task.getTaskRecord().correlationId();

    if (queue.stream().anyMatch(t -> t.getTaskRecord().correlationId().equals(correlationId))) {
      groupedQueue.computeIfAbsent(correlationId, k -> new ArrayDeque<>()).add(task);
    } else {
      queue.add(task);
    }
    return true;
  }

  @Override
  public Task peek() {
    return queue.peek();
  }

  @Override
  public synchronized Task poll() {
    final Task task = queue.poll();
    final String correlationId = task.getTaskRecord().correlationId();
    final Queue<Task> tasksQueue = groupedQueue.get(correlationId);

    if (tasksQueue != null) {
      queue.add(tasksQueue.poll());
      if (tasksQueue.isEmpty()) {
        groupedQueue.remove(correlationId);
      }
    }
    return task;
  }

  public synchronized int getGroupSize() {
    return groupedQueue.entrySet().stream().mapToInt(e -> e.getValue().size()).sum();
  }

  @Override
  public Stream<Task> stream() {
    return queue.stream();
  }

  // Only methods actually used in our code are implemented. All other methods will throw an exception if used as a reminder if the code would start
  // using these methods they need to be implemented.

  @Override
  public boolean addAll(final Collection<? extends Task> arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean contains(final Object arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean containsAll(final Collection<?> arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Iterator<Task> iterator() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean remove(final Object arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean removeAll(final Collection<?> arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean retainAll(final Collection<?> arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T[] toArray(final T[] arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Task element() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean offer(final Task arg0) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Task remove() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
