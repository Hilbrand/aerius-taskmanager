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
package nl.aerius.taskmanager.domain;

/**
 * The configuration of a task, which is used by Task Consumer to retrieve the queue name
 * and used by the scheduler to determine if a message should be handled or not.
 */
public class PriorityTaskQueue extends TaskQueue {

  private int priority;
  private double maxCapacityUse;

  /**
   * Empty constructor.
   */
  public PriorityTaskQueue() {
    // Constructor for gson parsing
  }

  /**
   * @param queueName The name of the queue the task corresponds to.
   * @param priority The priority this task should have.
   * @param maxCapacityUse The maximum capacity this task can use of the total workers assigned. Should be a fraction.
   */
  public PriorityTaskQueue(final String queueName, final int priority, final double maxCapacityUse) {
    super(queueName);
    this.priority = priority;
    this.maxCapacityUse = maxCapacityUse;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    final long temp = Double.doubleToLongBits(maxCapacityUse);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return prime * result + priority;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj) || getClass() != obj.getClass()) {
      return false;
    }
    final PriorityTaskQueue other = (PriorityTaskQueue) obj;

    return Double.doubleToLongBits(maxCapacityUse) == Double.doubleToLongBits(other.maxCapacityUse)
        && priority == other.priority;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(final int priority) {
    this.priority = priority;
  }

  public double getMaxCapacityUse() {
    return maxCapacityUse;
  }

  public void setMaxCapacityUse(final double maxCapacityUse) {
    this.maxCapacityUse = maxCapacityUse;
  }

  @Override
  public String toString() {
    return "PriorityTaskQueue [priority=" + priority + ", maxCapacityUse=" + maxCapacityUse + ", getQueueName()=" + getQueueName() + "]";
  }
}
