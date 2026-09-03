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
package nl.aerius.taskmanager.metrics;

import java.util.function.ToDoubleBiFunction;

/**
 * Class to keep track of work load per worker type.
 * Each time a new task is added (dispatched) or removed (work finished) the number of running workers is counted and calculated how long
 * these workers were running from the last time measured point. The time multiplied by the factor of number of running workers relative to the
 * number of workers available is added to a counter.
 * When {@link #process()} is called the average work load is calculated by dividing the counted running workers time by the total measured time.
 */
class LoadMetric {

  /**
   * Last time {@link #register(int, int)} was called.
   */
  private long last = System.currentTimeMillis();
  /**
   * Total measured time since the last time {@link #process()} was called.
   */
  private long totalMeasureTime;
  /**
   * Measured free workers as the sum of number of free workers for specific time moments. Sum of (free workers * time frame).
   * Dividing this number by the total time of the time frame will give an average number of free workers.
   */
  private double total;
  /**
   * Number of workers running at a time.
   */
  private int usedWorkers;
  /**
   * Total number of available workers.
   */
  private int numberOfWorkers;
  private final ToDoubleBiFunction<Integer, Integer> countFunction;
  private final ToDoubleBiFunction<Double, Long> sumFunction;
  private final Object lock = new Object();

  public LoadMetric(final ToDoubleBiFunction<Integer, Integer> countFunction, final ToDoubleBiFunction<Double, Long> sumFunction) {
    this.countFunction = countFunction;
    this.sumFunction = sumFunction;
  }

  /**
   * Register change in number of running workers.
   *
   * @param deltaUsedWorkers number of jobs on the workers being added or subtracted.
   * @param numberOfWorkers Number of available workers
   */
  public  void register(final int deltaUsedWorkers, final int numberOfWorkers) {
    synchronized (lock) {
      if (deltaUsedWorkers != 0 || this.numberOfWorkers != numberOfWorkers) {
        updateState(deltaUsedWorkers, numberOfWorkers);
      }
    }
  }


  /**
   * Resets the metric state. Sets running workers to 0, and resets the average load time by calling process.
   */
  public void reset() {
    synchronized(lock) {
      updateState(-usedWorkers, numberOfWorkers);
    }
  }

  /**
   * Calculates average duration over the last time frame since this method was called. Internals are reset in this method to a new measure point.
   *
   * @return Average load of the workers since the last time this method was called
   */
  public double process() {
    synchronized(lock) {
      // Call register here to set the end time this moment. This will calculate workers running up till now as being active.
      updateState(0, numberOfWorkers);
      final double averageTotal = totalMeasureTime > 0 ? sumFunction.applyAsDouble(total, totalMeasureTime) : 0;

      totalMeasureTime = 0;
      total = 0;
      return averageTotal;
    }
  }

  private void updateState(final int deltaUsedWorkers, final int updatedNumberOfWorkers) {
    final long newLast = System.currentTimeMillis();
    final long delta = newLast - last;

    // First calculate the total for passed time period with the worker values up to this time.
    total += delta * countFunction.applyAsDouble(numberOfWorkers, usedWorkers);
    totalMeasureTime += delta;
    last = newLast;
    numberOfWorkers = updatedNumberOfWorkers;
    usedWorkers = Math.max(0, usedWorkers + deltaUsedWorkers);
  }
}
