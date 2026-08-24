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
 * The {@link UsageMetricsProvider} for weighted load. limit and usage metrics for worker queues.
 * The values are calculated averages over the time between the last measurement point and the moment the metric value is requested.
 */
public class TaskManagerUsageMetricsProvider implements UsageMetricsProvider {

  private static final ToDoubleBiFunction<Double, Long> LOAD_SUM_FUNCTION = (total, totalMeasureTime) -> (total * 100.0) / totalMeasureTime;
  private static final ToDoubleBiFunction<Double, Long> COUNT_SUM_FUNCTION = (total, totalMeasureTime) -> Math.floor(total / totalMeasureTime);

  private final String workerQueueName;
  private final LoadMetric load;
  private final LoadMetric limit;
  private final LoadMetric used;
  private final LoadMetric free;

  public TaskManagerUsageMetricsProvider(final String workerQueueName) {
    this.workerQueueName = workerQueueName;
    load = new LoadMetric((numberOfWorkers, usedWorkers) -> (numberOfWorkers > 0 ? (usedWorkers / (double) numberOfWorkers) : 0), LOAD_SUM_FUNCTION);
    limit = new LoadMetric((numberOfWorkers, usedWorkers) -> numberOfWorkers, COUNT_SUM_FUNCTION);
    used = new LoadMetric((numberOfWorkers, usedWorkers) -> usedWorkers, COUNT_SUM_FUNCTION);
    free = new LoadMetric((numberOfWorkers, usedWorkers) -> numberOfWorkers - usedWorkers, COUNT_SUM_FUNCTION);
  }

  public synchronized void register(final int deltaUsedWorkers, final int numberOfWorkers) {
    load.register(deltaUsedWorkers, numberOfWorkers);
    limit.register(deltaUsedWorkers, numberOfWorkers);
    used.register(deltaUsedWorkers, numberOfWorkers);
    free.register(deltaUsedWorkers, numberOfWorkers);
  }

  @Override
  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public double getLoad() {
    return load.process();
  }

  @Override
  public int getNumberOfWorkers() {
    return (int) Math.max(0, limit.process());
  }

  @Override
  public int getNumberOfUsedWorkers() {
    return (int) Math.max(0, used.process());
  }

  @Override
  public int getNumberOfFreeWorkers() {
    return (int) Math.max(0, free.process());
  }

  public void reset() {
    load.reset();
    limit.reset();
    used.reset();
    free.reset();
  }
}
