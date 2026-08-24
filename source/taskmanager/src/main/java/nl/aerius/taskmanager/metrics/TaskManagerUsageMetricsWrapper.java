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

import io.opentelemetry.api.metrics.Meter;

/**
 * Class that wraps all Task Manager usage metrics reporters. TaskSchedulerBuckets should add the metric providers to this class.
 * Each of those providers added is for a specific worker queue. The {@link UsageMetricsWrapper} will manager metrics per worker queue.
 */
public class TaskManagerUsageMetricsWrapper {

  private final UsageMetricsWrapper rabbitMQUsageMetrics;
  private final UsageMetricsWrapper workerPoolUsageMetrics;
  private final UsageMetricsWrapper taskManagerUsageMetrics;
  private final UsageMetricsReporter loadUsageMetricsReporter;

  public TaskManagerUsageMetricsWrapper(final Meter meter) {
    rabbitMQUsageMetrics = new UsageMetricsWrapper(meter, "aer.rabbitmq", true);
    workerPoolUsageMetrics = new UsageMetricsWrapper(meter, "aer.taskmanager.workerpool", false);
    taskManagerUsageMetrics = new UsageMetricsWrapper(meter, "aer.taskmanager", false);
    loadUsageMetricsReporter = new UsageMetricsReporter(meter, "aer.taskmanager.work.load", "Report average worker load");
  }

  /**
   * Add the usage metric provider that registers metrics on the queue state of a worker from the RabbitMQ queue information.
   *
   * @param provider The provider that registers the RabbitMQ metrics.
   */
  public void addRabbitMQUsageMetricsProvider(final UsageMetricsProvider provider) {
    rabbitMQUsageMetrics.add(provider);
  }

  /**
   * Add the usage metric provider that registers metrics on the queue state in the internal worker pool.
   *
   * @param provider The provider that registers the worker pool metrics.
   */
  public void addWorkerPoolUsageMetricsProvider(final UsageMetricsProvider provider) {
    workerPoolUsageMetrics.add(provider);
  }

  /**
   * Add the usage metric provider that registers metrics on the usage state of a worker from the information collected by the Task Manager.
   *
   * @param provider The {@link TaskManagerUsageMetricsProvider}.
   */
  public void addTaskManagerUsageMetricsProvider(final TaskManagerUsageMetricsProvider provider) {
    taskManagerUsageMetrics.add(provider);
    loadUsageMetricsReporter.addMetrics(provider.getWorkerQueueName(), provider::getLoad,
        OpenTelemetryMetrics.workerAttributes(provider.getWorkerQueueName()));
  }

  /**
   * Remove metrics reporting for the given worker queue.
   *
   * @param workerQueueName worker queue to remove the metric reporting for
   */
  public void remove(final String workerQueueName) {
    rabbitMQUsageMetrics.remove(workerQueueName);
    workerPoolUsageMetrics.remove(workerQueueName);
    taskManagerUsageMetrics.remove(workerQueueName);
    loadUsageMetricsReporter.removeMetrics(workerQueueName);
  }

  public void close() {
    rabbitMQUsageMetrics.close();
    workerPoolUsageMetrics.close();
    taskManagerUsageMetrics.close();
    loadUsageMetricsReporter.close();
  }
}
