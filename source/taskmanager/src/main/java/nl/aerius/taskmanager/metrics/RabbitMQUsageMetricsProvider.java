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

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

public class RabbitMQUsageMetricsProvider implements WorkerSizeObserver, UsageMetricsProvider {

  private final String workerQueueName;

  private RabbitMQQueueStatus queueStatus = new RabbitMQQueueStatus(0,0,0);

  public RabbitMQUsageMetricsProvider(final String workerQueueName) {
    this.workerQueueName = workerQueueName;
  }

  @Override
  public void onNumberOfWorkersUpdate(final RabbitMQQueueStatus queueStatus) {
    this.queueStatus = queueStatus;
  }

  @Override
  public String getWorkerQueueName() {
    return workerQueueName;
  }

  @Override
  public int getNumberOfWorkers() {
    return queueStatus.consumers();
  }

  @Override
  public int getNumberOfUsedWorkers() {
    return queueStatus.unacknowledged();
  }

  @Override
  public int getNumberOfFreeWorkers() {
    return Math.max(0, getNumberOfWorkers() - getNumberOfUsedWorkers());
  }

  @Override
  public int getNumberOfWaiting() {
    return Math.max(0, queueStatus.messages() - getNumberOfUsedWorkers());
  }

}
