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
package nl.aerius.taskmanager.mq;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.adaptor.DynamicQueueConsumer;
import nl.aerius.taskmanager.adaptor.TaskMessageHandler;
import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.domain.QueueConfig;

/**
 * RabbitMQ implementation of the {@link AdaptorFactory}.
 */
public class RabbitMQAdaptorFactory implements AdaptorFactory {

  private final ScheduledExecutorService executorService;
  private final BrokerConnectionFactory factory;

  /**
   * Constructor, initialized with RabbitMQ connection factory.
   *
   * @param executorService executor service
   * @param factory connection factory
   */
  public RabbitMQAdaptorFactory(final ScheduledExecutorService executorService, final BrokerConnectionFactory factory) {
    this.executorService = executorService;
    this.factory = factory;
  }

  @Override
  public TaskMessageHandler createTaskMessageHandler(final QueueConfig queueConfig) throws IOException {
    return new RabbitMQMessageHandler(factory, queueConfig);
  }

  @Override
  public WorkerSizeProviderProxy createWorkerSizeProvider() {
    return new RabbitMQWorkerSizeProvider(executorService, factory);
  }

  @Override
  public WorkerProducer createWorkerProducer(final QueueConfig queueConfig) {
    return new RabbitMQWorkerProducer(factory, queueConfig);
  }

  @Override
  public DynamicQueueConsumer createDynamicQueueConsumer() throws IOException {
    return new RabbitMQDynamicQueueConsumer(factory.getConnection().createChannel());
  }
}
