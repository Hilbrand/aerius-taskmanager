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
package nl.aerius.taskmanager;

import java.io.IOException;

import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.domain.Message;

/**
 * Mock implementation of {@link WorkerProducer}.
 */
public class MockWorkerProducer implements WorkerProducer {

  private boolean shutdownExceptionOnForward = false;

  @Override
  public void addWorkerProducerHandler(final WorkerProducerHandler workerFinishedHandler) {
    // no-op
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void dispatchMessage(final Message message) throws IOException {
    if (shutdownExceptionOnForward) {
      throw new ShutdownSignalException(true, true, null, null);
    }
  }

  @Override
  public void shutdown() {
    // no-op
  }

  public void setShutdownExceptionOnForward(final boolean shutdownExceptionOnForward) {
    this.shutdownExceptionOnForward = shutdownExceptionOnForward;
  }

}
