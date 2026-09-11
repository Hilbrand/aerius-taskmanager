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

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.domain.RabbitMQQueueStatus;

/**
 * Class to be used at startup. The Scheduler should not start before the number of messages on the queue is zero.
 * Because the Task Manager has no information of the tasks already on the queue and therefore there is no tracking information of those messages.
 * As all tracking information only lives in memory and is reset when the Task Manager is restarted.
 */
public class StartupGuard implements WorkerSizeObserver {

  private final ReentrantLock lock = new ReentrantLock();
  private final Semaphore openSemaphore = new Semaphore(0);

  private boolean open;

  /**
   * @return Returns true once the number of messages has become zero for the first time.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Wait for the number of messages on the message queue to become zero.
   */
  public void waitForOpen() throws InterruptedException {
    openSemaphore.acquire();
  }

  @Override
  public void onNumberOfWorkersUpdate(final RabbitMQQueueStatus queueStatus) {
    lock.lock();
    try {
      if (!open) {
        open = true;
        openSemaphore.release();
      }
    } finally {
      lock.unlock();
    }
  }
}
