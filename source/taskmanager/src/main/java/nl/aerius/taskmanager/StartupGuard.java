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

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;

/**
 * Class to be used at startup. The Scheduler should not start before it is known how many messages are still on the queue.
 * This to register any work that is still on the queue and to properly calculate load metrics.
 * Because the Task Manager is not aware of the tasks already on the queue and therefore otherwise these messages won't be counted in the metrics.
 * This can result in the metrics being skewed, and thereby negatively reporting load metrics.
 */
public class StartupGuard implements WorkerSizeObserver {

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
  public void onNumberOfWorkersUpdate(final int numberOfWorkers, final int numberOfMessages, final int numberOfMessagesInProgress) {
    synchronized (openSemaphore) {
      if (!open) {
        open = true;
        openSemaphore.release();
      }
    }
  }
}
