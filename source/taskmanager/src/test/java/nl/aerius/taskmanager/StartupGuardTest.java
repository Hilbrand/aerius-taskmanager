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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for {@link StartupGuard}
 */
class StartupGuardTest {

  @Test
  void testOpen() {
    final StartupGuard guard = new StartupGuard();

    assertFalse(guard.isOpen(), "Guard should not be open.");
    guard.onNumberOfWorkersUpdate(0, 1, 0);
    assertTrue(guard.isOpen(), "Guard should be open when onNumberOfWorkersUpdate is called.");
    guard.onNumberOfWorkersUpdate(0, 1, 0);
    assertTrue(guard.isOpen(), "Guard should still remain open onNumberOfWorkersUpdate has been called.");
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.SECONDS)
  void testWaitForOpen() throws InterruptedException {
    final StartupGuard guard = new StartupGuard();
    final Semaphore waitForStart = new Semaphore(0);
    final Semaphore waitForOpen = new Semaphore(0);

    new Thread(() -> {
      try {
        waitForStart.release();
        guard.waitForOpen();
        waitForOpen.release();
      } catch (final InterruptedException e) {
        // Ignore exception
      }
    }).start();
    // First wait for first semaphore to be unlocked.
    waitForStart.acquire();
    assertFalse(guard.isOpen(), "Guard should not be open.");
    guard.onNumberOfWorkersUpdate(1, 1, 0);
    // Wait for semaphore that is called after waitForOpen is unlocked.
    waitForOpen.acquire();
    assertTrue(guard.isOpen(), "Guard should now be open.");
  }
}
