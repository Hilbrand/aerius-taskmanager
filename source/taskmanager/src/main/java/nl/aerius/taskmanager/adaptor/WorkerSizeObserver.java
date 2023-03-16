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
package nl.aerius.taskmanager.adaptor;

/**
 * Interface called to observer the number of workers.
 */
public interface WorkerSizeObserver {

  /**
   * Gives the number of workers processes connected on the queue.
   *
   * @param numberOfWorkers number of number of workers processes
   * @param numberOfMessages Actual total number of messages on the queue
   * @param numberOfMessagesUnacknowledged Actual number of unacknowledged messages on the queue
   */
  void onNumberOfWorkersUpdate(final int numberOfWorkers, final int numberOfMessages, int numberOfMessagesUnacknowledged);

  /**
   * Gives an increment or decrement number of workers processes connected on the queue.
   *
   * @param deltaNumberOfWorkers increase/decrease of number of workers processes
   */
  void onDeltaNumberOfWorkersUpdate(final int deltaNumberOfWorkers);
}
