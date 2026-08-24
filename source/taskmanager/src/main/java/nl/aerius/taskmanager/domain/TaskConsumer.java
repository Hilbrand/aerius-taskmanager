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
package nl.aerius.taskmanager.domain;

import java.io.IOException;

/**
 * Interface for Consumer that listens to queue for new tasks, and passes the task to the scheduler.
 */
public interface TaskConsumer extends MessageReceivedHandler {

  String getQueueName();

  /**
   * @return true if is running
   */
  boolean isRunning();

  /**
   * Inform the consumer the message delivery was successful.
   *
   * @param message the message that was successful
   * @throws IOException
   */
  void messageDelivered(final Message message) throws IOException;

  /**
   * Inform the consumer the message delivery failed.
   *
   * @param message the message that failed
   * @throws IOException
   */
  void messageDeliveryFailed(Message message) throws IOException;

  /**
   * Inform the consumer the message delivery failed.
   * @param message message that failed
   * @param exception the exception with which the message failed
   * @throws IOException
   */
  void messageDeliveryAborted(Message message, RuntimeException exception) throws IOException;

  /**
   * Start the consumer.
   */
  void start();

  /**
   * Shutdown the task consumer.
   */
  void shutdown();
}
