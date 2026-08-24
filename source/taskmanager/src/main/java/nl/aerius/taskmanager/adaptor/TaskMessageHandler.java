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
package nl.aerius.taskmanager.adaptor;

import java.io.IOException;

import nl.aerius.taskmanager.domain.Message;
import nl.aerius.taskmanager.domain.MessageReceivedHandler;

/**
 * Interface for service receiving messages from a queue and pass them to the {@link MessageReceivedHandler}.
 */
public interface TaskMessageHandler<M extends Message> {

  /**
   * Add handler to process message received handler.
   * @param messageReceivedHandler handler
   */
  void addMessageReceivedHandler(MessageReceivedHandler messageReceivedHandler);

  /**
   * Start listening to messages.
   * @throws IOException connection errors
   */
  void start() throws IOException;

  /**
   * Shuts down the message handler.
   * @throws IOException connection errors
   */
  void shutDown() throws IOException;

  /**
   * Called when a message is delivered to a worker.
   *
   * @param message message delivered to the worker
   * @throws IOException connection errors
   */
  void messageDeliveredToWorker(M message) throws IOException;

  /**
   * Called when a message couldn't be delivered to a worker because the worker was not available.
   * @param message message not delivered
   * @throws IOException connection errors
   */
  void messageDeliveryToWorkerFailed(M message) throws IOException;

  /**
   * Called when something is wrong with the message and this should be reported to the sender. The message will not be
   * processed any further.
   * @param message message not delivered
   * @param exception the exception with which the message was not delivered
   * @throws IOException connection errors
   */
  void messageDeliveryAborted(M message, RuntimeException exception) throws IOException;

}
