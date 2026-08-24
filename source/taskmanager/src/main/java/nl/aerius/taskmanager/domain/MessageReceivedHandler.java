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

/**
 * Class implementing this handler will get the messages received by the TaskMessageHandler.
 */
public interface MessageReceivedHandler {

  /**
   * Message received from queue.
   *
   * @param message the message
   */
  void onMessageReceived(Message message);

  /**
   * Called when the Consumer was shutdown.
   */
  void handleShutdownSignal();
}
