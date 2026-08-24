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

import nl.aerius.taskmanager.adaptor.TaskMessageHandler;
import nl.aerius.taskmanager.domain.Message;
import nl.aerius.taskmanager.domain.MessageReceivedHandler;

/**
 * Mock implementation of {@link TaskMessageHandler}.
 */
public class MockTaskMessageHandler implements TaskMessageHandler {

  private Message failedMessage;
  private Message message;
  private Message abortedMessage;

  @Override
  public void addMessageReceivedHandler(final MessageReceivedHandler messageReceivedHandler) {
    //no-op
  }

  @Override
  public void start() throws IOException {
    //no-op
  }

  @Override
  public void shutDown() throws IOException {
    //no-op
  }

  @Override
  public void messageDeliveredToWorker(final Message message) throws IOException {
    this.message = message;
  }

  public Message getMessage() {
    return message;
  }

  @Override
  public void messageDeliveryToWorkerFailed(final Message message) throws IOException {
    failedMessage = message;
  }

  @Override
  public void messageDeliveryAborted(final Message message, final RuntimeException exception) throws IOException {
    abortedMessage = message;
  }

  public Message getAbortedMessage() {
    return abortedMessage;
  }

  public Message getFailedMessage() {
    return failedMessage;
  }

}
