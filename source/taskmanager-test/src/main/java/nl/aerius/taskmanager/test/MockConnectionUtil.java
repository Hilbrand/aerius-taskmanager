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
package nl.aerius.taskmanager.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.stubbing.Answer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Util class for mocking RabbitMQ connections.
 *
 */
public final class MockConnectionUtil {

  private MockConnectionUtil() {
    // Util class.
  }

  /**
   * Mocks a Connect, that creates a channel that is open.
   * When createChannel is called multiple times, it will return the same channel.
   * This can be used to get the channel in test to use in verify test calls.
   *
   * @return mocked connection.
   */
  public static Connection mockConnection() {
    final Connection connection = mock(Connection.class);
    final Channel channel = mock(Channel.class);

    // Pass mocked channel to always return the same channel when createChannel will be called multiple times
    try {
      lenient().doReturn(channel).when(connection).createChannel();
      mockClose(connection, channel);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return connection;
  }

  private static void mockClose(final Connection connection, final Channel channel) throws IOException {
    final AtomicBoolean open = new AtomicBoolean(true);

    lenient().doReturn(open.get()).when(channel).isOpen();
    lenient().doReturn(open.get()).when(connection).isOpen();
    final Answer<Void> closeAnswer = inv -> {
      open.set(false);
      return null;
    };

    lenient().doAnswer(closeAnswer).when(connection).close();
    lenient().doAnswer(closeAnswer).when(connection).close(anyInt());
    lenient().doAnswer(closeAnswer).when(connection).close(anyInt(), any());
    lenient().doAnswer(closeAnswer).when(connection).close(anyInt(), any(), anyInt());
    lenient().doAnswer(closeAnswer).when(connection).abort();
    lenient().doAnswer(closeAnswer).when(connection).abort(anyInt());
    lenient().doAnswer(closeAnswer).when(connection).abort(anyInt(), any());
    lenient().doAnswer(closeAnswer).when(connection).abort(anyInt(), any(), anyInt());
  }
}
