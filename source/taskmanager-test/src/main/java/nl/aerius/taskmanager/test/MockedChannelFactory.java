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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

/**
 * Factory class that creates a mocked channel.
 *
 * This class keeps some state
 */
public final class MockedChannelFactory {

  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
  private static final Map<String, PriorityBlockingQueue<Body>> QUEUES = new ConcurrentHashMap<>();
  private static final Map<Long, Body> QUEUED = new ConcurrentHashMap<>();
  private static byte[] received;

  private MockedChannelFactory() {
    // Util class
  }

  /**
   * Creates a new mocked channel.
   *
   * @return mocked channel
   * @throws IOException Due to mocking methods
   */
  public static Channel create() throws IOException {
    return create((p, b) -> b);
  }

  /**
   * Creates a new mocked channel with hook to return mocked results when simulating a roundtrip to a service.
   * The mocked results can be used to simulate data as it would have been returned by the services that was called.
   *
   * @param mockResults method to return mocked results
   * @return mocked channel
   * @throws IOException Due to mocking methods
   */
  public static Channel create(final BiFunction<BasicProperties, byte[], byte[]> mockResults) throws IOException {
    reset();
    final Channel channel = Mockito.mock(Channel.class);
    mockBasicPublish(channel);
    mockBasicConsume(channel, mockResults);
    mockAck(channel);
    mockQueueDeclare(channel);
    try {
      mockClosed(channel);
    } catch (final TimeoutException e) {
      throw new IOException(e);
    }
    return channel;
  }

  private static void reset() {
    QUEUES.clear();
    QUEUED.clear();
    received = null;
  }

  public static byte[] getReceived() {
    return received.clone();
  }

  private static void mockAck(final Channel channel) throws IOException {
    lenient().doAnswer(inv -> {
      QUEUED.remove(inv.getArgument(0));
      return null;
    }).when(channel).basicAck(anyLong(), anyBoolean());
    lenient().doAnswer(inv -> {
      if (QUEUED.containsKey(inv.getArgument(0))) {
        final Body body = QUEUED.remove(inv.getArgument(0));
        body.setPriority(1);
        getQueue(body.getQueueName()).add(body);
      }
      return null;
    }).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  private static void mockBasicPublish(final Channel channel) throws IOException {
    lenient().doAnswer(inv -> basicPublish(inv, 1, 2, 3)).when(channel).basicPublish(any(), any(), any(), any());
    lenient().doAnswer(inv -> basicPublish(inv, 1, 3, 4)).when(channel).basicPublish(any(), any(), anyBoolean(), any(), any());
    lenient().doAnswer(inv -> basicPublish(inv, 1, 4, 5)).when(channel).basicPublish(any(), any(), anyBoolean(), anyBoolean(), any(), any());
  }

  private static Void basicPublish(final InvocationOnMock inv, final int rkIdx, final int bpIdx, final int bodyIdx) {
    basicPublish(inv.getArgument(rkIdx), inv.getArgument(bpIdx), inv.getArgument(bodyIdx));
    return null;
  }

  private static void basicPublish(final String routingKey, final BasicProperties props, final byte[] body) {
    received = body == null ? new byte[0] : body.clone();
    if (routingKey != null) {
      getQueue(routingKey).add(new Body(routingKey, received, props));
    }
    if (props.getReplyTo() != null) {
      getQueue(props.getReplyTo()).add(new Body(routingKey, received, props));
    }
  }

  private static void mockBasicConsume(final Channel channel, final BiFunction<BasicProperties, byte[], byte[]> mockResults) throws IOException {
    final BiFunction<String, Consumer, String> scheduleSupplier = (q, c) -> {
      scheduleCallback(q, c, mockResults);
      return null;
    };
    lenient().doAnswer(inv -> scheduleSupplier.apply(inv.getArgument(0), inv.getArgument(1))).when(channel).basicConsume(any(), any());
    lenient().doAnswer(inv -> scheduleSupplier.apply(inv.getArgument(0), inv.getArgument(2))).when(channel).basicConsume(any(), anyBoolean(), any());
    lenient().doAnswer(inv -> scheduleSupplier.apply(inv.getArgument(0), inv.getArgument(3))).when(channel).basicConsume(any(), anyBoolean(),
        anyString(), any());
    lenient().doAnswer(inv -> scheduleSupplier.apply(inv.getArgument(0), inv.getArgument(6))).when(channel).basicConsume(any(), anyBoolean(), any(),
        anyBoolean(), anyBoolean(), any(), any());
  }

  private static void scheduleCallback(final String queueName, final Consumer callback,
      final BiFunction<BasicProperties, byte[], byte[]> mockResults) {
    EXECUTOR.execute(() -> {
      final PriorityBlockingQueue<Body> queue = getQueue(queueName);
      try {
        final Body value = queue.take();
        final long id = new Random().nextLong();
        QUEUED.put(id, value);
        final Envelope envelope = new Envelope(id, false, "", "");
        callback.handleDelivery(null, envelope, value.getProperties(), mockResults.apply(value.getProperties(), value.getBody()));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  private static PriorityBlockingQueue<Body> getQueue(final String key) {
    return QUEUES.computeIfAbsent(key, k -> new PriorityBlockingQueue<>(10, (o1, o2) -> Integer.compare(o1.getPriority(), o2.getPriority())));
  }

  private static void mockQueueDeclare(final Channel channel) throws IOException {
    final DeclareOk mockDeclareOk = Mockito.mock(DeclareOk.class);
    lenient().when(mockDeclareOk.getQueue()).thenReturn(UUID.randomUUID().toString());
    lenient().when(channel.queueDeclare()).thenReturn(mockDeclareOk);
  }

  private static void mockClosed(final Channel channel) throws IOException, TimeoutException {
    final AtomicBoolean closed = new AtomicBoolean();
    final Function<Boolean, Void> close = c -> {
      closed.set(c);
      return null;
    };

    lenient().doAnswer(inv -> !closed.get()).when(channel).isOpen();
    lenient().doAnswer(inv -> close.apply(true)).when(channel).close();
    lenient().doAnswer(inv -> close.apply(true)).when(channel).close(anyInt(), any());
  }

  private static class Body {
    private int priority;
    private final byte[] content;
    private final String queueName;
    private final BasicProperties properties;

    public Body(final String queueName, final byte[] content, final BasicProperties properties) {
      this.queueName = queueName;
      this.content = content;
      this.properties = properties;
      priority = 1;
    }

    public byte[] getBody() {
      return content;
    }

    public String getQueueName() {
      return queueName;
    }

    public int getPriority() {
      return priority;
    }

    public BasicProperties getProperties() {
      return properties;
    }

    public void setPriority(final int priority) {
      this.priority = priority;
    }
  }
}
