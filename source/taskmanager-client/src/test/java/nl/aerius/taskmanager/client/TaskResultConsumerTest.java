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
package nl.aerius.taskmanager.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.client.util.QueueHelper;
import nl.aerius.taskmanager.test.MockedChannelFactory;

/**
 * Test class for {@link TaskResultConsumer}.
 */
class TaskResultConsumerTest {

  private static final String CONSUMER_TAG = "Unimportant";
  private static final String FINAL_RESULT = "FINALLY! FREEDOM!";

  private Channel channel;

  @BeforeEach
  void setUp() throws Exception {
    channel = MockedChannelFactory.create();
  }

  @Test
  void testHandleDeliveryNormalTask() throws IOException {
    final SingleResultCallback resultCallback = new SingleResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);

    final String propsCorrelationId = UUID.randomUUID().toString();
    final BasicProperties props = new BasicProperties.Builder().correlationId(propsCorrelationId).build();
    final String resultObject = "Success!";

    assertTrue(channel.isOpen(), "Before handling the channel should still be open");
    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(resultObject));

    assertNull(resultCallback.receivedException, "Received exception should be null");
    assertEquals(resultObject, resultCallback.successObject, "Received result object");
    assertEquals(propsCorrelationId, resultCallback.correlationId, "Received task ID");
  }

  @Test
  void testHandleDeliveryException() throws IOException {
    final SingleResultCallback resultCallback = new SingleResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);

    final String propsCorrelationId = UUID.randomUUID().toString();
    final BasicProperties props = new BasicProperties.Builder().correlationId(propsCorrelationId).build();
    final Exception resultObject = new RuntimeException();

    assertTrue(channel.isOpen(), "Before handling the channel should still be open");
    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(resultObject));

    assertNull(resultCallback.successObject, "Received result object should be null");
    //after serializing/deserializing by default objects aren't exactly equal (unless they override equals in some way).
    assertTrue(resultCallback.receivedException instanceof RuntimeException, "Received exception");
    assertEquals(propsCorrelationId, resultCallback.correlationId, "Received task ID");
  }

  @Test
  void testHandleDeliveryMultipleResultsTask() throws IOException {
    final MockResultCallback resultCallback = new MockResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);

    final String propsCorrelationId = UUID.randomUUID().toString();
    final BasicProperties props = new BasicProperties.Builder().correlationId(propsCorrelationId).build();
    final String resultObject = "Success!";

    assertTrue(channel.isOpen(), "Before handling the channel should still be open");
    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(resultObject));
    assertTrue(channel.isOpen(), "After handling the channel should still be open");

    assertNull(resultCallback.receivedException, "Received exception should be null");
    assertEquals(resultObject, resultCallback.successObject, "Received result object");
    assertEquals(propsCorrelationId, resultCallback.correlationId, "Received task ID");

    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(resultObject));
    assertTrue(channel.isOpen(), "After handling the channel still be open");

    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(FINAL_RESULT));

    assertNull(resultCallback.receivedException, "Received exception should be null");
    assertEquals(FINAL_RESULT, resultCallback.successObject, "Received result object");
    assertEquals(propsCorrelationId, resultCallback.correlationId, "Received task ID");
  }

  @Test
  void testHandleDeliveryMultipleResultsTaskException() throws IOException {
    final MockResultCallback resultCallback = new MockResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);

    final String propsCorrelationId = UUID.randomUUID().toString();
    final BasicProperties props = new BasicProperties.Builder().correlationId(propsCorrelationId).build();
    final String resultObject = "Success!";

    assertTrue(channel.isOpen(), "Before handling the channel should still be open");
    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(resultObject));
    assertTrue(channel.isOpen(), "After handling the channel should still be open");

    assertNull(resultCallback.receivedException, "Received exception should be null");
    assertEquals(resultObject, resultCallback.successObject, "Received result object");
    assertEquals(propsCorrelationId, resultCallback.correlationId, "Received task ID");

    consumer.handleDelivery(CONSUMER_TAG, null, props, QueueHelper.objectToBytes(new RuntimeException()));
    assertTrue(resultCallback.receivedException instanceof RuntimeException, "Received exception");
  }

  @Test
  void testHandleShutdownSignalNotSelfInitiated() {
    final SingleResultCallback resultCallback = new SingleResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);
    consumer.handleShutdownSignal(CONSUMER_TAG, new ShutdownSignalException(true, false, null, ""));
    assertSame(ShutdownSignalException.class, resultCallback.receivedException.getClass(), "Received exception should be null");
    assertNull(resultCallback.successObject, "Received result should be null");
    assertNull(resultCallback.correlationId, "Received task ID should be null");
  }

  @Test
  void testHandleShutdownSignalSelfInitiated() {
    final SingleResultCallback resultCallback = new SingleResultCallback();
    final TaskResultConsumer consumer = new TaskResultConsumer(channel, resultCallback);
    consumer.handleShutdownSignal(CONSUMER_TAG, new ShutdownSignalException(true, true, null, ""));
    assertNull(resultCallback.receivedException, "Received exception should be null");
    assertNull(resultCallback.successObject, "Received result should be null");
    assertNull(resultCallback.correlationId, "Received task ID should be null");
  }

  @Test
  void testWrapperWithoutCallback() {
    assertThrows(IllegalArgumentException.class, () -> new TaskResultConsumer(channel, null));
  }

  private static class SingleResultCallback implements TaskResultCallback {

    Object successObject;
    Exception receivedException;
    String correlationId;

    @Override
    public void onSuccess(final Object value, final String correlationId, final String messageId) {
      this.successObject = value;
      this.correlationId = correlationId;
    }

    @Override
    public void onFailure(final Exception exception, final String correlationId, final String messageId) {
      this.receivedException = exception;
      this.correlationId = correlationId;
    }

  }

  private static class MockResultCallback implements TaskResultCallback {
    Object successObject;
    Exception receivedException;
    String correlationId;

    @Override
    public void onSuccess(final Object value, final String correlationId, final String messageId) {
      this.successObject = value;
      this.correlationId = correlationId;
    }

    @Override
    public void onFailure(final Exception exception, final String correlationId, final String messageId) {
      this.receivedException = exception;
      this.correlationId = correlationId;
    }

  }
}
