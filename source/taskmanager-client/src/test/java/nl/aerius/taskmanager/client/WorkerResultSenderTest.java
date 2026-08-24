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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.LongStringHelper;

import nl.aerius.taskmanager.client.util.QueueHelper;

/**
 * Test class for {@link WorkerResultSender}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerResultSenderTest {

  private static final String TASKMANAGER_REPLY_QUEUE = "taskManagerreplyQueue";
  private static final String REPLY_QUEUE = "replyQueue";
  private static final String DATA = "data";
  private static final byte[] EMPTY = new byte[0];

  private @Mock Channel channel;
  private @Mock BasicProperties properties;
  private @Captor ArgumentCaptor<BasicProperties> propertiesCaptor;

  private WorkerResultSender workerResultSender;
  private Map<String, Object> headers;


  @BeforeEach
  void beforeEach() {
    headers = new HashMap<>();
    lenient().doReturn(headers).when(properties).getHeaders();
    lenient().doReturn("1").when(properties).getCorrelationId();
    lenient().doReturn("2").when(properties).getMessageId();
    headers.put(TaskMetrics.AER_WORK_STARTTIME, 1L);
    workerResultSender = new WorkerResultSender(channel, properties);
  }

  @Test
  void testSendIntermediateResult() throws IOException {
    doReturn(REPLY_QUEUE).when(properties).getReplyTo();

    workerResultSender.sendIntermediateResult(DATA);
    verify(channel).basicPublish(eq(""), eq(REPLY_QUEUE), propertiesCaptor.capture(), eq(QueueHelper.objectToBytes(DATA)));
    assertProperties();
  }

  @Test
  void testSendFinalResultNoHeaders() throws IOException {
    doReturn(null).when(properties).getHeaders();
    workerResultSender.sendFinalResult(DATA);
    verify(channel, never()).basicPublish(any(), any(), any(), any());
  }

  @Test
  void testSendFinalResultNoReplyQueue() throws IOException {
    workerResultSender.sendFinalResult(DATA);
    verify(channel, never()).basicPublish(any(), any(), any(), any());
  }

  @Test
  void testSendFinalResult() throws IOException {
    headers.put(QueueConstants.TASKMANAGER_REPLY_QUEUE, LongStringHelper.asLongString(TASKMANAGER_REPLY_QUEUE));
    workerResultSender.sendFinalResult(DATA);
    verify(channel).basicPublish(eq(""), eq(TASKMANAGER_REPLY_QUEUE), propertiesCaptor.capture(), eq(EMPTY));
    final Map<String, Object> replyHeaders = assertProperties();
    assertNotNull(replyHeaders, "Expected reply headers not to be null");
  }

  private Map<String, Object> assertProperties() {
    final BasicProperties replyProperties = propertiesCaptor.getValue();
    assertEquals("1", replyProperties.getCorrelationId(), "Expected same correlationid");
    assertEquals("2", replyProperties.getMessageId(), "Expected same messageid");
    return replyProperties.getHeaders();
  }
}
