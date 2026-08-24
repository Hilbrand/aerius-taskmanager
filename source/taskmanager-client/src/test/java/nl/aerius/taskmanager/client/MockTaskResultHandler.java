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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Util class to more easily mock a {@link TaskResultCallback}.
 */
class MockTaskResultHandler implements TaskResultCallback {

  private boolean closed;
  private Object lastResultObject;
  private String lastCorrelationId;
  private String lastMessageId;
  private final Map<String, Object> resultMap = new HashMap<>();
  private final Semaphore semaphore;

  public boolean isClosed() {
    return closed;
  }

  public MockTaskResultHandler() {
    semaphore = new Semaphore(0);
  }

  public void tryAcquire() throws InterruptedException {
    semaphore.tryAcquire(10, TimeUnit.SECONDS);
  }

  @Override
  public void onFailure(final Exception e, final String correlationId, final String messageId) {
    closed = true;
    onSuccess(e, correlationId, messageId);
  }

  @Override
  public void onSuccess(final Object value, final String correlationId, final String messageId) {
    this.lastResultObject = value;
    this.lastCorrelationId = correlationId;
    resultMap.put(correlationId, value);
    if (semaphore != null) {
      semaphore.release();
    }
  }

  public Object getLastResult() {
    return lastResultObject;
  }

  public String getLastMessageId() {
    return lastMessageId;
  }

  public String getLastCorrelationId() {
    return lastCorrelationId;
  }

  public Map<String, Object> getResultMap() {
    return resultMap;
  }

}
