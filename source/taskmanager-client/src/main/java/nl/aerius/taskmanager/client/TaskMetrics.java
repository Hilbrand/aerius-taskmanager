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

import java.util.Map;
import java.util.Optional;

import com.rabbitmq.client.LongString;

/**
 * Class to communicate metrics information. It wraps around the information passed as properties via the RabbitMQ messages header.
 */
public class TaskMetrics {

  /**
   * Work duration in milliseconds.
   */
  public static final String AER_WORK_DURATION = "aer.work.duration";
  public static final String AER_WORK_QUEUENAME = "aer.work.queuename";
  public static final String AER_WORK_STARTTIME = "aer.work.starttime";

  private long duration;
  private String queueName;
  private long startTime;

  /**
   * Create TaskMetrics based on properties map as can be received from a RabbitMQ message.
   *
   * @param properties map of properties
   */
  public TaskMetrics(final Map<String, Object> properties) {
    this.duration = TaskMetrics.longValue(properties, TaskMetrics.AER_WORK_DURATION);
    this.startTime = TaskMetrics.longValue(properties, TaskMetrics.AER_WORK_STARTTIME);
    this.queueName = TaskMetrics.stringValue(properties, TaskMetrics.AER_WORK_QUEUENAME);
  }

  /**
   * Creates a new metrics
   */
  public TaskMetrics() {
    this.startTime = System.currentTimeMillis();
    this.queueName = "";
  }

  public static long longValue(final Map<String, Object> messageMetaData, final String key) {
    return Optional.ofNullable(messageMetaData).map(m -> m.get(key)).map(Long.class::cast).orElse(0L);
  }

  public static String stringValue(final Map<String, Object> messageMetaData, final String key) {
    return Optional.ofNullable(messageMetaData.get(key))
        .filter(LongString.class::isInstance)
        .map(t -> new String(((LongString) t).getBytes()))
        .orElse("");
  }

  public long duration() {
    return duration;
  }

  public TaskMetrics determineDuration() {
    this.duration = startTime > 0 ? System.currentTimeMillis() - startTime : 0;
    return this;
  }

  public TaskMetrics duration(final long duration) {
    this.duration = duration;
    return this;
  }

  public String queueName() {
    return queueName;
  }

  public TaskMetrics queueName(final String queueName) {
    this.queueName = queueName;
    return this;
  }

  public TaskMetrics start(final long startTime) {
    this.startTime = startTime;
    return this;
  }

  public Map<String, Object> build() {
    return Map.of(
        TaskMetrics.AER_WORK_DURATION, duration,
        TaskMetrics.AER_WORK_QUEUENAME, queueName,
        TaskMetrics.AER_WORK_STARTTIME, startTime);
  }
}
