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

import io.opentelemetry.context.Context;

/**
 * Task manager internal data object to track messages. A Message is a generic interface which is used with a interface specific (e.g. RabbitMQ)
 * instances. This data object keeps track of the taskconsumer that send the message.
 */
public class Task {
  private Message data;
  private final TaskConsumer taskConsumer;
  private boolean alive = true;
  private Context context;
  private TaskRecord taskRecord;

  public Task(final TaskConsumer taskConsumer) {
    this.taskConsumer = taskConsumer;
  }

  public String getId() {
    return data == null ? null : data.getMessageId();
  }

  @SuppressWarnings("unchecked")
  public Message getMessage() {
    return data;
  }

  public TaskConsumer getTaskConsumer() {
    return taskConsumer;
  }

  public TaskRecord getTaskRecord() {
    return taskRecord;
  }

  public void setData(final Message data) {
    this.data = data;
    this.taskRecord = new TaskRecord(taskConsumer.getQueueName(), data.getCorrelationId(), data.getMessageId());
  }

  public void killTask() {
    this.alive = false;
  }

  public boolean isAlive() {
    return alive;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(final Context context) {
    this.context = context;
  }

  @Override
  public String toString() {
    return "Task [id=" + getId() + "]";
  }

}
