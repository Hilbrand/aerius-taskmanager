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

/**
 * Interface for processing calculation results, or either errors occurred during calculation.
 */
public interface TaskResultCallback {

  /**
   * The operation done by the worker was successful. The data returned by the
   * worker is in value. The receiving client knows the type. The correlationId is
   * the id given when sending this task to the worker and is to identify the
   * value with the input.
   *
   * @param value data retrieved returned by the worker
   * @param correlationId Correlation id related to this data
   * @param messageId Message id related to this data.
   */
  void onSuccess(Object value, String correlationId, String messageId);

  /**
   * The operation by the worker failed or sending the task failed. The exception
   * can be or from the worker or from the process sending the task to the worker.
   * If the correlationId is known, like when a worker threw the exception, this is
   * passed here or null if it's not known.
   *
   * @param exception Exception thrown by worker or process
   * @param correlationId Correlation id related to this data
   * @param messageId Message id related to this data.
   */
  void onFailure(Exception exception, String correlationId, String messageId);
}
