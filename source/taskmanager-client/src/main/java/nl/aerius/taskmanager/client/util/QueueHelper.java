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
package nl.aerius.taskmanager.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.LongString;

/**
 * Helper class to convert objects to and from byte arrays.
 *
 */
public final class QueueHelper {

  private QueueHelper() {}

  /**
   * Convert a byte array to an object.
   * @param bytes The byte array to convert.
   * @return The converted object.
   * @throws IOException If an I/O error occurs.
   * @throws ClassNotFoundException When the class of the object previously serialized is not found.
   */
  public static Serializable bytesToObject(final byte[] bytes) throws IOException, ClassNotFoundException {
    final Serializable object;
    if (bytes != null && bytes.length > 0) {
      try (final ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
          final GZIPInputStream gzip = new GZIPInputStream(bi);
          final ObjectInputStream in = new ObjectInputStream(gzip)) {
        object = (Serializable) in.readObject();
      }
    } else {
      object = null;
    }
    return object;
  }

  /**
   * Convert an object to a byte array.
   * @param object The object to convert.
   * @return The converted byte array.
   * @throws IOException If an I/O error occurs.
   */
  public static byte[] objectToBytes(final Serializable object) throws IOException {
    final byte[] bytes;
    if (object == null) {
      bytes = new byte[0];
    } else {
      try (final ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
        // separate try-block is intended, `GZIPOutputStream` must be closed before calling `toByteArray()`
        try (final GZIPOutputStream gzip = new GZIPOutputStream(bo);
            final ObjectOutputStream out = new ObjectOutputStream(gzip)) {
          out.writeObject(object);
        }
        bytes = bo.toByteArray();
      }
    }
    return bytes;
  }

  /**
   * Converts a header String (which becomes a LongString automatically to support very large strings) to a regular String.
   * @param properties The properties to get header of.
   * @param key The key to get out of the headers.
   * @return Regular String.
   */
  public static String getHeaderString(final BasicProperties properties, final String key) {
    final LongString str = (LongString) properties.getHeaders().get(key);
    return str == null ? null : new String(str.getBytes(), StandardCharsets.UTF_8);
  }
}
