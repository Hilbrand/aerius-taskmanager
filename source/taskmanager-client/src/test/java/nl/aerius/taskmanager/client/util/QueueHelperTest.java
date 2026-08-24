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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

/**
 * Test for class {@link QueueHelper}.
 */
class QueueHelperTest {

  @Test
  void testBytesToObject() throws IOException, ClassNotFoundException {
    final byte[] bytes = {31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 91, -13, -106, -127, -75, -124, -127, -85, 36,
        -75, -72, -60, 63, 41, 43, 53, -71, 4, 0, 20, -18, 2, 117, 17, 0, 0, 0};
    final String object = (String) QueueHelper.bytesToObject(bytes);
    assertEquals("testObject", object, "deserializing gzip stream");
  }

  @Test
  void testObjectToBytes() throws IOException {
    final String object = "testObject";
    final byte[] bytes = QueueHelper.objectToBytes(object);

    assertEquals(37, bytes.length, "serializing gzip stream");
  }

  @Test
  void testRoundTrip() throws IOException, ClassNotFoundException {
    final LocalDateTime object = LocalDateTime.now();
    final LocalDateTime newObject = (LocalDateTime) QueueHelper.bytesToObject(QueueHelper.objectToBytes(object));
    assertEquals(object, newObject, "serializing and deserializing should be the same");
  }
}
