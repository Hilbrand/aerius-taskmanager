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
package nl.aerius.taskmanager.config;

import java.util.function.Function;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Custom Deserializer Modifier to override property values with an environment variable.
 */
public class EnvOverrideBeanDeserializerModifier extends BeanDeserializerModifier {

  private static final long serialVersionUID = 1L;

  private final String prefix;
  private final Function<String, String> getEnv;

  private EnvOverrideBeanDeserializerModifier(final String prefix, final Function<String, String> getEnv) {
    this.prefix = prefix;
    this.getEnv = getEnv;
  }

  /**
   * Initializes the customized ObjectMapper and uses the System.getenv to get environment variables.
   *
   * @param prefix String prefixed to the property path of the string to use as environment variable name
   * @return customized object mapper
   */
  public static ObjectMapper objectMapper(final String prefix) {
    return objectMapper(prefix, System::getenv);
  }

  static ObjectMapper objectMapper(final String prefix, final Function<String, String> getEnv) {
    final SimpleModule module = new SimpleModule();

    module.setDeserializerModifier(new EnvOverrideBeanDeserializerModifier(prefix, getEnv));
    return new ObjectMapper().registerModule(module);
  }

  @Override
  public JsonDeserializer<?> modifyDeserializer(final DeserializationConfig config, final BeanDescription beanDesc,
      final JsonDeserializer<?> deserializer) {
    return new EnvOverrideDeserializer<>(deserializer, beanDesc.getBeanClass(), prefix, getEnv);
  }
}
