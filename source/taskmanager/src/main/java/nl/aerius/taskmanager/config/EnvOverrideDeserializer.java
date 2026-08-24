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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Custom deserializer that makes it possible to override values via environment variables.
 */
class EnvOverrideDeserializer<T> extends StdDeserializer<T> implements ResolvableDeserializer {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(EnvOverrideDeserializer.class);

  private final String prefix;
  private final Function<String, String> getEnv;
  private final JsonDeserializer<T> delegate;

  /**
   * Constructor.
   *
   * @param delegate deserializer delegate
   * @param clazz Target class to store the properties in
   * @param prefix environment variable prefix
   * @param getEnv method to do the actual call to get an environment value
   */
  public EnvOverrideDeserializer(final JsonDeserializer<?> delegate, final Class<?> clazz, final String prefix,
      final Function<String, String> getEnv) {
    super(clazz);
    this.delegate = (JsonDeserializer<T>) delegate;
    this.prefix = prefix;
    this.getEnv = getEnv;
  }

  @Override
  public T deserialize(final JsonParser p, final DeserializationContext ctx) throws IOException {
    // Read the entire object as a tree so we can patch it before delegating
    final ObjectMapper mapper = (ObjectMapper) p.getCodec();
    final JsonNode tree = mapper.readTree(p);

    if (tree.isObject()) {
      applyEnvOverrides((ObjectNode) tree, prefix, handledType());
    }

    // Re-parse the (possibly patched) tree
    final JsonParser patchedParser = tree.traverse(mapper);
    patchedParser.nextToken();
    return delegate.deserialize(patchedParser, ctx);
  }

  private void applyEnvOverrides(final ObjectNode node, final String parentPath, final Class<?> targetClass) {
    final Set<Entry<String, JsonNode>> fields = node.properties();

    applyEnvOverridesOnSetProperties(node, parentPath, targetClass, fields);
    applyEnvOverridesToUnsetProperties(node, parentPath, targetClass);
  }

  /**
   * Walk along properties set in the file and override from environment variable if present.
   */
  private void applyEnvOverridesOnSetProperties(final ObjectNode node, final String parentPath, final Class<?> targetClass, final Set<Entry<String, JsonNode>> fields) {
    for (final Map.Entry<String, JsonNode> entry : fields) {
      final String fieldPath = parentPath.isEmpty() ? entry.getKey() : parentPath + "." + entry.getKey();
      final String envValue = resolveEnvOverride(fieldPath);

      if (envValue != null) {
        LOG.info("Environment variable '{}' overrides property '{}'", toEnvVarName(fieldPath), entry.getKey());
        node.put(entry.getKey(), envValue);
      } else if (entry.getValue().isObject()) {
        applyEnvOverrides((ObjectNode) entry.getValue(), fieldPath, resolveFieldType(targetClass, entry.getKey()));
      } else {
        LOG.debug("Unexpected entry value (in parentPath: {}) that is not an object: {}", parentPath, entry.getValue());
      }
    }
  }

  /**
   * Walk along all fields in the target data class that were not set in the properties file and override from environment variable if present.
   */
  private void applyEnvOverridesToUnsetProperties(final ObjectNode node, final String parentPath, final Class<?> targetClass) {
    Class<?> clazzToCheck = targetClass;

    while (clazzToCheck != null) {
      for (final Field field : clazzToCheck.getDeclaredFields()) {
        final String fieldName = field.getName();
        final String fieldPath = parentPath.isEmpty() ? fieldName : parentPath + "." + fieldName;

        // Only inject if not already in the JSON node
        if (!node.has(fieldName)) {
          final String envValue = resolveEnvOverride(fieldPath);

          if (envValue != null) {
            LOG.info("Environment variable '{}' sets property '{}'", toEnvVarName(fieldPath), fieldName);
            node.put(fieldName, envValue);
          }
        }
      }
      clazzToCheck = clazzToCheck.getSuperclass();
    }
  }

  private Class<?> resolveFieldType(final Class<?> clazz, final String fieldName) {
    Class<?> resolvedType = null;

    if (clazz != null) {
      try {
        resolvedType = clazz.getDeclaredField(fieldName).getType();
      } catch (final NoSuchFieldException e) {
        // Walk up superclass hierarchy
        final Class<?> superClass = clazz.getSuperclass();

        if (superClass != null && superClass != Object.class) {
          resolvedType = resolveFieldType(superClass, fieldName);
        }
      }
    }
    return resolvedType;
  }

  @Override
  public void resolve(final DeserializationContext ctx) throws JsonMappingException {
    if (delegate instanceof ResolvableDeserializer) {
      ((ResolvableDeserializer) delegate).resolve(ctx);
    }
  }

  private static String toEnvVarName(final String jsonPath) {
    return jsonPath.toUpperCase()
        .replace(".", "_")
        .replace("-", "_")
        .replace("[", "_")
        .replace("]", "");
  }

  private String resolveEnvOverride(final String path) {
    return getEnv.apply(toEnvVarName(path));
  }
}
