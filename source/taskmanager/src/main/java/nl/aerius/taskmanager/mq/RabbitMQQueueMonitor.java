/*
 * Copyright the State of the Netherlands
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
package nl.aerius.taskmanager.mq;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.client.configuration.ConnectionConfiguration;

/**
 * RabbitMQ implementation to manage implementation specific part of the worker pool. This covers managing the total size of available workers and
 * informing the worker pool when a worker is finished.
 * <p>When the connection shuts down this pool manager will shutdown.
 */
public class RabbitMQQueueMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQQueueMonitor.class);
  private static final int TIMEOUT = (int) TimeUnit.SECONDS.toMillis(3);

  private final ConnectionConfiguration configuration;
  private final CloseableHttpClient httpClient;
  private final HttpHost targetHost;
  private final HttpClientContext context;

  /**
   * Constructor.
   *
   * @param configuration Connection configuration
   */
  public RabbitMQQueueMonitor(final ConnectionConfiguration configuration) {
    this.configuration = configuration;
    httpClient = HttpClientBuilder.create().setDefaultRequestConfig(getDefaultRequestConfig()).build();
    targetHost = new HttpHost(configuration.getBrokerHost(), configuration.getBrokerManagementPort(), "http");
    final CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(
        new AuthScope(configuration.getBrokerHost(), configuration.getBrokerManagementPort()),
        new UsernamePasswordCredentials(configuration.getBrokerUsername(), configuration.getBrokerPassword()));
    // Create AuthCache instance
    final AuthCache authCache = new BasicAuthCache();
    // Generate BASIC scheme object and add it to the local auth cache
    final BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    context = HttpClientContext.create();
    context.setCredentialsProvider(credsProvider);
    context.setAuthCache(authCache);
  }

  /**
   * Stops the worker pool.
   */
  public void shutdown() {
    close();
  }

  public void close() {
    try {
      httpClient.close();
    } catch (final IOException e) {
      LOG.trace("IOException on close httpclient", e);
    }
  }

  public void updateWorkerQueueState(final String queueName, final WorkerSizeObserver observer) {
    // Use RabbitMQ HTTP-API.
    // URL: [host]:[port]/api/queues/[virtualHost]/[QueueName]
    final String virtualHost = configuration.getBrokerVirtualHost().replace("/", "%2f");
    final String apiPath = "/api/queues/" + virtualHost + "/" + queueName;

    try {
      final JsonElement je = getJsonResultFromApi(apiPath);
      if (je == null) {
        LOG.error("Queue configuration from RabbitMQ admin json get call returned null.");
      } else {
        final JsonObject jsonObject = je.getAsJsonObject();
        final int numberOfWorkers = getJsonIntPrimitive(jsonObject, "consumers");
        final int numberOfMessages = getJsonIntPrimitive(jsonObject, "messages");
        final int numberOfMessagesUnacknowledged = getJsonIntPrimitive(jsonObject, "messages_unacknowledged");

        observer.onNumberOfWorkersUpdate(numberOfWorkers, numberOfMessages, numberOfMessagesUnacknowledged);
        LOG.trace("[{}] active workers:{}", queueName, numberOfWorkers);
      }
    } catch (final URISyntaxException | IOException e) {
      LOG.info("Error getting RabbitMQ status from admin api: {}", e.getMessage());
    }
  }

  private static int getJsonIntPrimitive(final JsonObject jsonObject, final String key) {
    final int value;
    if (jsonObject == null || jsonObject.getAsJsonPrimitive(key) == null) {
      value = 0;
    } else {
      value = jsonObject.getAsJsonPrimitive(key).getAsInt();
    }
    return value;
  }

  protected JsonElement getJsonResultFromApi(final String apiPath) throws URISyntaxException, IOException {
    JsonElement returnElement = null;
    final URI uri = new URI("http://" + configuration.getBrokerHost() + ":" + configuration.getBrokerManagementPort() + apiPath);
    try (final CloseableHttpResponse response = httpClient.execute(targetHost, new HttpGet(uri), context)) {
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        try (final InputStreamReader is = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8);
            final JsonReader jr = new JsonReader(is)) {
          returnElement = JsonParser.parseReader(jr);
        }
      } else {
        throw new IOException(String.format("Status code wasn't 200 when retrieving json result. Status was: %d, %s",
            response.getStatusLine().getStatusCode(), response.getStatusLine()));
      }
    }
    return returnElement;
  }

  private static RequestConfig getDefaultRequestConfig() {
    return RequestConfig.custom()
        .setConnectTimeout(TIMEOUT)
        .setConnectionRequestTimeout(TIMEOUT)
        .setSocketTimeout(TIMEOUT)
        .build();
  }
}
