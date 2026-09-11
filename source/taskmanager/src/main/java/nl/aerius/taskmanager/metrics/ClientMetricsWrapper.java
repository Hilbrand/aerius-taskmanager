package nl.aerius.taskmanager.metrics;

import io.opentelemetry.api.metrics.Meter;

public class ClientMetricsWrapper {
  private final UsageMetricsReporter limitReporter;
  private final UsageMetricsReporter usageReporter;

  public ClientMetricsWrapper(final Meter meter, final String metricPrefix) {
    limitReporter = new UsageMetricsReporter(meter, metricPrefix + ".worker.limit", "Report nunber of workers available");
    usageReporter = new UsageMetricsReporter(meter, metricPrefix + ".worker.usage", "Report worker usage");
  }

  public void add(final UsageMetricsProvider provider) {
    final String workerQueueName = provider.getWorkerQueueName();

    usageReporter.addMetrics(workerQueueName, provider::getNumberOfUsedWorkers,
        OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "used"));
    usageReporter.addMetrics(workerQueueName, provider::getNumberOfFreeWorkers,
        OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "free"));

    usageReporter.addMetrics(workerQueueName, provider::getNumberOfWaiting,
        OpenTelemetryMetrics.workerAttributes(workerQueueName, "state", "waiting"));
  }

  public void remove(final String workerQueueName) {
    limitReporter.removeMetrics(workerQueueName);
    usageReporter.removeMetrics(workerQueueName);
  }

  public void close() {
    limitReporter.close();
    usageReporter.close();
  }

}
