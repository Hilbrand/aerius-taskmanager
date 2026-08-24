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
package nl.aerius.taskmanager.metrics;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

/**
 * Test class for {@link UsageMetricsReporter}.
 */
@ExtendWith(MockitoExtension.class)
class UsageMetricsReporterTest {

  private @Mock Meter meter;
  private @Mock DoubleGaugeBuilder builder;
  private @Captor ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> callbackCaptor;
  private @Mock DoubleSupplier supplier;
  private @Mock Attributes attributes;
  private @Mock ObservableDoubleMeasurement measurement;

  @Test
  void testRecord() {
    doReturn(builder).when(meter).gaugeBuilder("TEST");
    doReturn(builder).when(builder).setDescription("description");
    doReturn(2.0).when(supplier).getAsDouble();

    final UsageMetricsReporter reporter = new UsageMetricsReporter(meter, "TEST", "description");
    reporter.addMetrics("TEST", supplier, attributes);
    verify(builder).buildWithCallback(callbackCaptor.capture());
    // Call the metric record callback method
    callbackCaptor.getValue().accept(measurement);
    // Verify the value 2 is recorded as metric with the provided attributes.
    verify(measurement).record(2.0, attributes);
  }
}
