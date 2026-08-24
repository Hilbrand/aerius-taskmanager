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
package nl.aerius.taskmanager;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.client.BrokerConnectionFactory;
import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.domain.TaskManagerConfiguration;
import nl.aerius.taskmanager.mq.RabbitMQAdaptorFactory;
import nl.aerius.taskmanager.scheduler.priorityqueue.PriorityTaskSchedulerFactory;

/**
 * The main class, used to start the task manager.
 */
public final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  /**
   * Number of threads for the scheduled threads.
   */
  private static final int THREAD_POOL_SIZE = 30;

  private Main() {
    // main class
  }

  /**
   * When this main method is used, the task manager will be started.
   *
   * @param args no arguments needed, but if supplied, they should fit the description given by using -help.
   */
  public static void main(final String[] args) {
    final CmdOptions cmdOptions = cmdOptions(args);

    if (cmdOptions == null || cmdOptions.printIfInfoOption()) {
      return;
    }
    LOG.info("--------------------------------TASKMANAGER STARTED------------------------------------");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("--------------------------------TASKMANAGER STOPPED-----------------------------------");
      }
    });
    Thread.currentThread().setName("Main");

    try (final ExecutorService executorService = Executors.newCachedThreadPool();
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(THREAD_POOL_SIZE)) {
      startupFromConfiguration(executorService, scheduledExecutorService, cmdOptions.getConfigFile());
    } catch (final IOException e) {
      LOG.error("IOException during startup.", e);
    }
  }

  private static CmdOptions cmdOptions(final String[] args) {
    try {
      return new CmdOptions(args);
    } catch (final ParseException e) {
      LOG.error("Command line options could not be parsed.", e);
      return null;
    }
  }

  /**
   * Starts the task manager.
   *
   * @param executorService dynamic execution service
   * @param scheduledExecutorService scheduled execution service for scheduled tasks
   * @param configurationFile configuration properties file
   * @throws IOException io errors
   */
  static void startupFromConfiguration(final ExecutorService executorService, final ScheduledExecutorService scheduledExecutorService,
      final String configurationFile) throws IOException {
    final Properties props = ConfigurationManager.getPropertiesFromFile(configurationFile);
    final TaskManagerConfiguration tmConfig = ConfigurationManager.loadConfiguration(props);
    final BrokerConnectionFactory bcFactory = new BrokerConnectionFactory(executorService, tmConfig.getBrokerConfiguration());
    final AdaptorFactory aFactory = new RabbitMQAdaptorFactory(scheduledExecutorService, bcFactory);
    final WorkerSizeProviderProxy workerSizeObserver = aFactory.createWorkerSizeProvider();
    final PriorityTaskSchedulerFactory schedulerFactory = new PriorityTaskSchedulerFactory();
    final TaskManager<PriorityTaskQueue, PriorityTaskSchedule> manager = new TaskManager<>(executorService, scheduledExecutorService, aFactory,
        schedulerFactory, workerSizeObserver);
    final TaskSchedulerWatcher<PriorityTaskQueue, PriorityTaskSchedule> watcher = new TaskSchedulerWatcher<>(manager, schedulerFactory,
        tmConfig.getConfigurationDirectory());

    try {
      workerSizeObserver.start();
      watcher.run(); // This will wait until shutdown.
    } finally {
      manager.shutdown();
      workerSizeObserver.shutdown();
    }
  }
}
