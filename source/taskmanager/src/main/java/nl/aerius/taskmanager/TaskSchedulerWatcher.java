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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.client.WorkerQueueType;
import nl.aerius.taskmanager.domain.TaskQueue;
import nl.aerius.taskmanager.domain.TaskSchedule;
import nl.aerius.taskmanager.scheduler.TaskScheduler.TaskSchedulerFactory;

/**
 * Class that watches the file system for queue configuration changes and passed any changes to the {@link TaskManager} class.
 */
class TaskSchedulerWatcher<T extends TaskQueue, S extends TaskSchedule<T>> implements Runnable {

  private static final String CONFIG_FILE_EXTENSION = ".json";

  private static final Kind<?>[] WATCH_KINDS = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY};

  private static final Logger LOG = LoggerFactory.getLogger(TaskSchedulerWatcher.class);

  private final TaskManager<T, S> schedulerManager;
  private final WatchService watchService;
  private final TaskSchedulerFactory<T, S> schedulerFactory;
  // Keep track of configuration files and which schedule they represent to remove deleted schedule configuration files.
  private final Map<String, String> schedulerFiles = new HashMap<>();
  private final Path watchDirectoryPath;

  private boolean running = true;

  public TaskSchedulerWatcher(final TaskManager<T, S> schedulerManager, final TaskSchedulerFactory<T, S> schedulerFactory, final File watchDirectory)
      throws IOException {
    this.watchDirectoryPath = watchDirectory.toPath();
    this.schedulerManager = schedulerManager;
    this.schedulerFactory = schedulerFactory;
    watchService = FileSystems.getDefault().newWatchService();
    watchDirectoryPath.register(watchService, WATCH_KINDS);
    LOG.info("Watch directory {} for taskmanager configuration files.", watchDirectoryPath);
  }

  @Override
  public void run() {
    try {
      init();
      while (running) {
        doRun();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final RuntimeException | IOException e) {
      LOG.error("Filewatcher stopped unexpected", e);
    }
  }

  private void doRun() throws InterruptedException, IOException {
    final WatchKey key = watchService.take();

    for (final WatchEvent<?> event : key.pollEvents()) {
      final Kind<?> kind = event.kind();
      final WatchEvent<Path> ev = (WatchEvent<Path>) event;
      final File file = new File(watchDirectoryPath.toFile(), ev.context().toFile().getName());

      if (!file.getName().endsWith(CONFIG_FILE_EXTENSION)) {
        continue;
      }
      if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
        updateTaskScheduler(file);
      } else if (kind == ENTRY_DELETE) {
        removeTaskScheduler(file);
      } else {
        LOG.debug("Unsupported watch event: {}", kind);
      }
    }
    //If the key is no longer valid, the directory is inaccessible so exit the loop.
    if (!key.reset()) {
      LOG.warn("Stopped watching directory for configuration changed, because directory not accessable.");
      running = false;
    }
  }

  private void updateTaskScheduler(final File file) throws IOException, InterruptedException {
    final TaskSchedule<T> schedule = rewriteQueueNames(file);
    schedulerManager.updateTaskScheduler(schedule);
    schedulerFiles.put(file.getName(), schedule.getWorkerQueueName());
  }

  private S rewriteQueueNames(final File file) throws IOException {
    final S schedule = schedulerFactory.getHandler().read(file);
    final WorkerQueueType workerQueueType = new WorkerQueueType(schedule.getWorkerQueueName());

    schedule.setWorkerQueueName(workerQueueType.getWorkerQueueName());
    schedule.getQueues().forEach(s -> s.setQueueName(workerQueueType.getTaskQueueName(s.getQueueName())));
    return schedule;
  }

  private void removeTaskScheduler(final File file) {
    final String workerQueueName = schedulerFiles.remove(file.getName());

    if (workerQueueName != null) {
      schedulerManager.removeTaskScheduler(workerQueueName);
    }
  }

  private void init() throws IOException, InterruptedException {
    final File directory = watchDirectoryPath.toFile();
    final String[] files = directory.list((f, s) -> s.endsWith(CONFIG_FILE_EXTENSION));

    for (final String file : files) {
      updateTaskScheduler(new File(directory, file));
    }
  }

  public void shutdown() {
    running = false;
    try {
      watchService.close();
    } catch (final IOException e) {
      // suppress closing exception
      LOG.trace("Error shutting down", e);
    }
  }
}
