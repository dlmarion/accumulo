/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.manager.recovery.HadoopLogCloser;
import org.apache.accumulo.server.manager.recovery.LogCloser;
import org.apache.accumulo.server.manager.recovery.RecoveryPath;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

public class RecoveryManager {

  private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);

  private final Map<String,Long> recoveryDelay = new HashMap<>();
  private final Set<String> closeTasksQueued = new HashSet<>();
  private final Set<String> sortsQueued = new HashSet<>();
  private final Cache<Path,Boolean> existenceCache;
  private final ScheduledExecutorService executor;
  private final Manager manager;

  public RecoveryManager(Manager manager, long timeToCacheExistsInMillis) {
    this.manager = manager;
    existenceCache = this.manager.getContext().getCaches()
        .createNewBuilder(CacheName.RECOVERY_MANAGER_PATH_CACHE, true)
        .expireAfterWrite(timeToCacheExistsInMillis, TimeUnit.MILLISECONDS)
        .maximumWeight(10_000_000).weigher((path, exist) -> path.toString().length()).build();

    executor =
        ThreadPools.getServerThreadPools().createScheduledExecutorService(4, "Walog sort starter");
    try {
      List<String> workIDs =
          new DistributedWorkQueue(Constants.ZRECOVERY, manager.getConfiguration(), manager)
              .getWorkQueued();
      sortsQueued.addAll(workIDs);
    } catch (Exception e) {
      log.warn("{}", e.getMessage(), e);
    }
  }

  private class LogSortTask implements Runnable {
    private final String source;
    private final String destination;
    private final String sortId;
    private final LogCloser closer;

    public LogSortTask(LogCloser closer, String source, String destination, String sortId) {
      this.closer = closer;
      this.source = source;
      this.destination = destination;
      this.sortId = sortId;
    }

    @Override
    public void run() {
      boolean rescheduled = false;
      try {
        long time = closer.close(manager.getConfiguration(), manager.getContext().getHadoopConf(),
            manager.getVolumeManager(), new Path(source));

        if (time > 0) {
          ScheduledFuture<?> future = executor.schedule(this, time, TimeUnit.MILLISECONDS);
          ThreadPools.watchNonCriticalScheduledTask(future);
          rescheduled = true;
        } else {
          initiateSort(sortId, source, destination);
        }
      } catch (FileNotFoundException e) {
        log.debug("Unable to initiate log sort for " + source + ": " + e);
      } catch (Exception e) {
        log.warn("Failed to initiate log sort " + source, e);
      } finally {
        if (!rescheduled) {
          synchronized (RecoveryManager.this) {
            closeTasksQueued.remove(sortId);
          }
        }
      }
    }

  }

  private void initiateSort(String sortId, String source, final String destination)
      throws KeeperException, InterruptedException {
    String work = source + "|" + destination;
    new DistributedWorkQueue(Constants.ZRECOVERY, manager.getConfiguration(), manager)
        .addWork(sortId, work.getBytes(UTF_8));

    synchronized (this) {
      sortsQueued.add(sortId);
    }

    log.info("Created zookeeper entry {} with data {}", Constants.ZRECOVERY + "/" + sortId, work);
  }

  private boolean exists(final Path path) throws IOException {
    try {
      return existenceCache.get(path, k -> {
        try {
          return manager.getVolumeManager().exists(path);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (UncheckedIOException e) {
      throw new IOException(e);
    }
  }

  public boolean recoverLogs(KeyExtent extent, Collection<LogEntry> walogs) throws IOException {
    boolean recoveryNeeded = false;

    for (LogEntry walog : walogs) {

      LogEntry switchedWalog =
          VolumeUtil.switchVolume(walog, manager.getContext().getVolumeReplacements());
      if (switchedWalog != null) {
        // replaces the volume used for sorting, but do not change entry in metadata table. When
        // the tablet loads it will change the metadata table entry. If
        // the tablet has the same replacement config, then it will find the sorted log.
        log.info("Volume replaced {} -> {}", walog, switchedWalog);
        walog = switchedWalog;
      }

      String sortId = walog.getUniqueID().toString();
      String filename = walog.getPath();
      String dest = RecoveryPath.getRecoveryPath(new Path(filename)).toString();

      boolean sortQueued;
      synchronized (this) {
        sortQueued = sortsQueued.contains(sortId);
      }

      if (sortQueued
          && this.manager.getContext().getZooCache().get(Constants.ZRECOVERY + "/" + sortId)
              == null) {
        synchronized (this) {
          sortsQueued.remove(sortId);
        }
      }

      if (exists(SortedLogState.getFinishedMarkerPath(dest))) {
        synchronized (this) {
          closeTasksQueued.remove(sortId);
          recoveryDelay.remove(sortId);
          sortsQueued.remove(sortId);
        }
        continue;
      }

      recoveryNeeded = true;
      synchronized (this) {
        if (!closeTasksQueued.contains(sortId) && !sortsQueued.contains(sortId)) {
          AccumuloConfiguration aconf = manager.getConfiguration();
          LogCloser closer = Property.createInstanceFromPropertyName(aconf,
              Property.MANAGER_WAL_CLOSER_IMPLEMENTATION, LogCloser.class, new HadoopLogCloser());
          Long delay = recoveryDelay.get(sortId);
          if (delay == null) {
            delay = aconf.getTimeInMillis(Property.MANAGER_RECOVERY_DELAY);
          } else {
            delay = Math.min(2 * delay, 1000 * 60 * 5L);
          }

          log.info("Starting recovery of {} (in : {}s), tablet {} holds a reference", filename,
              (delay / 1000), extent);

          ScheduledFuture<?> future = executor.schedule(
              new LogSortTask(closer, filename, dest, sortId), delay, TimeUnit.MILLISECONDS);
          ThreadPools.watchNonCriticalScheduledTask(future);
          closeTasksQueued.add(sortId);
          recoveryDelay.put(sortId, delay);
        }
      }
    }
    return recoveryNeeded;
  }
}
