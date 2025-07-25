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
package org.apache.accumulo.gc;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockSupport.HAServiceLockWatcher;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.gc.metrics.GcCycleMetrics;
import org.apache.accumulo.gc.metrics.GcMetrics;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

// Could/Should implement HighlyAvailableService but the Thrift server is already started before
// the ZK lock is acquired. The server is only for metrics, there are no concerns about clients
// using the service before the lock is acquired.
public class SimpleGarbageCollector extends AbstractServer implements Iface {

  private static final Logger log = LoggerFactory.getLogger(SimpleGarbageCollector.class);

  private final GCStatus status =
      new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());

  private final GcCycleMetrics gcCycleMetrics = new GcCycleMetrics();
  private ServiceLock gcLock;

  private final Timer lastCompactorCheck = Timer.startNew();

  SimpleGarbageCollector(ConfigOpts opts, String[] args) {
    super(ServerId.Type.GARBAGE_COLLECTOR, opts, ServerContext::new, args);

    final AccumuloConfiguration conf = getConfiguration();

    final long gcDelay = conf.getTimeInMillis(Property.GC_CYCLE_DELAY);
    final String useFullCompaction = conf.get(Property.GC_USE_FULL_COMPACTION);

    log.info("start delay: {} milliseconds", getStartDelay());
    log.info("time delay: {} milliseconds", gcDelay);
    log.info("safemode: {}", inSafeMode());
    log.info("candidate batch size: {} bytes", getCandidateBatchSize());
    log.info("delete threads: {}", getNumDeleteThreads());
    log.info("gc post metadata action: {}", useFullCompaction);
  }

  public static void main(String[] args) throws Exception {
    try (SimpleGarbageCollector gc = new SimpleGarbageCollector(new ConfigOpts(), args)) {
      gc.runServer();
    }
  }

  /**
   * Gets the delay before the first collection.
   *
   * @return start delay, in milliseconds
   */
  long getStartDelay() {
    return getConfiguration().getTimeInMillis(Property.GC_CYCLE_START);
  }

  /**
   * Gets the number of threads used for deleting files.
   *
   * @return number of delete threads
   */
  int getNumDeleteThreads() {
    return getConfiguration().getCount(Property.GC_DELETE_THREADS);
  }

  /**
   * Gets the batch size for garbage collecting.
   *
   * @return candidate batch size.
   */
  long getCandidateBatchSize() {
    return getConfiguration().getAsBytes(Property.GC_CANDIDATE_BATCH_SIZE);
  }

  /**
   * Checks if safemode is set - files will not be deleted.
   *
   * @return true if safe mode is set, false otherwise
   */
  boolean inSafeMode() {
    return getConfiguration().getBoolean(Property.GC_SAFEMODE);
  }

  @Override
  @SuppressFBWarnings(value = "DM_EXIT", justification = "main class can call System.exit")
  public void run() {

    try {
      waitForUpgrade();
    } catch (InterruptedException e) {
      log.error("Interrupted while waiting for upgrade to complete, exiting...");
      System.exit(1);
    }

    final VolumeManager fs = getContext().getVolumeManager();

    // Sleep for an initial period, giving the manager time to start up and
    // old data files to be unused
    log.info("Trying to acquire ZooKeeper lock for garbage collector");

    try {
      startStatsService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the gc client service", e1);
    }

    try {
      getZooLock(getAdvertiseAddress());
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
      System.exit(1);
    }
    this.getContext().setServiceLock(gcLock);

    MetricsInfo metricsInfo = getContext().getMetricsInfo();

    metricsInfo.addMetricsProducers(this, new GcMetrics(this));
    metricsInfo.init(MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        getAdvertiseAddress(), getResourceGroup()));
    try {
      long delay = getStartDelay();
      log.debug("Sleeping for {} milliseconds before beginning garbage collection cycles", delay);
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      log.warn("{}", e.getMessage(), e);
      return;
    }

    // This is created outside of the run loop and passed to the walogCollector so that
    // only a single timed task is created (internal to LiveTServerSet) using SimpleTimer.
    final LiveTServerSet liveTServerSet = new LiveTServerSet(getContext());
    liveTServerSet.startListeningForTabletServerChanges((current, deleted, added) -> {
      log.debug("Number of current servers {}, tservers added {}, removed {}",
          current == null ? -1 : current.size(), added, deleted);

      if (log.isTraceEnabled()) {
        log.trace("Current servers: {}\nAdded: {}\n Removed: {}", current, added, deleted);
      }
    });

    while (!isShutdownRequested()) {
      if (Thread.currentThread().isInterrupted()) {
        log.info("Server process thread has been interrupted, shutting down");
        break;
      }
      try {
        Span outerSpan = TraceUtil.startSpan(this.getClass(), "gc");
        try (Scope outerScope = outerSpan.makeCurrent()) {
          Span innerSpan = TraceUtil.startSpan(this.getClass(), "loop");
          try (Scope innerScope = innerSpan.makeCurrent()) {
            final long tStart = System.nanoTime();
            try {
              System.gc(); // make room

              status.current.started = System.currentTimeMillis();
              var rootGC = new GCRun(DataLevel.ROOT, getContext());
              var mdGC = new GCRun(DataLevel.METADATA, getContext());
              var userGC = new GCRun(DataLevel.USER, getContext());

              log.info("Starting Root table Garbage Collection.");
              status.current.bulks += new GarbageCollectionAlgorithm().collect(rootGC);
              incrementStatsForRun(rootGC);
              logStats();

              log.info("Starting Metadata table Garbage Collection.");
              status.current.bulks += new GarbageCollectionAlgorithm().collect(mdGC);
              incrementStatsForRun(mdGC);
              logStats();

              log.info("Starting User table Garbage Collection.");
              status.current.bulks += new GarbageCollectionAlgorithm().collect(userGC);
              incrementStatsForRun(userGC);
              logStats();

            } catch (Exception e) {
              TraceUtil.setException(innerSpan, e, false);
              log.error("{}", e.getMessage(), e);
            } finally {
              status.current.finished = System.currentTimeMillis();
              status.last = status.current;
              gcCycleMetrics.setLastCollect(status.current);
              status.current = new GcCycleStats();
            }

            final long tStop = System.nanoTime();
            log.info(String.format("Collect cycle took %.2f seconds",
                (TimeUnit.NANOSECONDS.toMillis(tStop - tStart) / 1000.0)));

            // Clean up any unused write-ahead logs
            Span walSpan = TraceUtil.startSpan(this.getClass(), "walogs");
            try (Scope walScope = walSpan.makeCurrent()) {
              GarbageCollectWriteAheadLogs walogCollector =
                  new GarbageCollectWriteAheadLogs(getContext(), fs, liveTServerSet);
              log.info("Beginning garbage collection of write-ahead logs");
              walogCollector.collect(status);
              gcCycleMetrics.setLastWalCollect(status.lastLog);
            } catch (Exception e) {
              TraceUtil.setException(walSpan, e, false);
              log.error("{}", e.getMessage(), e);
            } finally {
              walSpan.end();
            }
          } catch (Exception e) {
            TraceUtil.setException(innerSpan, e, true);
            throw e;
          } finally {
            innerSpan.end();
          }

          // we just made a lot of metadata changes: flush them out
          try {
            AccumuloClient accumuloClient = getContext();

            final long actionStart = System.nanoTime();

            String action = getConfiguration().get(Property.GC_USE_FULL_COMPACTION);
            log.debug("gc post action {} started", action);

            switch (action) {
              case "compact":
                accumuloClient.tableOperations().compact(SystemTables.METADATA.tableName(), null,
                    null, true, true);
                accumuloClient.tableOperations().compact(SystemTables.ROOT.tableName(), null, null,
                    true, true);
                break;
              case "flush":
                accumuloClient.tableOperations().flush(SystemTables.METADATA.tableName(), null,
                    null, true);
                accumuloClient.tableOperations().flush(SystemTables.ROOT.tableName(), null, null,
                    true);
                break;
              default:
                log.trace("'none - no action' or invalid value provided: {}", action);
            }

            final long actionComplete = System.nanoTime();

            gcCycleMetrics.setPostOpDurationNanos(actionComplete - actionStart);

            log.info("gc post action {} completed in {} seconds", action, String.format("%.2f",
                (TimeUnit.NANOSECONDS.toMillis(actionComplete - actionStart) / 1000.0)));

          } catch (Exception e) {
            TraceUtil.setException(outerSpan, e, false);
            log.warn("{}", e.getMessage(), e);
          }
        } catch (Exception e) {
          TraceUtil.setException(outerSpan, e, true);
          throw e;
        } finally {
          outerSpan.end();
        }
        try {

          gcCycleMetrics.incrementRunCycleCount();
          long gcDelay = getConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);

          if (lastCompactorCheck.hasElapsed(gcDelay * 3, MILLISECONDS)) {
            Map<ResourceGroupId,Set<TableId>> resourceMapping = new HashMap<>();
            for (TableId tid : SystemTables.tableIds()) {
              TableConfiguration tconf = getContext().getTableConfiguration(tid);
              String propVal = tconf.get(TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY);
              ResourceGroupId rg =
                  propVal == null ? ResourceGroupId.DEFAULT : ResourceGroupId.of(propVal);
              resourceMapping.computeIfAbsent(rg, k -> new HashSet<>()).add(tid);
            }
            for (Entry<ResourceGroupId,Set<TableId>> e : resourceMapping.entrySet()) {
              if (ExternalCompactionUtil.countCompactors(e.getKey(), getContext()) == 0) {
                log.warn("No Compactors exist in resource group {} for system table {}", e.getKey(),
                    e.getValue());
              }
            }
            lastCompactorCheck.restart();
          }

          log.debug("Sleeping for {} milliseconds", gcDelay);
          Thread.sleep(gcDelay);
        } catch (InterruptedException e) {
          log.warn("{}", e.getMessage(), e);
          throw e;
        }
      } catch (InterruptedException e) {
        log.info("Interrupt Exception received, shutting down");
        gracefulShutdown(getContext().rpcCreds());
      }
    }
    super.close();
    getShutdownComplete().set(true);
    log.info("stop requested. exiting ... ");
    try {
      gcLock.unlock();
    } catch (Exception e) {
      log.warn("Failed to release GarbageCollector lock", e);
    }

  }

  private void incrementStatsForRun(GCRun gcRun) {
    status.current.candidates += gcRun.getCandidatesStat();
    status.current.inUse += gcRun.getInUseStat();
    status.current.deleted += gcRun.getDeletedStat();
    status.current.errors += gcRun.getErrorsStat();
  }

  private void logStats() {
    log.info("Number of data file candidates for deletion: {}", status.current.candidates);
    log.info("Number of data file candidates still in use: {}", status.current.inUse);
    log.info("Number of successfully deleted data files: {}", status.current.deleted);
    log.info("Number of data files delete failures: {}", status.current.errors);
    log.info("Number of bulk imports in progress: {}", status.current.bulks);
  }

  /**
   * Moves a file to trash. If this garbage collector is not using trash, this method returns false
   * and leaves the file alone. If the file is missing, this method returns false as opposed to
   * throwing an exception.
   *
   * @return true if the file was moved to trash
   * @throws IOException if the volume manager encountered a problem
   */
  boolean moveToTrash(Path path) throws IOException {
    final VolumeManager fs = getContext().getVolumeManager();
    try {
      return fs.moveToTrash(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  private void getZooLock(HostAndPort addr) throws KeeperException, InterruptedException {
    var path = getContext().getServerPaths().createGarbageCollectorPath();

    UUID zooLockUUID = UUID.randomUUID();
    gcLock = new ServiceLock(getContext().getZooSession(), path, zooLockUUID);
    HAServiceLockWatcher gcLockWatcher =
        new HAServiceLockWatcher(Type.GARBAGE_COLLECTOR, () -> getShutdownComplete().get());

    while (true) {
      gcLock.lock(gcLockWatcher, new ServiceLockData(zooLockUUID, addr.toString(), ThriftService.GC,
          this.getResourceGroup()));

      gcLockWatcher.waitForChange();

      if (gcLockWatcher.isLockAcquired()) {
        break;
      }

      if (!gcLockWatcher.isFailedToAcquireLock()) {
        throw new IllegalStateException("gc lock in unknown state");
      }

      gcLock.tryToCancelAsyncLockOrUnlock();

      log.debug("Failed to get GC ZooKeeper lock, will retry");
      sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }

    log.info("Got GC lock.");

  }

  private void startStatsService() throws UnknownHostException {
    var processor = ThriftProcessorTypes.getGcTProcessor(this, this, getContext());
    IntStream port = getConfiguration().getPortStream(Property.GC_PORT);
    HostAndPort[] addresses = TServerUtils.getHostAndPorts(getBindAddress(), port);
    long maxMessageSize = getConfiguration().getAsBytes(Property.RPC_MAX_MESSAGE_SIZE);
    updateThriftServer(() -> {
      return TServerUtils.createThriftServer(getConfiguration(), getContext().getThriftServerType(),
          processor, this.getClass().getSimpleName(), 2, ThreadPools.DEFAULT_TIMEOUT_MILLISECS,
          1000, maxMessageSize, getContext().getServerSslParams(), getContext().getSaslParams(), 0,
          getConfiguration().getCount(Property.RPC_BACKLOG), getContext().getMetricsInfo(), false,
          addresses);
    }, true);
  }

  /**
   * Checks if the given string is a directory.
   *
   * @param delete possible directory
   * @return true if string is a directory
   */
  static boolean isDir(String delete) {
    if (delete == null) {
      return false;
    }

    int slashCount = 0;
    for (int i = 0; i < delete.length(); i++) {
      if (delete.charAt(i) == '/') {
        slashCount++;
      }
    }
    return slashCount == 1;
  }

  @Override
  public GCStatus getStatus(TInfo info, TCredentials credentials) {
    return status;
  }

  public GcCycleMetrics getGcCycleMetrics() {
    return gcCycleMetrics;
  }

  @Override
  public ServiceLock getLock() {
    return gcLock;
  }

}
