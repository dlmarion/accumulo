/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.threads.ThreadPools.watchCriticalScheduledTask;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.ScanServerBusyException;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerTypes;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyWatcher;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.compactions.ExternalCompactionJob;
import org.apache.accumulo.tserver.log.DfsLogger.ServerResources;
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.managermessage.ManagerMessage;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.CompactableImpl;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.zookeeper.KeeperException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

public class ScanServer extends AbstractServer implements TabletHostingServer {

  /**
   * Tablet implementation that is meant to only be used with scans
   */
  static class ReadOnlyTablet extends Tablet {

    public ReadOnlyTablet(TabletHostingServer scanServer, KeyExtent extent,
        TabletResourceManager trm, TabletData data) throws IOException, IllegalArgumentException {
      super(scanServer, extent, trm, data);
    }

    @Override
    protected CompactableImpl getCompactable(TabletData data) {
      return new CompactableImpl(this, tabletServer.getCompactionManager(), Map.of());
    }

    @Override
    protected void recoverTablet(SortedMap<StoredTabletFile,DataFileValue> datafiles,
        List<LogEntry> logEntries) {
      // do nothing as this is a read-only tablet
    }

    @Override
    protected void removeOldScanRefs(HashSet<StoredTabletFile> scanFiles) {
      // do nothing as this is a read-only tablet
    }

    @Override
    protected void removeOldTemporaryFiles(
        Map<ExternalCompactionId,ExternalCompactionMetadata> externalCompactions) {
      // do nothing as this is a read-only tablet
    }

    @Override
    public void close(boolean saveState) throws IOException {
      // Never save the state
      super.close(false);
    }

    // TODO: Should we override all methods that mutate Tablet to throw
    // UnsupportedOperationException ?
  }

  /**
   * A compaction manager that does nothing
   */
  private static class ScanServerCompactionManager extends CompactionManager {

    public ScanServerCompactionManager(ServerContext context,
        CompactionExecutorsMetrics ceMetrics) {
      super(new ArrayList<>(), context, ceMetrics);
    }

    @Override
    public void compactableChanged(Compactable compactable) {}

    @Override
    public void start() {}

    @Override
    public CompactionServices getServices() {
      return null;
    }

    @Override
    public boolean isCompactionQueued(KeyExtent extent, Set<CompactionServiceId> servicesUsed) {
      return false;
    }

    @Override
    public int getCompactionsRunning() {
      return 0;
    }

    @Override
    public int getCompactionsQueued() {
      return 0;
    }

    @Override
    public ExternalCompactionJob reserveExternalCompaction(String queueName, long priority,
        String compactorId, ExternalCompactionId externalCompactionId) {
      return null;
    }

    @Override
    public void registerExternalCompaction(ExternalCompactionId ecid, KeyExtent extent,
        CompactionExecutorId ceid) {}

    @Override
    public void commitExternalCompaction(ExternalCompactionId extCompactionId,
        KeyExtent extentCompacted, Map<KeyExtent,Tablet> currentTablets, long fileSize,
        long entries) {}

    @Override
    public void externalCompactionFailed(ExternalCompactionId ecid, KeyExtent extentCompacted,
        Map<KeyExtent,Tablet> currentTablets) {}

    @Override
    public List<TCompactionQueueSummary> getCompactionQueueSummaries() {
      return null;
    }

    @Override
    public Collection<ExtCompMetric> getExternalMetrics() {
      return Collections.emptyList();
    }

    @Override
    public void compactableClosed(KeyExtent extent, Set<CompactionServiceId> servicesUsed,
        Set<ExternalCompactionId> ecids) {}

  }

  /**
   * CompactionExecutorMetrics implementation for the ScanServer that does not start the background
   * update thread
   */
  public static class ScanServerCompactionExecutorMetrics extends CompactionExecutorsMetrics {

    @Override
    protected void startUpdateThread() {}

  }

  /**
   * CacheLoader implementation for loading TabletMetadata
   */
  private static class TabletMetadataLoader implements CacheLoader<KeyExtent,TabletMetadata> {

    private final Ample ample;

    private TabletMetadataLoader(Ample ample) {
      this.ample = ample;
    }

    @Override
    public @Nullable TabletMetadata load(KeyExtent keyExtent) {
      long t1 = System.currentTimeMillis();
      var tm = ample.readTablet(keyExtent);
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for 1 tablet in {} ms", t2 - t1);
      return tm;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<? extends KeyExtent,? extends TabletMetadata>
        loadAll(Set<? extends KeyExtent> keys) {
      long t1 = System.currentTimeMillis();
      var tms = ample.readTablets().forTablets((Collection<KeyExtent>) keys).build().stream()
          .collect(Collectors.toMap(tm -> tm.getExtent(), tm -> tm));
      long t2 = System.currentTimeMillis();
      LOG.trace("Read metadata for {} tablets in {} ms", keys.size(), t2 - t1);
      return tms;
    }
  }

  static class ReservedFile {
    Set<Long> activeReservations = new ConcurrentSkipListSet<>();
    volatile long lastUseTime;

    boolean shouldDelete(long expireTimeMs) {
      return activeReservations.isEmpty()
          && System.currentTimeMillis() - lastUseTime > expireTimeMs;
    }
  }

  private class FilesLock implements AutoCloseable {

    private final Collection<StoredTabletFile> files;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FilesLock(Collection<StoredTabletFile> files) {
      this.files = files;
    }

    Collection<StoredTabletFile> getLockedFiles() {
      return files;
    }

    @Override
    public void close() {
      // only allow close to be called once
      if (!closed.compareAndSet(false, true)) {
        return;
      }

      synchronized (lockedFiles) {
        for (StoredTabletFile file : files) {
          if (!lockedFiles.remove(file)) {
            throw new IllegalStateException("tried to unlock file that was not locked");
          }
        }

        lockedFiles.notifyAll();
      }
    }
  }

  class ScanReservation implements AutoCloseable {

    private final Collection<StoredTabletFile> files;
    private final long myReservationId;
    private final Map<KeyExtent,TabletMetadata> tabletsMetadata;

    ScanReservation(Map<KeyExtent,TabletMetadata> tabletsMetadata, long myReservationId) {
      this.tabletsMetadata = tabletsMetadata;
      this.files = tabletsMetadata.values().stream().flatMap(tm -> tm.getFiles().stream())
          .collect(Collectors.toUnmodifiableSet());
      this.myReservationId = myReservationId;
    }

    ScanReservation(Collection<StoredTabletFile> files, long myReservationId) {
      this.tabletsMetadata = null;
      this.files = files;
      this.myReservationId = myReservationId;
    }

    public TabletMetadata getTabletMetadata(KeyExtent extent) {
      return tabletsMetadata.get(extent);
    }

    ReadOnlyTablet newTablet(KeyExtent extent) throws IOException {
      var tabletMetadata = getTabletMetadata(extent);
      TabletData data = new TabletData(tabletMetadata);
      TabletResourceManager trm =
          resourceManager.createTabletResourceManager(tabletMetadata.getExtent(),
              getContext().getTableConfiguration(tabletMetadata.getExtent().tableId()));
      return new ReadOnlyTablet(ScanServer.this, tabletMetadata.getExtent(), trm, data);
    }

    @Override
    public void close() {
      try (FilesLock flock = lockFiles(files)) {
        for (StoredTabletFile file : flock.getLockedFiles()) {
          var reservedFile = reservedFiles.get(file);

          if (!reservedFile.activeReservations.remove(myReservationId)) {
            throw new IllegalStateException("reservation id was not in set as expected");
          }

          LOG.trace("RFFS {} unreserved reference for file {}", myReservationId, file);

          // TODO maybe use nano time
          reservedFile.lastUseTime = System.currentTimeMillis();
        }
      }
    }
  }

  public static class ScanServerThriftScanClientHandler extends ThriftScanClientHandler
      implements TabletScanClientService.Iface {

    protected ScanServer sserver;

    public ScanServerThriftScanClientHandler(ScanServer server) {
      super(server);
      this.sserver = server;
    }

    @Override
    public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent,
        TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
        Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
        boolean isolated, long readaheadThreshold, TSamplerConfiguration samplerConfig,
        long batchTimeOut, String classLoaderContext, Map<String,String> executionHints,
        long busyTimeout) throws ThriftSecurityException, NotServingTabletException,
        TooManyFilesException, TSampleNotPresentException, ScanServerBusyException {

      KeyExtent extent = sserver.getKeyExtent(textent);

      try (ScanReservation reservation = sserver.reserveFiles(Collections.singleton(extent))) {

        ReadOnlyTablet tablet = reservation.newTablet(extent);

        InitialScan is = super.startScan(tinfo, credentials, extent, range, columns, batchSize,
            ssiList, ssio, authorizations, waitForWrites, isolated, readaheadThreshold,
            samplerConfig, batchTimeOut, classLoaderContext, executionHints,
            sserver.getScanTabletResolver(tablet), busyTimeout);

        return is;

      } catch (TException e) {
        throw e;
      } catch (AccumuloException | IOException e) {
        LOG.error("Error starting scan", e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public ScanResult continueScan(TInfo tinfo, long scanID, long busyTimeout)
        throws NoSuchScanIDException, NotServingTabletException, TooManyFilesException,
        TSampleNotPresentException, ScanServerBusyException {
      LOG.debug("continue scan: {}", scanID);

      try (ScanReservation reservation = sserver.reserveFiles(scanID)) {
        return super.continueScan(tinfo, scanID, busyTimeout);
      }
    }

    @Override
    public void closeScan(TInfo tinfo, long scanID) {
      LOG.debug("close scan: {}", scanID);
      super.closeScan(tinfo, scanID);
    }

    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
        Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
        Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
        TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
        Map<String,String> executionHints, long busyTimeout)
        throws ThriftSecurityException, TSampleNotPresentException, ScanServerBusyException {

      if (tbatch.size() == 0) {
        throw new RuntimeException("Scan Server batch must include at least one extent");
      }

      final Map<KeyExtent,List<TRange>> batch = new HashMap<>();

      for (Entry<TKeyExtent,List<TRange>> entry : tbatch.entrySet()) {
        KeyExtent extent = sserver.getKeyExtent(entry.getKey());
        batch.put(extent, entry.getValue());
      }

      try (ScanReservation reservation = sserver.reserveFiles(batch.keySet())) {

        HashMap<KeyExtent,ReadOnlyTablet> tablets = new HashMap<>();
        batch.keySet().forEach(extent -> {
          try {
            tablets.put(extent, reservation.newTablet(extent));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

        InitialMultiScan ims = super.startMultiScan(tinfo, credentials, tcolumns, ssiList, batch,
            ssio, authorizations, waitForWrites, tSamplerConfig, batchTimeOut, contextArg,
            executionHints, sserver.getBatchScanTabletResolver(tablets), busyTimeout);

        LOG.debug("started scan: {}", ims.getScanID());
        return ims;
      } catch (AccumuloException | TException e) {
        LOG.error("Error starting scan", e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public MultiScanResult continueMultiScan(TInfo tinfo, long scanID, long busyTimeout)
        throws NoSuchScanIDException, TSampleNotPresentException, ScanServerBusyException {
      LOG.debug("continue multi scan: {}", scanID);

      try (ScanReservation reservation = sserver.reserveFiles(scanID)) {
        return super.continueMultiScan(tinfo, scanID, busyTimeout);
      }
    }

    @Override
    public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException {
      LOG.debug("close multi scan: {}", scanID);
      super.closeMultiScan(tinfo, scanID);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(ScanServer.class);

  protected ScanServerThriftScanClientHandler handler;
  private UUID serverLockUUID;
  private final TabletMetadataLoader tabletMetadataLoader;
  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;
  protected Set<StoredTabletFile> lockedFiles = new HashSet<>();
  protected Map<StoredTabletFile,ReservedFile> reservedFiles = new ConcurrentHashMap<>();
  protected AtomicLong nextScanReservationId = new AtomicLong();

  private final ServerContext context;
  private final ZooCache zooCache;
  private final SessionManager sessionManager;
  private final TabletServerResourceManager resourceManager;
  private final ZooAuthenticationKeyWatcher authKeyWatcher;
  private HostAndPort clientAddress;
  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();

  private volatile boolean serverStopRequested = false;
  private ServiceLock scanServerLock;
  protected CompactionManager compactionManager;
  protected TabletServerScanMetrics scanMetrics;

  public ScanServer(ServerOpts opts, String[] args) {
    super("sserver", opts, args);

    context = super.getContext();
    context.setupCrypto();
    this.zooCache = new ZooCache(context.getZooReader(), null);
    final AccumuloConfiguration aconf = getConfiguration();
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + getContext().getInstanceID());
    this.sessionManager = new SessionManager(context);
    this.resourceManager = new TabletServerResourceManager(context, this);
    watchCriticalScheduledTask(
        context.getScheduledExecutor().scheduleWithFixedDelay(TabletLocator::clearLocators,
            TabletServer.jitter(), TabletServer.jitter(), TimeUnit.MILLISECONDS));

    // Create the secret manager
    context.setSecretManager(new AuthenticationTokenSecretManager(context.getInstanceID(),
        aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME)));
    if (aconf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      LOG.info("SASL is enabled, creating ZooKeeper watcher for AuthenticationKeys");
      // Watcher to notice new AuthenticationKeys which enable delegation tokens
      authKeyWatcher =
          new ZooAuthenticationKeyWatcher(context.getSecretManager(), context.getZooReaderWriter(),
              context.getZooKeeperRoot() + Constants.ZDELEGATION_TOKEN_KEYS);
    } else {
      authKeyWatcher = null;
    }
    LOG.info("Tablet server starting on {}", getHostname());
    clientAddress = HostAndPort.fromParts(getHostname(), 0);

    Runnable gcDebugTask = () -> gcLogger.logGCInfo(getConfiguration());
    ScheduledFuture<?> future = context.getScheduledExecutor().scheduleWithFixedDelay(gcDebugTask,
        0, TabletServer.TIME_BETWEEN_GC_CHECKS, TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);

    // Note: The way to control the number of concurrent scans that a ScanServer will
    // perform is by using Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS or the number
    // of threads in Property.SSERV_SCAN_EXECUTORS_PREFIX.

    long cacheExpiration =
        getConfiguration().getTimeInMillis(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION);

    long scanServerReservationExpiration =
        getConfiguration().getTimeInMillis(Property.SSERVER_SCAN_REFERENCE_EXPIRATION_TIME);

    tabletMetadataLoader = new TabletMetadataLoader(getContext().getAmple());

    if (cacheExpiration == 0L) {
      LOG.warn("Tablet metadata caching disabled, may cause excessive scans on metadata table.");
      tabletMetadataCache = null;
    } else {
      if (cacheExpiration < 60000) {
        LOG.warn(
            "Tablet metadata caching less than one minute, may cause excessive scans on metadata table.");
      }
      tabletMetadataCache =
          Caffeine.newBuilder().expireAfterWrite(cacheExpiration, TimeUnit.MILLISECONDS)
              .scheduler(Scheduler.systemScheduler()).build(tabletMetadataLoader);
    }
    handler = getHandler();

    ThreadPools.watchCriticalScheduledTask(getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(() -> cleanUpReservedFiles(scanServerReservationExpiration),
            scanServerReservationExpiration, scanServerReservationExpiration,
            TimeUnit.MILLISECONDS));

  }

  @Override
  public GarbageCollectionLogger getGCLogger() {
    return this.gcLogger;
  }

  @Override
  public SessionManager getSessionManager() {
    return this.sessionManager;
  }

  @Override
  public TabletServerResourceManager getResourceManager() {
    return this.resourceManager;
  }

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return this.scanMetrics;
  }

  @Override
  public ServiceLock getLock() {
    return scanServerLock;
  }

  @Override
  public HostAndPort getClientAddress() {
    return this.clientAddress;
  }

  @Override
  public ZooCache getZooCache() {
    return this.zooCache;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  public void requestStop() {
    serverStopRequested = true;
  }

  @Override
  public Tablet getOnlineTablet(KeyExtent extent) {
    throw new UnsupportedOperationException("ScanServers use TabletResolvers");
  }

  @Override
  public ServerResources getServerConfig() {
    throw new UnsupportedOperationException("ScanServers do not support DFSLogger operations");
  }

  @Override
  public int createLogId() {
    throw new UnsupportedOperationException("ScanServers do not support WALog operations");
  }

  @Override
  public CompactionManager getCompactionManager() {
    return this.compactionManager;
  }

  @Override
  public void enqueueManagerMessage(ManagerMessage m) {
    throw new UnsupportedOperationException("ScanServers do not support WALog operations");
  }

  @Override
  public TabletServerMinCMetrics getMinCMetrics() {
    throw new UnsupportedOperationException("ScanServers do not support WALog operations");
  }

  @Override
  public void minorCompactionFinished(CommitSession tablet, long walogSeq) throws IOException {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @Override
  public void minorCompactionStarted(CommitSession tablet, long lastUpdateSequence,
      String newMapfileLocation) throws IOException {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @Override
  public void executeSplit(Tablet tablet) {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @Override
  public void updateBulkImportState(List<String> files, BulkImportState state) {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @Override
  public void removeBulkImportState(List<String> files) {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @Override
  public void recover(VolumeManager fs, KeyExtent extent, List<LogEntry> logEntries,
      Set<String> tabletFiles, MutationReceiver mutationReceiver) throws IOException {
    throw new UnsupportedOperationException("ScanServers are read-only");
  }

  @VisibleForTesting
  protected ScanServerThriftScanClientHandler getHandler() {
    return new ScanServerThriftScanClientHandler(this);
  }

  /**
   * Start the thrift service to handle incoming client requests
   *
   * @return address of this client service
   * @throws UnknownHostException
   *           host unknown
   */
  protected ServerAddress startScanServerClientService() throws UnknownHostException {

    TProcessor processor = null;
    try {
      processor = ThriftServerTypes.getScanServerThriftServer(getHandler(), getContext(),
          getConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Error creating thrift server processor", e);
    }

    Property maxMessageSizeProperty =
        (getConfiguration().get(Property.SSERV_MAX_MESSAGE_SIZE) != null
            ? Property.SSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), getHostname(),
        Property.SSERV_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.SSERV_PORTSEARCH, Property.SSERV_MINTHREADS,
        Property.SSERV_MINTHREADS_TIMEOUT, Property.SSERV_THREADCHECK, maxMessageSizeProperty);

    LOG.info("address = {}", sp.address);
    return sp;
  }

  /**
   * Set up nodes and locks in ZooKeeper for this Compactor
   */
  private ServiceLock announceExistence() {
    ZooReaderWriter zoo = getContext().getZooReaderWriter();
    try {

      var zLockPath = ServiceLock.path(
          getContext().getZooKeeperRoot() + Constants.ZSSERVERS + "/" + getClientAddressString());

      try {
        // Old zk nodes can be cleaned up by ZooZap
        zoo.putPersistentData(zLockPath.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NOAUTH) {
          LOG.error("Failed to write to ZooKeeper. Ensure that"
              + " accumulo.properties, specifically instance.secret, is consistent.");
        }
        throw e;
      }

      serverLockUUID = UUID.randomUUID();
      scanServerLock = new ServiceLock(zoo.getZooKeeper(), zLockPath, serverLockUUID);

      LockWatcher lw = new LockWatcher() {

        @Override
        public void lostLock(final LockLossReason reason) {
          Halt.halt(serverStopRequested ? 0 : 1, () -> {
            if (!serverStopRequested) {
              LOG.error("Lost tablet server lock (reason = {}), exiting.", reason);
            }
            gcLogger.logGCInfo(getConfiguration());
          });
        }

        @Override
        public void unableToMonitorLockNode(final Exception e) {
          Halt.halt(1, () -> LOG.error("Lost ability to monitor scan server lock, exiting.", e));
        }
      };

      // Don't use the normal ServerServices lock content, instead put the server UUID here.
      byte[] lockContent = serverLockUUID.toString().getBytes(UTF_8);

      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], NodeExistsPolicy.SKIP);

        if (scanServerLock.tryLock(lw, lockContent)) {
          LOG.debug("Obtained scan server lock {}", scanServerLock.getLockPath());
          return scanServerLock;
        }
        LOG.info("Waiting for scan server lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      LOG.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      LOG.info("Could not obtain scan server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    SecurityUtil.serverLogin(getConfiguration());

    if (authKeyWatcher != null) {
      LOG.info("Seeding ZooKeeper watcher for authentication keys");
      try {
        authKeyWatcher.updateAuthKeys();
      } catch (KeeperException | InterruptedException e) {
        // TODO Does there need to be a better check? What are the error conditions that we'd fall
        // out here? AUTH_FAILURE?
        // If we get the error, do we just put it on a timer and retry the exists(String, Watcher)
        // call?
        LOG.error("Failed to perform initial check for authentication tokens in"
            + " ZooKeeper. Delegation token authentication will be unavailable.", e);
      }
    }

    ServerAddress address = null;
    try {
      address = startScanServerClientService();
      clientAddress = address.getAddress();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }

    try {
      MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName,
          clientAddress);
    } catch (Exception e1) {
      LOG.error("Error initializing metrics, metrics will not be emitted.", e1);
    }
    scanMetrics = new TabletServerScanMetrics();
    MetricsUtil.initializeProducers(scanMetrics);

    // We need to set the compaction manager so that we don't get an NPE in CompactableImpl.close
    this.compactionManager =
        new ScanServerCompactionManager(getContext(), new ScanServerCompactionExecutorMetrics());

    ServiceLock lock = announceExistence();

    try {
      while (!serverStopRequested) {
        UtilWaitThread.sleep(1000);
      }
    } finally {
      LOG.info("Stopping Thrift Servers");
      address.server.stop();

      LOG.info("Removing server scan references");
      this.getContext().getAmple().deleteScanServerFileReferences(clientAddress.toString(),
          serverLockUUID);

      try {
        LOG.debug("Closing filesystems");
        VolumeManager mgr = getContext().getVolumeManager();
        if (null != mgr) {
          mgr.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close filesystem : {}", e.getMessage(), e);
      }

      gcLogger.logGCInfo(getConfiguration());
      LOG.info("stop requested. exiting ... ");
      try {
        if (null != lock) {
          lock.unlock();
        }
      } catch (Exception e) {
        LOG.warn("Failed to release scan server lock", e);
      }

    }
  }

  @SuppressWarnings("unchecked")
  private Map<KeyExtent,TabletMetadata> getTabletMetadata(Collection<KeyExtent> extents) {
    if (tabletMetadataCache == null) {
      return (Map<KeyExtent,TabletMetadata>) tabletMetadataLoader
          .loadAll((Set<? extends KeyExtent>) extents);
    } else {
      return tabletMetadataCache.getAll(extents);
    }
  }

  private FilesLock lockFiles(Collection<StoredTabletFile> files) {

    // lets ensure we lock and unlock that same set of files even if the passed in files changes
    var filesCopy = Set.copyOf(files);

    synchronized (lockedFiles) {

      while (!Collections.disjoint(filesCopy, lockedFiles)) {
        try {
          lockedFiles.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      for (StoredTabletFile file : filesCopy) {
        if (!lockedFiles.add(file)) {
          throw new IllegalStateException("file unexpectedly not added");
        }
      }
    }

    return new FilesLock(filesCopy);
  }

  private Map<KeyExtent,TabletMetadata> reserveFilesInner(Collection<KeyExtent> extents,
      long myReservationId) throws NotServingTabletException, AccumuloException {
    // RFS is an acronym for Reference files for scan
    LOG.trace("RFFS {} ensuring files are referenced for scan of extents {}", myReservationId,
        extents);

    Map<KeyExtent,TabletMetadata> tabletsMetadata = getTabletMetadata(extents);

    for (KeyExtent extent : extents) {
      var tabletMetadata = tabletsMetadata.get(extent);
      if (tabletMetadata == null) {
        LOG.trace("RFFS {} extent not found in metadata table {}", myReservationId, extent);
        throw new NotServingTabletException(extent.toThrift());
      }

      if (!AssignmentHandler.checkTabletMetadata(extent, getTabletSession(), tabletMetadata,
          true)) {
        LOG.trace("RFFS {} extent unable to load {} as AssignmentHandler returned false",
            myReservationId, extent);
        throw new NotServingTabletException(extent.toThrift());
      }
    }

    Map<StoredTabletFile,KeyExtent> allFiles = new HashMap<>();

    tabletsMetadata.forEach((extent, tm) -> {
      tm.getFiles().forEach(file -> allFiles.put(file, extent));
    });

    try (FilesLock flock = lockFiles(allFiles.keySet())) {
      Set<StoredTabletFile> filesToReserve = new HashSet<>();
      List<ScanServerRefTabletFile> refs = new ArrayList<>();
      Set<KeyExtent> tabletsToCheck = new HashSet<>();

      String serverAddress = clientAddress.toString();

      for (StoredTabletFile file : flock.getLockedFiles()) {
        if (!reservedFiles.containsKey(file)) {
          refs.add(new ScanServerRefTabletFile(file.getPathStr(), serverAddress, serverLockUUID));
          filesToReserve.add(file);
          tabletsToCheck.add(Objects.requireNonNull(allFiles.get(file)));
          LOG.trace("RFFS {} need to add scan ref for file {}", myReservationId, file);
        }
      }

      if (!filesToReserve.isEmpty()) {
        getContext().getAmple().putScanServerFileReferences(refs);

        // After we insert the scan server refs we need to check and see if the tablet is still
        // using the file. As long as the tablet is still using the files then the Accumulo GC
        // should not have deleted the files. This assumes the Accumulo GC reads scan server refs
        // after tablet refs from the metadata table.

        if (tabletMetadataCache != null) {
          // lets clear the cache so we get the latest
          tabletMetadataCache.invalidateAll(tabletsToCheck);
        }

        var tabletsToCheckMetadata = getTabletMetadata(tabletsToCheck);

        for (KeyExtent extent : tabletsToCheck) {
          TabletMetadata metadataAfter = tabletsToCheckMetadata.get(extent);
          if (metadataAfter == null) {
            getContext().getAmple().deleteScanServerFileReferences(refs);
            throw new NotServingTabletException(extent.toThrift());
          }

          // remove files that are still referenced
          filesToReserve.removeAll(metadataAfter.getFiles());
        }

        // if this is not empty it means some files that we reserved are no longer referenced by
        // tablets. This means there could have been a time gap where nothing referenced a file
        // meaning it could have been GCed.
        if (!filesToReserve.isEmpty()) {
          LOG.trace("RFFS {} tablet files changed while attempting to reference files {}",
              myReservationId, filesToReserve);
          getContext().getAmple().deleteScanServerFileReferences(refs);
          return null;
        }
      }

      for (StoredTabletFile file : flock.getLockedFiles()) {
        if (!reservedFiles.computeIfAbsent(file, k -> new ReservedFile()).activeReservations
            .add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for startScan {}", myReservationId, file);
      }

    }

    return tabletsMetadata;
  }

  protected ScanReservation reserveFiles(Collection<KeyExtent> extents)
      throws NotServingTabletException, AccumuloException {

    long myReservationId = nextScanReservationId.incrementAndGet();

    Map<KeyExtent,TabletMetadata> tabletsMetadata = reserveFilesInner(extents, myReservationId);
    while (tabletsMetadata == null) {
      tabletsMetadata = reserveFilesInner(extents, myReservationId);
    }

    return new ScanReservation(tabletsMetadata, myReservationId);
  }

  protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
    var session = (ScanSession) sessionManager.getSession(scanId);
    if (session == null) {
      throw new NoSuchScanIDException();
    }

    Set<StoredTabletFile> scanSessionFiles;

    if (session instanceof SingleScanSession) {
      var sss = (SingleScanSession) session;
      scanSessionFiles =
          Set.copyOf(session.getTabletResolver().getTablet(sss.extent).getDatafiles().keySet());
    } else if (session instanceof MultiScanSession) {
      var mss = (MultiScanSession) session;
      scanSessionFiles = mss.exents.stream()
          .flatMap(e -> mss.getTabletResolver().getTablet(e).getDatafiles().keySet().stream())
          .collect(Collectors.toUnmodifiableSet());
    } else {
      throw new IllegalArgumentException("Unknown session type " + session.getClass().getName());
    }

    long myReservationId = nextScanReservationId.incrementAndGet();

    try (FilesLock flock = lockFiles(scanSessionFiles)) {
      if (!reservedFiles.keySet().containsAll(scanSessionFiles)) {
        // the files are no longer reserved in the metadata table, so lets pretend there is no scan
        // session
        if (LOG.isTraceEnabled()) {
          LOG.trace("RFFS {} files are no longer referenced on continue scan {} {}",
              myReservationId, scanId, Sets.difference(scanSessionFiles, reservedFiles.keySet()));
        }
        throw new NoSuchScanIDException();
      }

      for (StoredTabletFile file : flock.getLockedFiles()) {
        if (!reservedFiles.get(file).activeReservations.add(myReservationId)) {
          throw new IllegalStateException("reservation id unexpectedly already in set");
        }

        LOG.trace("RFFS {} reserved reference for continue scan {} {}", myReservationId, scanId,
            file);
      }
    }

    return new ScanReservation(scanSessionFiles, myReservationId);
  }

  private void cleanUpReservedFiles(long expireTimeMs) {
    List<StoredTabletFile> candidates = new ArrayList<>();

    reservedFiles.forEach((file, reservationInfo) -> {
      if (reservationInfo.shouldDelete(expireTimeMs)) {
        candidates.add(file);
      }
    });

    if (!candidates.isEmpty()) {
      // gain exclusive access to files to avoid multiple threads adding/deleting file reservations
      // at same time
      try (FilesLock flock = lockFiles(candidates)) {
        List<ScanServerRefTabletFile> refsToDelete = new ArrayList<>();
        List<StoredTabletFile> confirmed = new ArrayList<>();

        String serverAddress = clientAddress.toString();

        // check that is still a candidate now that files are locked and no other thread should be
        // modifying them
        for (StoredTabletFile candidate : flock.getLockedFiles()) {
          var reservation = reservedFiles.get(candidate);
          if (reservation != null && reservation.shouldDelete(expireTimeMs)) {
            refsToDelete.add(
                new ScanServerRefTabletFile(candidate.getPathStr(), serverAddress, serverLockUUID));
            confirmed.add(candidate);
            LOG.trace("RFFS referenced files has not been used recently, removing reference {}",
                candidate);
          }
        }

        getContext().getAmple().deleteScanServerFileReferences(refsToDelete);

        // those refs were successfully removed from metadata table, so remove them from the map
        reservedFiles.keySet().removeAll(confirmed);

      }
    }
  }

  /*
   * This simple method exists to be overridden in tests
   */
  protected KeyExtent getKeyExtent(TKeyExtent textent) {
    return KeyExtent.fromThrift(textent);
  }

  protected TabletResolver getScanTabletResolver(final ReadOnlyTablet tablet) {
    return new TabletResolver() {
      final ReadOnlyTablet t = tablet;

      @Override
      public ReadOnlyTablet getTablet(KeyExtent extent) {
        if (extent.equals(t.getExtent())) {
          return t;
        } else {
          return null;
        }
      }

      @Override
      public void close() {
        try {
          t.close(false);
        } catch (IOException e) {
          throw new UncheckedIOException("Error closing tablet", e);
        }
      }
    };
  }

  protected TabletResolver
      getBatchScanTabletResolver(final HashMap<KeyExtent,ReadOnlyTablet> tablets) {
    return new TabletResolver() {
      @Override
      public ReadOnlyTablet getTablet(KeyExtent extent) {
        return tablets.get(extent);
      }

      @Override
      public void close() {
        tablets.forEach((e, t) -> {
          try {
            t.close(false);
          } catch (IOException ex) {
            throw new UncheckedIOException("Error closing tablet: " + e.toString(), ex);
          }
        });
      }
    };
  }

  @Override
  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf) {
    return new BlockCacheConfiguration(acuConf, Property.SSERV_PREFIX,
        Property.SSERV_INDEXCACHE_SIZE, Property.SSERV_DATACACHE_SIZE,
        Property.SSERV_SUMMARYCACHE_SIZE, Property.SSERV_DEFAULT_BLOCKSIZE);
  }

  public static void main(String[] args) throws Exception {
    try (ScanServer tserver = new ScanServer(new ServerOpts(), args)) {
      tserver.runServer();
    }
  }

}
