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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.managermessage.ManagerMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TabletHostingServer {

  static final Logger LOG = LoggerFactory.getLogger(TabletHostingServer.class);

  ServerContext getContext();

  AccumuloConfiguration getConfiguration();

  GarbageCollectionLogger getGCLogger();

  ServiceLock getLock();

  Tablet getOnlineTablet(KeyExtent extent);

  SessionManager getSessionManager();

  TabletServerResourceManager getResourceManager();

  TabletServerScanMetrics getScanMetrics();

  HostAndPort getClientAddress();

  ZooCache getZooCache();

  void enqueueManagerMessage(ManagerMessage m);

  CompactionManager getCompactionManager();

  TabletServerMinCMetrics getMinCMetrics();

  void minorCompactionFinished(CommitSession tablet, long walogSeq) throws IOException;

  void minorCompactionStarted(CommitSession tablet, long lastUpdateSequence,
      String newMapfileLocation) throws IOException;

  void executeSplit(Tablet tablet);

  void updateBulkImportState(List<String> files, BulkImportState state);

  void removeBulkImportState(List<String> files);

  DfsLogger.ServerResources getServerConfig();

  int createLogId();

  void recover(VolumeManager fs, KeyExtent extent, List<LogEntry> logEntries,
      Set<String> tabletFiles, MutationReceiver mutationReceiver) throws IOException;

  boolean isReadOnly();

  void requestStop();

  public BlockCacheConfiguration getBlockCacheConfiguration(AccumuloConfiguration acuConf);

  default String getClientAddressString() {
    HostAndPort clientAddress = getClientAddress();
    if (clientAddress == null) {
      return null;
    }
    return clientAddress.getHost() + ":" + clientAddress.getPort();
  }

  default TServerInstance getTabletSession() {
    String address = getClientAddressString();
    if (address == null) {
      return null;
    }

    try {
      return new TServerInstance(address, getLock().getSessionId());
    } catch (Exception ex) {
      LOG.warn("Unable to read session from tablet server lock" + ex);
      return null;
    }
  }

}
