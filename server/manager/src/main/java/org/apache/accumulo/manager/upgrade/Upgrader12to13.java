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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade12to13.COMPACT_COL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.init.FileSystemInitializer;
import org.apache.accumulo.server.init.InitialConfiguration;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

//TODO when removing this class, also remove MetadataSchema.Upgrader12to13
public class Upgrader12to13 implements Upgrader {

  private static final Logger LOG = LoggerFactory.getLogger(Upgrader12to13.class);

  @VisibleForTesting
  static final String ZTABLE_NAME = "/name";

  @Override
  public void upgradeZookeeper(ServerContext context) {
    LOG.info("Ensuring all worker server processes are down.");
    validateEmptyZKWorkerServerPaths(context);
    LOG.info("setting root table stored hosting availability");
    addHostingGoals(context, TabletAvailability.HOSTED, DataLevel.ROOT);
    LOG.info("Removing nodes no longer used from ZooKeeper");
    removeUnusedZKNodes(context);
    LOG.info("Removing compact columns from root tablet");
    removeCompactColumnsFromRootTabletMetadata(context);
    LOG.info("Adding compactions node to zookeeper");
    addCompactionsNode(context);
    LOG.info("Creating ZooKeeper entries for accumulo.fate table");
    initializeFateTable(context);
    LOG.info("Adding table mappings to zookeeper");
    addTableMappingsToZooKeeper(context);
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, SystemTables.ROOT.tableName());
    LOG.info("setting metadata table hosting availability");
    addHostingGoals(context, TabletAvailability.HOSTED, DataLevel.METADATA);
    LOG.info("Removing MetadataBulkLoadFilter iterator from root table");
    removeMetaDataBulkLoadFilter(context, SystemTables.ROOT.tableId());
    LOG.info("Removing compact columns from metadata tablets");
    removeCompactColumnsFromTable(context, SystemTables.ROOT.tableName());
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    LOG.info("Creating table {}", SystemTables.FATE.tableName());
    createFateTable(context);
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, SystemTables.METADATA.tableName());
    LOG.info("setting hosting availability on user tables");
    addHostingGoals(context, TabletAvailability.ONDEMAND, DataLevel.USER);
    LOG.info("Deleting external compaction final states from user tables");
    deleteExternalCompactionFinalStates(context);
    LOG.info("Deleting external compaction from user tables");
    deleteExternalCompactions(context);
    LOG.info("Removing MetadataBulkLoadFilter iterator from metadata table");
    removeMetaDataBulkLoadFilter(context, SystemTables.METADATA.tableId());
    LOG.info("Removing compact columns from user tables");
    removeCompactColumnsFromTable(context, SystemTables.METADATA.tableName());
    LOG.info("Removing bulk file columns from metadata table");
    removeBulkFileColumnsFromTable(context, SystemTables.METADATA.tableName());
  }

  private static void addCompactionsNode(ServerContext context) {
    try {
      context.getZooSession().asReaderWriter().putPersistentData(Constants.ZCOMPACTIONS,
          new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private void createFateTable(ServerContext context) {
    try {
      FileSystemInitializer initializer = new FileSystemInitializer(
          new InitialConfiguration(context.getHadoopConf(), context.getSiteConfiguration()));
      // For upgrading an existing system set to never merge. If the mergeability is changed
      // then we would look to use the thrift client to look up the current Manager time to
      // set as part of the mergeability metadata
      FileSystemInitializer.InitialTablet fateTableTableTablet =
          initializer.createFateRefTablet(context, TabletMergeabilityMetadata.never());
      // Add references to the Metadata Table
      try (BatchWriter writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
        writer.addMutation(fateTableTableTablet.createMutation());
      } catch (MutationsRejectedException | TableNotFoundException e) {
        LOG.error("Failed to write tablet refs to metadata table");
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      LOG.error("Problem attempting to create Fate table", e);
    }
    LOG.info("Created Fate table");
  }

  private void removeCompactColumnsFromRootTabletMetadata(ServerContext context) {

    try {
      var zrw = context.getZooSession().asReaderWriter();
      Stat stat = new Stat();
      byte[] rootData = zrw.getData(ZROOT_TABLET, stat);

      String json = new String(rootData, UTF_8);

      var rtm = new RootTabletMetadata(json);

      ArrayList<Mutation> mutations = new ArrayList<>();
      for (Map.Entry<Key,Value> entry : rtm.toKeyValues().entrySet()) {
        var key = entry.getKey();

        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          // TODO will metadata contraint fail when this is written?
          COMPACT_COL.putDelete(m);
          mutations.add(m);
        }
      }

      Preconditions.checkState(mutations.size() <= 1);

      if (!mutations.isEmpty()) {
        LOG.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(ZROOT_TABLET, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        LOG.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper due to interrupt", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write root metadata in ZooKeeper because of ZooKeeper exception", ex);
    }

  }

  private void removeCompactColumnsFromTable(ServerContext context, String tableName) {

    try (var scanner = context.createScanner(tableName, Authorizations.EMPTY);
        var writer = context.createBatchWriter(tableName)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      COMPACT_COL.fetch(scanner);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          COMPACT_COL.putDelete(m);
          writer.addMutation(m);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeBulkFileColumnsFromTable(ServerContext context, String tableName) {
    // FATE transaction ids have changed from 3.x to 4.x which are used as the value for the bulk
    // file column. FATE ops won't persist through upgrade, so these columns can be safely deleted
    // if they exist.
    try (var scanner = context.createScanner(tableName, Authorizations.EMPTY);
        var writer = context.createBatchWriter(tableName)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      scanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        Mutation m = new Mutation(key.getRow());
        Preconditions.checkState(
            key.getColumnFamily().equals(TabletsSection.BulkFileColumnFamily.NAME),
            "Expected family %s, saw %s ", TabletsSection.BulkFileColumnFamily.NAME,
            key.getColumnFamily());
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeUnusedZKNodes(ServerContext context) {
    try {
      final var zrw = context.getZooSession().asReaderWriter();

      final String ZCOORDINATOR = "/coordinators";
      final String BULK_ARBITRATOR_TYPE = "bulkTx";

      zrw.recursiveDelete(ZCOORDINATOR, ZooUtil.NodeMissingPolicy.SKIP);
      zrw.recursiveDelete("/" + BULK_ARBITRATOR_TYPE, ZooUtil.NodeMissingPolicy.SKIP);

      final String ZTABLE_COMPACT_ID = "/compact-id";
      final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";

      for (String tId : zrw.getChildren(Constants.ZTABLES)) {
        final String zTablePath = Constants.ZTABLES + "/" + tId;
        zrw.delete(zTablePath + ZTABLE_COMPACT_ID);
        zrw.delete(zTablePath + ZTABLE_COMPACT_CANCEL_ID);
      }
    } catch (KeeperException | InterruptedException e1) {
      throw new IllegalStateException(e1);
    }
  }

  private void removeMetaDataBulkLoadFilter(ServerContext context, TableId tableId) {
    final String propName = Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter";
    PropUtil.removeProperties(context, TablePropKey.of(tableId), List.of(propName));
  }

  private void deleteExternalCompactionFinalStates(ServerContext context) {
    // This metadata was only written for user tablets as part of the compaction commit process.
    // Compactions are committed in a completely different way now, so delete these entries. Its
    // possible some completed compactions may need to be redone, but processing these entries would
    // not be easy to test so its better for correctness to delete them and redo the work.
    try (
        var scanner =
            context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
        var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      var section = new Section(RESERVED_PREFIX + "ecomp", true, RESERVED_PREFIX + "ecomq", false);
      scanner.setRange(section.getRange());

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        var row = key.getRow();
        Preconditions.checkState(row.toString().startsWith(section.getRowPrefix()));
        Mutation m = new Mutation(row);
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void addHostingGoals(ServerContext context, TabletAvailability availability,
      DataLevel level) {
    try (
        TabletsMetadata tm =
            context.getAmple().readTablets().forLevel(level).fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(t -> mut.mutateTablet(t.getExtent()).putTabletAvailability(availability).mutate());
    }
  }

  private void deleteExternalCompactions(ServerContext context) {
    // External compactions were only written for user tablets in 3.x and earlier, so only need to
    // process the metadata table. The metadata related to an external compaction has changed so
    // delete any that exists. Not using Ample in case there are problems deserializing the old
    // external compaction metadata.
    try (
        var scanner =
            context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
        var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      scanner.setRange(TabletsSection.getRange());
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        Mutation m = new Mutation(key.getRow());
        Preconditions.checkState(key.getColumnFamily().equals(ExternalCompactionColumnFamily.NAME),
            "Expected family %s, saw %s ", ExternalCompactionColumnFamily.NAME,
            key.getColumnFamily());
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void handlePartialSplits(ServerContext context, String table) {
    try (var scanner = context.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(TabletsSection.getRange());
      TabletsSection.Upgrade12to13.SPLIT_RATIO_COLUMN.fetch(scanner);

      for (var entry : scanner) {
        SplitRecovery12to13.fixSplit(context, entry.getKey().getRow());
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void validateEmptyZKWorkerServerPaths(ServerContext context) {
    // #4861 added the resource group to the compactor, sserver, tserver
    // and dead tserver zookeeper paths. Make sure that the these paths
    // are empty. This means that for the Accumulo 4.0 upgrade, the Manager
    // should be started first before any other process.
    final ZooReader zr = context.getZooSession().asReader();
    for (String serverPath : new String[] {Constants.ZCOMPACTORS, Constants.ZSSERVERS,
        Constants.ZTSERVERS, Constants.ZDEADTSERVERS}) {
      try {
        List<String> children = zr.getChildren(serverPath);
        for (String child : children) {
          if (child.contains(":")) {
            String childPath = serverPath + "/" + child;
            if (zr.getChildren(childPath).isEmpty()) {
              // child is likely host:port and is an empty directory. Since there
              // is no lock here, then the server is likely down (or should be).
              // Remove the entry and move on.
              context.getZooSession().asReaderWriter().recursiveDelete(childPath,
                  NodeMissingPolicy.SKIP);
            } else {
              throw new IllegalStateException("Found server address at " + serverPath + "/" + child
                  + " with content in the directory. Was expecting either a nothing, a resource group name or an empty directory."
                  + " Stop any referenced servers.");
            }
          }
        }
      } catch (InterruptedException | KeeperException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void initializeFateTable(ServerContext context) {
    try {
      Upgrader11to12.preparePre4_0NewTableState(context, SystemTables.FATE.tableId(),
          Namespace.ACCUMULO.id(), SystemTables.FATE.tableName(), TableState.ONLINE,
          ZooUtil.NodeExistsPolicy.FAIL);
    } catch (InterruptedException | KeeperException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error creating fate table", ex);
    }
  }

  void addTableMappingsToZooKeeper(ServerContext context) {
    var zrw = context.getZooSession().asReaderWriter();
    try {
      List<String> tableIds = zrw.getChildren(Constants.ZTABLES);
      Map<String,Map<String,String>> mapOfTableMaps = new HashMap<>();

      for (String tableId : tableIds) {
        var tableName =
            new String(zrw.getData(Constants.ZTABLES + "/" + tableId + ZTABLE_NAME), UTF_8);
        var namespaceId = new String(
            zrw.getData(Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE), UTF_8);
        mapOfTableMaps.computeIfAbsent(namespaceId, k -> new HashMap<>()).compute(tableId,
            (tid, existingName) -> {
              if (existingName != null) {
                throw new IllegalStateException(
                    "Table id " + tid + " already present in map for namespace id " + namespaceId);
              }
              return tableName;
            });
      }
      for (Map.Entry<String,Map<String,String>> entry : mapOfTableMaps.entrySet()) {
        zrw.putPersistentData(Constants.ZNAMESPACES + "/" + entry.getKey() + Constants.ZTABLES,
            NamespaceMapping.serializeMap(entry.getValue()), ZooUtil.NodeExistsPolicy.FAIL);
      }
      for (String tableId : tableIds) {
        String tableNamePath = Constants.ZTABLES + "/" + tableId + ZTABLE_NAME;
        zrw.delete(tableNamePath);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Could not read metadata from ZooKeeper due to interrupt",
          ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write metadata in ZooKeeper because of ZooKeeper exception", ex);
    }
  }
}
