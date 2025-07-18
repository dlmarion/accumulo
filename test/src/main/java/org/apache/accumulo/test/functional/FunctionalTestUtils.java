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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.AdminUtil.FateStatus;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.TestIngest;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Iterators;

public class FunctionalTestUtils {

  public static int countRFiles(AccumuloClient c, String tableName) throws Exception {
    try (Scanner scanner =
        c.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
      TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      scanner.setRange(TabletsSection.getRange(tableId));
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      return Iterators.size(scanner.iterator());
    }
  }

  public static List<String> getRFilePaths(AccumuloClient c, String tableName) throws Exception {
    return getStoredTabletFiles(c, tableName).stream().map(StoredTabletFile::getMetadataPath)
        .collect(Collectors.toList());
  }

  public static List<StoredTabletFile> getStoredTabletFiles(AccumuloClient c, String tableName)
      throws Exception {
    List<StoredTabletFile> files = new ArrayList<>();
    try (Scanner scanner =
        c.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
      TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      scanner.setRange(TabletsSection.getRange(tableId));
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.forEach(entry -> files.add(StoredTabletFile.of(entry.getKey().getColumnQualifier())));
    }
    return files;
  }

  static void checkRFiles(AccumuloClient c, String tableName, int minTablets, int maxTablets,
      int minRFiles, int maxRFiles) throws Exception {
    try (Scanner scanner =
        c.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      scanner.setRange(new Range(new Text(tableId + ";"), true, new Text(tableId + "<"), true));
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);

      HashMap<Text,Integer> tabletFileCounts = new HashMap<>();

      for (Entry<Key,Value> entry : scanner) {

        Text row = entry.getKey().getRow();

        Integer count = tabletFileCounts.get(row);
        if (count == null) {
          count = 0;
        }
        if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          count = count + 1;
        }

        tabletFileCounts.put(row, count);
      }

      if (tabletFileCounts.size() < minTablets || tabletFileCounts.size() > maxTablets) {
        throw new Exception("table " + tableName + " has unexpected number of tablets. Found: "
            + tabletFileCounts.size() + ". expected " + minTablets + " < numTablets < "
            + maxTablets);
      }

      Set<Entry<Text,Integer>> es = tabletFileCounts.entrySet();
      for (Entry<Text,Integer> entry : es) {
        if (entry.getValue() > maxRFiles || entry.getValue() < minRFiles) {
          throw new Exception(
              "tablet " + entry.getKey() + " has unexpected number of data files. Found: "
                  + entry.getValue() + ". expected " + minTablets + " < numFiles < " + maxTablets);
        }
      }
    }
  }

  public static void checkSplits(AccumuloClient c, String table, int min, int max)
      throws Exception {
    Collection<Text> splits = c.tableOperations().listSplits(table);
    if (splits.size() < min || splits.size() > max) {
      throw new Exception("# of table splits points out of range, #splits=" + splits.size()
          + " table=" + table + " min=" + min + " max=" + max);
    }
  }

  public static void createRFiles(final AccumuloClient c, final FileSystem fs, String path,
      int rows, int splits, int threads) throws Exception {
    fs.delete(new Path(path), true);
    ExecutorService threadPool = Executors.newFixedThreadPool(threads);
    final AtomicBoolean fail = new AtomicBoolean(false);
    for (int i = 0; i < rows; i += rows / splits) {
      TestIngest.IngestParams params = new TestIngest.IngestParams(c.properties());
      params.outputFile = String.format("%s/mf%s", path, i);
      params.random = 56;
      params.timestamp = 1;
      params.dataSize = 50;
      params.rows = rows / splits;
      params.startRow = i;
      params.cols = 1;
      threadPool.execute(() -> {
        try {
          TestIngest.ingest(c, fs, params);
        } catch (Exception e) {
          fail.set(true);
        }
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(1, TimeUnit.HOURS);
    assertFalse(fail.get());
  }

  public static HttpResponse<String> readWebPage(URL url)
      throws IOException, InterruptedException, URISyntaxException {
    return HttpClient.newHttpClient().send(HttpRequest.newBuilder(url.toURI()).build(),
        BodyHandlers.ofString());
  }

  public static String readAll(InputStream is) throws IOException {
    return IOUtils.toString(is, UTF_8);
  }

  static Mutation nm(String row, String cf, String cq, Value value) {
    Mutation m = new Mutation(new Text(row));
    m.put(new Text(cf), new Text(cq), value);
    return m;
  }

  static Mutation nm(String row, String cf, String cq, String value) {
    return nm(row, cf, cq, new Value(value));
  }

  public static SortedSet<Text> splits(String[] splits) {
    SortedSet<Text> result = new TreeSet<>();
    for (String split : splits) {
      result.add(new Text(split));
    }
    return result;
  }

  public static void assertNoDanglingFateLocks(AccumuloCluster cluster) {
    FateStatus fateStatus = getFateStatus(cluster);
    assertEquals(0, fateStatus.getDanglingHeldLocks().size(),
        "Dangling FATE locks : " + fateStatus.getDanglingHeldLocks());
    assertEquals(0, fateStatus.getDanglingWaitingLocks().size(),
        "Dangling FATE locks : " + fateStatus.getDanglingWaitingLocks());
  }

  private static FateStatus getFateStatus(AccumuloCluster cluster) {
    try {
      AdminUtil<String> admin = new AdminUtil<>();
      ServerContext context = cluster.getServerContext();
      var zk = context.getZooSession();
      ReadOnlyFateStore<String> readOnlyUFS =
          new UserFateStore<>(context, SystemTables.FATE.tableName(), null, null);
      ReadOnlyFateStore<String> readOnlyMFS = new MetaFateStore<>(zk, null, null);
      Map<FateInstanceType,ReadOnlyFateStore<String>> readOnlyFateStores =
          Map.of(FateInstanceType.META, readOnlyMFS, FateInstanceType.USER, readOnlyUFS);
      var lockPath = context.getServerPaths().createTableLocksPath();
      return admin.getStatus(readOnlyFateStores, zk, lockPath, null, null, null);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verify that flush ID gets updated properly and is the same for all tablets.
   */
  static Map<KeyExtent,OptionalLong> getFlushIds(ClientContext c, TableId tableId)
      throws Exception {

    Map<KeyExtent,OptionalLong> flushValues = new HashMap<>();

    try (TabletsMetadata metaScan =
        c.getAmple().readTablets().forTable(tableId).fetch(FLUSH_ID).checkConsistency().build()) {

      for (TabletMetadata tabletMetadata : metaScan) {
        flushValues.put(tabletMetadata.getExtent(), tabletMetadata.getFlushId());
      }

    }
    return flushValues;
  }
}
