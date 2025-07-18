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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CloneTestIT_SimpleSuite extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void testProps() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    String table2 = tableNames[1];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.tableOperations().create(table1);

      c.tableOperations().setProperty(table1, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(),
          "1M");
      c.tableOperations().setProperty(table1,
          Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "2M");
      c.tableOperations().setProperty(table1, Property.TABLE_FILE_MAX.getKey(), "23");

      writeDataAndClone(c, table1, table2);

      checkData(table2, c);

      checkMetadata(table2, c);

      Map<String,String> tableProps = Map.copyOf(c.tableOperations().getConfiguration(table2));

      assertEquals("500K", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey()));
      assertEquals(Property.TABLE_FILE_MAX.getDefaultValue(),
          tableProps.get(Property.TABLE_FILE_MAX.getKey()));
      assertEquals("2M", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey()));

      c.tableOperations().delete(table1);
      c.tableOperations().delete(table2);
    }
  }

  private void assertTableState(String tableName, AccumuloClient c, TableState expected) {
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    TableState tableState = ((ClientContext) c).getTableState(TableId.of(tableId));
    assertEquals(expected, tableState);
  }

  private void checkData(String table2, AccumuloClient c) throws TableNotFoundException {
    try (Scanner scanner = c.createScanner(table2, Authorizations.EMPTY)) {

      HashMap<String,String> expected = new HashMap<>();
      expected.put("001:x", "9");
      expected.put("001:y", "7");
      expected.put("008:x", "3");
      expected.put("008:y", "4");

      HashMap<String,String> actual = new HashMap<>();

      for (Entry<Key,Value> entry : scanner) {
        actual.put(entry.getKey().getRowData() + ":" + entry.getKey().getColumnQualifierData(),
            entry.getValue().toString());
      }

      assertEquals(expected, actual);
    }
  }

  private void checkMetadata(String table, AccumuloClient client) throws Exception {
    try (
        Scanner s = client.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {

      s.fetchColumnFamily(DataFileColumnFamily.NAME);
      ServerColumnFamily.DIRECTORY_COLUMN.fetch(s);
      String tableId = client.tableOperations().tableIdMap().get(table);

      assertNotNull(tableId, "Could not get table id for " + table);

      s.setRange(Range.prefix(tableId));

      Key k;
      Text cf = new Text();
      Text cq = new Text();
      int itemsInspected = 0;
      var pattern = Pattern.compile("[tc]-[0-9a-z]+");
      for (Entry<Key,Value> entry : s) {
        itemsInspected++;
        k = entry.getKey();
        k.getColumnFamily(cf);
        k.getColumnQualifier(cq);

        if (cf.equals(DataFileColumnFamily.NAME)) {
          Path p = StoredTabletFile.of(cq).getPath();
          FileSystem fs = getCluster().getFileSystem();
          assertTrue(fs.exists(p), "File does not exist: " + p);
        } else if (cf.equals(ServerColumnFamily.DIRECTORY_COLUMN.getColumnFamily())) {
          assertEquals(ServerColumnFamily.DIRECTORY_COLUMN.getColumnQualifier(), cq,
              "Saw unexpected cq");

          String dirName = entry.getValue().toString();

          assertTrue(pattern.matcher(dirName).matches(), "Bad dir name " + dirName);
        } else {
          fail("Got unexpected key-value: " + entry);
          throw new RuntimeException();
        }
      }
      assertTrue(itemsInspected > 0, "Expected to find metadata entries");
    }
  }

  private BatchWriter writeData(String table1, AccumuloClient c) throws Exception {
    BatchWriter bw = c.createBatchWriter(table1);

    Mutation m1 = new Mutation("001");
    m1.put("data", "x", "9");
    m1.put("data", "y", "7");

    Mutation m2 = new Mutation("008");
    m2.put("data", "x", "3");
    m2.put("data", "y", "4");

    bw.addMutation(m1);
    bw.addMutation(m2);

    bw.flush();
    return bw;
  }

  private void writeDataAndClone(AccumuloClient c, String table1, String table2) throws Exception {
    try (BatchWriter bw = writeData(table1, c)) {
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "500K");

      Set<String> exclude = new HashSet<>();
      exclude.add(Property.TABLE_FILE_MAX.getKey());

      c.tableOperations().clone(table1, table2, true, props, exclude);

      assertTableState(table2, c, TableState.ONLINE);

      Mutation m3 = new Mutation("009");
      m3.put("data", "x", "1");
      m3.put("data", "y", "2");
      bw.addMutation(m3);
    }
  }

  @Test
  public void testDeleteClone() throws Exception {
    String[] tableNames = getUniqueNames(3);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    String table3 = tableNames[2];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      AccumuloCluster cluster = getCluster();
      assumeTrue(cluster instanceof MiniAccumuloClusterImpl);
      MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) cluster;

      // verify that deleting a new table removes the files
      c.tableOperations().create(table3);
      writeData(table3, c).close();
      c.tableOperations().flush(table3, null, null, true);
      // check for files
      FileSystem fs = getCluster().getFileSystem();
      final String id = c.tableOperations().tableIdMap().get(table3);

      // the following path expects mini to be configured with a single volume
      final Path tablePath = new Path(mac.getSiteConfiguration().get(Property.INSTANCE_VOLUMES)
          + "/" + Constants.TABLE_DIR + "/" + id);
      FileStatus[] status = fs.listStatus(tablePath);
      assertTrue(status.length > 0);
      // verify disk usage
      List<DiskUsage> diskUsage = c.tableOperations().getDiskUsage(Collections.singleton(table3));
      assertEquals(1, diskUsage.size());
      assertTrue(diskUsage.get(0).getUsage() > 100);
      // delete the table
      c.tableOperations().delete(table3);
      // verify its gone from the file system
      if (fs.exists(tablePath)) {
        status = fs.listStatus(tablePath);
        assertTrue(status == null || status.length == 0);
      }

      c.tableOperations().create(table1);

      writeDataAndClone(c, table1, table2);

      // delete source table, should not affect clone
      c.tableOperations().delete(table1);

      checkData(table2, c);

      c.tableOperations().compact(table2, null, null, true, true);

      checkData(table2, c);

      c.tableOperations().delete(table2);
    }
  }

  @Test
  public void testOfflineClone() throws Exception {
    String[] tableNames = getUniqueNames(3);
    String table1 = tableNames[0];
    String table2 = tableNames[1];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      AccumuloCluster cluster = getCluster();
      assumeTrue(cluster instanceof MiniAccumuloClusterImpl);

      c.tableOperations().create(table1);

      writeData(table1, c);

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "500K");

      Set<String> exclude = new HashSet<>();
      exclude.add(Property.TABLE_FILE_MAX.getKey());

      c.tableOperations().clone(table1, table2, CloneConfiguration.builder().setFlush(true)
          .setPropertiesToSet(props).setPropertiesToExclude(exclude).setKeepOffline(true).build());

      assertTableState(table2, c, TableState.OFFLINE);

      // delete tables
      c.tableOperations().delete(table1);
      c.tableOperations().delete(table2);
    }
  }

  @Test
  public void testCloneWithSplits() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      List<Mutation> mutations = new ArrayList<>();
      TreeSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new Text(Integer.toString(i)));
        Mutation m = new Mutation(Integer.toString(i));
        m.put("", "", "");
        mutations.add(m);
      }

      String[] tables = getUniqueNames(2);

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      client.tableOperations().create(tables[0], ntc);

      try (BatchWriter bw = client.createBatchWriter(tables[0])) {
        bw.addMutations(mutations);
      }

      client.tableOperations().clone(tables[0], tables[1], CloneConfiguration.empty());

      client.tableOperations().deleteRows(tables[1], new Text("4"), new Text("8"));

      List<String> rows = Arrays.asList("0", "1", "2", "3", "4", "9");
      List<String> actualRows = new ArrayList<>();
      try (var scanner = client.createScanner(tables[1], Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          actualRows.add(entry.getKey().getRow().toString());
        }
      }

      assertEquals(rows, actualRows);
    }
  }

  @Test
  public void testCloneSystemTables() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      var sysTables = SystemTables.values();
      var tableNames = getUniqueNames(sysTables.length);

      for (int i = 0; i < sysTables.length; i++) {
        var sysTable = sysTables[i];
        var cloneTableName = tableNames[i];
        assertThrows(Exception.class, () -> client.tableOperations().clone(sysTable.tableName(),
            cloneTableName, CloneConfiguration.empty()), () -> "Table:" + sysTable.tableName());
        assertFalse(client.tableOperations().exists(cloneTableName));
      }
    }
  }

  private void baseCloneNamespace(AccumuloClient client, String src, String dest) throws Exception {
    writeData(src, client).close();
    // Don't force a flush on the table, let's make sure the
    // clone operation does this
    client.tableOperations().clone(src, dest, CloneConfiguration.empty());
    checkData(dest, client);
  }

  @Test
  public void testCloneSameNamespace() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.namespaceOperations().create("old3");
      client.tableOperations().create("old3." + tableName);
      assertThrows(TableExistsException.class,
          () -> baseCloneNamespace(client, "old3." + tableName, "old3." + tableName));
    }
  }

  @Test
  public void testCloneIntoDiffNamespace() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.namespaceOperations().create("old4");
      client.tableOperations().create("old4." + tableName);
      client.namespaceOperations().create("new4");
      baseCloneNamespace(client, "old4." + tableName, "new4." + tableName);
    }
  }

  @Test
  public void testCloneIntoDiffNamespaceTableExists() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.namespaceOperations().create("old5");
      client.tableOperations().create("old5." + tableName);
      client.namespaceOperations().create("new5");
      client.tableOperations().create("new5." + tableName);
      assertThrows(TableExistsException.class,
          () -> baseCloneNamespace(client, "old5." + tableName, "new5." + tableName));
    }
  }

  @Test
  public void testCloneIntoDiffNamespaceDoesntExist() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.namespaceOperations().create("old1");
      client.tableOperations().create("old1." + tableName);
      assertThrows(AccumuloException.class,
          () -> baseCloneNamespace(client, "old1." + tableName, "missing." + tableName));
    }
  }

  @Test
  public void testCloneIntoAccumuloNamespace() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.namespaceOperations().create("old2");
      client.tableOperations().create("old2." + tableName);
      assertThrows(AccumuloException.class,
          () -> baseCloneNamespace(client, "old2." + tableName, "accumulo." + tableName));
    }
  }

  @Test
  public void testCloneNamespaceIncorrectPermissions() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final String newUserName = "NEW_USER";
    final String srcNs = "src";
    final String srcTableName = srcNs + "." + tableName;
    final String destNs = "dst";
    final String dstTableName = destNs + "." + tableName;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.namespaceOperations().create(srcNs);
      client.tableOperations().create(srcTableName);
      client.namespaceOperations().create(destNs);
      client.securityOperations().createLocalUser(newUserName, new PasswordToken(newUserName));
      // User needs WRITE or ALTER_TABLE on the src table to flush it as part of the clone operation
      client.securityOperations().grantTablePermission(newUserName, srcTableName,
          TablePermission.ALTER_TABLE);
      client.securityOperations().grantNamespacePermission(newUserName, destNs,
          NamespacePermission.READ);
      client.securityOperations().grantNamespacePermission(newUserName, destNs,
          NamespacePermission.CREATE_TABLE);
    }

    try (AccumuloClient client =
        Accumulo.newClient().to(getCluster().getInstanceName(), getCluster().getZooKeepers())
            .as(newUserName, newUserName).build()) {
      // READ permission is needed on the src table, not the dst namespace
      assertThrows(AccumuloSecurityException.class, () -> client.tableOperations()
          .clone(srcTableName, dstTableName, CloneConfiguration.empty()));
    }
  }

  @Test
  public void testCloneNamespacePermissions() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final String newUserName = "NEW_USER2";
    final String srcNs = "src2";
    final String srcTableName = srcNs + "." + tableName;
    final String destNs = "dst2";
    final String dstTableName = destNs + "." + tableName;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.namespaceOperations().create(srcNs);
      client.tableOperations().create(srcTableName);
      client.namespaceOperations().create(destNs);
      client.securityOperations().createLocalUser(newUserName, new PasswordToken(newUserName));
      // User needs WRITE or ALTER_TABLE on the src table to flush it as part of the clone operation
      client.securityOperations().grantTablePermission(newUserName, srcTableName,
          TablePermission.ALTER_TABLE);
      client.securityOperations().grantTablePermission(newUserName, srcTableName,
          TablePermission.READ);
      client.securityOperations().grantNamespacePermission(newUserName, destNs,
          NamespacePermission.CREATE_TABLE);
    }

    try (AccumuloClient client =
        Accumulo.newClient().to(getCluster().getInstanceName(), getCluster().getZooKeepers())
            .as(newUserName, newUserName).build()) {
      client.tableOperations().clone(srcTableName, dstTableName, CloneConfiguration.empty());
    }
  }
}
