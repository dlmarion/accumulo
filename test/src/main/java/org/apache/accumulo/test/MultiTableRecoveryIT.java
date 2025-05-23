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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

public class MultiTableRecoveryIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    // test sorted rfile recovery options
    cfg.setProperty(Property.TSERV_WAL_SORT_FILE_PREFIX + "compress.type", "none");
  }

  @Test
  public void testRecoveryOverMultipleTables() throws Exception {
    final int N = 3;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      final String[] tables = getUniqueNames(N);
      final BatchWriter[] writers = new BatchWriter[N];
      final byte[][] values = new byte[N][];
      int i = 0;
      System.out.println("Creating tables");
      for (String tableName : tables) {
        c.tableOperations().create(tableName);
        values[i] = Integer.toString(i).getBytes(UTF_8);
        writers[i] = c.createBatchWriter(tableName);
        i++;
      }
      System.out.println("Creating agitator");
      final AtomicBoolean stop = new AtomicBoolean(false);
      final Thread agitator = agitator(stop);
      agitator.start();
      System.out.println("writing");
      for (i = 0; i < 1_000_000; i++) {
        // make non-negative avoiding Math.abs, because that can still be negative
        long randomRow = RANDOM.get().nextLong() & Long.MAX_VALUE;
        assertTrue(randomRow >= 0);
        final int table = (int) (randomRow % N);
        final Mutation m = new Mutation(Long.toHexString(randomRow));
        m.put(new byte[0], new byte[0], values[table]);
        writers[table].addMutation(m);
        if (i % 10_000 == 0) {
          System.out.println("flushing");
          for (int w = 0; w < N; w++) {
            writers[w].flush();
          }
        }
      }
      System.out.println("closing");
      for (int w = 0; w < N; w++) {
        writers[w].close();
      }
      System.out.println("stopping the agitator");
      stop.set(true);
      agitator.join();
      System.out.println("checking the data");
      long count = 0;
      for (int w = 0; w < N; w++) {
        try (Scanner scanner = c.createScanner(tables[w], Authorizations.EMPTY)) {
          for (Entry<Key,Value> entry : scanner) {
            int value = Integer.parseInt(entry.getValue().toString());
            assertEquals(w, value);
            count++;
          }
        }
      }
      assertEquals(1_000_000, count);
    }
  }

  private Thread agitator(final AtomicBoolean stop) {
    return new Thread(() -> {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
        int i = 0;
        while (!stop.get()) {
          Thread.sleep(SECONDS.toMillis(10));
          System.out.println("Restarting");
          getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
          getCluster().start();
          // read the metadata table to know everything is back up
          try (Scanner scanner =
              client.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
            scanner.forEach((k, v) -> {});
          }
          i++;
        }
        System.out.println("Restarted " + i + " times");
      } catch (IOException | InterruptedException | TableNotFoundException ex) {
        log.error("{}", ex.getMessage(), ex);
      }
    });
  }
}
