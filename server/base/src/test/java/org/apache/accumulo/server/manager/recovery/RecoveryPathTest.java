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
package org.apache.accumulo.server.manager.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

class RecoveryPathTest {

  private static final String ID = "11111111-1111-1111-1111-111111111111";

  @Test
  void mapsNewWalLayout() {
    var log = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/" + ID);
    assertEquals(new Path("hdfs://nn/accumulo/recovery/" + ID), RecoveryPath.getRecoveryPath(log));
  }

  @Test
  void mapsLegacyWalLayout() {
    var log = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/host+1234/" + ID);
    assertEquals(new Path("hdfs://nn/accumulo/recovery/" + ID), RecoveryPath.getRecoveryPath(log));
  }

  @Test
  void requiresUuidAsBasenameAndWalDirectory() {
    var wrongWalDirectory = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/other/" + ID);
    assertThrows(IllegalArgumentException.class,
        () -> RecoveryPath.getRecoveryPath(wrongWalDirectory));

    var original = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/" + ID);
    var wrongUuid = LogEntry.withSwitchedVolume(original,
        new Path("hdfs://nn/accumulo/wal/22222222-2222-2222-2222-222222222222"));
    assertThrows(IllegalArgumentException.class, () -> RecoveryPath.getRecoveryPath(wrongUuid));
  }
}
