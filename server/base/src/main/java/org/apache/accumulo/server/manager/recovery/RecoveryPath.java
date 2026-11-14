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

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;

public class RecoveryPath {

  // given a WAL entry, transform it to a recovery path
  public static Path getRecoveryPath(LogEntry logEntry) {
    Path walPath = logEntry.getPath();
    if (walPath.toUri().getScheme() == null || !walPath.isAbsolute()
        || !walPath.getName().equals(logEntry.getUniqueID().toString())) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }

    Path parent = walPath.getParent();
    if (parent == null) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }

    Path walDir;
    if (parent.getName().equals(FileType.WAL.getDirectory())) {
      // New paths are <volume>/wal/<uuid>.
      walDir = parent;
    } else {
      // Legacy paths are <volume>/wal/<tserver>/<uuid>. The tserver component is not needed
      // to determine the recovery path.
      walDir = parent.getParent();
      if (walDir == null || !walDir.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException("Bad WAL path " + walPath);
      }
    }

    Path volume = walDir.getParent();
    if (volume == null) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }
    return new Path(new Path(volume, FileType.RECOVERY.getDirectory()),
        logEntry.getUniqueID().toString());
  }

}
