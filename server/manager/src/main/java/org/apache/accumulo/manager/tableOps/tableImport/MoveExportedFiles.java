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
package org.apache.accumulo.manager.tableOps.tableImport;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

class MoveExportedFiles extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(MoveExportedFiles.class);

  private static final long serialVersionUID = 1L;

  private final ImportedTableInfo tableInfo;

  MoveExportedFiles(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    VolumeManager fs = manager.getVolumeManager();
    Map<Path,Path> oldToNewPaths = new HashMap<>();

    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Map<String,String> fileNameMappings = new HashMap<>();
      PopulateMetadataTable.readMappingFile(fs, tableInfo, dm.importDir, fileNameMappings);

      FileStatus[] exportedFiles = fs.listStatus(new Path(dm.exportDir));
      FileStatus[] importedFiles = fs.listStatus(new Path(dm.importDir));

      Function<FileStatus,String> fileStatusName = fstat -> fstat.getPath().getName();

      Set<Path> importing =
          Arrays.stream(exportedFiles).map(fileStatusName).map(fileNameMappings::get)
              .filter(Objects::nonNull).map(Path::new).collect(Collectors.toSet());

      Set<Path> imported =
          Arrays.stream(importedFiles).map(FileStatus::getPath).collect(Collectors.toSet());

      if (log.isDebugEnabled()) {
        log.debug("{} files already present in imported (target) directory: {}", fateId,
            imported.stream().map(Path::getName).collect(Collectors.joining(",")));
      }

      Set<Path> missingFiles = Sets.difference(
          fileNameMappings.values().stream().map(Path::new).collect(Collectors.toSet()),
          new HashSet<>(Sets.union(importing, imported)));

      if (!missingFiles.isEmpty()) {
        throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
            tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Missing source files corresponding to files "
                + missingFiles.stream().map(Path::getName).collect(Collectors.joining(",")));
      }

      for (FileStatus fileStatus : exportedFiles) {
        Path originalPath = fileStatus.getPath();
        String newName = fileNameMappings.get(originalPath.getName());
        // Need to exclude any other files which may be present in the exported directory
        if (newName != null) {
          Path newPath = new Path(dm.importDir, newName);

          // No try-catch here, as we do not expect any "benign" exceptions. Prior code already
          // accounts for files which were already moved. So anything returned by the rename
          // operation would be truly unexpected
          oldToNewPaths.put(originalPath, newPath);
        } else {
          log.debug("{} not moving (unmapped) file {}", fateId, originalPath);
        }
      }
    }
    try {
      fs.bulkRename(oldToNewPaths, manager.getRenamePool(), fateId);
    } catch (IOException ioe) {
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(), null,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER, ioe.getCause().getMessage());
    }

    return new FinishImportTable(tableInfo);
  }
}
