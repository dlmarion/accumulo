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
package org.apache.accumulo.manager.tableOps.tableExport;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.fs.Path;

public class ExportTable extends ManagerRepo {
  private static final long serialVersionUID = 1L;

  private final ExportInfo tableInfo;

  public ExportTable(NamespaceId namespaceId, String tableName, TableId tableId, String exportDir) {
    tableInfo = new ExportInfo();
    tableInfo.tableName = tableName;
    tableInfo.exportDir = exportDir;
    tableInfo.tableID = tableId;
    tableInfo.namespaceID = namespaceId;
  }

  @Override
  public long isReady(FateId fateId, Manager environment) throws Exception {
    return Utils.reserveHdfsDirectory(environment, new Path(tableInfo.exportDir).toString(),
        fateId);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) {
    return new WriteExportFiles(tableInfo);
  }

  @Override
  public void undo(FateId fateId, Manager env) throws Exception {
    Utils.unreserveHdfsDirectory(env, new Path(tableInfo.exportDir).toString(), fateId);
  }

  /**
   * Defines export / version.
   * <ul>
   * <li>version 1 exported by Accumulo &lt; 4.0</li>
   * <li>version 2 exported by Accumulo =&gt; 4.0 - uses file references with ranges.</li>
   * </ul>
   */
  public static final int VERSION_2 = 2;
  public static final int CURR_VERSION = VERSION_2;

  public static final String DATA_VERSION_PROP = "srcDataVersion";
  public static final String EXPORT_VERSION_PROP = "exportVersion";

}
