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
package org.apache.accumulo.server.util;

import java.util.List;

import org.apache.accumulo.core.metadata.ScanServerStoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanScanServerScanEntries {

  private static Logger LOG = LoggerFactory.getLogger(CleanScanServerScanEntries.class);

  public static void main(String[] args) {

    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(CleanScanServerScanEntries.class.getName(), args);

    ServerContext context = opts.getServerContext();
    List<String> scanServers = context.getScanServers();

    TabletsMetadata tm = TabletsMetadata.builder(context).forLevel(DataLevel.METADATA)
        .fetch(ColumnType.SCANS).build();

    final Ample ample = context.getAmple();
    tm.forEach(m -> {
      m.getScans().forEach(stf -> {
        if (stf instanceof ScanServerStoredTabletFile) {
          ScanServerStoredTabletFile ssstf = (ScanServerStoredTabletFile) stf;
          if (!scanServers.contains(ssstf.getScanServerAddress())) {
            LOG.info("ScanServer {} not found, removing scan entry for file: {}",
                ssstf.getScanServerAddress(), ssstf.toString());
            TabletMutator mutator = ample.mutateTablet(m.getExtent());
            mutator.deleteScan(ssstf);
            mutator.mutate();
          }
        }
      });
    });

  }
}
