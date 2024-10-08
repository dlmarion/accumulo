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
package org.apache.accumulo.manager.compaction.coordinator.commit;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;

public class PutGcCandidates extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final CompactionCommitData commitData;
  private final String refreshLocation;

  public PutGcCandidates(CompactionCommitData commitData, String refreshLocation) {
    this.commitData = commitData;
    this.refreshLocation = refreshLocation;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    // add the GC candidates
    manager.getContext().getAmple().putGcCandidates(commitData.getTableId(),
        commitData.getJobFiles());

    if (refreshLocation == null) {
      manager.getCompactionCoordinator().recordCompletion(ExternalCompactionId.of(commitData.ecid));
      return null;
    }

    // For user initiated table compactions, the fate operation will refresh tablets. Can also
    // refresh as part of this compaction commit as it may run sooner.
    return new RefreshTablet(commitData.ecid, commitData.textent, refreshLocation);
  }
}
