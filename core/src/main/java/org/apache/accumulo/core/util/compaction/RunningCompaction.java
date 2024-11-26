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
package org.apache.accumulo.core.util.compaction;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;

public class RunningCompaction {

  private final TExternalCompactionJob job;
  private final String compactorAddress;
  private final String groupName;
  private final Map<Long,TCompactionStatusUpdate> updates = new TreeMap<>();

  // If this object were to be added to a time sorted list before the start time
  // is set, then it will end up at the end of the list.
  private Long startTime = Long.MAX_VALUE;

  public RunningCompaction(TExternalCompactionJob job, String compactorAddress, String groupName) {
    this.job = job;
    this.compactorAddress = compactorAddress;
    this.groupName = groupName;
  }

  public RunningCompaction(TExternalCompaction tEC) {
    this(tEC.getJob(), tEC.getCompactor(), tEC.getGroupName());
  }

  public Map<Long,TCompactionStatusUpdate> getUpdates() {
    synchronized (updates) {
      return new TreeMap<>(updates);
    }
  }

  public void addUpdate(Long timestamp, TCompactionStatusUpdate update) {
    synchronized (updates) {
      this.updates.put(timestamp, update);
      if (update.getState() == TCompactionState.STARTED) {
        startTime = timestamp;
      }
    }
  }

  public TExternalCompactionJob getJob() {
    return job;
  }

  public String getCompactorAddress() {
    return compactorAddress;
  }

  public String getGroupName() {
    return groupName;
  }

  public Long getStartTime() {
    if (startTime == Long.MAX_VALUE) {
      throw new IllegalStateException("Start time has not been set for RunningCompcation");
    } else {
      return startTime;
    }
  }

  public void setStartTime(Long time) {
    if (startTime == Long.MAX_VALUE) {
      startTime = time;
    } else {
      throw new IllegalStateException("Start time already set for RunningCompcation");
    }
  }

}
