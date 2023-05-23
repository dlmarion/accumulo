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
package org.apache.accumulo.manager.tableOps.split;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class UpdateTablets extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(UpdateTablets.class);
  private static final long serialVersionUID = 1L;
  private final SplitInfo splitInfo;
  private final List<String> dirNames;

  public UpdateTablets(SplitInfo splitInfo, List<String> dirNames) {
    this.splitInfo = splitInfo;
    this.dirNames = dirNames;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    TabletMetadata tabletMetadata =
        manager.getContext().getAmple().readTablet(splitInfo.getOriginal());

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    if (tabletMetadata == null) {
      // check to see if this operation has already succeeded.
      TabletMetadata newTabletMetadata =
          manager.getContext().getAmple().readTablet(splitInfo.getTablets().last());

      if (newTabletMetadata != null && opid.equals(newTabletMetadata.getOperationId())) {
        // have already created the new tablet and failed before we could return the next step, so
        // lets go ahead and return the next step.
        log.trace(
            "{} creating new tablet was rejected because it existed, operation probably failed before.",
            FateTxId.formatTid(tid));
        return new DeleteOperationIds(splitInfo);
      } else {
        throw new IllegalStateException("Tablet is in an unexpected condition "
            + splitInfo.getOriginal() + " " + (newTabletMetadata == null) + " "
            + (newTabletMetadata == null ? null : newTabletMetadata.getOperationId()));
      }
    }

    Preconditions.checkState(tabletMetadata.getOperationId().equals(opid),
        "Tablet %s does not have expected operation id %s it has %s", splitInfo.getOriginal(), opid,
        tabletMetadata.getOperationId());

    var newTablets = splitInfo.getTablets();

    var newTabletsFiles = getNewTabletFiles(newTablets, tabletMetadata,
        file -> manager.getSplitter().getCachedFileInfo(splitInfo.getOriginal().tableId(), file));

    addNewTablets(tid, manager, tabletMetadata, opid, newTablets, newTabletsFiles);

    // Only update the original tablet after successfully creating the new tablets, this is
    // important for failure cases where this operation partially runs a then runs again.

    updateExistingTablet(manager, tabletMetadata, opid, newTablets, newTabletsFiles);

    return new DeleteOperationIds(splitInfo);
  }

  /**
   * Determine which files from the original tablet go to each new tablet being created by the
   * split.
   */
  static Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> getNewTabletFiles(
      Set<KeyExtent> newTablets, TabletMetadata tabletMetadata,
      Function<StoredTabletFile,FileUtil.FileInfo> fileInfoProvider) {

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> tabletsFiles = new TreeMap<>();

    newTablets.forEach(extent -> tabletsFiles.put(extent, new HashMap<>()));

    // determine while files overlap which tablets and their estimated sizes
    tabletMetadata.getFilesMap().forEach((file, dataFileValue) -> {
      FileUtil.FileInfo fileInfo = fileInfoProvider.apply(file);

      Range fileRange;
      if (fileInfo != null) {
        fileRange = new Range(fileInfo.getFirstRow(), fileInfo.getLastRow());
      } else {
        fileRange = new Range();
      }

      // count how many of the new tablets the file will overlap
      double numOverlapping = newTablets.stream().map(KeyExtent::toDataRange)
          .filter(range -> range.clip(fileRange, true) != null).count();

      Preconditions.checkState(numOverlapping > 0);

      // evenly split the tablets estimates between the number of tablets it actually overlaps
      double sizePerTablet = dataFileValue.getSize() / numOverlapping;
      double entriesPerTablet = dataFileValue.getNumEntries() / numOverlapping;

      // add the file to the tablets it overlaps
      newTablets.forEach(newTablet -> {
        if (newTablet.toDataRange().clip(fileRange, true) != null) {
          DataFileValue ndfv = new DataFileValue((long) sizePerTablet, (long) entriesPerTablet,
              dataFileValue.getTime());
          tabletsFiles.get(newTablet).put(file, ndfv);
        }
      });
    });

    if (log.isTraceEnabled()) {
      tabletMetadata.getFilesMap().forEach((f, v) -> {
        log.trace("{} original file {} {} {}", tabletMetadata.getExtent(), f.getFileName(),
            v.getSize(), v.getNumEntries());
      });

      tabletsFiles.forEach((extent, files) -> {
        files.forEach((f, v) -> {
          log.trace("{} split file {} {} {}", extent, f.getFileName(), v.getSize(),
              v.getNumEntries());
        });
      });
    }

    return tabletsFiles;
  }

  private void addNewTablets(long tid, Manager manager, TabletMetadata tabletMetadata,
      TabletOperationId opid, SortedSet<KeyExtent> newTablets,
      Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> newTabletsFiles) {
    Iterator<String> dirNameIter = dirNames.iterator();

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
      for (var newExtent : newTablets) {
        if (newExtent.equals(newTablets.last())) {
          // Skip the last tablet, its done in the next fate step.
          continue;
        }

        var mutator = tabletsMutator.mutateTablet(newExtent).requireAbsentTablet();

        mutator.putOperation(opid);
        mutator.putDirName(dirNameIter.next());
        mutator.putTime(tabletMetadata.getTime());
        tabletMetadata.getFlushId().ifPresent(mutator::putFlushId);
        mutator.putPrevEndRow(newExtent.prevEndRow());
        tabletMetadata.getCompactId().ifPresent(mutator::putCompactionId);
        mutator.putHostingGoal(tabletMetadata.getHostingGoal());

        tabletMetadata.getLoaded().forEach(mutator::putBulkFile);
        tabletMetadata.getLogs().forEach(mutator::putWal);

        newTabletsFiles.get(newExtent).forEach(mutator::putFile);

        mutator.submit(afterMeta -> afterMeta.getOperationId() != null
            && afterMeta.getOperationId().equals(opid));
      }

      var results = tabletsMutator.process();
      results.values().forEach(result -> {
        var status = result.getStatus();
        if (status == ConditionalWriter.Status.REJECTED) {
          // lets see if this was rejected because this operation is running again in the case of
          // failure
          var newTabletMetadata = result.readMetadata();
          if (newTabletMetadata != null && opid.equals(newTabletMetadata.getOperationId())) {
            log.trace(
                "{} {} creating new tablet was rejected because it existed, operation probably failed before.",
                FateTxId.formatTid(tid), result.getExtent());
            return;
          }
        }

        Preconditions.checkState(status == ConditionalWriter.Status.ACCEPTED,
            "Failed to add new tablet %s %s %s", status, splitInfo.getOriginal(),
            result.getExtent());
      });
    }
  }

  private void updateExistingTablet(Manager manager, TabletMetadata tabletMetadata,
      TabletOperationId opid, SortedSet<KeyExtent> newTablets,
      Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> newTabletsFiles) {
    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
      var newExtent = newTablets.last();

      var mutator = tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireOperation(opid)
          .requirePrevEndRow(splitInfo.getOriginal().prevEndRow());

      mutator.putPrevEndRow(newExtent.prevEndRow());

      newTabletsFiles.get(newExtent).forEach(mutator::putFile);

      // remove the files from the original tablet that did not end up in the tablet
      tabletMetadata.getFiles().forEach(existingFile -> {
        if (!newTabletsFiles.get(newExtent).containsKey(existingFile)) {
          mutator.deleteFile(existingFile);
        }
      });

      mutator.submit();

      var result = tabletsMutator.process().get(splitInfo.getOriginal());

      if (result.getStatus() == ConditionalWriter.Status.UNKNOWN) {
        // Can not use Ample's built in code for checking unknown because we are changing the prev
        // end row, so much check it manually

        var tabletMeta = manager.getContext().getAmple().readTablet(newExtent);

        if (tabletMeta == null || !tabletMeta.getOperationId().equals(opid)) {
          // ELASTICITY_TODO need to retry when an UNKNOWN condition is seen, its possible the
          // mutation never made it to the tserver. May want ample to always retry on unknown and
          // change the unknown handler to a rejected handler.
          throw new IllegalStateException("Failed to update existing tablet in split "
              + splitInfo.getOriginal() + " " + result.getStatus() + " " + result.getExtent());
        }
      } else if (result.getStatus() != ConditionalWriter.Status.ACCEPTED) {
        // maybe this step is being run again and the update was already made
        throw new IllegalStateException("Failed to update existing tablet in split "
            + splitInfo.getOriginal() + " " + result.getStatus() + " " + result.getExtent());
      }
    }
  }
}