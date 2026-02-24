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
package org.apache.accumulo.core.clientImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.hadoop.io.Text;

public class OfflineTabletLocatorImpl extends TabletLocator {

  public static class OfflineTabletLocation extends TabletLocation {

    public static final String SERVER = "offline_table_marker";

    public OfflineTabletLocation(KeyExtent tablet_extent) {
      super(tablet_extent, SERVER, SERVER);
    }

  }

  private final TableId tid;
  private final TreeSet<KeyExtent> extents = new TreeSet<>();

  public OfflineTabletLocatorImpl(ClientContext context, TableId tableId) {
    tid = tableId;
    if (context.getTableState(tid) != TableState.OFFLINE) {
      throw new IllegalStateException("Table " + tableId + " is not offline");
    }
  }

  private synchronized void populateExtents(ClientContext context) {
    if (extents.size() > 0) {
      return;
    }
    try (TabletsMetadata tm = context.getAmple().readTablets().forTable(tid)
        .fetch(ColumnType.PREV_ROW, ColumnType.LOCATION).build()) {
      tm.forEach(t -> {
        KeyExtent ke = t.getExtent();
        Location loc = t.getLocation();
        if (loc != null && loc.getType() != LocationType.LAST) {
          throw new IllegalStateException("Extent has current or future location: " + ke);
        }
        extents.add(ke);
      });
    }
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    populateExtents(context);

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    Text metadataRow = new Text(tid.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());

    Set<KeyExtent> results = KeyExtent.findOverlapping(KeyExtent.fromMetaRow(metadataRow), extents);
    if (results.isEmpty()) {
      // nothing found
      return null;
    } else {
      // return the 1st (lowest) KeyExtent
      return new OfflineTabletLocation(results.iterator().next());
    }
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    populateExtents(context);
    List<TabletLocation> tabletLocations = new ArrayList<>(ranges.size());
    List<Range> failures = new ArrayList<>();

    l1: for (Range r : ranges) {
      tabletLocations.clear();
      Text startRow;

      if (r.getStartKey() != null) {
        startRow = r.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      TabletLocation tl = this.locateTablet(context, startRow, false, false);
      if (tl == null) {
        failures.add(r);
        continue;
      }
      tabletLocations.add(tl);

      while (tl.tablet_extent.endRow() != null
          && !r.afterEndKey(new Key(tl.tablet_extent.endRow()).followingKey(PartialKey.ROW))) {
        tl = locateTablet(context, tl.tablet_extent.endRow(), false, false);

        if (tl == null) {
          failures.add(r);
          continue l1;
        }
        tabletLocations.add(tl);
      }

      // Ensure the extents found are non overlapping and have no holes. When reading some extents
      // from the cache and other from the metadata table in the loop above we may end up with
      // non-contiguous extents. This can happen when a subset of exents are placed in the cache and
      // then after that merges and splits happen.
      if (TabletLocatorImpl.isContiguous(tabletLocations)) {
        for (TabletLocation tl2 : tabletLocations) {
          TabletLocatorImpl.addRange(binnedRanges, tl2.tablet_location, tl2.tablet_extent, r);
        }
      } else {
        failures.add(r);
      }

    }
    return failures;
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {}

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {}

  @Override
  public void invalidateCache() {}

  @Override
  public void invalidateCache(ClientContext context, String server) {}

}
