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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;

public class OfflineTabletLocatorImpl extends TabletLocator {

  private static final Logger LOG = LoggerFactory.getLogger(OfflineTabletLocatorImpl.class);

  public static class OfflineTabletLocation extends TabletLocation {

    public static final String SERVER = "offline_table_marker";

    public OfflineTabletLocation(KeyExtent tablet_extent) {
      super(tablet_extent, SERVER, SERVER);
    }

  }

  private class OfflineTabletsCache implements RemovalListener<KeyExtent,KeyExtent> {

    // This object uses a Caffeine cache to manage the duration of the extents
    // cached in the TreeSet. The TreeSet is necessary for expedient operations.

    private final ClientContext context;
    private final int prefetch;
    private final Cache<KeyExtent,KeyExtent> cache;
    private final Set<KeyExtent> evictions = new HashSet<>();
    private final NavigableSet<KeyExtent> extents =
        Collections.synchronizedNavigableSet(new TreeSet<>());
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private OfflineTabletsCache(ClientContext context) {
      this.context = context;
      Properties clientProperties = ClientConfConverter.toProperties(context.getConfiguration());
      Duration cacheDuration = Duration.ofMillis(
          ClientProperty.OFFLINE_LOCATOR_CACHE_DURATION.getTimeInMillis(clientProperties));
      int maxCacheSize =
          Integer.parseInt(ClientProperty.OFFLINE_LOCATOR_CACHE_SIZE.getValue(clientProperties));
      prefetch = Integer
          .parseInt(ClientProperty.OFFLINE_LOCATOR_CACHE_PREFETCH.getValue(clientProperties));
      cache = Caffeine.newBuilder().expireAfterAccess(cacheDuration).initialCapacity(maxCacheSize)
          .maximumSize(maxCacheSize).evictionListener(this).removalListener(this)
          .ticker(Ticker.systemTicker()).build();
    }

    @Override
    public void onRemoval(KeyExtent key, KeyExtent value, RemovalCause cause) {
      LOG.trace("Extent was evicted from cache: {}", key);
      synchronized (evictions) {
        evictions.add(key);
      }
    }

    private KeyExtent findOrLoadExtent(KeyExtent start) {
      lock.readLock().lock();
      try {
        KeyExtent match = extents.ceiling(start);
        if (match != null && match.contains(start)) {
          LOG.trace("Extent {} found in cache for start row {}", match, start);
          return match;
        }
      } finally {
        lock.readLock().unlock();
      }
      lock.writeLock().lock();
      // process prior evictions since we have the write lock
      processEvictions();
      // Load TabletMetadata
      try (TabletsMetadata tm =
          context.getAmple().readTablets().forTable(tid).overlapping(start.endRow(), true, null)
              .fetch(ColumnType.PREV_ROW, ColumnType.LOCATION).build()) {
        Iterator<TabletMetadata> iter = tm.iterator();
        for (int i = 0; i < prefetch && iter.hasNext(); i++) {
          TabletMetadata t = iter.next();
          KeyExtent ke = t.getExtent();
          Location loc = t.getLocation();
          if (loc != null && loc.getType() != LocationType.LAST) {
            throw new IllegalStateException("Extent has current or future location: " + ke);
          }
          LOG.trace("Caching extent: {}", ke);
          cache.put(ke, ke);
          extents.add(ke);
        }
        return extents.ceiling(start);
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void processEvictions() {
      synchronized (evictions) {
        LOG.trace("Processing prior evictions");
        extents.removeAll(evictions);
        evictions.clear();
      }
    }

    private void invalidate(KeyExtent failedExtent) {
      cache.invalidate(failedExtent);
    }

    private void invalidate(Collection<KeyExtent> keySet) {
      cache.invalidateAll(keySet);
    }

    private void invalidateAll() {
      cache.invalidateAll();
    }

  }

  private final TableId tid;
  private final OfflineTabletsCache extentCache;

  public OfflineTabletLocatorImpl(ClientContext context, TableId tableId) {
    tid = tableId;
    if (context.getTableState(tid) != TableState.OFFLINE) {
      throw new IllegalStateException("Table " + tableId + " is not offline");
    }
    extentCache = new OfflineTabletsCache(context);
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    Text metadataRow = new Text(tid.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());

    LOG.trace("Locating offline tablet for row: {}", metadataRow);
    KeyExtent start = KeyExtent.fromMetaRow(metadataRow);
    KeyExtent match = extentCache.findOrLoadExtent(start);
    if (match != null) {
      if (match.prevEndRow() == null || match.prevEndRow().compareTo(row) < 0) {
        LOG.trace("Found match for row: {}, extent = {}", row, match);
        return new OfflineTabletLocation(match);
      }
    }
    LOG.trace("Found no matching extent for row: {}", row);
    return null;
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    List<TabletLocation> tabletLocations = new ArrayList<>(ranges.size());
    List<Range> failures = new ArrayList<>();

    l1: for (Range r : ranges) {
      LOG.trace("Looking up locations for range: {}", r);
      tabletLocations.clear();
      Text startRow;

      if (r.getStartKey() != null) {
        startRow = r.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      TabletLocation tl = this.locateTablet(context, startRow, false, false);
      if (tl == null) {
        LOG.trace("NOT FOUND first tablet in range: {}", r);
        failures.add(r);
        continue;
      }
      LOG.trace("Found first tablet in range: {}, extent: {}", r, tl.tablet_extent);
      tabletLocations.add(tl);

      while (tl.tablet_extent.endRow() != null
          && !r.afterEndKey(new Key(tl.tablet_extent.endRow()).followingKey(PartialKey.ROW))) {
        tl = locateTablet(context, tl.tablet_extent.endRow(), true, false);

        if (tl == null) {
          LOG.trace("NOT FOUND following tablet in range: {}", r);
          failures.add(r);
          continue l1;
        }
        LOG.trace("Found following tablet in range: {}, extent: {}", r, tl.tablet_extent);
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
        LOG.trace("Found non-contiguous tablet in range: {}", r);
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
  public void invalidateCache(KeyExtent failedExtent) {
    extentCache.invalidate(failedExtent);
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    extentCache.invalidate(keySet);
  }

  @Override
  public void invalidateCache() {
    extentCache.invalidateAll();
  }

  @Override
  public void invalidateCache(ClientContext context, String server) {
    invalidateCache();
  }

}
