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
package org.apache.accumulo.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooLock implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(ZooLock.class);

  private static final String ZLOCK_PREFIX = "zlock#";
  private static final String ZLOCK_UUID = UUID.randomUUID().toString();
  private static final String VM_ZLOCK_PREFIX = ZLOCK_PREFIX + ZLOCK_UUID + "#";

  public static class Prefix {
    private final String prefix;

    public Prefix(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public String toString() {
      return this.prefix;
    }

  }

  public enum LockLossReason {
    LOCK_DELETED, SESSION_EXPIRED
  }

  public interface LockWatcher {
    void lostLock(LockLossReason reason);

    /**
     * lost the ability to monitor the lock node, and its status is unknown
     */
    void unableToMonitorLockNode(Exception e);
  }

  public interface AccumuloLockWatcher extends LockWatcher {
    void acquiredLock();

    void failedToAcquireLock(Exception e);
  }

  private final String path;
  protected final ZooKeeper zooKeeper;

  private LockWatcher lockWatcher;
  private String lockNodeName;
  private volatile boolean lockWasAcquired;
  private volatile boolean watchingParent = false;

  private String createdNodeName;
  private String watchingNodeName;
  private Prefix vmLockPrefix;

  public ZooLock(AccumuloConfiguration conf, String path) {
    this(conf.get(Property.INSTANCE_ZK_HOST),
        (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
        conf.get(Property.INSTANCE_SECRET), path);
  }

  public ZooLock(String zookeepers, int timeInMillis, String secret, String path) {
    this.zooKeeper = ZooSession.getAuthenticatedSession(zookeepers, timeInMillis, "digest",
        ("accumulo" + ":" + secret).getBytes(UTF_8));
    this.path = path;
    try {
      zooKeeper.exists(path, this);
      watchingParent = true;
      this.setVMLockPrefix(new Prefix(VM_ZLOCK_PREFIX));
    } catch (Exception ex) {
      LOG.warn("Error getting setting initial watch on ZooLock", ex);
      throw new RuntimeException(ex);
    }
  }

  public void setVMLockPrefix(Prefix p) {
    this.vmLockPrefix = p;
  }

  private static class LockWatcherWrapper implements AccumuloLockWatcher {

    boolean acquiredLock = false;
    LockWatcher lw;

    public LockWatcherWrapper(LockWatcher lw2) {
      this.lw = lw2;
    }

    @Override
    public void acquiredLock() {
      acquiredLock = true;
    }

    @Override
    public void failedToAcquireLock(Exception e) {}

    @Override
    public void lostLock(LockLossReason reason) {
      lw.lostLock(reason);
    }

    @Override
    public void unableToMonitorLockNode(Exception e) {
      lw.unableToMonitorLockNode(e);
    }

  }

  public synchronized boolean tryLock(LockWatcher lw, byte[] data)
      throws KeeperException, InterruptedException {

    LockWatcherWrapper lww = new LockWatcherWrapper(lw);

    lock(lww, data);

    if (lww.acquiredLock) {
      return true;
    }

    // If we didn't acquire the lock, then delete the path we just created
    if (createdNodeName != null) {
      String pathToDelete = path + "/" + createdNodeName;
      LOG.debug("[{}] Failed to acquire lock in tryLock(), deleting all at path: {}", vmLockPrefix,
          pathToDelete);
      recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);
      createdNodeName = null;
    }

    return false;
  }

  /**
   * This method will delete a node and all its children.
   */
  private void recursiveDelete(String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    if (policy == NodeMissingPolicy.CREATE) {
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    }
    try {
      // delete children
      for (String child : zooKeeper.getChildren(zPath, null)) {
        recursiveDelete(zPath + "/" + child, NodeMissingPolicy.SKIP);
      }

      // delete self
      zooKeeper.delete(zPath, -1);
    } catch (KeeperException e) {
      // new child appeared; try again
      if (e.code() == Code.NOTEMPTY) {
        recursiveDelete(zPath, policy);
      }
      if (policy == NodeMissingPolicy.SKIP && e.code() == Code.NONODE) {
        return;
      }
      throw e;
    }
  }

  /**
   * Sort list of ephemeral nodes by their sequence number. Any ephemeral nodes that are not of the
   * correct form will sort last.
   *
   * @param children
   *          list of ephemeral nodes
   */
  public static void sortChildrenByLockPrefix(List<String> children) {
    children.sort(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {

        // Lock should be of the form:
        // zlock#UUID#sequenceNumber
        // Example:
        // zlock#44755fbe-1c9e-40b3-8458-03abaf950d7e#0000000000

        boolean lValid = true;
        if (o1.length() != 53 && o1.lastIndexOf('#') != 42) {
          lValid = false;
        }
        boolean rValid = true;
        if (o2.length() != 53 && o2.lastIndexOf('#') != 42) {
          rValid = false;
        }
        // Make any invalid's sort last (higher)
        if (lValid && rValid) {
          int leftIdx = 43, rightIdx = 43;
          return Integer.valueOf(o1.substring(leftIdx))
              .compareTo(Integer.valueOf(o2.substring(rightIdx)));
        } else if (!lValid && rValid) {
          return 1;
        } else if (lValid && !rValid) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    if (LOG.isDebugEnabled()) {
      LOG.debug("Children nodes: {}", children.size());
      children.forEach(c -> LOG.debug("- {}", c));
    }
  }

  /**
   * Given a pre-sorted set of children ephemeral nodes where the node name is of the form
   * "zlock#UUID#sequenceNumber", find the ephemeral node that sorts before the ephemeralNode
   * parameter with the lowest sequence number
   *
   * @param children
   *          list of sequential ephemera nodes, already sorted
   * @param ephemeralNode
   *          starting node for the search
   * @return next lowest prefix with the lowest sequence number
   */
  public static String findLowestPrevPrefix(final List<String> children,
      final String ephemeralNode) {
    int idx = children.indexOf(ephemeralNode);
    // Get the prefix from the prior ephemeral node
    String prev = children.get(idx - 1);
    int prefixIdx = prev.lastIndexOf('#');
    String prevPrefix = prev.substring(0, prefixIdx);

    // Find the lowest sequential ephemeral node with prevPrefix
    int i = 2;
    String lowestPrevNode = prev;
    while ((idx - i) >= 0) {
      prev = children.get(idx - i);
      i++;
      if (prev.startsWith(prevPrefix)) {
        lowestPrevNode = prev;
      } else {
        break;
      }
    }
    return lowestPrevNode;
  }

  private synchronized void determineLockOwnership(final String createdEphemeralNode,
      final AccumuloLockWatcher lw) throws KeeperException, InterruptedException {

    if (createdNodeName == null) {
      throw new IllegalStateException(
          "Called determineLockOwnership() when ephemeralNodeName == null");
    }

    List<String> children = new ArrayList<>();
    zooKeeper.getChildren(path, null).forEach(s -> {
      if (s.startsWith(ZLOCK_PREFIX)) {
        children.add(s);
      }
    });

    if (!children.contains(createdEphemeralNode)) {
      throw new RuntimeException(
          "Lock attempt ephemeral node no longer exist " + createdEphemeralNode);
    }
    sortChildrenByLockPrefix(children);

    if (children.get(0).equals(createdEphemeralNode)) {
      LOG.debug("[{}] First candidate is my lock, acquiring", vmLockPrefix);
      if (!watchingParent) {
        throw new IllegalStateException(
            "Can not acquire lock, no longer watching parent : " + path);
      }
      this.lockWatcher = lw;
      this.lockNodeName = createdEphemeralNode;
      createdNodeName = null;
      lockWasAcquired = true;
      lw.acquiredLock();
    } else {
      LOG.debug("[{}] Lock held by another process with ephemeral node: {}", vmLockPrefix,
          children.get(0));

      String lowestPrevNode = findLowestPrevPrefix(children, createdEphemeralNode);

      watchingNodeName = path + "/" + lowestPrevNode;
      final String nodeToWatch = watchingNodeName;
      LOG.debug("[{}] Establishing watch on prior node {}", vmLockPrefix, nodeToWatch);
      Watcher priorNodeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("[{}] Processing event:", vmLockPrefix);
            LOG.trace("- type  {}", event.getType());
            LOG.trace("- path  {}", event.getPath());
            LOG.trace("- state {}", event.getState());
          }
          boolean renew = true;
          if (event.getType() == EventType.NodeDeleted && event.getPath().equals(nodeToWatch)) {
            LOG.debug("[{}] Detected deletion of prior node {}, attempting to acquire lock",
                vmLockPrefix, nodeToWatch);
            synchronized (ZooLock.this) {
              try {
                if (createdNodeName != null) {
                  determineLockOwnership(createdEphemeralNode, lw);
                } else if (LOG.isDebugEnabled()) {
                  LOG.debug("[{}] While waiting for another lock {}, {} was deleted", vmLockPrefix,
                      nodeToWatch, createdEphemeralNode);
                }
              } catch (Exception e) {
                if (lockNodeName == null) {
                  // have not acquired lock yet
                  lw.failedToAcquireLock(e);
                }
              }
            }
            renew = false;
          }

          if (event.getState() == KeeperState.Expired
              || event.getState() == KeeperState.Disconnected) {
            synchronized (ZooLock.this) {
              if (lockNodeName == null) {
                LOG.info("Zookeeper Session expired / disconnected");
                lw.failedToAcquireLock(new Exception("Zookeeper Session expired / disconnected"));
              }
            }
            renew = false;
          }
          if (renew) {
            LOG.debug("[{}] Renewing watch on prior node  {}", vmLockPrefix, nodeToWatch);
            try {
              Stat restat = zooKeeper.exists(nodeToWatch, this);
              if (restat == null) {
                // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
                // created a Watcher on a node that does not exist. Delete the watcher we just
                // created.
                zooKeeper.removeWatches(nodeToWatch, this, WatcherType.Any, true);
                determineLockOwnership(createdEphemeralNode, lw);
              }
            } catch (KeeperException | InterruptedException e) {
              lw.failedToAcquireLock(new Exception("Failed to renew watch on other master node"));
            }
          }
        }

      };

      Stat stat = zooKeeper.exists(nodeToWatch, priorNodeWatcher);
      if (stat == null) {
        // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
        // created a Watcher on a node that does not exist. Delete the watcher we just created.
        zooKeeper.removeWatches(nodeToWatch, priorNodeWatcher, WatcherType.Any, true);
        determineLockOwnership(createdEphemeralNode, lw);
      }
    }

  }

  private void lostLock(LockLossReason reason) {
    LockWatcher localLw = lockWatcher;
    lockNodeName = null;
    lockWatcher = null;

    localLw.lostLock(reason);
  }

  public synchronized void lock(final AccumuloLockWatcher lw, byte[] data) {

    if (lockWatcher != null || lockNodeName != null || createdNodeName != null) {
      throw new IllegalStateException();
    }

    lockWasAcquired = false;

    try {
      final String lockPathPrefix = path + "/" + vmLockPrefix.toString();
      // Implement recipe at https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks
      // except that instead of the ephemeral lock node being of the form guid-lock- use lock-guid-.
      // Another deviation from the recipe is that we cleanup any extraneous ephemeral nodes that
      // were created.
      final String createPath =
          zooKeeper.create(lockPathPrefix, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL);
      LOG.debug("[{}] Ephemeral node {} created", vmLockPrefix, createPath);

      // It's possible that the call above was retried several times and multiple ephemeral nodes
      // were created but the client missed the response for some reason. Find the ephemeral nodes
      // with this ZLOCK_UUID and lowest sequential number.
      List<String> children = new ArrayList<>();
      zooKeeper.getChildren(path, null).forEach(s -> {
        if (s.startsWith(ZLOCK_PREFIX)) {
          children.add(s);
        }
      });
      String lowestSequentialPath = null;
      sortChildrenByLockPrefix(children);
      boolean msgLoggedOnce = false;
      for (String child : children) {
        if (child.startsWith(vmLockPrefix.toString())) {
          if (null == lowestSequentialPath) {
            if (createPath.equals(path + "/" + child)) {
              // the path returned from create is the lowest sequential one
              lowestSequentialPath = createPath;
              break;
            }
            lowestSequentialPath = path + "/" + child;
            LOG.debug("[{}] lowest sequential node found: {}", vmLockPrefix, lowestSequentialPath);
          } else {
            if (!msgLoggedOnce) {
              LOG.info(
                  "[{}] Zookeeper client missed server response, multiple ephemeral child nodes created at {}",
                  vmLockPrefix, lockPathPrefix);
              msgLoggedOnce = true;
            }
            LOG.debug("[{}] higher sequential node found: {}, deleting it", vmLockPrefix, child);
            try {
              zooKeeper.delete(path + "/" + child, -1);
            } catch (KeeperException e) {
              // ignore the case where the node doesn't exist
              if (e.code() != Code.NONODE) {
                throw e;
              }
            }
          }
        }
      }
      final String pathForWatcher = lowestSequentialPath;

      // Set a watcher on the lowest sequential node that we created, this handles the case
      // where the node we created is deleted or if this client becomes disconnected.
      LOG.debug("[{}] Setting watcher on {}", vmLockPrefix, pathForWatcher);
      Watcher watcherForNodeWeCreated = new Watcher() {

        private void failedToAcquireLock() {
          LOG.debug("[{}] Lock deleted before acquired, setting createdNodeName {} to null",
              vmLockPrefix, createdNodeName);
          lw.failedToAcquireLock(new Exception("Lock deleted before acquired"));
          createdNodeName = null;
        }

        @Override
        public void process(WatchedEvent event) {
          synchronized (ZooLock.this) {
            if (lockNodeName != null && event.getType() == EventType.NodeDeleted
                && event.getPath().equals(path + "/" + lockNodeName)) {
              LOG.debug("[{}] {} was deleted", vmLockPrefix, lockNodeName);
              lostLock(LockLossReason.LOCK_DELETED);
            } else if (createdNodeName != null && event.getType() == EventType.NodeDeleted
                && event.getPath().equals(path + "/" + createdNodeName)) {
              LOG.debug("[{}] {} was deleted", vmLockPrefix, createdNodeName);
              failedToAcquireLock();
            } else if (event.getState() != KeeperState.Disconnected
                && event.getState() != KeeperState.Expired
                && (lockNodeName != null || createdNodeName != null)) {
              LOG.debug("Unexpected event watching lock node {} {}", event, pathForWatcher);
              try {
                Stat stat2 = zooKeeper.exists(pathForWatcher, this);
                if (stat2 == null) {
                  // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
                  // created a Watcher on a node that does not exist. Delete the watcher we just
                  // created.
                  zooKeeper.removeWatches(pathForWatcher, this, WatcherType.Any, true);

                  if (lockNodeName != null)
                    lostLock(LockLossReason.LOCK_DELETED);
                  else if (createdNodeName != null)
                    failedToAcquireLock();
                }
              } catch (Exception e) {
                lockWatcher.unableToMonitorLockNode(e);
                LOG.error("Failed to stat lock node " + pathForWatcher, e);
              }
            }

          }
        }
      };

      Stat stat = zooKeeper.exists(pathForWatcher, watcherForNodeWeCreated);
      if (stat == null) {
        // if stat is null from the zookeeper.exists(path, Watcher) call, then we just
        // created a Watcher on a node that does not exist. Delete the watcher we just created.
        zooKeeper.removeWatches(pathForWatcher, watcherForNodeWeCreated, WatcherType.Any, true);
        lw.failedToAcquireLock(new Exception("Lock does not exist after create"));
        return;
      }

      createdNodeName = pathForWatcher.substring(path.length() + 1);

      // We have created a node, do we own the lock?
      determineLockOwnership(createdNodeName, lw);

    } catch (KeeperException | InterruptedException e) {
      lw.failedToAcquireLock(e);
    }
  }

  public synchronized boolean tryToCancelAsyncLockOrUnlock()
      throws InterruptedException, KeeperException {
    boolean del = false;

    if (createdNodeName != null) {
      String pathToDelete = path + "/" + createdNodeName;
      LOG.debug("[{}] Deleting all at path {} due to lock cancellation", vmLockPrefix,
          pathToDelete);
      recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);
      del = true;
    }

    if (lockNodeName != null) {
      unlock();
      del = true;
    }

    return del;
  }

  public synchronized void unlock() throws InterruptedException, KeeperException {
    if (lockNodeName == null) {
      throw new IllegalStateException();
    }

    LockWatcher localLw = lockWatcher;
    String localLock = lockNodeName;

    lockNodeName = null;
    lockWatcher = null;

    final String pathToDelete = path + "/" + localLock;
    LOG.debug("[{}] Deleting all at path {} due to unlock", vmLockPrefix, pathToDelete);
    recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);

    localLw.lostLock(LockLossReason.LOCK_DELETED);
  }

  /**
   * @return path of node that this lock is watching
   */
  public synchronized String getWatching() {
    return watchingNodeName;
  }

  public synchronized String getLockPath() {
    if (lockNodeName == null) {
      return null;
    }
    return path + "/" + lockNodeName;
  }

  public synchronized String getLockName() {
    return lockNodeName;
  }

  public synchronized LockID getLockID() {
    if (lockNodeName == null) {
      throw new IllegalStateException("Lock not held");
    }
    return new LockID(path, lockNodeName, zooKeeper.getSessionId());
  }

  /**
   * indicates if the lock was acquired in the past.... helps discriminate between the case where
   * the lock was never held, or held and lost....
   *
   * @return true if the lock was acquired, otherwise false.
   */
  public synchronized boolean wasLockAcquired() {
    return lockWasAcquired;
  }

  public synchronized boolean isLocked() {
    return lockNodeName != null;
  }

  public synchronized void replaceLockData(byte[] b) throws KeeperException, InterruptedException {
    if (getLockPath() != null)
      zooKeeper.setData(getLockPath(), b, -1);
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("event {} {} {}", event.getPath(), event.getType(), event.getState());
    }

    watchingParent = false;

    if (event.getState() == KeeperState.Expired && lockNodeName != null) {
      lostLock(LockLossReason.SESSION_EXPIRED);
    } else {

      try { // set the watch on the parent node again
        zooKeeper.exists(path, this);
        watchingParent = true;
      } catch (KeeperException.ConnectionLossException ex) {
        // we can't look at the lock because we aren't connected, but our session is still good
        LOG.warn("lost connection to zookeeper");
      } catch (Exception ex) {
        if (lockNodeName != null || createdNodeName != null) {
          lockWatcher.unableToMonitorLockNode(ex);
          LOG.error("Error resetting watch on ZooLock {} {}",
              lockNodeName != null ? lockNodeName : createdNodeName, event, ex);
        }
      }

    }

  }

  public static boolean isLockHeld(ZooCache zc, LockID lid) {

    List<String> children = zc.getChildren(lid.path);

    if (children == null || children.isEmpty()) {
      return false;
    }

    children = new ArrayList<>(children);

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);
    if (!lid.node.equals(lockNode))
      return false;

    ZcStat stat = new ZcStat();
    return zc.get(lid.path + "/" + lid.node, stat) != null && stat.getEphemeralOwner() == lid.eid;
  }

  public static byte[] getLockData(ZooKeeper zk, String path)
      throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, false);

    if (children == null || children.isEmpty()) {
      return null;
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    return zk.getData(path + "/" + lockNode, false, null);
  }

  public static byte[] getLockData(org.apache.accumulo.fate.zookeeper.ZooCache zc, String path,
      ZcStat stat) {

    List<String> children = zc.getChildren(path);

    if (children == null || children.isEmpty()) {
      return null;
    }

    children = new ArrayList<>(children);
    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    return zc.get(path + "/" + lockNode, stat);
  }

  public static long getSessionId(ZooCache zc, String path) {
    List<String> children = zc.getChildren(path);

    if (children == null || children.isEmpty()) {
      return 0;
    }

    children = new ArrayList<>(children);
    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    ZcStat stat = new ZcStat();
    if (zc.get(path + "/" + lockNode, stat) != null)
      return stat.getEphemeralOwner();
    return 0;
  }

  public long getSessionId() throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(path, null);

    if (children == null || children.isEmpty()) {
      return 0;
    }

    children = new ArrayList<>(children);
    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    Stat stat = zooKeeper.exists(path + "/" + lockNode, null);
    if (null != stat) {
      return stat.getEphemeralOwner();
    } else {
      return 0;
    }
  }

  public static void deleteLock(ZooReaderWriter zk, String path)
      throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.isEmpty()) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    String pathToDelete = path + "/" + lockNode;
    LOG.debug("Deleting all at path {} due to lock deletion", pathToDelete);
    zk.recursiveDelete(pathToDelete, NodeMissingPolicy.SKIP);

  }

  public static boolean deleteLock(ZooReaderWriter zk, String path, String lockData)
      throws InterruptedException, KeeperException {
    List<String> children;

    children = zk.getChildren(path);

    if (children == null || children.isEmpty()) {
      throw new IllegalStateException("No lock is held at " + path);
    }

    sortChildrenByLockPrefix(children);

    String lockNode = children.get(0);

    if (!lockNode.startsWith(ZLOCK_PREFIX)) {
      throw new RuntimeException("Node " + lockNode + " at " + path + " is not a lock node");
    }

    byte[] data = zk.getData(path + "/" + lockNode);

    if (lockData.equals(new String(data, UTF_8))) {
      String pathToDelete = path + "/" + lockNode;
      LOG.debug("Deleting all at path {} due to lock deletion", pathToDelete);
      zk.recursiveDelete(pathToDelete, NodeMissingPolicy.FAIL);
      return true;
    }

    return false;
  }
}
