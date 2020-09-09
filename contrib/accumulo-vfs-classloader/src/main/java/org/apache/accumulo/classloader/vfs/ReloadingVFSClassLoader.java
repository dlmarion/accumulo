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
package org.apache.accumulo.classloader.vfs;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.classloader.ClassLoaderDescription;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classloader that delegates operations to a VFSClassLoader object. This class also listens for
 * changes in any of the files/directories that are in the classpath and will recreate the delegate
 * object if there is any change in the classpath.
 * 
 * The filesystem monitor will look for changes in the classpath at 5 minute interval, unless the
 * system property <b>general.vfs.classpath.monitor.seconds</b> is defined.
 */
@Deprecated
public class ReloadingVFSClassLoader extends ClassLoader
    implements ClassLoaderDescription, Closeable, FileListener {

  public static final String VFS_CLASSPATH_MONITOR_INTERVAL =
      "general.vfs.classpath.monitor.seconds";

  private static final Logger LOG = LoggerFactory.getLogger(ReloadingVFSClassLoader.class);

  // set to 5 mins. The rationale behind this large time is to avoid a gazillion tservers all asking
  // the name node for info too frequently.
  private static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  private volatile long maxWaitInterval = 60000;
  private volatile long maxRetries = -1;
  private volatile long sleepInterval = 5000;

  private final DefaultFileMonitor monitor;
  private final ThreadPoolExecutor executor;
  private FileObject[] files;
  private String uris;
  protected final ClassLoader parent;
  private final boolean preDelegate;
  private VFSClassLoaderWrapper cl = null;
  private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock(true);
  private final String name;
  private final String description;

  {
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(2);
    ThreadFactory factory = r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    };
    executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, queue, factory);
  }

  private static long getMonitorInterval() {
    String interval = System.getProperty(VFS_CLASSPATH_MONITOR_INTERVAL);
    if (null != interval && !interval.isBlank()) {
      try {
        return TimeUnit.SECONDS.toMillis(Long.valueOf(interval));
      } catch (NumberFormatException e) {
        return DEFAULT_TIMEOUT;
      }
    }
    return DEFAULT_TIMEOUT;
  }

  private final Runnable refresher = new Runnable() {
    @Override
    public void run() {
      while (!executor.isTerminating()) {
        try {
          FileSystemManager vfs = AccumuloVFSManager.generateVfs();
          FileObject[] files = AccumuloVFSManager.resolve(vfs, uris);

          long retries = 0;
          long currentSleepMillis = sleepInterval;

          if (files.length == 0) {
            while (files.length == 0 && retryPermitted(retries)) {

              try {
                LOG.debug("VFS path was empty.  Waiting " + currentSleepMillis + " ms to retry");
                Thread.sleep(currentSleepMillis);

                files = AccumuloVFSManager.resolve(vfs, uris);
                retries++;

                currentSleepMillis = Math.min(maxWaitInterval, currentSleepMillis + sleepInterval);

              } catch (InterruptedException e) {
                LOG.error("VFS Retry Interruped", e);
                throw new RuntimeException(e);
              }

            }

            // There is a chance that the listener was removed from the top level directory or
            // its children if they were deleted within some time window. Re-add files to be
            // monitored. The Monitor will ignore files that are already/still being monitored.
            // forEachCatchRTEs will capture a stream of thrown exceptions.
            // and can collect them to list or reduce into one exception

            forEachCatchRTEs(Arrays.stream(files), o -> {
              addFileToMonitor(o);
              LOG.debug("monitoring {}", o);
            });
          }

          LOG.debug("Rebuilding dynamic classloader using files- {}", stringify(files));

          VFSClassLoaderWrapper cl;
          if (preDelegate)
            // This is the normal classloader parent delegation model
            cl = new VFSClassLoaderWrapper(files, vfs, parent);
          else
            // This delegates to the parent after we lookup locally first.
            cl = new VFSClassLoaderWrapper(files, vfs) {
              @Override
              protected synchronized Class<?> loadClass(String name, boolean resolve)
                  throws ClassNotFoundException {
                Class<?> c = findLoadedClass(name);
                if (c != null)
                  return c;
                try {
                  // try finding this class here instead of parent
                  return findClass(name);
                } catch (ClassNotFoundException e) {

                }
                return super.loadClass(name, resolve);
              }
            };
          updateClassloader(files, cl);
          return;
        } catch (Exception e) {
          LOG.error("{}", e.getMessage(), e);
          try {
            Thread.sleep(getMonitorInterval());
          } catch (InterruptedException ie) {
            LOG.error("{}", ie.getMessage(), ie);
          }
        }
      }
    }
  };

  public ReloadingVFSClassLoader(String name, String description, ClassLoader parent,
      String classpath, boolean preDelegation, FileSystemManager vfs) throws IOException {
    super(name, parent);
    this.name = name;
    this.description = description;
    this.parent = parent;
    this.uris = classpath;

    if (!preDelegation) {
      this.preDelegate = false;
    } else {
      this.preDelegate = true;
    }

    ArrayList<FileObject> pathsToMonitor = new ArrayList<>();
    this.files = AccumuloVFSManager.resolve(vfs, uris, pathsToMonitor);
    this.cl = new VFSClassLoaderWrapper(files, vfs, parent);

    // An HDFS FileSystem and Configuration object were created for each unique HDFS namespace
    // in the call to resolve above. The HDFS Client did us a favor and cached these objects
    // so that the next time someone calls FileSystem.get(uri), they get the cached object.
    // However, these objects were created not with the system VFS classloader, but the
    // classloader above it. We need to override the classloader on the Configuration objects.
    // Ran into an issue were log recovery was being attempted and SequenceFile$Reader was
    // trying to instantiate the key class via WritableName.getClass(String, Configuration)
    for (FileObject fo : this.files) {
      if (fo instanceof HdfsFileObject) {
        String uri = fo.getName().getRootURI();
        Configuration c = new Configuration(true);
        c.set(FileSystem.FS_DEFAULT_NAME_KEY, uri);
        FileSystem fs = FileSystem.get(c);
        fs.getConf().setClassLoader(this.cl);
      }
    }

    this.monitor = new DefaultFileMonitor(this);
    monitor.setDelay(getMonitorInterval());
    monitor.setRecursive(false);

    forEachCatchRTEs(pathsToMonitor.stream(), o -> {
      addFileToMonitor(o);
      LOG.debug("monitoring {}", o);
    });
    monitor.start();
  }

  private void addFileToMonitor(FileObject file) throws RuntimeException {
    try {
      if (monitor != null)
        monitor.addFile(file);
    } catch (RuntimeException re) {
      if (re.getMessage().contains("files-cache"))
        LOG.error("files-cache error adding {} to VFS monitor. "
            + "There is no implementation for files-cache in VFS2", file, re);
      else
        LOG.error("Runtime error adding {} to VFS monitor", file, re);

      throw re;
    }
  }

  private synchronized void updateClassloader(FileObject[] files, VFSClassLoaderWrapper cl) {
    this.files = files;
    try {
      updateLock.writeLock().lock();
      this.cl = cl;
    } finally {
      updateLock.writeLock().unlock();
    }
  }

  private void removeFile(FileObject file) throws RuntimeException {
    try {
      if (monitor != null)
        monitor.removeFile(file);
    } catch (RuntimeException re) {
      LOG.error("Error removing file from VFS cache {}", file, re);
      throw re;
    }
  }

  @Override
  public void fileCreated(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} created, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  @Override
  public void fileDeleted(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} deleted, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  @Override
  public void fileChanged(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} changed, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  private void scheduleRefresh() {
    try {
      executor.execute(refresher);
    } catch (RejectedExecutionException e) {
      LOG.trace("Ignoring refresh request (already refreshing)");
    }
  }

  @Override
  public void close() {

    forEachCatchRTEs(Stream.of(files), o -> {
      removeFile(o);
      LOG.debug("Removing file from monitoring {}", o);
    });

    executor.shutdownNow();
    monitor.stop();

  }

  public static <T> void forEachCatchRTEs(Stream<T> stream, Consumer<T> consumer) {
    stream.flatMap(o -> {
      try {
        consumer.accept(o);
        return null;
      } catch (RuntimeException e) {
        return Stream.of(e);
      }
    }).reduce((e1, e2) -> {
      e1.addSuppressed(e2);
      return e1;
    }).ifPresent(e -> {
      throw e;
    });
  }

  // VisibleForTesting intentionally not using annotation from Guava
  // because it adds unwanted dependency
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  private boolean retryPermitted(long retries) {
    return (maxRetries < 0 || retries < maxRetries);
  }

  public String stringify(FileObject[] files) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    String delim = "";
    for (FileObject file : files) {
      sb.append(delim);
      delim = ", ";
      sb.append(file.getName());
    }
    sb.append(']');
    return sb.toString();
  }

  private VFSClassLoaderWrapper getClassLoader() {
    try {
      updateLock.readLock().lock();
      return cl;
    } finally {
      updateLock.readLock().unlock();
    }
  }

  public VFSClassLoaderWrapper getWrapper() {
    return getClassLoader();
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    return getClassLoader().findClass(name);
  }

  @Override
  public URL findResource(String name) {
    return getClassLoader().findResource(name);
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    return getClassLoader().findResources(name);
  }

  @Override
  public int hashCode() {
    return getClassLoader().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return getClassLoader().equals(obj);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    for (FileObject f : files) {
      try {
        buf.append("\t").append(f.getURL()).append("\n");
      } catch (FileSystemException e) {
        LOG.error("Error getting URL for file", e);
      }
    }
    return buf.toString();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return getClassLoader().loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return getClassLoader().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return getClassLoader().getResources(name);
  }

  @Override
  public Stream<URL> resources(String name) {
    return getClassLoader().resources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return getClassLoader().getResourceAsStream(name);
  }

  @Override
  public void setDefaultAssertionStatus(boolean enabled) {
    getClassLoader().setDefaultAssertionStatus(enabled);
  }

  @Override
  public void setPackageAssertionStatus(String packageName, boolean enabled) {
    getClassLoader().setPackageAssertionStatus(packageName, enabled);
  }

  @Override
  public void setClassAssertionStatus(String className, boolean enabled) {
    getClassLoader().setClassAssertionStatus(className, enabled);
  }

  @Override
  public void clearAssertionStatus() {
    getClassLoader().clearAssertionStatus();
  }

  @Override
  public String getDescription() {
    return this.description;
  }

}
