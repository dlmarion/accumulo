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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.Property;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classloader that delegates operations to a VFSClassLoader object. This class also listens for
 * changes in any of the files/directories that are in the classpath and will recreate the delegate
 * object if there is any change in the classpath.
 */
public class AccumuloReloadingVFSClassLoader {

  private static final Logger log = LoggerFactory.getLogger(AccumuloReloadingVFSClassLoader.class);

  private FileObject[] files;
  private VFSClassLoader cl;
  private final ReloadingClassLoader parent;
  private final String uris;
  private final boolean preDelegate;

  public AccumuloReloadingVFSClassLoader() {
    String prop = AccumuloVFSManager.VFS_CONTEXT_CLASSPATH_PROPERTY + context;
    Map<String,String> props = getConfiguration().getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);

    String uris = props.get(prop);

    if (uris == null) {
      throw new IllegalStateException("Msg HERE");
    }

    String delegate = props.getOrDefault(prop + ".delegation", "");

    if (delegate.trim().equalsIgnoreCase("post")) {
      preDelegate = false;
    } else {
      preDelegate = true;
    }
    
    this.uris = uris;
    this.parent = AccumuloVFSManager.getClassLoader();
    this.monitor = new DefaultFileMonitor(this);

    create(uris, vfs, parent, DEFAULT_TIMEOUT, preDelegate);
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs,
      final ReloadingClassLoader parent, boolean preDelegate) throws FileSystemException {
    this(uris, vfs, parent, DEFAULT_TIMEOUT, preDelegate);
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs,
      ReloadingClassLoader parent, long monitorDelay, boolean preDelegate)
      throws FileSystemException {
    this.uris = uris;
    this.parent = parent;
    this.preDelegate = preDelegate;
    create(uris, vfs, parent, monitorDelay, preDelegate);
  }

    public void create(String uris, FileSystemManager vfs,
        ReloadingClassLoader parent, long monitorDelay, boolean preDelegate)
        throws FileSystemException {


    if (preDelegate)
      cl = new VFSClassLoader(files, vfs, parent.getClassLoader());
    else
      cl = new PostDelegatingVFSClassLoader(files, vfs, parent.getClassLoader());

  }

  /**
   * Should be ok if this is not called because the thread started by DefaultFileMonitor is a daemon
   * thread
   */
  public void close() {
    super.close();
  }


  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    for (FileObject f : files) {
      try {
        buf.append("\t").append(f.getURL()).append("\n");
      } catch (FileSystemException e) {
        log.error("Error getting URL for file", e);
      }
    }

    return buf.toString();
  }

}
