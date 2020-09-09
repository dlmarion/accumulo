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
package org.apache.accumulo.core.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.classloader.vfs.AccumuloVFSManager;
import org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.commons.vfs2.FileSystemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ContextClassLoaderFactory.class);

  @Deprecated
  public static final String VFS_CONTEXT_CLASSPATH_PROPERTY = "general.vfs.context.classpath.";
  public static final String CONTEXT_PREFIX = "general.classpath.context.";

  private static final Map<String,ClassLoader> CONTEXTS = new ConcurrentHashMap<>();

  public static ClassLoader getClassLoader(String contextName) {
    if (CONTEXTS.containsKey(contextName)) {
      return CONTEXTS.get(contextName);
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return CONTEXTS.toString();
  }

  private static Map<String,Map<String,String>> parseProperties(Map<String,String> props) {
    Map<String,Map<String,String>> contextProperties = new HashMap<>();
    // Run through and get the context names and create the subproperties map
    props.forEach((k, v) -> {
      if (!k.contains(".")) {
        contextProperties.put(k, new HashMap<String,String>());
        contextProperties.get(k).put("FACTORY", v);
      }
    });
    // Run through again and populate the subproperties
    props.forEach((k, v) -> {
      if (k.contains(".")) {
        String[] s = k.split("\\.");
        var contextName = s[0];
        var propName = s[1];
        contextProperties.get(contextName).put(propName, v);
      }
    });
    return contextProperties;
  }

  @SuppressWarnings("deprecation")
  public static void createContexts(AccumuloConfiguration conf) throws Exception {
    var contextProperties = parseProperties(
        conf.getAllPropertiesWithPrefixStripped(Property.GENERAL_CLASSPATH_CONTEXT));
    for (var e : contextProperties.entrySet()) {
      if (CONTEXTS.containsKey(e.getKey())) {
        continue;
      }
      var properties = e.getValue();
      var factoryName = properties.get("FACTORY");
      LOG.debug("context: {} uses ClassLoaderFactory: {}", e.getKey(), factoryName);
      try {
        var factoryClass = Class.forName(factoryName);
        if (ClassLoaderFactory.class.isAssignableFrom(factoryClass)) {
          @SuppressWarnings("unchecked")
          ClassLoaderFactory clf = ((Class<? extends ClassLoaderFactory>) factoryClass)
              .getDeclaredConstructor().newInstance();
          ClassLoader cl = clf.create(properties);
          CONTEXTS.put(e.getKey(), cl);
        }
      } catch (ClassNotFoundException e1) {
        LOG.error(
            "Unable to load and initialize class: {}. Ensure that the jar containing the ContextClassLoader is on the classpath",
            e.getValue());
        throw e1;
      }
    }

    // Handle deprecated VFS context classloader property
    var vfsContexts =
        conf.getAllPropertiesWithPrefixStripped(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    if (null != vfsContexts && !vfsContexts.isEmpty()) {
      FileSystemManager vfs = AccumuloVFSManager.generateVfs();
      for (var e : vfsContexts.entrySet()) {
        if (CONTEXTS.containsKey(e.getKey())) {
          continue;
        }
        LOG.debug("VFS context: {} uses classpath: {}", e.getKey(), e.getValue());
        String delegation = System.getProperty(
            Property.VFS_CONTEXT_CLASSPATH_PROPERTY.toString() + e.getKey() + ".delegation", "");
        boolean preDelegation = (delegation.equals("post")) ? false : true;
        ClassLoader cl = new ReloadingVFSClassLoader(e.getKey() + " context",
            e.getKey() + " Context Classloader", Thread.currentThread().getContextClassLoader(),
            e.getValue(), preDelegation, vfs);
        CONTEXTS.put(e.getKey(), cl);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static void removeUnusedContexts(AccumuloConfiguration config) throws IOException {

    var contextProperties = parseProperties(
        config.getAllPropertiesWithPrefixStripped(Property.GENERAL_CLASSPATH_CONTEXT)).keySet();
    var vfsContextProperties =
        config.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY).keySet();
    var configuredContexts = new HashSet<>();
    configuredContexts.addAll(contextProperties);
    for (String prop : vfsContextProperties) {
      configuredContexts
          .add(prop.substring(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.name().length()));
    }

    Map<String,ClassLoader> unused = new HashMap<>(CONTEXTS);
    // ContextManager knows of some set of contexts. This method will be called with
    // the set of currently configured contexts. We will close the contexts that are
    // no longer in the configuration.
    unused.keySet().removeAll(configuredContexts);
    CONTEXTS.keySet().removeAll(unused.keySet());

    for (Entry<String,ClassLoader> e : unused.entrySet()) {
      if (e.getValue() instanceof Closeable) {
        LOG.info("Closing unused context: {}", e.getKey());
        ((Closeable) e.getValue()).close();
      }
    }
  }

  public static void updateContexts(AccumuloConfiguration config) throws Exception {
    createContexts(config);
    removeUnusedContexts(config);
  }

}
