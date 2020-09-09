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

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLClassLoaderFactory implements ClassLoaderFactory {

  /**
   * Will be defined in the Accumulo configuration prefixed with
   * "general.classpath.context.&lt;contextName&gt;.", which will be stripped from the property name
   * when passed to the create method. This property will contain a comma separated list of
   * classpath entries.
   */
  private static final String CP = "classpath";

  private static final String COLON = ",";
  private static final Logger LOG = LoggerFactory.getLogger(URLClassLoaderFactory.class);

  @Override
  public ClassLoader create(Map<String,String> properties) {
    var classpath = properties.getOrDefault(CP, null);
    if (null == classpath) {
      throw new RuntimeException("classpath property must be set for URLClassLoaderFactory");
    }
    var parts = classpath.split(COLON);
    var urls = new ArrayList<URL>();
    for (String p : parts) {
      try {
        urls.add(new URL(p));
      } catch (MalformedURLException e) {
        LOG.error("Error creating URL from classpath segment: " + p);
        throw new RuntimeException("Error creating URL from classpath segment: " + p, e);
      }
    }
    return URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));
  }

}
