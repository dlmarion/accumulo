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
package org.apache.accumulo.classloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.vfs.AccumuloVFSManager;
import org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader;
import org.apache.commons.vfs2.FileSystemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * <p>
 * ClassLoader that implements the approach documented in
 * <a href="https://accumulo.apache.org/blog/2014/05/03/accumulo-classloader.html">The Accumulo
 * ClassLoader</a>
 * 
 * <p>
 * This classloader can be used as the <a href=
 * "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/ClassLoader.html#getSystemClassLoader()">JVM
 * System ClassLoader</a> by setting the system property <b>java.system.class.loader</b> to the name
 * of this class.
 * 
 * <p>
 * This class first creates a classloader that is configured to load resources from
 * <b>java.class.path</b> and <b>general.classpaths</b> (Deprecated) if specified. Next, this class
 * will create a {@link ReloadingVFSClassLoader} that is configured to load the resources at
 * <b>general.vfs.classpaths</b>, if specified, or <b>general.dynamic.classpaths</b>, if specified.
 * If neither of these properties are specified the {@link ReloadingVFSClassLoader} uses the default
 * path of $ACCUMULO_HOME/lib/ext.
 *
 */
@Deprecated
public class AccumuloClassLoader extends URLClassLoader implements ClassLoaderDescription {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloClassLoader.class);
  public static final String GENERAL_CLASSPATHS = "general.classpaths";
  public static final String DYNAMIC_CLASSPATH_PROPERTY_NAME = "general.dynamic.classpaths";
  public static final String VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY = "general.vfs.classpaths";
  public static final String DEFAULT_DYNAMIC_CLASSPATH_VALUE = "$ACCUMULO_HOME/lib/ext";

  private static URL accumuloConfigUrl;
  private static URL[] classpath = null;
  private String description;

  static {
    ClassLoader.registerAsParallelCapable();

    String cp = System.getProperty("java.class.path");
    String[] cpElements = cp.split(File.pathSeparator);
    ArrayList<URL> urls = new ArrayList<>();
    for (String element : cpElements) {
      if (null != element && !element.isEmpty()) {
        try {
          urls.add(new URL("file", "", new File(element).getCanonicalFile().getAbsolutePath()));
        } catch (IOException e) {
          LOG.error("Error adding classpath element {}, error: {}", element, e.getMessage());
        }
      }
    }

    String configFile = System.getProperty("accumulo.properties", "accumulo.properties");
    if (configFile.startsWith("file://")) {
      try {
        File f = new File(new URI(configFile));
        if (f.exists() && !f.isDirectory()) {
          accumuloConfigUrl = f.toURI().toURL();
        } else {
          LOG.warn("Failed to load Accumulo configuration from " + configFile);
        }
      } catch (URISyntaxException | MalformedURLException e) {
        LOG.warn("Failed to load Accumulo configuration from " + configFile, e);
      }
    } else {
      accumuloConfigUrl = AccumuloClassLoader.class.getResource(configFile);
      if (accumuloConfigUrl == null)
        LOG.warn("Failed to load Accumulo configuration '{}' from classpath", configFile);
    }
    if (accumuloConfigUrl != null) {
      LOG.info("Using Accumulo configuration at {}", accumuloConfigUrl.getFile());
    }

    // Handle Deprecated GENERAL_CLASSPATHS property
    try {
      ArrayList<URL> genClassPathUrls = findAccumuloURLs();
      if (null != genClassPathUrls) {
        urls.addAll(genClassPathUrls);
      }
    } catch (IOException e) {
      LOG.warn("IOException processing {} property: {}", GENERAL_CLASSPATHS, e.getMessage());
    }
    classpath = urls.toArray(new URL[urls.size()]);
  }

  public AccumuloClassLoader(ClassLoader parent) throws IOException {
    super("AccumuloClassLoader", classpath, parent);
    this.setDescription("loads classes from java.class.path and general.classpaths");
    Thread.currentThread().setContextClassLoader(this);

    try {
      this.loadClass("org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader");
    } catch (ClassNotFoundException | IllegalArgumentException | SecurityException e1) {
      LOG.error("Problem initializing the VFS class loader", e1);
      System.exit(1);
    }

    FileSystemManager vfs = AccumuloVFSManager.generateVfs();

    ReloadingVFSClassLoader dynamicClassLoader = null;
    String vfsDynamicClassPath =
        AccumuloClassLoader.getAccumuloProperty(VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY, "");
    if (null != vfsDynamicClassPath && !vfsDynamicClassPath.isBlank()) {
      vfsDynamicClassPath = replaceEnvVars(vfsDynamicClassPath, System.getenv());
      dynamicClassLoader = new ReloadingVFSClassLoader("VFS system",
          "loads classes from general.vfs.classpaths", this, vfsDynamicClassPath, true, vfs);
    } else {
      String dynamicClassPath = AccumuloClassLoader
          .getAccumuloProperty(DYNAMIC_CLASSPATH_PROPERTY_NAME, DEFAULT_DYNAMIC_CLASSPATH_VALUE);
      dynamicClassPath = replaceEnvVars(dynamicClassPath, System.getenv());
      dynamicClassLoader = new ReloadingVFSClassLoader("default dynamic",
          "loads classes from general.dynamic.classpaths", this, dynamicClassPath, true, vfs);
    }
    Thread.currentThread().setContextClassLoader(dynamicClassLoader);

  }

  /**
   * Returns value of property in accumulo.properties file, otherwise default value
   *
   * @param propertyName
   *          Name of the property to pull
   * @param defaultValue
   *          Value to default to if not found.
   * @return value of property or default
   */
  public static String getAccumuloProperty(String propertyName, String defaultValue) {
    if (accumuloConfigUrl == null) {
      LOG.warn(
          "Using default value '{}' for '{}' as there is no Accumulo configuration on classpath",
          defaultValue, propertyName);
      return defaultValue;
    }
    try (InputStream is =
        Files.newInputStream(Paths.get(accumuloConfigUrl.toURI()), StandardOpenOption.READ)) {
      // Not using FileBasedConfigurationBuilder here as we would need commons-configuration
      // on the classpath
      Properties config = new Properties();
      config.load(is);
      return config.getProperty(propertyName, defaultValue);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to look up property " + propertyName + " in " + accumuloConfigUrl.getFile(), e);
    }
  }

  /**
   * Replace environment variables in a string with their actual value
   */
  public static String replaceEnvVars(String input, Map<String,String> env) {
    Pattern envPat = Pattern.compile("\\$[A-Za-z][a-zA-Z0-9_]*");
    Matcher envMatcher = envPat.matcher(input);
    while (envMatcher.find(0)) {
      // name comes after the '$'
      String varName = envMatcher.group().substring(1);
      String varValue = env.get(varName);
      if (varValue == null) {
        varValue = "";
      }
      input =
          (input.substring(0, envMatcher.start()) + varValue + input.substring(envMatcher.end()));
      envMatcher.reset(input);
    }
    return input;
  }

  /**
   * Populate the list of URLs with the items in the classpath string
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "class path configuration is controlled by admin, not unchecked user input")
  private static void addUrl(String classpath, ArrayList<URL> urls) throws MalformedURLException {
    classpath = classpath.trim();
    if (classpath.isEmpty())
      return;

    classpath = replaceEnvVars(classpath, System.getenv());

    // Try to make a URI out of the classpath
    URI uri = null;
    try {
      uri = new URI(classpath);
    } catch (URISyntaxException e) {
      // Not a valid URI
    }

    if (uri == null || !uri.isAbsolute()
        || (uri.getScheme() != null && uri.getScheme().equals("file://"))) {
      // Then treat this URI as a File.
      // This checks to see if the url string is a dir if it expand and get all jars in that
      // directory
      final File extDir = new File(classpath);
      if (extDir.isDirectory())
        urls.add(extDir.toURI().toURL());
      else {
        if (extDir.getParentFile() != null) {
          File[] extJars =
              extDir.getParentFile().listFiles((dir, name) -> name.matches("^" + extDir.getName()));
          if (extJars != null && extJars.length > 0) {
            for (File jar : extJars)
              urls.add(jar.toURI().toURL());
          } else {
            LOG.debug("ignoring classpath entry {}", classpath);
          }
        } else {
          LOG.debug("ignoring classpath entry {}", classpath);
        }
      }
    } else {
      urls.add(uri.toURL());
    }

  }

  private static ArrayList<URL> findAccumuloURLs() throws IOException {
    String cp = getAccumuloProperty(GENERAL_CLASSPATHS, null);
    if (cp == null)
      return new ArrayList<>();
    LOG.warn("'{}' is deprecated but was set to '{}' ", GENERAL_CLASSPATHS, cp);
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }

  private void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String getDescription() {
    return this.description;
  }
}
