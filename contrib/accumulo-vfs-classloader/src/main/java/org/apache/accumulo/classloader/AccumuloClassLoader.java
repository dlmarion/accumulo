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
 * <b>general.classpaths</b> (Deprecated) if specified. Next, this class will create a
 * {@link ReloadingVFSClassLoader} that is configured to load the resources at
 * <b>general.vfs.classpaths</b>, if specified, or <b>general.dynamic.classpaths</b>, if specified.
 * If neither of these properties are specified the {@link ReloadingVFSClassLoader} uses the default
 * path of $ACCUMULO_HOME/lib/ext.
 *
 */
@Deprecated
@SuppressFBWarnings(value = "DM_EXIT",
    justification = "classloader should exit VM if not configured correctly")
public class AccumuloClassLoader extends ReloadingVFSClassLoader {

  public static final String GENERAL_CLASSPATHS = "general.classpaths";
  public static final String DYNAMIC_CLASSPATH_PROPERTY_NAME = "general.dynamic.classpaths";
  public static final String VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY = "general.vfs.classpaths";
  public static final String DEFAULT_DYNAMIC_CLASSPATH_VALUE = "$ACCUMULO_HOME/lib/ext";

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloClassLoader.class);
  private static URL ACCUMULO_CONFIG_URL;
  private static URLClassLoader GENERAL_CLASSPATH_LOADER;
  private static String NAME = null;
  private static String CLASSPATH = null;

  static {
    ClassLoader.registerAsParallelCapable();

    findAccumuloConfig();

    createGeneralClasspathClassLoader();

    try {
      GENERAL_CLASSPATH_LOADER
          .loadClass("org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader");
    } catch (ClassNotFoundException | IllegalArgumentException | SecurityException e1) {
      LOG.error("Problem initializing the VFS class loader", e1);
      System.err.println(
          "Problem initializing ReloadingVFSClassLoader, something is wrong with the classpath");
      // System.exit(1);
    }

    getDynamicClassLoaderClasspath();
  }

  private static void findAccumuloConfig() {
    String configFile = System.getProperty("accumulo.properties", "accumulo.properties");
    if (configFile.startsWith("file://")) {
      try {
        File f = new File(new URI(configFile));
        if (f.exists() && !f.isDirectory()) {
          ACCUMULO_CONFIG_URL = f.toURI().toURL();
        } else {
          LOG.warn("Failed to load Accumulo configuration from {}", configFile);
        }
      } catch (URISyntaxException | MalformedURLException e) {
        LOG.warn("Failed to load Accumulo configuration from {}", configFile, e);
      }
    } else {
      ACCUMULO_CONFIG_URL = AccumuloClassLoader.class.getResource(configFile);
      if (ACCUMULO_CONFIG_URL == null)
        LOG.warn("Failed to load Accumulo configuration '{}' from classpath", configFile);
    }
    if (ACCUMULO_CONFIG_URL != null) {
      LOG.info("Using Accumulo configuration at {}", ACCUMULO_CONFIG_URL.getFile());
    }
  }

  /**
   * Reset the general classpath classloader and dynamic classloader classpath after
   * ACCUMULO_CONFIG_URL is set manually by a test
   */
  public static void resetForTests() {
    createGeneralClasspathClassLoader();
    getDynamicClassLoaderClasspath();
  }

  private static void createGeneralClasspathClassLoader() {
    // Handle Deprecated GENERAL_CLASSPATHS property
    ArrayList<URL> urls = new ArrayList<>();
    try {
      ArrayList<URL> genClassPathUrls = findAccumuloURLs();
      if (null != genClassPathUrls) {
        urls.addAll(genClassPathUrls);
      }
    } catch (IOException e) {
      LOG.warn("IOException processing {} property: {}", sanitize(GENERAL_CLASSPATHS),
          e.getMessage());
    }
    URL[] classpath = urls.toArray(new URL[urls.size()]);
    GENERAL_CLASSPATH_LOADER = new URLClassLoader("AccumuloClassLoader", classpath,
        AccumuloClassLoader.class.getClassLoader());
  }

  private static void getDynamicClassLoaderClasspath() {
    String vfsDynamicClassPath =
        AccumuloClassLoader.getAccumuloProperty(VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY, "");
    if (null != vfsDynamicClassPath && !vfsDynamicClassPath.isBlank()) {
      vfsDynamicClassPath = replaceEnvVars(vfsDynamicClassPath, System.getenv());
      NAME = "VFS System";
      CLASSPATH = vfsDynamicClassPath;
    } else {
      String dynamicClassPath = AccumuloClassLoader
          .getAccumuloProperty(DYNAMIC_CLASSPATH_PROPERTY_NAME, DEFAULT_DYNAMIC_CLASSPATH_VALUE);
      dynamicClassPath = replaceEnvVars(dynamicClassPath, System.getenv());
      NAME = "Default Dynamic";
      CLASSPATH = dynamicClassPath;
    }

  }

  /**
   * Prevent potential CRLF injection into logs from read in user data See
   * https://find-sec-bugs.github.io/bugs.htm#CRLF_INJECTION_LOGS
   */
  private static String sanitize(String msg) {
    if (null == msg) {
      return "null";
    }
    return msg.replaceAll("[\r\n]", "");
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
    if (ACCUMULO_CONFIG_URL == null) {
      LOG.warn(
          "Using default value '{}' for '{}' as there is no Accumulo configuration on classpath",
          sanitize(defaultValue), sanitize(propertyName));
      return defaultValue;
    }
    try (InputStream is =
        Files.newInputStream(Paths.get(ACCUMULO_CONFIG_URL.toURI()), StandardOpenOption.READ)) {
      // Not using FileBasedConfigurationBuilder here as we would need commons-configuration
      // on the classpath
      Properties config = new Properties();
      config.load(is);
      return config.getProperty(propertyName, defaultValue);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to look up property " + propertyName + " in " + ACCUMULO_CONFIG_URL.getFile(), e);
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
            LOG.debug("ignoring classpath entry {}", sanitize(classpath));
          }
        } else {
          LOG.debug("ignoring classpath entry {}", sanitize(classpath));
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
    LOG.warn("'{}' is deprecated but was set to '{}' ", sanitize(GENERAL_CLASSPATHS), sanitize(cp));
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }

  public AccumuloClassLoader(ClassLoader parent) throws IOException {
    super(NAME, GENERAL_CLASSPATH_LOADER, CLASSPATH, true, AccumuloVFSManager.generateVfs());
  }

  public void close() {
    try {
      GENERAL_CLASSPATH_LOADER.close();
      GENERAL_CLASSPATH_LOADER = null;
    } catch (IOException e) {
      LOG.error("Error closing URLClassLoader parent", e);
    }
    super.close();
  }

}
