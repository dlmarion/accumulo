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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.net.URLClassLoader;

import org.apache.accumulo.classloader.AccumuloClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
@RunWith(PowerMockRunner.class)
@PrepareForTest(AccumuloVFSManager.class)
@SuppressStaticInitializationFor({"org.apache.accumulo.start.classloader.AccumuloClassLoader",
    "org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.apache.log4j.*", "org.apache.hadoop.log.metrics",
    "org.apache.commons.logging.*", "org.xml.*", "javax.management.*", "javax.xml.*",
    "org.w3c.dom.*", "org.apache.hadoop.*", "com.sun.org.apache.xerces.*"})
public class AccumuloVFSClassLoaderTest {

  @Rule
  public TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  /*
   * Test that the default (empty dynamic class paths) does not create the 2nd level loader
   */
  @Test
  public void testDefaultConfig() throws Exception {

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "ACCUMULO_CONFIG_URL",
        conf.toURI().toURL());
    AccumuloClassLoader.resetForTests();
    ClassLoader acl = new AccumuloClassLoader(AccumuloVFSClassLoaderTest.class.getClassLoader());
    assertTrue((acl instanceof AccumuloClassLoader));
    assertTrue((acl.getParent() instanceof URLClassLoader));
  }

  /*
   * Test that if configured with dynamic class paths, that the code creates the 2nd level loader
   */
  @Test
  public void testDynamicConfig() throws Exception {

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.append("general.dynamic.classpaths=" + System.getProperty("user.dir") + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "ACCUMULO_CONFIG_URL",
        conf.toURI().toURL());
    AccumuloClassLoader.resetForTests();
    AccumuloClassLoader acl =
        new AccumuloClassLoader(AccumuloVFSClassLoaderTest.class.getClassLoader());
    assertTrue((acl instanceof AccumuloClassLoader));
    assertTrue((acl.getParent() instanceof URLClassLoader));
  }

  /*
   * Test with default context configured
   */
  @Test
  public void testDefaultContextConfigured() throws Exception {

    // Copy jar file to TEST_DIR
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld.jar"));

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append(
        "general.vfs.classpaths=" + new File(folder1.getRoot(), "HelloWorld.jar").toURI() + "\n");
    out.append("general.dynamic.classpaths=" + System.getProperty("user.dir") + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "ACCUMULO_CONFIG_URL",
        conf.toURI().toURL());
    AccumuloClassLoader.resetForTests();
    AccumuloClassLoader acl =
        new AccumuloClassLoader(AccumuloVFSClassLoaderTest.class.getClassLoader());
    assertTrue((acl instanceof AccumuloClassLoader));
    assertTrue((acl.getParent() instanceof URLClassLoader));
    assertEquals(1, ((ReloadingVFSClassLoader) acl).getWrapper().getFileObjects().length);
    assertTrue(((ReloadingVFSClassLoader) acl).getWrapper().getFileObjects()[0].getURL().toString()
        .contains("HelloWorld.jar"));
    Class<?> clazz1 = ((ReloadingVFSClassLoader) acl).getWrapper().loadClass("test.HelloWorld");
    Object o1 = clazz1.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o1.toString());
  }

  @Test
  public void testDefaultCacheDirectory() throws Exception {

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append("general.vfs.classpaths=\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "ACCUMULO_CONFIG_URL",
        conf.toURI().toURL());
    AccumuloClassLoader.resetForTests();
    FileSystemManager manager = AccumuloVFSManager.generateVfs();
    UniqueFileReplicator replicator = Whitebox.getInternalState(manager, "fileReplicator");
    File tempDir = Whitebox.getInternalState(replicator, "tempDir");
    String tempDirParent = tempDir.getParent();
    String tempDirName = tempDir.getName();
    String javaIoTmpDir = System.getProperty("java.io.tmpdir");

    // trim off any final separator, because java.io.File does the same.
    if (javaIoTmpDir.endsWith(File.separator)) {
      javaIoTmpDir = javaIoTmpDir.substring(0, javaIoTmpDir.length() - File.separator.length());
    }
    assertEquals(javaIoTmpDir, tempDirParent);
    assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));
  }

  @Test
  public void testCacheDirectoryConfigured() throws Exception {

    String cacheDir = "/some/random/cache/dir";

    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append(AccumuloVFSManager.VFS_CACHE_DIR + "=" + cacheDir + "\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "ACCUMULO_CONFIG_URL",
        conf.toURI().toURL());
    AccumuloClassLoader.resetForTests();
    FileSystemManager manager = AccumuloVFSManager.generateVfs();
    UniqueFileReplicator replicator = Whitebox.getInternalState(manager, "fileReplicator");
    File tempDir = Whitebox.getInternalState(replicator, "tempDir");
    String tempDirParent = tempDir.getParent();
    String tempDirName = tempDir.getName();
    assertEquals(cacheDir, tempDirParent);
    assertTrue(tempDirName.startsWith("accumulo-vfs-cache-"));
    assertTrue(tempDirName.endsWith(System.getProperty("user.name", "nouser")));
  }
}
