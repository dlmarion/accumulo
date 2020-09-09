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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;

import org.apache.accumulo.classloader.AccumuloClassLoader;
import org.apache.accumulo.classloader.ClassPathPrinter;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ClassPathPrinterTest {

  @Rule
  public TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private final ClassLoader parent = ClassPathPrinterTest.class.getClassLoader();

  private static void assertPattern(String output, String pattern, boolean shouldMatch) {
    if (shouldMatch) {
      assertTrue("Pattern " + pattern + " did not match output: " + output,
          output.matches(pattern));
    } else {
      assertFalse("Pattern " + pattern + " should not match output: " + output,
          output.matches(pattern));
    }
  }

  @Test
  public void testACLwithDefaultDynamicChild() throws Exception {
    File conf = folder1.newFile("accumulo.properties");
    FileWriter out = new FileWriter(conf);
    out.append("general.classpaths=\n");
    out.append("general.dynamic.classpaths=" + System.getProperty("user.dir") + "\n");
    out.append("general.vfs.classpaths=\n");
    out.close();

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());
    try (AccumuloClassLoader acl = new AccumuloClassLoader(parent)) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      assertPattern(ClassPathPrinter.getClassPath(cl, true), "(?s).*\\s+.*\\n$", true);
      assertTrue(ClassPathPrinter.getClassPath(cl, true)
          .contains("Level 3: AccumuloClassLoader Classloader"));
      assertTrue(
          ClassPathPrinter.getClassPath(cl, true).contains("Level 4: default dynamic Classloader"));
      assertTrue(ClassPathPrinter.getClassPath(cl, true).length()
          > ClassPathPrinter.getClassPath(cl, false).length());
      assertPattern(ClassPathPrinter.getClassPath(cl, false), "(?s).*\\s+.*\\n$", false);
      assertFalse(ClassPathPrinter.getClassPath(cl, false)
          .contains("Level 3: AccumuloClassLoader Classloader"));
      assertFalse(ClassPathPrinter.getClassPath(cl, false)
          .contains("Level 4: default dynamic Classloader"));
    } finally {
      Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", (Object) null);
    }
  }

  @Test
  public void testACLwithGeneralVFSChild() throws Exception {
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

    Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", conf.toURI().toURL());

    try (AccumuloClassLoader acl = new AccumuloClassLoader(parent)) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      assertPattern(ClassPathPrinter.getClassPath(cl, true), "(?s).*\\s+.*\\n$", true);
      assertTrue(ClassPathPrinter.getClassPath(cl, true)
          .contains("Level 3: AccumuloClassLoader Classloader"));
      assertTrue(
          ClassPathPrinter.getClassPath(cl, true).contains("Level 4: VFS system Classloader"));
      assertTrue(ClassPathPrinter.getClassPath(cl, true).length()
          > ClassPathPrinter.getClassPath(cl, false).length());
      assertPattern(ClassPathPrinter.getClassPath(cl, false), "(?s).*\\s+.*\\n$", false);
      assertFalse(ClassPathPrinter.getClassPath(cl, false)
          .contains("Level 3: AccumuloClassLoader Classloader"));
      assertFalse(
          ClassPathPrinter.getClassPath(cl, false).contains("Level 4: VFS system Classloader"));
    } finally {
      Whitebox.setInternalState(AccumuloClassLoader.class, "accumuloConfigUrl", (Object) null);
    }
  }
}
