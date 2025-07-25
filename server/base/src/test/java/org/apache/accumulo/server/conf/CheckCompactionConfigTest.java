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
package org.apache.accumulo.server.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.accumulo.server.WithTestNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParseException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path not set by user input")
public class CheckCompactionConfigTest extends WithTestNames {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfigTest.class);

  @TempDir
  private static Path tempDir;

  @Test
  public void testValidInput1() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'large'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testValidInput2() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'cs1_small','maxSize':'16M'},{'group':'cs1_medium','maxSize':'128M'},\\\n"
        + "{'group':'cs1_large'}] \ncompaction.service.cs2.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs2.planner.opts.groups=\\\n"
        + "[{'group':'cs2_small','maxSize':'16M'},{'group':'cs2_medium','maxSize':'128M'},\\\n"
        + "{'group':'cs2_large'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);

    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testValidInput3() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'cs1_small','maxSize':'16M'},{'group':'cs1_medium','maxSize':'128M'},\\\n"
        + "{'group':'cs1_large'}] \ncompaction.service.cs2.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs2.planner.opts.groups=\\\n"
        + "[{'group':'cs2_small','maxSize':'16M'}, {'group':'cs2_medium','maxSize':'128M'},\\\n"
        + "{'group':'cs2_large'}] \ncompaction.service.cs3.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs3.planner.opts.groups=\\\n"
        + "[{'group':'cs3_small','maxSize':'16M'},{'group':'cs3_large'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);
    CheckCompactionConfig.main(new String[] {filePath});
  }

  @Test
  public void testThrowsInvalidFieldsError() throws IOException {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'large','numThreads':2}]").replaceAll("'", "\"");
    String expectedErrorMsg =
        "Invalid fields: [numThreads] provided for class: org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner$GroupConfig";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(JsonParseException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertEquals(expectedErrorMsg, e.getMessage());
  }

  @Test
  public void testNoPlanner() throws Exception {
    String inputString = ("compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'}, {'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'large'}]").replaceAll("'", "\"");
    String expectedErrorMsg =
        "Incomplete compaction service definitions, missing planner class [cs1]";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testRepeatedCompactionGroup() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'small'}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Duplicate compactor group for group: small";

    final String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testInvalidMaxSize() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'0M'},\\\n"
        + "{'group':'large'}]").replaceAll("'", "\"");
    String expectedErrorMsg = "Invalid value for maxSize";

    String filePath = writeToFileAndReturnPath(inputString);

    var e = assertThrows(IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
    assertTrue(e.getMessage().startsWith(expectedErrorMsg));
  }

  @Test
  public void testBadPropsFilePath() {
    String[] args = {"/home/foo/bar/myProperties.properties"};
    String expectedErrorMsg = "File at given path could not be found";
    var e = assertThrows(FileNotFoundException.class, () -> CheckCompactionConfig.main(args));
    assertEquals(expectedErrorMsg, e.getMessage());
  }

  private String writeToFileAndReturnPath(String inputString) throws IOException {
    Path file = tempDir.resolve(testName() + ".properties");
    if (!Files.isRegularFile(file)) {
      Files.createFile(file);
    }
    try (BufferedWriter bufferedWriter = Files.newBufferedWriter(file)) {
      bufferedWriter.write(inputString);
    }
    log.info("Wrote to path: {}\nWith string:\n{}", file.toAbsolutePath(), inputString);
    return file.toAbsolutePath().toString();
  }

  @Test
  public void testGroupReuse() throws Exception {
    String inputString = ("compaction.service.cs1.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs1.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'large'}] \ncompaction.service.cs2.planner="
        + "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner \n"
        + "compaction.service.cs2.planner.opts.groups=\\\n"
        + "[{'group':'small','maxSize':'16M'},{'group':'medium','maxSize':'128M'},\\\n"
        + "{'group':'large'}]").replaceAll("'", "\"");

    String filePath = writeToFileAndReturnPath(inputString);

    assertThrows(IllegalStateException.class,
        () -> CheckCompactionConfig.main(new String[] {filePath}));
  }

}
