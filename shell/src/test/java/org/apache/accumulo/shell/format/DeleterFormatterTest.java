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
package org.apache.accumulo.shell.format;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.shell.Shell;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleterFormatterTest {
  // default timestamp
  private static final long TIMESTAMP = Long.MAX_VALUE;
  DeleterFormatter formatter;
  Map<Key,Value> data;
  BatchWriter writer;
  Shell shellState;
  LineReader reader;
  Terminal terminal;
  PrintWriter pw;

  ByteArrayOutputStream baos;

  SettableInputStream input;

  class SettableInputStream extends InputStream {
    ByteArrayInputStream bais;

    @Override
    public int read() throws IOException {
      return bais.read();
    }

    public void set(String in) {
      bais = new ByteArrayInputStream(in.getBytes(UTF_8));
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    input = new SettableInputStream();
    baos = new ByteArrayOutputStream();

    writer = createMock(BatchWriter.class);
    writer.addMutation(anyObject());
    expectLastCall().anyTimes();
    writer.close();
    expectLastCall().anyTimes();

    shellState = createMock(Shell.class);

    terminal = new DumbTerminal(input, baos);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    pw = terminal.writer();

    expect(shellState.getReader()).andReturn(reader).anyTimes();
    expect(shellState.getWriter()).andReturn(pw).anyTimes();

    data = new TreeMap<>();
    data.put(new Key("r", "cf", "cq"), new Value("value"));
  }

  @AfterEach
  public void verifyCommonMocks() {
    verify(writer, shellState);
  }

  @Test
  public void testEmpty() {
    replay(writer, shellState);
    formatter = new DeleterFormatter(writer, Collections.<Key,Value>emptyMap().entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);
    assertFalse(formatter.hasNext());
  }

  @Test
  public void testSingle() throws IOException {
    replay(writer, shellState);
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verifyOut("[DELETED]", " r ", "cf", "cq", "value");
  }

  @Test
  public void testNo() throws IOException {
    expect(shellState.confirm("Delete { r cf:cq [] " + TIMESTAMP + "\tvalue } ? "))
        .andReturn(Optional.of(false));
    expectLastCall().once();
    replay(writer, shellState);

    input.set("no\n");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verifyOut("[SKIPPED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
  }

  @Test
  public void testNoConfirmation() throws IOException {
    expect(shellState.confirm("Delete { r cf:cq [] " + TIMESTAMP + "\tvalue } ? "))
        .andReturn(Optional.empty());
    expectLastCall().once();
    replay(writer, shellState);

    input.set("");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verifyOut("[SKIPPED]", " r ", "cf", "cq", "value");

    assertFalse(formatter.hasNext());
  }

  @Test
  public void testYes() throws IOException {
    expect(shellState.confirm("Delete { r cf:cq [] " + TIMESTAMP + "\tvalue } ? "))
        .andReturn(Optional.of(true));
    expectLastCall().once();

    expect(shellState.confirm("Delete { z : [] " + TIMESTAMP + "\tv2 } ? "))
        .andReturn(Optional.of(true));
    expectLastCall().once();
    replay(writer, shellState);

    input.set("y\nyes\n");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verifyOut("[DELETED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verifyOut("[DELETED]", " z ", "v2");
  }

  @Test
  public void testMutationException() throws MutationsRejectedException {
    MutationsRejectedException mre = createMock(MutationsRejectedException.class);
    BatchWriter exceptionWriter = createMock(BatchWriter.class);
    exceptionWriter.close();
    expectLastCall().andThrow(mre);
    exceptionWriter.addMutation(anyObject(Mutation.class));
    expectLastCall().andThrow(mre);
    replay(mre, writer, exceptionWriter, shellState);
    formatter = new DeleterFormatter(exceptionWriter, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    assertFalse(formatter.hasNext());
    verify(mre, exceptionWriter);
  }

  private void verifyOut(String... chunks) throws IOException {
    reader.getTerminal().writer().flush();

    String output = baos.toString();
    for (String chunk : chunks) {
      assertTrue(output.contains(chunk), "Output is missing chunk: " + chunk);
    }
  }

}
