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
package org.apache.accumulo.core.spi.file.rfile.compression;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloQATCodecTest {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloQATCodecTest.class);

  private static final String DATA1 = "Dogs don't know it's not bacon!\n";
  private AccumuloQATCodec codec;

  @Before
  public void setUp() throws Exception {
    codec = new AccumuloQATCodec(new Configuration(false));
  }

  @Test
  public void testDefaultWriteCodecRead() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DefaultCodec gzc = new DefaultCodec();
    gzc.setConf(new Configuration(false));
    CompressionOutputStream os = gzc.createOutputStream(baos);
    os.write(DATA1.getBytes(StandardCharsets.UTF_8));
    os.finish();
    os.close();
    LOG.info("{}", baos.toByteArray());
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    InputStream is = codec.createInputStream(bais);
    byte[] buf = new byte[1024];
    int len = is.read(buf);
    String result = new String(buf, 0, len, StandardCharsets.UTF_8);
    assertEquals("Input must match output", DATA1, result);
  }

  @Test
  public void testGzipWriteCodecRead() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GzipCodec gzc = new GzipCodec();
    gzc.setConf(new Configuration(false));
    CompressionOutputStream os = gzc.createOutputStream(baos);
    os.write(DATA1.getBytes(StandardCharsets.UTF_8));
    os.finish();
    os.close();
    LOG.info("{}", baos.toByteArray());
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    InputStream is = codec.createInputStream(bais);
    byte[] buf = new byte[1024];
    int len = is.read(buf);
    String result = new String(buf, 0, len, StandardCharsets.UTF_8);
    assertEquals("Input must match output", DATA1, result);
  }

  @Test
  public void testWriteReadQatCodec() throws Exception {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionOutputStream out = codec.createOutputStream(baos);
    out.write(DATA1.getBytes(StandardCharsets.UTF_8));
    out.finish();
    out.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    InputStream is = codec.createInputStream(bais);
    byte[] buf = new byte[1024];
    int len = is.read(buf);
    String result = new String(buf, 0, len, StandardCharsets.UTF_8);
    assertEquals("Input must match output", DATA1, result);
  }
}
