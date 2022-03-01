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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DirectDecompressionCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Codec that compresses data using the Intel® QuickAssist codec and decompresses data using either
 * the GzipCodec or the Intel® QuickAssist codec.
 *
 */
@DoNotPool
public class AccumuloQATCodec implements Configurable, CompressionCodec, DirectDecompressionCodec {

  public static final String CONF_QAT_CLASS = "io.compression.codec.qat.class";

  /** Intel® QuickAssist codec **/
  private static final String DEFAULT_QAT_CLASS = "org.apache.hadoop.io.compress.QatCodec";

  /* Taken from GzipCodec */
  private static final byte[] GZIP_HEADER =
      new byte[] {0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloQATCodec.class);

  private final Configuration conf;
  private final CompressionCodec qatCodec;
  private final GzipCodec gzipCodec;
  private final DefaultCodec defaultCodec;

  public AccumuloQATCodec(Configuration conf) {

    this.conf = conf;

    String qatClass =
        (this.conf.get(CONF_QAT_CLASS) == null ? System.getProperty(CONF_QAT_CLASS) : null);
    String clazz = (qatClass != null) ? qatClass : DEFAULT_QAT_CLASS;
    try {
      LOG.info("Trying to load qat codec class: " + clazz);
      CompressionCodec c =
          (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), this.conf);
      // Enforce native library loading
      c.getCompressorType();
      this.qatCodec = c;
    } catch (Exception e) {
      LOG.error("Unable to load QAT codec class", e);
      throw new RuntimeException("Unable to initialize AccumuloQATCodec", e);
    }
    defaultCodec = new DefaultCodec();
    defaultCodec.setConf(this.conf);
    gzipCodec = new GzipCodec();
    gzipCodec.setConf(this.conf);
  }

  @Override
  public void setConf(Configuration conf) {
    // does nothing
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /* Compression methods, use QAT */

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return qatCodec.getCompressorType();
  }

  @Override
  public Compressor createCompressor() {
    return qatCodec.createCompressor();
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return qatCodec.createOutputStream(out);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream arg0, Compressor arg1)
      throws IOException {
    return qatCodec.createOutputStream(arg0, arg1);
  }

  /* Decompressor methods, use Gzip if necessary, else QAT */

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    // This method appears to only be called from CodecPool. This class has a
    // DoNotPool annotation, so it shouldn't be called.
    throw new UnsupportedOperationException("This method should not be called.");
  }

  @Override
  public Decompressor createDecompressor() {
    // This method appears to only be called from CodecPool. This class has a
    // DoNotPool annotation, so it shouldn't be called.
    throw new UnsupportedOperationException("This method should not be called.");
  }

  @Override
  public DirectDecompressor createDirectDecompressor() {
    throw new UnsupportedOperationException("This method should not be called.");
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    PushbackInputStream pbis = new PushbackInputStream(in, 10);
    if (isZlibStream(pbis)) {
      return defaultCodec.createInputStream(pbis);
    } else if (isGzipStream(pbis)) {
      return gzipCodec.createInputStream(pbis);
    } else {
      return qatCodec.createInputStream(pbis);
    }
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
      throws IOException {
    PushbackInputStream pbis = new PushbackInputStream(in, 10);
    if (isZlibStream(pbis)) {
      return defaultCodec.createInputStream(pbis);
    } else if (isGzipStream(pbis)) {
      return gzipCodec.createInputStream(pbis, decompressor);
    } else {
      return qatCodec.createInputStream(pbis, decompressor);
    }
  }

  @Override
  public String getDefaultExtension() {
    return qatCodec.getDefaultExtension();
  }

  private boolean isZlibStream(PushbackInputStream is) throws IOException {
    byte[] header = new byte[2];
    is.read(header, 0, header.length);
    boolean isZlib = false;
    if (header[0] == 0x78
        && (header[1] == (byte) 0x01 || header[1] == (byte) 0x9C || header[1] == (byte) 0xDA)) {
      isZlib = true;
    }
    is.unread(header);
    return isZlib;
  }

  private boolean isGzipStream(PushbackInputStream is) throws IOException {
    byte[] header = new byte[GZIP_HEADER.length];
    is.read(header, 0, header.length);
    boolean isGzip = isGzipHeader(header);
    is.unread(header);
    return isGzip;
  }

  private boolean isGzipHeader(byte[] b) throws IOException {
    if (Arrays.equals(GZIP_HEADER, b)) {
      return true;
    }
    return false;
  }

}
