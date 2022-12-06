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
package org.apache.accumulo.core;

import java.io.BufferedInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem.HdfsDataOutputStreamBuilder;

public class WriteDropBehindTest {
  public static void main(String[] args) throws Exception {

    if (args.length != 6) {
      System.err.printf(
          "Usage: %s <hdfs uri> <file> <buffer size> <syncOnStream (y|n)> <setDropBehind (y|n)> <callHsync (y|n)>\n",
          WriteDropBehindTest.class.getSimpleName());
      System.exit(1);
    }

    final String hdfsURI = args[0];
    final String outputFilename = args[1];
    final int bufferSize = Integer.parseInt(args[2]);
    final boolean setSyncOnStream = args[3].equalsIgnoreCase("y");
    final boolean setDropBehind = args[4].equalsIgnoreCase("y");
    final boolean callHsync = args[5].equalsIgnoreCase("y");

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(hdfsURI), conf);

    FSDataOutputStreamBuilder<?,?> builder = fs.createFile(new Path(outputFilename));
    if (builder instanceof HdfsDataOutputStreamBuilder) {
      if (setSyncOnStream) {
        System.out.println("Setting syncBlock on OutputStream");
        ((HdfsDataOutputStreamBuilder) builder).syncBlock();
      }
    }
    builder.blockSize(64 * 1048576); // 64 MB
    builder.overwrite(true);
    var os = builder.build();

    if (setDropBehind) {
      System.out.println("calling setDropBehind(true)");
      os.setDropBehind(true);
    }

    if (callHsync) {
      System.out.println("calling hsync()");
      os.hsync();
    }

    byte[] data = new byte[bufferSize];

    BufferedInputStream is = new BufferedInputStream(System.in);

    int read;
    long total = 0;
    while ((read = is.read(data, 0, data.length)) != -1) {
      os.write(data, 0, read);
      total += read;
    }
    os.close();

    System.out.printf("Total read : %,d\n", total);
  }
}
