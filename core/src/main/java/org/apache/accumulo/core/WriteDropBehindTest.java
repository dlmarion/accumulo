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

    if (args.length != 5) {
      System.err.printf("Usage: %s <hdfs uri> <file> <buffer size> <y|n> <y|n>\n",
          WriteDropBehindTest.class.getSimpleName());
      System.exit(1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(args[0]), conf);

    FSDataOutputStreamBuilder<?,?> builder = fs.createFile(new Path(args[1]));
    if (builder instanceof HdfsDataOutputStreamBuilder) {
      System.out.println("Setting sync on OutputStream");
      ((HdfsDataOutputStreamBuilder) builder).syncBlock();
    }
    builder.blockSize(64 * 1048576); // 64 MB
    builder.overwrite(true);
    var os = builder.build();

    if (args[3].equals("y")) {
      System.out.println("calling setDropBehind(true)");
      os.setDropBehind(true);
    }

    if (args[4].equals("y")) {
      System.out.println("calling hsync()");
      os.hsync();
    }

    byte[] data = new byte[Integer.parseInt(args[2])];

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
