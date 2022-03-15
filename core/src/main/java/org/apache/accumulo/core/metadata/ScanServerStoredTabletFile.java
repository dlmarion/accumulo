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
package org.apache.accumulo.core.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils;
import org.apache.hadoop.io.Text;

public class ScanServerStoredTabletFile extends StoredTabletFile {

  public static final String IDENTIFIER_STR = "SSERV";
  private static final byte[] IDENTIFIER = IDENTIFIER_STR.getBytes(UTF_8);
  private final String scanServerAddress;
  private final Text metadataEntry;

  public static ScanServerStoredTabletFile parse(String columnQualifier)
      throws IllegalArgumentException {
    byte[][] parts = ByteUtils.split(columnQualifier.getBytes(UTF_8));
    if (parts.length == 4) {
      if (!Arrays.equals(IDENTIFIER, parts[0])) {
        throw new IllegalArgumentException("Not a ScanServerTabletFile entry");
      }
      return new ScanServerStoredTabletFile(new String(parts[1], UTF_8),
          new String(parts[2], UTF_8), new String(parts[3], UTF_8));
    }
    throw new IllegalArgumentException("Not a ScanServerTabletFile entry");
  }

  public ScanServerStoredTabletFile(String datafilePath, String scanServerAddress, String scanID) {
    super(datafilePath);
    this.scanServerAddress = scanServerAddress;
    this.metadataEntry = new Text(ByteUtils.concat(datafilePath.getBytes(UTF_8),
        scanServerAddress.getBytes(UTF_8), scanID.getBytes(UTF_8)));
  }

  @Override
  public String getMetaInsert() {
    return this.metadataEntry.toString();
  }

  @Override
  public Text getMetaInsertText() {
    return this.metadataEntry;
  }

  @Override
  public String getMetaUpdateDelete() {
    return this.metadataEntry.toString();
  }

  @Override
  public Text getMetaUpdateDeleteText() {
    return this.metadataEntry;
  }

  public String getScanServerAddress() {
    return this.scanServerAddress;
  }

}
