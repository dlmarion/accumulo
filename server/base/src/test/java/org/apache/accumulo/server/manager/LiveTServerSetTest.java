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
package org.apache.accumulo.server.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerInfo;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class LiveTServerSetTest {

  @Test
  public void testSessionIds() {
    Map<String,TServerInfo> servers = new HashMap<>();
    TServerConnection mockConn = EasyMock.createMock(TServerConnection.class);

    TServerInfo server1 =
        new TServerInfo(new TServerInstance(HostAndPort.fromParts("localhost", 1234), "5555"),
            mockConn, ResourceGroupId.DEFAULT);
    servers.put("server1", server1);

    assertEquals(server1.instance, LiveTServerSet.find(servers, "localhost:1234"));
    assertNull(LiveTServerSet.find(servers, "localhost:4321"));
    assertEquals(server1.instance, LiveTServerSet.find(servers, "localhost:1234[5555]"));
    assertNull(LiveTServerSet.find(servers, "localhost:1234[55755]"));
  }

}
