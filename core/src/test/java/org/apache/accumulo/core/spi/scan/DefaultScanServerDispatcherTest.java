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
package org.apache.accumulo.core.spi.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DefaultScanServerDispatcherTest {

  static class InitParams implements ScanServerDispatcher.InitParameters {

    private final Map<String,String> opts;
    private final Set<String> scanServers;

    InitParams(Map<String,String> opts, Set<String> scanServers) {
      this.opts = opts;
      this.scanServers = scanServers;
    }

    InitParams(Set<String> scanServers) {
      this.opts = Map.of();
      this.scanServers = scanServers;
    }

    @Override
    public Map<String,String> getOptions() {
      return opts;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Supplier<Set<String>> getScanServers() {
      return () -> scanServers;
    }
  }

  static class DaParams implements ScanServerDispatcher.DispatcherParameters {

    private final Collection<TabletId> tablets;
    private final Map<TabletId,Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts;

    DaParams(TabletId tablet) {
      this.tablets = Set.of(tablet);
      this.attempts = Map.of();
    }

    DaParams(TabletId tablet, Map<TabletId, Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts) {
      this.tablets = Set.of(tablet);
      this.attempts = attempts;
    }

    @Override
    public Collection<TabletId> getTablets() {
      return tablets;
    }

    @Override
    public Collection<? extends ScanServerDispatcher.ScanAttempt> getAttempts(TabletId tabletId) {
      return attempts.getOrDefault(tabletId, Set.of());
    }
  }

  static class TestScanAttempt implements ScanServerDispatcher.ScanAttempt {

    private final String server;
    private final long endTime;
    private final Result result;

    TestScanAttempt(String server, long endTime, Result result) {
      this.server = server;
      this.endTime = endTime;
      this.result = result;
    }

    @Override public String getServer() {
      return server;
    }

    @Override public long getEndTime() {
      return endTime;
    }

    @Override public Result getResult() {
      return result;
    }
  }

  TabletId nti(String tableId, String endRow) {
    return new TabletIdImpl(new KeyExtent(TableId.of(tableId), new Text(endRow), null));
  }

  @Test
  public void testBasic() {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    dispatcher.init(new InitParams(
        Set.of("ss1:1", "ss2:2", "ss3:3", "ss4:4", "ss5:5", "ss6:6", "ss7:7", "ss8:8")));

    Set<String> servers = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      var tabletId = nti("1", "m");

      ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));

      servers.add(actions.getScanServer(tabletId));
    }
    
    assertEquals(3, servers.size());
  }

  private void runBusyTest(int numServers, int busyAttempts, int expectedServers, long expectedBusyTimeout) {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();

    var servers = Stream.iterate(1, i -> i <= numServers, i->i+1).map(i -> "s"+i+":"+i).collect(Collectors.toSet());

    dispatcher.init(new InitParams(servers));

    Set<String> serversSeen = new HashSet<>();

    var tabletId = nti("1", "m");

    var tabletAttempts = Stream.iterate(1, i -> i <= busyAttempts, i->i+1).map(i -> (new TestScanAttempt("ss"+i+":"+i, i, ScanServerDispatcher.ScanAttempt.Result.BUSY))).collect(Collectors.toList());

    Map<TabletId, Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts = new HashMap<>();
    attempts.put(tabletId, tabletAttempts);

    for (int i = 0; i < 100*numServers; i++) {
      ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId, attempts));

      assertEquals(expectedBusyTimeout, actions.getBusyTimeout().toMillis());
      assertEquals(0, actions.getDelay().toMillis());

      serversSeen.add(actions.getScanServer(tabletId));
    }

    assertEquals(expectedServers, serversSeen.size());
  }

  @Test
  public void testBusy(){
  runBusyTest(1000, 0, 3,33);
    runBusyTest(1000, 1, 21, 33);
    runBusyTest(1000, 2, 144,33);
    runBusyTest(1000, 3, 1000, 33);
    runBusyTest(1000, 4, 1000,33);
    runBusyTest(1000, 5, 1000,33*8);
    runBusyTest(1000, 9, 1000,33*8*8*8*8*8);
    runBusyTest(1000, 10, 1000,1800000);

    runBusyTest(27, 0, 3,33);
    runBusyTest(27, 1, 6,33);
    runBusyTest(27, 2, 13,33);
    runBusyTest(27, 3, 27,33);
    runBusyTest(27, 4, 27,33);
    runBusyTest(27, 5, 27,33*8);

    runBusyTest(6, 0, 3,33);
    runBusyTest(6, 1, 4,33);
    runBusyTest(6, 2, 5,33);
    runBusyTest(6, 3, 6,33);
    runBusyTest(6, 4, 6,33);
    runBusyTest(6, 5, 6,33*8);


    for(int i=0;i<4;i++) {
      runBusyTest(1, i, 1,33);
      runBusyTest(2, i, 2,33);
      runBusyTest(3, i, 3,33);
    }
  }

  @Test
  public void testCoverage(){
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    var servers = Stream.iterate(1, i -> i <= 20, i->i+1).map(i -> "s"+i+":"+i).collect(Collectors.toSet());
    dispatcher.init(new InitParams(servers));

    Set<String> allServersSeen = new HashSet<>();

    for(int t = 0; t<100; t++) {
      Set<String> serversSeen = new HashSet<>();

      var tabletId = nti(""+t, "m");

      for (int i = 0; i < 100; i++) {
        ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));
        serversSeen.add(actions.getScanServer(tabletId));
        allServersSeen.add(actions.getScanServer(tabletId));
      }

      assertEquals(3, serversSeen.size());
    }

    assertEquals(20, allServersSeen.size());
  }

}
