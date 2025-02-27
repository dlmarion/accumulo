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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.metrics.Metric.SERVER_IDLE;
import static org.apache.accumulo.core.metrics.MetricsInfo.PROCESS_NAME_TAG_KEY;
import static org.apache.accumulo.core.metrics.MetricsInfo.RESOURCE_GROUP_TAG_KEY;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdleProcessMetricsIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(IdleProcessMetricsIT.class);

  static final Duration idleProcessInterval = Duration.ofSeconds(10);

  public static final String IDLE_RESOURCE_GROUP = "IDLE_PROCESS_TEST";

  public static class IdleStopITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

      // Verify expectations about the default config. Want to ensure there no other resource groups
      // configured.
      assertEquals(Map.of(Constants.DEFAULT_COMPACTION_SERVICE_NAME, 1),
          cfg.getClusterServerConfiguration().getCompactorConfiguration());

      // Disable the default scan servers and compactors, just start 1
      // tablet server in the default group to host the root and metadata
      // tables
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(0);

      // Add servers in a resource group that will not get any work. These
      // are the servers that should stop because they are idle.
      cfg.getClusterServerConfiguration().addTabletServerResourceGroup(IDLE_RESOURCE_GROUP, 1);
      cfg.getClusterServerConfiguration().addScanServerResourceGroup(IDLE_RESOURCE_GROUP, 1);
      cfg.getClusterServerConfiguration().addCompactorResourceGroup(IDLE_RESOURCE_GROUP, 1);
      cfg.getClusterServerConfiguration().addCompactorResourceGroup(GROUP1, 0);

      cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner",
          RatioBasedCompactionPlanner.class.getName());
      cfg.setProperty(Property.COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.groups",
          "[{'group':'" + GROUP1 + "'}]");

      cfg.setProperty(Property.GENERAL_IDLE_PROCESS_INTERVAL,
          idleProcessInterval.toSeconds() + "s");

      // need to set this to a low value since one of the idle conditions for the scan server is an
      // empty cache
      cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "1s");

      // Tell the server processes to use a StatsDMeterRegistry that will be configured
      // to push all metrics to the sink we started.
      cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
      cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
          TestStatsDRegistryFactory.class.getName());
      Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
          TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
      cfg.setSystemProperties(sysProps);

    }

  }

  private static TestStatsDSink sink;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
    SharedMiniClusterBase.startMiniClusterWithConfig(new IdleStopITConfig());
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Test that the idle process metrics are emitted correctly for the compactor, scan server and
   * tserver.
   */
  @Test
  public void testIdleStopMetrics() throws Exception {

    // should emit the idle metric after the configured duration of GENERAL_IDLE_PROCESS_INTERVAL
    Thread.sleep(idleProcessInterval.toMillis());

    AtomicBoolean sawCompactor = new AtomicBoolean(false);
    AtomicBoolean sawSServer = new AtomicBoolean(false);
    AtomicBoolean sawTServer = new AtomicBoolean(false);

    Wait.waitFor(() -> {
      List<String> statsDMetrics = sink.getLines();
      statsDMetrics.stream().filter(line -> line.startsWith(SERVER_IDLE.getName())).peek(log::info)
          .map(TestStatsDSink::parseStatsDMetric)
          .filter(a -> a.getTags().get(RESOURCE_GROUP_TAG_KEY).equals(IDLE_RESOURCE_GROUP))
          .forEach(a -> {
            String processName = a.getTags().get(PROCESS_NAME_TAG_KEY);
            int value = Integer.parseInt(a.getValue());
            assertTrue(value == 0 || value == 1 || value == -1, "Unexpected value " + value);
            // check that the idle metric was emitted for each
            if (ServerId.Type.TABLET_SERVER.name().equals(processName) && value == 1) {
              sawTServer.set(true);
            } else if (ServerId.Type.SCAN_SERVER.name().equals(processName) && value == 1) {
              sawSServer.set(true);
            } else if (ServerId.Type.COMPACTOR.name().equals(processName) && value == 1) {
              sawCompactor.set(true);
            }

          });
      return sawCompactor.get() && sawSServer.get() && sawTServer.get();
    });
  }

  /**
   * Test that before during and after a compaction, the compactor will emit the appropriate value
   * for the idle metric.
   */
  @Test
  public void idleCompactorTest() throws Exception {
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(GROUP1, 1);
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

      // should emit the idle metric after the configured duration of GENERAL_IDLE_PROCESS_INTERVAL
      Thread.sleep(idleProcessInterval.toMillis());

      final String processName = ServerId.Type.COMPACTOR.name();

      log.info("Waiting for compactor to go idle");
      waitForIdleMetricValueToBe(1, processName);

      String table1 = getUniqueNames(1)[0];
      createTable(client, table1, "cs1");
      writeData(client, table1);

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 5);
      client.tableOperations().attachIterator(table1, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));

      compact(client, table1, 2, GROUP1, true);

      log.info("Waiting for compactor to be not idle after starting compaction");
      waitForIdleMetricValueToBe(0, processName);

      log.info("Waiting for compactor to go idle once compaction completes");
      waitForIdleMetricValueToBe(1, processName);

      verify(client, table1, 2);
    }

  }

  /**
   * Test that before during and after a scan, the scan server will emit the appropriate value for
   * the idle metric.
   */
  @Test
  public void idleScanServerTest() throws Exception {
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      getCluster().getConfig().getClusterServerConfiguration().setNumDefaultScanServers(1);
      getCluster().getClusterControl().start(ServerType.SCAN_SERVER);

      // should emit the idle metric after the configured duration of GENERAL_IDLE_PROCESS_INTERVAL
      Thread.sleep(idleProcessInterval.toMillis());

      final String processName = ServerId.Type.SCAN_SERVER.name();

      log.info("Waiting for sserver to go idle");
      waitForIdleMetricValueToBe(1, processName);

      String table1 = getUniqueNames(1)[0];
      createTable(client, table1, "cs1");
      writeData(client, table1);

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 5);
      client.tableOperations().attachIterator(table1, setting,
          EnumSet.of(IteratorUtil.IteratorScope.scan));

      try (Scanner scanner = client.createScanner(table1, Authorizations.EMPTY)) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        assertEquals(MAX_DATA, scanner.stream().count()); // Ensure that data exists
      }

      log.info("Waiting for sserver to be not idle after starting a scan");
      waitForIdleMetricValueToBe(0, processName);

      log.info("Waiting for sserver to go idle once scan completes completes");
      waitForIdleMetricValueToBe(1, processName);
    }

  }

  private static void waitForIdleMetricValueToBe(int expectedValue, String processName) {
    Wait.waitFor(
        () -> sink.getLines().stream().filter(line -> line.startsWith(SERVER_IDLE.getName()))
            .map(TestStatsDSink::parseStatsDMetric)
            .filter(a -> a.getTags().get("process.name").equals(processName))
            .peek(a -> log.info("Idle metric: {}", a))
            .anyMatch(a -> Integer.parseInt(a.getValue()) == expectedValue),
        60_000, 2000, "Idle metric did not reach the expected value " + expectedValue);
  }

}
