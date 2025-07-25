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
package org.apache.accumulo.core.spi.balancer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletStatisticsImpl;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.junit.jupiter.api.Test;

@Deprecated
public class HostRegexTableLoadBalancerTest extends BaseHostRegexTableLoadBalancerTest {

  public void init(Map<String,String> tableProperties) {
    HashMap<String,TableId> tables = new HashMap<>();
    tables.put(FOO.getTableName(), FOO.getId());
    tables.put(BAR.getTableName(), BAR.getId());
    tables.put(BAZ.getTableName(), BAZ.getId());

    var configImpl = ServiceEnvironment.Configuration.from(tableProperties, false);
    BalancerEnvironment environment = createMock(BalancerEnvironment.class);
    expect(environment.getConfiguration()).andReturn(configImpl).anyTimes();
    expect(environment.getTableIdMap()).andReturn(tables).anyTimes();
    expect(environment.getConfiguration(anyObject(TableId.class))).andReturn(configImpl).anyTimes();
    replay(environment);
    init(environment);
  }

  @Test
  public void testInit() {
    init(DEFAULT_TABLE_PROPERTIES);
    assertEquals(7000, this.getOobCheckMillis(), "OOB check interval value is incorrect");
    assertEquals(4, this.getMaxMigrations(), "Max migrations is incorrect");
    assertEquals(10, this.getMaxOutstandingMigrations(), "Max outstanding migrations is incorrect");
    assertFalse(isIpBasedRegex());
    Map<String,Pattern> patterns = this.getPoolNameToRegexPattern();
    assertEquals(2, patterns.size());
    assertTrue(patterns.containsKey(FOO.getTableName()));
    assertEquals(Pattern.compile("r01.*").pattern(), patterns.get(FOO.getTableName()).pattern());
    assertTrue(patterns.containsKey(BAR.getTableName()));
    assertEquals(Pattern.compile("r02.*").pattern(), patterns.get(BAR.getTableName()).pattern());
  }

  @Test
  public void testBalance() {
    init(DEFAULT_TABLE_PROPERTIES);
    Set<TabletId> migrations = new HashSet<>();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    SortedMap<TabletServerId,TServerStatus> current = createCurrent(15);
    long wait = this
        .balance(new BalanceParamsImpl(current, Map.of(ResourceGroupId.DEFAULT, current.keySet()),
            migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(20000, wait);
    // should balance four tablets in one of the tables before reaching max
    assertEquals(4, migrationsOut.size());

    // now balance again passing in the new migrations
    for (TabletMigration m : migrationsOut) {
      migrations.add(m.getTablet());
    }
    migrationsOut.clear();
    SortedMap<TabletServerId,TServerStatus> current2 = createCurrent(15);
    wait = this
        .balance(new BalanceParamsImpl(current2, Map.of(ResourceGroupId.DEFAULT, current2.keySet()),
            migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(20000, wait);
    // should balance four tablets in one of the other tables before reaching max
    assertEquals(4, migrationsOut.size());

    // now balance again passing in the new migrations
    for (TabletMigration m : migrationsOut) {
      migrations.add(m.getTablet());
    }
    migrationsOut.clear();
    SortedMap<TabletServerId,TServerStatus> current3 = createCurrent(15);
    wait = this
        .balance(new BalanceParamsImpl(current3, Map.of(ResourceGroupId.DEFAULT, current3.keySet()),
            migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(20000, wait);
    // should balance four tablets in one of the other tables before reaching max
    assertEquals(4, migrationsOut.size());

    // now balance again passing in the new migrations
    for (TabletMigration m : migrationsOut) {
      migrations.add(m.getTablet());
    }
    migrationsOut.clear();
    SortedMap<TabletServerId,TServerStatus> current4 = createCurrent(15);
    wait = this
        .balance(new BalanceParamsImpl(current4, Map.of(ResourceGroupId.DEFAULT, current4.keySet()),
            migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(20000, wait);
    // no more balancing to do
    assertEquals(0, migrationsOut.size());
  }

  @Test
  public void testBalanceWithTooManyOutstandingMigrations() {
    List<TabletMigration> migrationsOut = new ArrayList<>();
    init(DEFAULT_TABLE_PROPERTIES);
    // lets say we already have migrations ongoing for the FOO and BAR table extends (should be 5 of
    // each of them) for a total of 10
    Set<TabletId> migrations = new HashSet<>();
    migrations.addAll(tableTablets.get(FOO.getTableName()));
    migrations.addAll(tableTablets.get(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> current = createCurrent(15);
    long wait = this
        .balance(new BalanceParamsImpl(current, Map.of(ResourceGroupId.DEFAULT, current.keySet()),
            migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(20000, wait);
    // no migrations should have occurred as 10 is the maxOutstandingMigrations
    assertEquals(0, migrationsOut.size());
  }

  @Test
  public void testSplitCurrentByRegexUsingHostname() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));
    assertEquals(3, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(FOO.getTableName());
    assertEquals(5, fooHosts.size());
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(5, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
    // confirms that the default pool contains un-assigned tservers
    assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TabletServerId,TServerStatus> defHosts = groups.get(DEFAULT_POOL);
    assertEquals(5, defHosts.size());
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));
  }

  @Test
  public void testDefaultPoolAll() {
    // If all tservers are included in regular expressions, the the default pool
    // contains all tservers
    HashMap<String,String> props = new HashMap<>(DEFAULT_TABLE_PROPERTIES);
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAZ.getTableName(), "r03.*");
    init(props);
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));
    assertEquals(4, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(FOO.getTableName());
    assertEquals(5, fooHosts.size());
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(5, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAZ.getTableName()));
    SortedMap<TabletServerId,TServerStatus> bazHosts = groups.get(BAZ.getTableName());
    assertEquals(5, bazHosts.size());
    assertTrue(
        bazHosts.containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
    assertTrue(
        bazHosts.containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
    assertTrue(
        bazHosts.containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
    assertTrue(
        bazHosts.containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
    assertTrue(
        bazHosts.containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TabletServerId,TServerStatus> defHosts = groups.get(DEFAULT_POOL);
    assertEquals(15, defHosts.size());
  }

  @Test
  public void testSplitCurrentByRegexDefineDefaultPoolOverlapping() {
    HashMap<String,String> props = new HashMap<>(DEFAULT_TABLE_PROPERTIES);
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
    // Normally the DEFAULT pool would be comprised of the hosts not included in a regex.
    // Here we are going to define it as also being on rack1
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + DEFAULT_POOL, "r01.*");
    init(props);
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));
    assertEquals(3, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(FOO.getTableName());
    assertEquals(5, fooHosts.size());
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(5, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TabletServerId,TServerStatus> defHosts = groups.get(DEFAULT_POOL);
    assertEquals(10, defHosts.size());
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));
  }

  @Test
  public void testSplitCurrentByRegexDefineDefaultPoolNonOverlapping() {
    HashMap<String,String> props = new HashMap<>(DEFAULT_TABLE_PROPERTIES);
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r01.*");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r02.*");
    // Normally the DEFAULT pool would be comprised of the hosts not included in a regex.
    // Here we are going to define it as also being on rack3
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + DEFAULT_POOL, "r03.*");
    init(props);
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));
    assertEquals(3, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(FOO.getTableName());
    assertEquals(5, fooHosts.size());
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(5, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TabletServerId,TServerStatus> defHosts = groups.get(DEFAULT_POOL);
    assertEquals(5, defHosts.size());
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));

  }

  @Test
  public void testSplitCurrentByRegexUsingOverlappingPools() {
    HashMap<String,String> props = new HashMap<>(DEFAULT_TABLE_PROPERTIES);
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(), "r.*");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r01.*|r02.*");
    init(props);
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));

    // Groups foo, bar, and the default pool which contains all known hosts
    assertEquals(3, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    assertTrue(groups.containsKey(DEFAULT_POOL));
    for (String pool : new String[] {FOO.getTableName(), DEFAULT_POOL}) {
      SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(pool);
      assertEquals(15, fooHosts.size());
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
      assertTrue(fooHosts
          .containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));
    }

    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(10, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
  }

  @Test
  public void testSplitCurrentByRegexUsingIP() {
    HashMap<String,String> props = new HashMap<>();
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_OOB_CHECK_KEY, "30s");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_REGEX_USING_IPS_KEY, "true");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + FOO.getTableName(),
        "192\\.168\\.0\\.[1-5]");
    props.put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(),
        "192\\.168\\.0\\.[6-9]|192\\.168\\.0\\.10");
    init(props);

    assertTrue(isIpBasedRegex());
    Map<String,SortedMap<TabletServerId,TServerStatus>> groups =
        this.splitCurrentByRegex(createCurrent(15));
    assertEquals(3, groups.size());
    assertTrue(groups.containsKey(FOO.getTableName()));
    SortedMap<TabletServerId,TServerStatus> fooHosts = groups.get(FOO.getTableName());
    assertEquals(5, fooHosts.size());
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.1", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.2", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.3", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.4", 9997, Integer.toHexString(1))));
    assertTrue(
        fooHosts.containsKey(new TabletServerIdImpl("192.168.0.5", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(BAR.getTableName()));
    SortedMap<TabletServerId,TServerStatus> barHosts = groups.get(BAR.getTableName());
    assertEquals(5, barHosts.size());
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.6", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.7", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.8", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.9", 9997, Integer.toHexString(1))));
    assertTrue(
        barHosts.containsKey(new TabletServerIdImpl("192.168.0.10", 9997, Integer.toHexString(1))));
    assertTrue(groups.containsKey(DEFAULT_POOL));
    SortedMap<TabletServerId,TServerStatus> defHosts = groups.get(DEFAULT_POOL);
    assertEquals(5, defHosts.size());
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.11", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.12", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.13", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.14", 9997, Integer.toHexString(1))));
    assertTrue(
        defHosts.containsKey(new TabletServerIdImpl("192.168.0.15", 9997, Integer.toHexString(1))));
  }

  @Test
  public void testAllUnassigned() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    Map<TabletId,TabletServerId> unassigned = new HashMap<>();
    for (List<TabletId> extents : tableTablets.values()) {
      for (TabletId tabletId : extents) {
        unassigned.put(tabletId, null);
      }
    }
    this.getAssignments(
        new AssignmentParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
            Map.of(ResourceGroupId.DEFAULT, allTabletServers.keySet()),
            Collections.unmodifiableMap(unassigned), assignments));
    assertEquals(15, assignments.size());
    // Ensure unique tservers
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      for (Entry<TabletId,TabletServerId> e2 : assignments.entrySet()) {
        if (e.getKey().equals(e2.getKey())) {
          continue;
        }
        if (e.getValue().equals(e2.getValue())) {
          fail("Assignment failure");
        }
      }
    }
    // Ensure assignments are correct
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
  }

  @Test
  public void testAllAssigned() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    this.getAssignments(
        new AssignmentParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
            Map.of(ResourceGroupId.DEFAULT, allTabletServers.keySet()), Map.of(), assignments));
    assertEquals(0, assignments.size());
  }

  @Test
  public void testPartiallyAssigned() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    Map<TabletId,TabletServerId> unassigned = new HashMap<>();
    int i = 0;
    for (List<TabletId> tablets : tableTablets.values()) {
      for (TabletId tabletId : tablets) {
        if ((i % 2) == 0) {
          unassigned.put(tabletId, null);
        }
        i++;
      }
    }
    this.getAssignments(
        new AssignmentParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
            Map.of(ResourceGroupId.DEFAULT, allTabletServers.keySet()),
            Collections.unmodifiableMap(unassigned), assignments));
    assertEquals(unassigned.size(), assignments.size());
    // Ensure unique tservers
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      for (Entry<TabletId,TabletServerId> e2 : assignments.entrySet()) {
        if (e.getKey().equals(e2.getKey())) {
          continue;
        }
        if (e.getValue().equals(e2.getValue())) {
          fail("Assignment failure");
        }
      }
    }
    // Ensure assignments are correct
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
  }

  @Test
  public void testUnassignedWithNoTServers() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    Map<TabletId,TabletServerId> unassigned = new HashMap<>();
    for (TabletId tabletId : tableTablets.get(BAR.getTableName())) {
      unassigned.put(tabletId, null);
    }
    SortedMap<TabletServerId,TServerStatus> current = createCurrent(15);
    // Remove the BAR tablet servers from current
    List<TabletServerId> removals = new ArrayList<>();
    for (Entry<TabletServerId,TServerStatus> e : current.entrySet()) {
      if (e.getKey().getHost().equals("192.168.0.6") || e.getKey().getHost().equals("192.168.0.7")
          || e.getKey().getHost().equals("192.168.0.8")
          || e.getKey().getHost().equals("192.168.0.9")
          || e.getKey().getHost().equals("192.168.0.10")) {
        removals.add(e.getKey());
      }
    }
    for (TabletServerId r : removals) {
      current.remove(r);
    }
    this.getAssignments(new AssignmentParamsImpl(Collections.unmodifiableSortedMap(current),
        Map.of(ResourceGroupId.DEFAULT, allTabletServers.keySet()),
        Collections.unmodifiableMap(unassigned), assignments));
    assertEquals(unassigned.size(), assignments.size());
    // Ensure assignments are correct
    // Ensure tablets are assigned in default pool
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      if (tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet unexpectedly in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
  }

  @Test
  public void testUnassignedWithNoDefaultPool() {
    init(DEFAULT_TABLE_PROPERTIES);
    Map<TabletId,TabletServerId> assignments = new HashMap<>();
    Map<TabletId,TabletServerId> unassigned = new HashMap<>();
    for (TabletId tabletId : tableTablets.get(BAR.getTableName())) {
      unassigned.put(tabletId, null);
    }

    SortedMap<TabletServerId,TServerStatus> current = createCurrent(15);
    // Remove the BAR tablet servers and default pool from current
    List<TabletServerId> removals = new ArrayList<>();
    for (Entry<TabletServerId,TServerStatus> e : current.entrySet()) {
      if (e.getKey().getHost().equals("192.168.0.6") || e.getKey().getHost().equals("192.168.0.7")
          || e.getKey().getHost().equals("192.168.0.8")
          || e.getKey().getHost().equals("192.168.0.9")
          || e.getKey().getHost().equals("192.168.0.10")
          || e.getKey().getHost().equals("192.168.0.11")
          || e.getKey().getHost().equals("192.168.0.12")
          || e.getKey().getHost().equals("192.168.0.13")
          || e.getKey().getHost().equals("192.168.0.14")
          || e.getKey().getHost().equals("192.168.0.15")) {
        removals.add(e.getKey());
      }
    }

    for (TabletServerId r : removals) {
      current.remove(r);
    }

    this.getAssignments(new AssignmentParamsImpl(Collections.unmodifiableSortedMap(current),
        Map.of(ResourceGroupId.DEFAULT, allTabletServers.keySet()),
        Collections.unmodifiableMap(unassigned), assignments));
    assertEquals(unassigned.size(), assignments.size());

    // Ensure tablets are assigned in default pool
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      if (tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet unexpectedly in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
  }

  @Test
  public void testOutOfBoundsTablets() {
    // calls to balance will clear the lastOOBCheckTimes map
    // in the HostRegexTableLoadBalancer. For this test we want
    // to get into the out of bounds checking code, so we need to
    // populate the map with an older time value
    this.lastOOBCheckTimes.put(DataLevel.USER, System.currentTimeMillis() / 2);
    init(DEFAULT_TABLE_PROPERTIES);
    Set<TabletId> migrations = new HashSet<>();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    SortedMap<TabletServerId,TServerStatus> current = createCurrent(15);
    this.balance(new BalanceParamsImpl(current, Map.of(ResourceGroupId.DEFAULT, current.keySet()),
        migrations, migrationsOut, DataLevel.USER, environment.getTableIdMap()));
    assertEquals(2, migrationsOut.size());
  }

  @Override
  public List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver, TableId tableId) {
    // Report incorrect information so that balance will create an assignment
    List<TabletStatistics> tablets = new ArrayList<>();
    if (tableId.equals(BAR.getId()) && tserver.getHost().equals("192.168.0.1")) {
      // Report that we have a bar tablet on this server
      TKeyExtent tke = new TKeyExtent();
      tke.setTable(BAR.getId().canonical().getBytes(UTF_8));
      tke.setEndRow("11".getBytes(UTF_8));
      tke.setPrevEndRow("10".getBytes(UTF_8));
      TabletStats tstats = new TabletStats();
      tstats.setExtent(tke);
      TabletStatistics ts = new TabletStatisticsImpl(tstats);
      tablets.add(ts);
    } else if (tableId.equals(FOO.getId()) && tserver.getHost().equals("192.168.0.6")) {
      // Report that we have a foo tablet on this server
      TKeyExtent tke = new TKeyExtent();
      tke.setTable(FOO.getId().canonical().getBytes(UTF_8));
      tke.setEndRow("1".getBytes(UTF_8));
      tke.setPrevEndRow("0".getBytes(UTF_8));
      TabletStats tstats = new TabletStats();
      tstats.setExtent(tke);
      TabletStatistics ts = new TabletStatisticsImpl(tstats);
      tablets.add(ts);
    }
    return tablets;
  }

}
