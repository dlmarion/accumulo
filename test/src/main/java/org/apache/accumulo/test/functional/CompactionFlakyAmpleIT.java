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

import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ample.FlakyAmpleManager;
import org.apache.accumulo.test.ample.FlakyAmpleTserver;
import org.apache.hadoop.conf.Configuration;

public class CompactionFlakyAmpleIT extends CompactionITBase {
  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configureMiniCluster(cfg, hadoopCoreSite);
    cfg.setServerClass(ServerType.MANAGER, rg -> FlakyAmpleManager.class);
    cfg.setServerClass(ServerType.TABLET_SERVER, rg -> FlakyAmpleTserver.class);
  }
}
