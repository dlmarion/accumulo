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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;

public class EventuallyConsistentTabletServerBatchReader extends TabletServerBatchReader {

  protected EventuallyConsistentTabletServerBatchReader(ClientContext context, Class<?> scopeClass,
      TableId tableId, String tableName, Authorizations authorizations, int numQueryThreads) {
    super(context, scopeClass, tableId, tableName, authorizations, numQueryThreads);
    this.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
  }

  public EventuallyConsistentTabletServerBatchReader(ClientContext context, TableId tableId,
      String tableName, Authorizations authorizations, int numQueryThreads) {
    super(context, tableId, tableName, authorizations, numQueryThreads);
    this.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
  }

}
