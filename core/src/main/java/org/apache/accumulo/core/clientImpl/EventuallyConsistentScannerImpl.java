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

/**
 * provides scanner functionality
 *
 * "Clients can iterate over multiple column families, and there are several mechanisms for limiting
 * the rows, columns, and timestamps traversed by a scan. For example, we could restrict [a] scan
 * ... to only produce anchors whose columns match [a] regular expression ..., or to only produce
 * anchors whose timestamps fall within ten days of the current time."
 *
 */
public class EventuallyConsistentScannerImpl extends ScannerImpl {

  public EventuallyConsistentScannerImpl(ClientContext context, TableId tableId,
      Authorizations authorizations) {
    super(context, tableId, authorizations);
    this.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
  }

}
