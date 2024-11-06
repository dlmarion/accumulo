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
import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { fetchGroups } from '../api';

/**
 * Component that displays the Resource Group overview page.
 */
function ResourceGroupsOverviewPage() {
  const [groups, setGroups] = useState<string[]>([]);

  useEffect(() => {
    async function getGroups() {
      try {
        const data = await fetchGroups();
        setGroups(data);
      } catch (error) {
        console.error('Error fetching groups:', error);
      }
    }
    void getGroups();
  }, []);

  return (
    <div>
      <h1>Resource Groups</h1>
      <ul>
        {groups.map((group) => (
          <li key={group}>
            <Link to={`/resource-groups/${group}`}>{group}</Link>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default ResourceGroupsOverviewPage;
